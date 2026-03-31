param(
    [string]$ProjectRoot = "",
    [string]$PythonExe = "",
    [string]$BatchDate = "",
    [string]$SummaryPath = "data/raw_ticks/upbit/_meta/ticks_daily_latest.json",
    [string]$PlanPath = "data/raw_ticks/upbit/_meta/ticks_plan_daily_auto.json",
    [string]$Quote = "KRW",
    [int]$TopN = 50,
    [int]$DaysAgo = 1,
    [string]$DaysAgoCsv = "",
    [string]$RawRoot = "data/raw_ticks/upbit/trades",
    [string]$MetaDir = "data/raw_ticks/upbit/_meta",
    [int]$Workers = 1,
    [int]$MaxPagesPerTarget = 50,
    [string]$RateLimitStrict = "true",
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

. (Join-Path $PSScriptRoot "systemd_service_utils.ps1")

function Resolve-ProjectPath {
    param(
        [string]$Root,
        [string]$PathValue
    )
    if ([string]::IsNullOrWhiteSpace($PathValue)) {
        return $Root
    }
    if ([System.IO.Path]::IsPathRooted($PathValue)) {
        return [System.IO.Path]::GetFullPath($PathValue)
    }
    return [System.IO.Path]::GetFullPath((Join-Path $Root $PathValue))
}

function Invoke-ProjectPythonStep {
    param(
        [string]$PythonPath,
        [string]$StepName,
        [string[]]$ArgList
    )
    $commandText = $PythonPath + " " + (($ArgList | ForEach-Object { Quote-ShellArg ([string]$_) }) -join " ")
    Write-Host ("[raw-ticks-daily] step={0}" -f $StepName)
    Write-Host ("[raw-ticks-daily] command={0}" -f $commandText)
    if ($DryRun) {
        return [ordered]@{
            step = $StepName
            command = $commandText
            exit_code = 0
            dry_run = $true
            output_preview = ""
        }
    }
    $output = & $PythonPath @ArgList 2>&1
    $exitCode = [int]$LASTEXITCODE
    $outputText = [string]($output -join [Environment]::NewLine)
    if (-not [string]::IsNullOrWhiteSpace($outputText)) {
        Write-Host $outputText
    }
    if ($exitCode -ne 0) {
        throw ("step failed: " + $StepName + " exit_code=" + $exitCode)
    }
    return [ordered]@{
        step = $StepName
        command = $commandText
        exit_code = $exitCode
        dry_run = $false
        output_preview = if ([string]::IsNullOrWhiteSpace($outputText)) { "" } elseif ($outputText.Length -le 2000) { $outputText } else { $outputText.Substring(0, 2000) }
    }
}

function Resolve-DaysAgoSpec {
    param(
        [int]$SingleDay,
        [string]$DaysAgoCsvText,
        [string]$BatchDateText
    )
    if (-not [string]::IsNullOrWhiteSpace($BatchDateText)) {
        $batchDateValue = [DateTime]::ParseExact(
            $BatchDateText,
            "yyyy-MM-dd",
            [System.Globalization.CultureInfo]::InvariantCulture,
            [System.Globalization.DateTimeStyles]::None
        )
        $todayLocal = (Get-Date).Date
        $deltaDays = [int]($todayLocal - $batchDateValue.Date).TotalDays
        return ([string]([Math]::Max($deltaDays, 1)))
    }
    if (-not [string]::IsNullOrWhiteSpace($DaysAgoCsvText)) {
        return $DaysAgoCsvText.Trim()
    }
    return ([string]([Math]::Max([int]$SingleDay, 1)))
}

$resolvedProjectRoot = if ([string]::IsNullOrWhiteSpace($ProjectRoot)) { Resolve-DefaultProjectRoot } else { $ProjectRoot }
$resolvedProjectRoot = [System.IO.Path]::GetFullPath($resolvedProjectRoot)
$resolvedPythonExe = if ([string]::IsNullOrWhiteSpace($PythonExe)) { Resolve-DefaultPythonExe -Root $resolvedProjectRoot } else { $PythonExe }
$resolvedSummaryPath = Resolve-ProjectPath -Root $resolvedProjectRoot -PathValue $SummaryPath
$resolvedPlanPath = Resolve-ProjectPath -Root $resolvedProjectRoot -PathValue $PlanPath
$resolvedRawRoot = Resolve-ProjectPath -Root $resolvedProjectRoot -PathValue $RawRoot
$resolvedMetaDir = Resolve-ProjectPath -Root $resolvedProjectRoot -PathValue $MetaDir
$resolvedBatchDate = if ([string]::IsNullOrWhiteSpace($BatchDate)) { (Get-Date).Date.AddDays(-1).ToString("yyyy-MM-dd") } else { $BatchDate.Trim() }
$daysAgoSpec = Resolve-DaysAgoSpec -SingleDay $DaysAgo -DaysAgoCsvText $DaysAgoCsv -BatchDateText $resolvedBatchDate
$daysAgoValues = @(
    $daysAgoSpec.Split(",") |
        ForEach-Object { $_.Trim() } |
        Where-Object { -not [string]::IsNullOrWhiteSpace($_) } |
        ForEach-Object { [Math]::Max([int]$_, 1) } |
        Sort-Object -Unique
)
$validateDates = @(
    $daysAgoValues |
        ForEach-Object { (Get-Date).ToUniversalTime().AddDays(-1 * [int]$_).ToString("yyyy-MM-dd") }
)

$planArgs = @(
    "-m", "autobot.cli",
    "collect", "plan-ticks",
    "--parquet-root", "data/parquet",
    "--base-dataset", "candles_api_v1",
    "--out", $resolvedPlanPath,
    "--quote", $Quote,
    "--market-mode", "top_n_by_recent_value_est",
    "--top-n", ([string]([Math]::Max([int]$TopN, 1))),
    "--days-ago", $daysAgoSpec
)

$collectArgs = @(
    "-m", "autobot.cli",
    "collect", "ticks",
    "--plan", $resolvedPlanPath,
    "--mode", "daily",
    "--quote", $Quote,
    "--top-n", ([string]([Math]::Max([int]$TopN, 1))),
    "--days-ago", $daysAgoSpec,
    "--raw-root", $resolvedRawRoot,
    "--meta-dir", $resolvedMetaDir,
    "--rate-limit-strict", $RateLimitStrict,
    "--workers", ([string]([Math]::Max([int]$Workers, 1))),
    "--max-pages-per-target", ([string]([Math]::Max([int]$MaxPagesPerTarget, 1))),
    "--dry-run", "false"
)

$stepResults = @()
Push-Location $resolvedProjectRoot
try {
    $stepResults += ,(Invoke-ProjectPythonStep -PythonPath $resolvedPythonExe -StepName "plan_raw_ticks_daily" -ArgList $planArgs)
    $stepResults += ,(Invoke-ProjectPythonStep -PythonPath $resolvedPythonExe -StepName "collect_raw_ticks_daily" -ArgList $collectArgs)
    foreach ($validateDate in $validateDates) {
        $validateArgs = @(
            "-m", "autobot.cli",
            "collect", "ticks", "validate",
            "--date", $validateDate,
            "--raw-root", $resolvedRawRoot,
            "--meta-dir", $resolvedMetaDir
        )
        $stepResults += ,(Invoke-ProjectPythonStep -PythonPath $resolvedPythonExe -StepName ("validate_raw_ticks_" + $validateDate) -ArgList $validateArgs)
    }
} finally {
    Pop-Location
}

$summary = [ordered]@{
    policy = "raw_ticks_daily_v1"
    generated_at_utc = (Get-Date).ToUniversalTime().ToString("o")
    project_root = $resolvedProjectRoot
    python_exe = $resolvedPythonExe
    raw_root = $resolvedRawRoot
    meta_dir = $resolvedMetaDir
    plan_path = $resolvedPlanPath
    batch_date = $resolvedBatchDate
    days_ago = @($daysAgoValues)
    validate_dates = @($validateDates)
    steps = @($stepResults)
}
$summaryDir = Split-Path -Parent $resolvedSummaryPath
if (-not [string]::IsNullOrWhiteSpace($summaryDir)) {
    New-Item -ItemType Directory -Force -Path $summaryDir | Out-Null
}
$summary | ConvertTo-Json -Depth 8 | Set-Content -Path $resolvedSummaryPath -Encoding UTF8
Write-Host $resolvedSummaryPath
