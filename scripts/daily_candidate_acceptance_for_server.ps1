param(
    [string]$ProjectRoot = "",
    [string]$PythonExe = "",
    [string]$AcceptanceScript = "",
    [string]$BatchDate = "",
    [string[]]$BlockOnActiveUnits = @(),
    [string[]]$AcceptanceArgs = @(),
    [switch]$SkipDailyPipeline,
    [switch]$SkipReportRefresh,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

. (Join-Path $PSScriptRoot "systemd_service_utils.ps1")

function Resolve-DefaultAcceptanceScript {
    param([string]$Root)
    return (Join-Path $Root "scripts/v4_governed_candidate_acceptance.ps1")
}

function Resolve-ReportedJsonPath {
    param([string]$OutputText)
    $regex = [System.Text.RegularExpressions.Regex]::new("(?m)^\[[^\]]+\]\s+report=(.+)$")
    $match = $regex.Match([string]$OutputText)
    if (-not $match.Success) {
        return ""
    }
    return [string]$match.Groups[1].Value.Trim()
}

function Load-JsonOrEmpty {
    param([string]$PathValue)
    if ([string]::IsNullOrWhiteSpace($PathValue) -or (-not (Test-Path $PathValue))) {
        return @{}
    }
    $raw = Get-Content -Path $PathValue -Raw -Encoding UTF8
    if ([string]::IsNullOrWhiteSpace($raw)) {
        return @{}
    }
    return $raw | ConvertFrom-Json
}

function Get-PropValue {
    param(
        [Parameter(Mandatory = $false)]$ObjectValue,
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $false)]$DefaultValue = $null
    )
    if ($null -eq $ObjectValue) {
        return $DefaultValue
    }
    if ($ObjectValue -is [System.Collections.IDictionary]) {
        if ($ObjectValue.Contains($Name)) {
            return $ObjectValue[$Name]
        }
        return $DefaultValue
    }
    if ($ObjectValue.PSObject -and $ObjectValue.PSObject.Properties.Name -contains $Name) {
        return $ObjectValue.$Name
    }
    return $DefaultValue
}

function Test-ObjectHasValues {
    param([Parameter(Mandatory = $false)]$ObjectValue)
    if ($null -eq $ObjectValue) {
        return $false
    }
    if ($ObjectValue -is [System.Collections.IDictionary]) {
        return ($ObjectValue.Count -gt 0)
    }
    if ($ObjectValue.PSObject) {
        return (@($ObjectValue.PSObject.Properties).Count -gt 0)
    }
    return $true
}

function Get-StringArray {
    param([Parameter(Mandatory = $false)]$Value)
    return @(Expand-DelimitedStringArray -Value $Value)
}

function Resolve-BatchDateValue {
    param([string]$DateText)
    if (-not [string]::IsNullOrWhiteSpace($DateText)) {
        return [DateTime]::ParseExact(
            $DateText,
            "yyyy-MM-dd",
            [System.Globalization.CultureInfo]::InvariantCulture,
            [System.Globalization.DateTimeStyles]::None
        ).ToString("yyyy-MM-dd")
    }
    return (Get-Date).Date.AddDays(-1).ToString("yyyy-MM-dd")
}

function Test-SystemdUnitActive {
    param([string]$UnitName)
    if ($DryRun) {
        return $false
    }
    $systemctl = Get-Command systemctl -ErrorAction SilentlyContinue
    if ($null -eq $systemctl) {
        return $false
    }
    & $systemctl.Source is-active --quiet $UnitName
    return ($LASTEXITCODE -eq 0)
}

function Invoke-CommandCapture {
    param(
        [string]$Exe,
        [string[]]$ArgList,
        [switch]$AllowFailure
    )
    $commandText = $Exe + " " + (($ArgList | ForEach-Object { Quote-ShellArg ([string]$_) }) -join " ")
    if ($DryRun) {
        Write-Host ("[daily-accept][dry-run] {0}" -f $commandText)
        return [PSCustomObject]@{
            ExitCode = 0
            Output = "[dry-run] $commandText"
            Command = $commandText
        }
    }
    $output = & $Exe @ArgList 2>&1
    $exitCode = [int]$LASTEXITCODE
    if ((-not $AllowFailure) -and $exitCode -ne 0) {
        throw ("command failed: " + $commandText)
    }
    return [PSCustomObject]@{
        ExitCode = $exitCode
        Output = ($output -join "`n")
        Command = $commandText
    }
}

function Test-AcceptanceFatalFailure {
    param(
        [int]$ExitCode,
        [Parameter(Mandatory = $false)]$AcceptanceReport
    )
    if ($ExitCode -eq 0) {
        return $false
    }
    if (($ExitCode -ne 2) -or (-not (Test-ObjectHasValues -ObjectValue $AcceptanceReport))) {
        return $true
    }
    $steps = Get-PropValue -ObjectValue $AcceptanceReport -Name "steps" -DefaultValue @{}
    $exceptionStep = Get-PropValue -ObjectValue $steps -Name "exception" -DefaultValue @{}
    if (Test-ObjectHasValues -ObjectValue $exceptionStep) {
        return $true
    }
    $reasons = Get-StringArray -Value (Get-PropValue -ObjectValue $AcceptanceReport -Name "reasons" -DefaultValue @())
    foreach ($reason in $reasons) {
        if (@(
            "UNHANDLED_EXCEPTION",
            "DAILY_PIPELINE_FAILED",
            "TRAIN_OR_CANDIDATE_POINTER_FAILED"
        ) -contains [string]$reason) {
            return $true
        }
    }
    return $false
}

function Test-ScoutNonFatalRejection {
    param([Parameter(Mandatory = $false)]$AcceptanceReport)
    $reasons = Get-StringArray -Value (Get-PropValue -ObjectValue $AcceptanceReport -Name "reasons" -DefaultValue @())
    if ($reasons -contains "SCOUT_ONLY_BUDGET_EVIDENCE") {
        return $true
    }
    $backtestGate = Get-PropValue -ObjectValue (Get-PropValue -ObjectValue $AcceptanceReport -Name "gates" -DefaultValue @{}) -Name "backtest" -DefaultValue @{}
    $budgetReasons = Get-StringArray -Value (Get-PropValue -ObjectValue $backtestGate -Name "budget_contract_reasons" -DefaultValue @())
    return ($budgetReasons -contains "SCOUT_ONLY_BUDGET_EVIDENCE")
}

$resolvedProjectRoot = if ([string]::IsNullOrWhiteSpace($ProjectRoot)) { Resolve-DefaultProjectRoot } else { $ProjectRoot }
$resolvedProjectRoot = [System.IO.Path]::GetFullPath($resolvedProjectRoot)
$resolvedPythonExe = if ([string]::IsNullOrWhiteSpace($PythonExe)) { Resolve-DefaultPythonExe -Root $resolvedProjectRoot } else { $PythonExe }
$resolvedAcceptanceScript = if ([string]::IsNullOrWhiteSpace($AcceptanceScript)) { Resolve-DefaultAcceptanceScript -Root $resolvedProjectRoot } else { $AcceptanceScript }
$resolvedBatchDate = Resolve-BatchDateValue -DateText $BatchDate
$resolvedBlockOnActiveUnits = @(Get-StringArray -Value $BlockOnActiveUnits)
$resolvedAcceptanceArgs = @(Get-StringArray -Value $AcceptanceArgs)

$activeBlockUnits = @()
foreach ($unit in $resolvedBlockOnActiveUnits) {
    $trimmed = [string]$unit
    if ([string]::IsNullOrWhiteSpace($trimmed)) {
        continue
    }
    if (Test-SystemdUnitActive -UnitName $trimmed.Trim()) {
        $activeBlockUnits += $trimmed.Trim()
    }
}

if ($activeBlockUnits.Count -gt 0) {
    Write-Host ("[daily-accept][skip] blocking_units_active={0}" -f ($activeBlockUnits -join ","))
    exit 0
}

$psExe = if ([System.IO.Path]::DirectorySeparatorChar -eq '\') { "powershell.exe" } else { Resolve-PwshExe }
$argList = @(
    "-NoProfile",
    "-ExecutionPolicy", "Bypass",
    "-File", $resolvedAcceptanceScript,
    "-ProjectRoot", $resolvedProjectRoot,
    "-PythonExe", $resolvedPythonExe,
    "-BatchDate", $resolvedBatchDate
)
if ($SkipDailyPipeline) {
    $argList += "-SkipDailyPipeline"
}
if ($SkipReportRefresh) {
    $argList += "-SkipReportRefresh"
}
if ($DryRun) {
    $argList += "-DryRun"
}
$argList += $resolvedAcceptanceArgs

$commandPreview = $psExe + " " + (($argList | ForEach-Object { Quote-ShellArg ([string]$_) }) -join " ")
Write-Host ("[daily-accept] batch_date={0}" -f $resolvedBatchDate)
Write-Host ("[daily-accept] acceptance_script={0}" -f $resolvedAcceptanceScript)
Write-Host ("[daily-accept] command={0}" -f $commandPreview)

$exec = Invoke-CommandCapture -Exe $psExe -ArgList $argList -AllowFailure
$reportPath = Resolve-ReportedJsonPath -OutputText $exec.Output
$acceptanceReport = Load-JsonOrEmpty -PathValue $reportPath
if (Test-AcceptanceFatalFailure -ExitCode $exec.ExitCode -AcceptanceReport $acceptanceReport) {
    exit $exec.ExitCode
}
if (($exec.ExitCode -ne 0) -and (Test-ScoutNonFatalRejection -AcceptanceReport $acceptanceReport)) {
    Write-Host ("[daily-accept] scout_nonfatal_reason=SCOUT_ONLY_BUDGET_EVIDENCE")
    exit 0
}
exit $exec.ExitCode
