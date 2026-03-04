param(
    [string]$PythonExe = "C:\Python314\python.exe",
    [string]$ProjectRoot = "D:\MyApps\Autobot",
    [string]$Quote = "KRW",
    [int]$TopN = 50,
    [int]$DaysAgo = 1,
    [int]$MaxPagesPerTarget = 50,
    [int]$Workers = 1,
    [string]$RawTicksRoot = "data/raw_ticks/upbit/trades",
    [string]$RawWsRoot = "data/raw_ws/upbit/public",
    [string]$OutRoot = "data/parquet/micro_v1",
    [switch]$SkipTicks,
    [switch]$SkipAggregate,
    [switch]$SkipValidate,
    [string]$Date = ""
)

$ErrorActionPreference = "Stop"
Set-Location $ProjectRoot

$vendorSitePackages = Join-Path $ProjectRoot "python\site-packages"
if (Test-Path $vendorSitePackages) {
    if ([string]::IsNullOrWhiteSpace($env:PYTHONPATH)) {
        $env:PYTHONPATH = $vendorSitePackages
    } elseif ($env:PYTHONPATH -notlike "*$vendorSitePackages*") {
        $env:PYTHONPATH = "$vendorSitePackages;$($env:PYTHONPATH)"
    }
}

function Invoke-CheckedCommand {
    param(
        [string]$Exe,
        [string[]]$ArgList
    )
    & $Exe @ArgList
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed (exit=$LASTEXITCODE): $Exe $($ArgList -join ' ')"
    }
}

function Invoke-CheckedCommandWithOutput {
    param(
        [string]$Exe,
        [string[]]$ArgList
    )
    $output = & $Exe @ArgList 2>&1
    if ($LASTEXITCODE -ne 0) {
        $outputText = $output -join "`n"
        throw "Command failed (exit=$LASTEXITCODE): $Exe $($ArgList -join ' ')`n$outputText"
    }
    return ($output -join "`n")
}

function Load-JsonOrEmpty {
    param([string]$PathValue)
    if (-not (Test-Path $PathValue)) {
        return @{}
    }
    $raw = Get-Content -Path $PathValue -Raw -Encoding UTF8
    if ([string]::IsNullOrWhiteSpace($raw)) {
        return @{}
    }
    return $raw | ConvertFrom-Json
}

$targetDate = $Date
if ([string]::IsNullOrWhiteSpace($targetDate)) {
    $targetDate = (Get-Date).Date.AddDays(-1).ToString("yyyy-MM-dd")
}

$ticksPlanPath = "data/raw_ticks/upbit/_meta/ticks_plan_daily_auto.json"
if (Test-Path $ticksPlanPath) {
    Remove-Item -Path $ticksPlanPath -Force
}

$ticksArgs = @(
    "-m", "autobot.cli",
    "collect", "ticks",
    "--plan", $ticksPlanPath,
    "--mode", "daily",
    "--quote", $Quote,
    "--top-n", $TopN,
    "--days-ago", $DaysAgo,
    "--raw-root", $RawTicksRoot,
    "--rate-limit-strict", "true",
    "--workers", $Workers,
    "--max-pages-per-target", $MaxPagesPerTarget,
    "--dry-run", "false"
)
if (-not $SkipTicks) {
    Invoke-CheckedCommand -Exe $PythonExe -ArgList $ticksArgs
}

$aggregateArgs = @(
    "-m", "autobot.cli",
    "micro", "aggregate",
    "--start", $targetDate,
    "--end", $targetDate,
    "--quote", $Quote,
    "--top-n", $TopN,
    "--raw-ticks-root", $RawTicksRoot,
    "--raw-ws-root", $RawWsRoot,
    "--out-root", $OutRoot
)
if (-not $SkipAggregate) {
    Invoke-CheckedCommand -Exe $PythonExe -ArgList $aggregateArgs
}

$validateArgs = @(
    "-m", "autobot.cli",
    "micro", "validate",
    "--out-root", $OutRoot
)
if (-not $SkipValidate) {
    Invoke-CheckedCommand -Exe $PythonExe -ArgList $validateArgs
}

$statsArgs = @(
    "-m", "autobot.cli",
    "micro", "stats",
    "--out-root", $OutRoot
)
$statsRaw = Invoke-CheckedCommandWithOutput -Exe $PythonExe -ArgList $statsArgs
$stats = $statsRaw | ConvertFrom-Json

$aggregateReportPath = Join-Path $OutRoot "_meta/aggregate_report.json"
$validateReportPath = Join-Path $OutRoot "_meta/validate_report.json"
$manifestPath = Join-Path $OutRoot "_meta/manifest.parquet"
$aggregateReport = Load-JsonOrEmpty -PathValue $aggregateReportPath
$validateReport = Load-JsonOrEmpty -PathValue $validateReportPath
$ticksReport = Load-JsonOrEmpty -PathValue "data/raw_ticks/upbit/_meta/ticks_collect_report.json"

$reportDir = Join-Path $ProjectRoot "docs\reports"
New-Item -ItemType Directory -Path $reportDir -Force | Out-Null
$reportPath = Join-Path $reportDir ("DAILY_MICRO_REPORT_" + $targetDate + ".md")

$bookAvailableRatio = 0.0
$tradeSourceWsRatio = 0.0
$microAvailableRatio = 0.0
$joinMatchRatio = "NA"

if ($stats.PSObject.Properties.Name -contains "book_available_ratio") {
    $bookAvailableRatio = [double]$stats.book_available_ratio
}
if ($stats.PSObject.Properties.Name -contains "trade_source_ws_ratio") {
    $tradeSourceWsRatio = [double]$stats.trade_source_ws_ratio
} elseif ($stats.PSObject.Properties.Name -contains "trade_source_ratio") {
    $tradeSourceWsRatio = [double]$stats.trade_source_ratio.ws
}
if ($stats.PSObject.Properties.Name -contains "micro_available_ratio") {
    $microAvailableRatio = [double]$stats.micro_available_ratio
}
if ($validateReport.PSObject.Properties.Name -contains "join") {
    if ($validateReport.join.PSObject.Properties.Name -contains "join_match_ratio") {
        if ($null -ne $validateReport.join.join_match_ratio) {
            $joinMatchRatio = [string]$validateReport.join.join_match_ratio
        }
    }
}

$reportLines = @(
    "# DAILY_MICRO_REPORT_$targetDate",
    "",
    "## Summary",
    "- target_date: $targetDate",
    "- quote/top_n: $Quote / $TopN",
    ("- book_available_ratio: {0:N6}" -f $bookAvailableRatio),
    ("- trade_source_ws_ratio: {0:N6}" -f $tradeSourceWsRatio),
    ("- micro_available_ratio: {0:N6}" -f $microAvailableRatio),
    "- join_match_ratio: $joinMatchRatio",
    "",
    "## Commands",
    "- python -m autobot.cli collect ticks --mode daily --quote $Quote --top-n $TopN --days-ago $DaysAgo --raw-root $RawTicksRoot --rate-limit-strict true --workers $Workers --max-pages-per-target $MaxPagesPerTarget --dry-run false",
    "- python -m autobot.cli micro aggregate --start $targetDate --end $targetDate --quote $Quote --top-n $TopN --raw-ticks-root $RawTicksRoot --raw-ws-root $RawWsRoot --out-root $OutRoot",
    "- python -m autobot.cli micro validate --out-root $OutRoot",
    "- python -m autobot.cli micro stats --out-root $OutRoot",
    "",
    "## Artifacts",
    "- ticks_collect_report: data/raw_ticks/upbit/_meta/ticks_collect_report.json",
    "- micro_aggregate_report: $aggregateReportPath",
    "- micro_validate_report: $validateReportPath",
    "- micro_manifest: $manifestPath",
    "",
    "## Excerpts",
    ("- ticks run_id: {0}" -f $ticksReport.run_id),
    ("- micro aggregate run_id: {0}" -f $aggregateReport.run_id),
    ("- micro rows_written_total: {0}" -f $aggregateReport.rows_written_total),
    ("- micro parts: {0}" -f $stats.parts)
)
$report = $reportLines -join "`n"

Set-Content -Path $reportPath -Value $report -Encoding UTF8
Write-Host "[daily-micro] report=$reportPath"
