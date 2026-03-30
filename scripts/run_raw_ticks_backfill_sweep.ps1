param(
    [string]$ProjectRoot = "",
    [string]$PythonExe = "",
    [string]$SummaryPath = "data/raw_ticks/upbit/_meta/ticks_backfill_latest.json",
    [string]$PlanPath = "data/raw_ticks/upbit/_meta/ticks_plan_backfill_auto.json",
    [string]$Quote = "KRW",
    [int]$TopN = 50,
    [string]$DaysAgoCsv = "2,3,4,5,6,7",
    [string]$RawRoot = "data/raw_ticks/upbit/trades",
    [string]$MetaDir = "data/raw_ticks/upbit/_meta",
    [int]$Workers = 1,
    [int]$MaxPagesPerTarget = 50,
    [string]$RateLimitStrict = "true",
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$baseScript = Join-Path $PSScriptRoot "run_raw_ticks_daily.ps1"
$invokeParams = @{
    ProjectRoot = $ProjectRoot
    PythonExe = $PythonExe
    SummaryPath = $SummaryPath
    PlanPath = $PlanPath
    Quote = $Quote
    TopN = $TopN
    DaysAgoCsv = $DaysAgoCsv
    RawRoot = $RawRoot
    MetaDir = $MetaDir
    Workers = $Workers
    MaxPagesPerTarget = $MaxPagesPerTarget
    RateLimitStrict = $RateLimitStrict
}
if ($DryRun) {
    $invokeParams["DryRun"] = $true
}
& $baseScript @invokeParams
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}
