param(
    [string]$PythonExe = "C:\Python314\python.exe",
    [string]$ProjectRoot = "D:\MyApps\Autobot",
    [int]$DurationSec = 360,
    [string]$Quote = "KRW",
    [int]$TopN = 20,
    [string]$Tf = "5m",
    [string]$ChampionModelRef = "",
    [string]$ChallengerModelRef = "",
    [string]$ModelFamily = "train_v5_panel_ensemble",
    [string]$ChampionModelFamily = "",
    [string]$ChallengerModelFamily = "",
    [string]$FeatureSet = "v4",
    [string]$Preset = "live_v5",
    [string]$PaperMicroProvider = "live_ws",
    [string]$PaperFeatureProvider = "live_v4",
    [int]$WarmupSec = 60,
    [int]$WarmupMinTradeEventsPerMarket = 1,
    [int]$MinMatchedOpportunities = 1,
    [double]$MinChallengerHours = 12.0,
    [int]$MinOrdersFilled = 2,
    [double]$MinRealizedPnlQuote = 0.0,
    [double]$MinMicroQualityScore = 0.25,
    [double]$MinNonnegativeRatio = 0.34,
    [double]$MaxDrawdownDeteriorationFactor = 1.10,
    [double]$MicroQualityTolerance = 0.02,
    [double]$NonnegativeRatioTolerance = 0.05,
    [double]$MaxTimeToFillDeteriorationFactor = 1.25,
    [double]$ReplayTimeScale = 0.001,
    [double]$ReplayMaxSleepSec = 0.25,
    [string]$OutDir = "logs/paired_paper"
)

$ErrorActionPreference = "Stop"
Set-Location $ProjectRoot
$script:IsWindowsPlatform = [System.IO.Path]::DirectorySeparatorChar -eq '\'

$vendorSitePackages = Join-Path $ProjectRoot "python\site-packages"
if ($script:IsWindowsPlatform -and (Test-Path $vendorSitePackages)) {
    if ([string]::IsNullOrWhiteSpace($env:PYTHONPATH)) {
        $env:PYTHONPATH = $vendorSitePackages
    } elseif ($env:PYTHONPATH -notlike "*$vendorSitePackages*") {
        $env:PYTHONPATH = "$vendorSitePackages;$($env:PYTHONPATH)"
    }
}

function Invoke-CommandCapture {
    param(
        [string]$Exe,
        [string[]]$ArgList
    )
    $output = & $Exe @ArgList 2>&1
    return [PSCustomObject]@{
        ExitCode = [int]$LASTEXITCODE
        Output = ($output -join "`n")
    }
}

$resolvedChampionModelFamily = if ([string]::IsNullOrWhiteSpace($ChampionModelFamily)) { $ModelFamily } else { $ChampionModelFamily }
$resolvedChallengerModelFamily = if ([string]::IsNullOrWhiteSpace($ChallengerModelFamily)) { $ModelFamily } else { $ChallengerModelFamily }

$args = @(
    "-m", "autobot.paper.paired_runtime",
    "run-live",
    "--config-dir", "config",
    "--duration-sec", $DurationSec,
    "--quote", $Quote,
    "--top-n", $TopN,
    "--tf", $Tf,
    "--champion-model-ref", $ChampionModelRef,
    "--challenger-model-ref", $ChallengerModelRef,
    "--model-family", $ModelFamily,
    "--champion-model-family", $resolvedChampionModelFamily,
    "--challenger-model-family", $resolvedChallengerModelFamily,
    "--feature-set", $FeatureSet,
    "--preset", $Preset,
    "--paper-micro-provider", $PaperMicroProvider,
    "--paper-feature-provider", $PaperFeatureProvider,
    "--paper-micro-warmup-sec", $WarmupSec,
    "--paper-micro-warmup-min-trade-events-per-market", $WarmupMinTradeEventsPerMarket,
    "--min-matched-opportunities", $MinMatchedOpportunities,
    "--min-challenger-hours", $MinChallengerHours,
    "--min-orders-filled", $MinOrdersFilled,
    "--min-realized-pnl-quote", $MinRealizedPnlQuote,
    "--min-micro-quality-score", $MinMicroQualityScore,
    "--min-nonnegative-ratio", $MinNonnegativeRatio,
    "--max-drawdown-deterioration-factor", $MaxDrawdownDeteriorationFactor,
    "--micro-quality-tolerance", $MicroQualityTolerance,
    "--nonnegative-ratio-tolerance", $NonnegativeRatioTolerance,
    "--max-time-to-fill-deterioration-factor", $MaxTimeToFillDeteriorationFactor,
    "--replay-time-scale", $ReplayTimeScale,
    "--replay-max-sleep-sec", $ReplayMaxSleepSec,
    "--out-dir", $OutDir
)

$exec = Invoke-CommandCapture -Exe $PythonExe -ArgList $args
if ($exec.ExitCode -ne 0) {
    throw "paired paper soak failed (exit=$($exec.ExitCode)): $($exec.Output)"
}

$payload = $null
try {
    $payload = ([string]$exec.Output) | ConvertFrom-Json
} catch {
    throw "paired paper soak completed but stdout was not valid JSON: $($exec.Output)"
}

$reportPath = [string]($payload.report_path)
if ([string]::IsNullOrWhiteSpace($reportPath)) {
    throw "paired paper soak completed but report_path was missing"
}
if (-not (Test-Path $reportPath)) {
    throw "paired paper soak report_path does not exist: $reportPath"
}

Write-Host ("[paired-paper] report={0}" -f $reportPath)
Write-Host ("[paired-paper] run_root={0}" -f ([string]($payload.run_root)))
Write-Host ("[paired-paper] gate_pass={0}" -f ([bool](($payload.gate).pass)))
Write-Host $exec.Output
