param(
    [string]$ProjectRoot = "",
    [string]$PythonExe = "",
    [string]$DataPlatformRefreshScript = "",
    [switch]$SkipDataPlatformRefresh,
    [switch]$DryRun
)

# V5 governed acceptance now executes the full blueprint-aligned expert chain:
# data refresh -> v5_panel_ensemble -> v5_sequence -> v5_lob -> v5_fusion.
$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

. (Join-Path $PSScriptRoot "v4_acceptance_contract.ps1")

function Resolve-DefaultDataPlatformRefreshScript {
    param([string]$Root)
    return (Join-Path $Root "scripts/refresh_data_platform_layers.ps1")
}

function Resolve-DefaultPwshExe {
    if ($IsWindows) {
        return "powershell.exe"
    }
    $cmd = Get-Command pwsh -ErrorAction SilentlyContinue
    if ($null -ne $cmd -and -not [string]::IsNullOrWhiteSpace($cmd.Source)) {
        return [string]$cmd.Source
    }
    return "pwsh"
}

$resolvedProjectRoot = if ([string]::IsNullOrWhiteSpace($ProjectRoot)) { Resolve-DefaultProjectRoot } else { [System.IO.Path]::GetFullPath($ProjectRoot) }
$resolvedPythonExe = if ([string]::IsNullOrWhiteSpace($PythonExe)) { Resolve-DefaultPythonExe -Root $resolvedProjectRoot } else { $PythonExe }
$resolvedDataPlatformRefreshScript = if ([string]::IsNullOrWhiteSpace($DataPlatformRefreshScript)) {
    Resolve-DefaultDataPlatformRefreshScript -Root $resolvedProjectRoot
} else {
    $DataPlatformRefreshScript
}

if ((-not $SkipDataPlatformRefresh) -and (Test-Path $resolvedDataPlatformRefreshScript)) {
    $pwshExe = Resolve-DefaultPwshExe
    $refreshArgs = @(
        "-NoProfile",
        "-ExecutionPolicy", "Bypass",
        "-File", $resolvedDataPlatformRefreshScript,
        "-ProjectRoot", $resolvedProjectRoot,
        "-PythonExe", $resolvedPythonExe
    )
    if ($DryRun) {
        $refreshArgs += "-DryRun"
    }
    & $pwshExe @refreshArgs
    if ($LASTEXITCODE -ne 0) {
        throw ("data platform refresh failed: " + $resolvedDataPlatformRefreshScript)
    }
}

$knownRuntimeUnits = @("autobot-paper-v5.service", "autobot-live-alpha.service")
$trainDataQualityFloorDate = Get-V4TrainDataQualityFloorDate

& (Join-Path $PSScriptRoot "candidate_acceptance.ps1") `
    -ProjectRoot $resolvedProjectRoot `
    -PythonExe $resolvedPythonExe `
    -ModelFamily "train_v5_fusion" `
    -Trainer "v5_fusion" `
    -DependencyTrainers @("v5_panel_ensemble", "v5_sequence", "v5_lob") `
    -FeatureSet "v4" `
    -LabelSet "v3" `
    -Task "cls" `
    -RunScope "scheduled_daily" `
    -CandidateModelRef "latest_candidate" `
    -ChampionModelRef "champion" `
    -PaperFeatureProvider "live_v5" `
    -PromotionPolicy "paper_final_balanced" `
    -TrainerEvidenceMode "required" `
    -BacktestTopPct 0.5 `
    -BacktestMinProb 0.0 `
    -BacktestMinCandidatesPerTs 1 `
    -BacktestMinOrdersFilled 30 `
    -BacktestMinRealizedPnlQuote 0.0 `
    -BacktestMinPnlDeltaVsChampion 0.0 `
    -PaperMaxFallbackRatio 0.20 `
    -PaperMinOrdersSubmitted 1 `
    -PaperMinOrdersFilled 2 `
    -PaperMinRealizedPnlQuote 0.0 `
    -PaperMinTierCount 1 `
    -PaperMinPolicyEvents 0 `
    -TrainDataQualityFloorDate $trainDataQualityFloorDate `
    -SplitPolicyHistoricalSelectorEnabled:$false `
    -KnownRuntimeUnits $knownRuntimeUnits `
    -OutDir "logs/model_v5_acceptance" `
    -ReportPrefix "v5_candidate_acceptance" `
    -ReportTitle "V5 Fusion Candidate Acceptance" `
    -LogTag "v5-accept" `
    -DryRun:$DryRun `
    @args
exit $LASTEXITCODE
