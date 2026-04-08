param(
    [string]$ProjectRoot = "",
    [string]$PythonExe = "",
    [string]$BatchDate = "",
    [string]$OutDir = "logs/model_v5_runtime_parity_fast",
    [int]$TrainLookbackDays = 30,
    [int]$BacktestLookbackDays = 8,
    [switch]$EnableVariantMatrixSelection,
    [switch]$EnableFusionInputAblationMatrix,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

. (Join-Path $PSScriptRoot "v4_acceptance_contract.ps1")

$resolvedProjectRoot = if ([string]::IsNullOrWhiteSpace($ProjectRoot)) { Resolve-DefaultProjectRoot } else { [System.IO.Path]::GetFullPath($ProjectRoot) }
$resolvedPythonExe = if ([string]::IsNullOrWhiteSpace($PythonExe)) { Resolve-DefaultPythonExe -Root $resolvedProjectRoot } else { $PythonExe }
$trainDataQualityFloorDate = Get-V4TrainDataQualityFloorDate

& (Join-Path $PSScriptRoot "candidate_acceptance.ps1") `
    -ProjectRoot $resolvedProjectRoot `
    -PythonExe $resolvedPythonExe `
    -BatchDate $BatchDate `
    -ModelFamily "train_v5_fusion" `
    -Trainer "v5_fusion" `
    -DependencyTrainers @("v5_panel_ensemble", "v5_sequence", "v5_lob", "v5_tradability") `
    -FeatureSet "v4" `
    -LabelSet "v3" `
    -Task "cls" `
    -RunScope "scheduled_daily" `
    -CandidateModelRef "latest_candidate" `
    -ChampionModelRef "champion" `
    -PaperFeatureProvider "live_v5" `
    -PromotionPolicy "paper_final_balanced" `
    -TrainerEvidenceMode "required" `
    -TrainLookbackDays $TrainLookbackDays `
    -BacktestLookbackDays $BacktestLookbackDays `
    -BacktestTopPct 0.5 `
    -BacktestMinProb 0.0 `
    -BacktestMinCandidatesPerTs 1 `
    -BacktestMinOrdersFilled 30 `
    -BacktestMinRealizedPnlQuote 0.0 `
    -BacktestMinPnlDeltaVsChampion 0.0 `
    -TrainDataQualityFloorDate $trainDataQualityFloorDate `
    -BacktestRuntimeParityEnabled:$true `
    -EnableVariantMatrixSelection:$EnableVariantMatrixSelection `
    -EnableFusionInputAblationMatrix:$EnableFusionInputAblationMatrix `
    -SkipDailyPipeline `
    -SkipPaperSoak `
    -SkipPromote `
    -SkipReportRefresh `
    -OutDir $OutDir `
    -ReportPrefix "v5_candidate_runtime_parity_fast" `
    -ReportTitle "V5 Fusion Runtime Parity Fast Validation" `
    -LogTag "v5-fast-runtime-parity" `
    -DryRun:$DryRun `
    @args
exit $LASTEXITCODE
