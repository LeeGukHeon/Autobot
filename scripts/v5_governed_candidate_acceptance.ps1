# V5 governed acceptance uses the practical first production-bound build from the
# training blueprint: v5_panel_ensemble on top of features_v4 + label_v3.
$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

. (Join-Path $PSScriptRoot "v4_acceptance_contract.ps1")

$knownRuntimeUnits = @("autobot-paper-v4.service", "autobot-live-alpha.service")
$trainDataQualityFloorDate = Get-V4TrainDataQualityFloorDate

& (Join-Path $PSScriptRoot "candidate_acceptance.ps1") `
    -ModelFamily "train_v5_panel_ensemble" `
    -Trainer "v5_panel_ensemble" `
    -FeatureSet "v4" `
    -LabelSet "v3" `
    -Task "cls" `
    -RunScope "scheduled_daily" `
    -CandidateModelRef "latest_candidate" `
    -ChampionModelRef "champion" `
    -PaperFeatureProvider "live_v4" `
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
    -OutDir "logs/model_v4_acceptance" `
    -ReportPrefix "v5_candidate_acceptance" `
    -ReportTitle "V5 Candidate Acceptance" `
    -LogTag "v5-accept" `
    @args
exit $LASTEXITCODE
