# Rank governed acceptance promotes the rank lane from shadow evidence into the
# primary promotable candidate lane without changing the acceptance contract.
. (Join-Path $PSScriptRoot "v4_acceptance_contract.ps1")
$knownRuntimeUnits = @("autobot-paper-v4.service", "autobot-live-alpha.service")
$trainDataQualityFloorDate = Get-V4TrainDataQualityFloorDate
& (Join-Path $PSScriptRoot "candidate_acceptance.ps1") `
    -ModelFamily "train_v4_crypto_cs" `
    -Trainer "v4_crypto_cs" `
    -FeatureSet "v4" `
    -LabelSet "v2" `
    -Task "rank" `
    -RunScope "scheduled_daily_rank_governed" `
    -CandidateModelRef "latest_candidate_v4" `
    -ChampionModelRef "champion_v4" `
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
    -SplitPolicyHistoricalSelectorEnabled `
    -KnownRuntimeUnits $knownRuntimeUnits `
    -OutDir "logs/model_v4_acceptance" `
    -ReportPrefix "v4_candidate_acceptance" `
    -ReportTitle "V4 Candidate Acceptance" `
    -LogTag "v4-accept" `
    @args
exit $LASTEXITCODE
