# Rank shadow acceptance evaluates the rank lane under the frozen v4 certification
# contract without allowing silent replacement of the production cls lane.
$knownRuntimeUnits = @("autobot-paper-v4.service", "autobot-live-alpha.service")
& (Join-Path $PSScriptRoot "candidate_acceptance.ps1") `
    -ModelFamily "train_v4_crypto_cs" `
    -Trainer "v4_crypto_cs" `
    -FeatureSet "v4" `
    -LabelSet "v2" `
    -Task "rank" `
    -RunScope "manual_daily_rank_shadow_scout" `
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
    -SkipPromote `
    -KnownRuntimeUnits $knownRuntimeUnits `
    -OutDir "logs/model_v4_acceptance_rank_shadow" `
    -ReportPrefix "v4_candidate_acceptance_rank_shadow" `
    -ReportTitle "V4 Rank Shadow Acceptance" `
    -LogTag "v4-accept-rank-shadow" `
    @args
exit $LASTEXITCODE
