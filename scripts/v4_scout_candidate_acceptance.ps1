# Scout acceptance keeps the same evaluation contract but pins the run to the
# non-promotable daily lane so the result is treated as exploration evidence.
$knownRuntimeUnits = @("autobot-paper-v4.service", "autobot-live-alpha.service")
& (Join-Path $PSScriptRoot "candidate_acceptance.ps1") `
    -ModelFamily "train_v4_crypto_cs" `
    -Trainer "v4_crypto_cs" `
    -FeatureSet "v4" `
    -LabelSet "v2" `
    -Task "cls" `
    -RunScope "manual_daily" `
    -TrainStartFloorDate "2026-03-04" `
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
    -KnownRuntimeUnits $knownRuntimeUnits `
    -OutDir "logs/model_v4_acceptance_scout" `
    -ReportPrefix "v4_candidate_acceptance_scout" `
    -ReportTitle "V4 Scout Candidate Acceptance" `
    -LogTag "v4-accept-scout" `
    @args
exit $LASTEXITCODE
