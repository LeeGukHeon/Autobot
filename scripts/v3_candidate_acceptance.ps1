$psExe = if ([System.IO.Path]::DirectorySeparatorChar -eq '\') { "powershell.exe" } else { "pwsh" }
# Acceptance intentionally uses the same fixed compare profile as v4.
# Runtime/live_v3 paper continues to use learned registry thresholds, while acceptance stays
# on a shared apples-to-apples test profile for champion promotion decisions.
& $psExe -NoProfile -ExecutionPolicy Bypass -File (Join-Path $PSScriptRoot "candidate_acceptance.ps1") `
    -ModelFamily "train_v3_mtf_micro" `
    -Trainer "v3_mtf_micro" `
    -FeatureSet "v3" `
    -LabelSet "v1" `
    -Task "cls" `
    -CandidateModelRef "latest_candidate_v3" `
    -ChampionModelRef "champion_v3" `
    -PaperFeatureProvider "live_v3" `
    -PromotionPolicy "paper_final_balanced" `
    -TrainerEvidenceMode "ignore" `
    -BacktestTopPct 0.50 `
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
    -KnownRuntimeUnits @("autobot-paper-alpha.service", "autobot-live-alpha.service") `
    -OutDir "logs/model_v3_acceptance" `
    -ReportPrefix "v3_candidate_acceptance" `
    -ReportTitle "V3 Candidate Acceptance" `
    -LogTag "v3-accept" `
    @args
exit $LASTEXITCODE
