$psExe = if ([System.IO.Path]::DirectorySeparatorChar -eq '\') { "powershell.exe" } else { "pwsh" }
& $psExe -NoProfile -ExecutionPolicy Bypass -File (Join-Path $PSScriptRoot "candidate_acceptance.ps1") `
    -ModelFamily "train_v3_mtf_micro" `
    -Trainer "v3_mtf_micro" `
    -FeatureSet "v3" `
    -LabelSet "v1" `
    -Task "cls" `
    -CandidateModelRef "latest_candidate_v3" `
    -ChampionModelRef "champion_v3" `
    -PaperFeatureProvider "live_v3" `
    -PromotionPolicy "balanced_pareto" `
    -TrainerEvidenceMode "ignore" `
    -BacktestTopPct 0.10 `
    -BacktestMinProb 0.52 `
    -BacktestMinCandidatesPerTs 3 `
    -BacktestMinOrdersFilled 30 `
    -BacktestMinRealizedPnlQuote 0.0 `
    -BacktestMinPnlDeltaVsChampion 0.0 `
    -PaperMaxFallbackRatio 0.10 `
    -PaperMinOrdersSubmitted 1 `
    -PaperMinTierCount 1 `
    -PaperMinPolicyEvents 0 `
    -KnownRuntimeUnits @("autobot-paper-alpha.service", "autobot-live-alpha.service") `
    -OutDir "logs/model_v3_acceptance" `
    -ReportPrefix "v3_candidate_acceptance" `
    -ReportTitle "V3 Candidate Acceptance" `
    -LogTag "v3-accept" `
    @args
exit $LASTEXITCODE
