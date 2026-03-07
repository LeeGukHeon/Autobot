$psExe = if ([System.IO.Path]::DirectorySeparatorChar -eq '\') { "powershell.exe" } else { "pwsh" }
& $psExe -NoProfile -ExecutionPolicy Bypass -File (Join-Path $PSScriptRoot "candidate_acceptance.ps1") `
    -ModelFamily "train_v4_crypto_cs" `
    -Trainer "v4_crypto_cs" `
    -FeatureSet "v4" `
    -LabelSet "v2" `
    -Task "cls" `
    -CandidateModelRef "latest_candidate_v4" `
    -ChampionModelRef "champion_v4" `
    -PaperFeatureProvider "live_v4" `
    -TrainerEvidenceMode "required" `
    -BacktestTopPct 0.5 `
    -BacktestMinProb 0.0 `
    -BacktestMinCandidatesPerTs 1 `
    -KnownRuntimeUnits @("autobot-paper-v4.service", "autobot-live-alpha.service") `
    -OutDir "logs/model_v4_acceptance" `
    -ReportPrefix "v4_candidate_acceptance" `
    -ReportTitle "V4 Candidate Acceptance" `
    -LogTag "v4-accept" `
    @args
exit $LASTEXITCODE
