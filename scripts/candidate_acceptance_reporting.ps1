$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

function Sync-WindowRampState {
    param($WindowRampValue)
    $script:windowRamp = $WindowRampValue
    $script:certificationStartDate = [string](Get-PropValue -ObjectValue $WindowRampValue -Name "certification_start_date" -DefaultValue "")
    $script:trainEndDate = [string](Get-PropValue -ObjectValue $WindowRampValue -Name "train_end_date" -DefaultValue "")
    $script:trainStartDate = [string](Get-PropValue -ObjectValue $WindowRampValue -Name "train_start_date" -DefaultValue "")
    $report.config.train_lookback_days = [int](Get-PropValue -ObjectValue $WindowRampValue -Name "effective_train_lookback_days" -DefaultValue $TrainLookbackDays)
    $report.config.train_lookback_days_effective = [int](Get-PropValue -ObjectValue $WindowRampValue -Name "effective_train_lookback_days" -DefaultValue $TrainLookbackDays)
    $report.config.train_window_ramp_enabled = [bool](Get-PropValue -ObjectValue $WindowRampValue -Name "enabled" -DefaultValue $false)
    $report.config.train_window_ramp_active = [bool](Get-PropValue -ObjectValue $WindowRampValue -Name "ramp_active" -DefaultValue $false)
    $report.config.train_window_ramp_reason = [string](Get-PropValue -ObjectValue $WindowRampValue -Name "reason" -DefaultValue "")
    $report.config.train_window_ramp_micro_root = [string](Get-PropValue -ObjectValue $WindowRampValue -Name "micro_root" -DefaultValue "")
    $report.config.train_window_ramp_min_markets_per_date = [int](Get-PropValue -ObjectValue $WindowRampValue -Name "min_markets_per_date" -DefaultValue 1)
    $report.config.train_window_ramp_available_contiguous_micro_days = [int](Get-PropValue -ObjectValue $WindowRampValue -Name "available_contiguous_micro_days" -DefaultValue 0)
    $report.config.train_window_ramp_first_available_micro_date = [string](Get-PropValue -ObjectValue $WindowRampValue -Name "first_available_micro_date" -DefaultValue "")
    $report.config.train_window_ramp_last_available_micro_date = [string](Get-PropValue -ObjectValue $WindowRampValue -Name "last_available_micro_date" -DefaultValue "")
    $report.config.train_data_quality_floor_date = [string](Get-PropValue -ObjectValue $WindowRampValue -Name "train_data_quality_floor_date" -DefaultValue "")
    $report.config.train_data_quality_floor_applied = [bool](Get-PropValue -ObjectValue $WindowRampValue -Name "train_data_quality_floor_applied" -DefaultValue $false)
    $report.config.train_start_floor_date = [string](Get-PropValue -ObjectValue $WindowRampValue -Name "train_start_floor_date" -DefaultValue "")
    $report.config.train_start_floor_applied = [bool](Get-PropValue -ObjectValue $WindowRampValue -Name "train_start_floor_applied" -DefaultValue $false)
    $report.windows_by_step = [ordered]@{
        train = [ordered]@{ start = $script:trainStartDate; end = $script:trainEndDate }
        research = [ordered]@{ start = $script:trainStartDate; end = $script:trainEndDate; source = "train_command_window" }
        certification = [ordered]@{ start = $script:certificationStartDate; end = $effectiveBatchDate }
        backtest = [ordered]@{ start = $script:certificationStartDate; end = $effectiveBatchDate; alias_of = "certification" }
        bootstrap = [ordered]@{
            start = [string](Get-PropValue -ObjectValue $WindowRampValue -Name "train_data_quality_floor_date" -DefaultValue "")
            end = $effectiveBatchDate
            source = "bootstrap_latest_inclusive_candidate"
        }
    }
    $report.window_ramp = $WindowRampValue
    $report.steps.window_ramp = $WindowRampValue
}

function Sync-SplitPolicyState {
    $report.split_policy = Build-SplitPolicyDecisionRecord `
        -PolicyId $script:splitPolicyId `
        -LaneMode $script:splitPolicyLaneMode `
        -PromotionEligible $script:splitPolicyPromotionEligible `
        -SelectedBy $script:splitPolicySelectedBy `
        -RequestedHoldoutDays ([int]$BacktestLookbackDays) `
        -SelectedHoldoutDays $script:splitPolicySelectedHoldoutDays `
        -QualityFloorDate $script:bootstrapWindowStart `
        -ReasonCodes @($script:splitPolicyReasonCodes) `
        -StrictTrainability $script:splitPolicyStrictTrainability `
        -BootstrapTrainability $script:splitPolicyBootstrapTrainability `
        -TrainWindowStart $script:trainStartDate `
        -TrainWindowEnd $script:trainEndDate `
        -CertificationWindowStart $script:certificationStartDate `
        -CertificationWindowEnd $effectiveBatchDate `
        -BootstrapWindowStart $script:bootstrapWindowStart `
        -BootstrapWindowEnd $script:bootstrapWindowEnd `
        -HistoricalAnchorCount 0 `
        -ArtifactPath $script:splitPolicyArtifactPath `
        -CandidateRunId ([string](Get-PropValue -ObjectValue $report.candidate -Name "run_id" -DefaultValue "")) `
        -CandidateRunDir ([string](Get-PropValue -ObjectValue $report.candidate -Name "run_dir" -DefaultValue ""))
    $report.split_policy.candidate_holdout_days = @($script:splitPolicyCandidateHoldoutDays)
    $report.split_policy.historical_anchor_min_required = [int]$SplitPolicyMinHistoricalAnchors
    $report.split_policy.historical_anchor_count = [int]$script:splitPolicyHistoricalAnchorCount
    $report.split_policy.selection_summary = @($script:splitPolicySelectionSummary)
    $report.split_policy.history_path = $script:splitPolicyHistoryPath
    $report.split_policy.new_evaluations = @($script:splitPolicyNewEvaluations)
}

function Sync-ReportTopLevelSummary {
    $candidate = Get-PropValue -ObjectValue $report -Name "candidate" -DefaultValue @{}
    $gates = Get-PropValue -ObjectValue $report -Name "gates" -DefaultValue @{}
    $backtestGate = Get-PropValue -ObjectValue $gates -Name "backtest" -DefaultValue @{}
    $runtimeParityGate = Get-PropValue -ObjectValue $gates -Name "runtime_parity" -DefaultValue @{}
    $paperGate = Get-PropValue -ObjectValue $gates -Name "paper" -DefaultValue @{}
    $splitPolicy = Get-PropValue -ObjectValue $report -Name "split_policy" -DefaultValue @{}
    $report.candidate_run_id = [string](Get-PropValue -ObjectValue $candidate -Name "run_id" -DefaultValue "")
    $report.candidate_run_dir = [string](Get-PropValue -ObjectValue $candidate -Name "run_dir" -DefaultValue "")
    $report.champion_before_run_id = [string](Get-PropValue -ObjectValue $candidate -Name "champion_before_run_id" -DefaultValue "")
    $report.champion_after_run_id = [string](Get-PropValue -ObjectValue $candidate -Name "champion_after_run_id" -DefaultValue "")
    $report.lane_mode = [string](Get-PropValue -ObjectValue $splitPolicy -Name "lane_mode" -DefaultValue "")
    $report.promotion_eligible = [bool](Get-PropValue -ObjectValue $splitPolicy -Name "promotion_eligible" -DefaultValue $false)
    $report.overall_pass = Get-PropValue -ObjectValue $gates -Name "overall_pass" -DefaultValue $null
    $report.backtest_pass = Get-PropValue -ObjectValue $backtestGate -Name "pass" -DefaultValue $null
    $report.runtime_parity_pass = Get-PropValue -ObjectValue $runtimeParityGate -Name "pass" -DefaultValue $null
    $report.paper_pass = Get-PropValue -ObjectValue $paperGate -Name "pass" -DefaultValue $null
    $report.completed_at = (Get-Date).ToString("o")
}

function Save-Report {
    Sync-ReportTopLevelSummary
    $stamp = Get-Date -Format "yyyyMMdd-HHmmss"
    $runReportPath = Join-Path $resolvedOutDir ($ReportPrefix + "_" + $stamp + ".json")
    $latestReportPath = Join-Path $resolvedOutDir "latest.json"
    $runMarkdownPath = Join-Path $resolvedOutDir ($ReportPrefix + "_" + $stamp + ".md")
    $latestMarkdownPath = Join-Path $resolvedOutDir "latest.md"
    $reportJson = $report | ConvertTo-Json -Depth 8
    $reportMarkdown = Build-ReportMarkdown -ReportValue $report
    $reportJson | Set-Content -Path $runReportPath -Encoding UTF8
    $reportJson | Set-Content -Path $latestReportPath -Encoding UTF8
    $reportMarkdown | Set-Content -Path $runMarkdownPath -Encoding UTF8
    $reportMarkdown | Set-Content -Path $latestMarkdownPath -Encoding UTF8
    $candidateRunDirValue = [string](Get-PropValue -ObjectValue (Get-PropValue -ObjectValue $report -Name "candidate" -DefaultValue @{}) -Name "run_dir" -DefaultValue "")
    if (-not [string]::IsNullOrWhiteSpace($candidateRunDirValue) -and (Test-Path $candidateRunDirValue)) {
        $candidateReportJsonPath = Join-Path $candidateRunDirValue "acceptance_report.json"
        $candidateReportMdPath = Join-Path $candidateRunDirValue "acceptance_report.md"
        $reportJson | Set-Content -Path $candidateReportJsonPath -Encoding UTF8
        $reportMarkdown | Set-Content -Path $candidateReportMdPath -Encoding UTF8
    }
    return [PSCustomObject]@{
        RunReportPath = $runReportPath
        LatestReportPath = $latestReportPath
        RunMarkdownPath = $runMarkdownPath
        LatestMarkdownPath = $latestMarkdownPath
    }
}

function Write-ReportPointers {
    param(
        [string]$LogTag,
        [object]$Paths,
        [bool]$OverallPass
    )
    Write-Host ("[{0}] overall_pass={1}" -f $LogTag, $OverallPass)
    Write-Host ("[{0}] report={1}" -f $LogTag, $Paths.RunReportPath)
    Write-Host ("[{0}] latest={1}" -f $LogTag, $Paths.LatestReportPath)
}
