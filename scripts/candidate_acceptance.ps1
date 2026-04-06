param(
    [string]$PythonExe = "",
    [string]$ProjectRoot = "",
    [string]$BatchDate = "",
    [string]$DailyPipelineScript = "",
    [string]$TrainSnapshotCloseReportPath = "data/collect/_meta/train_snapshot_close_latest.json",
    [string]$PaperSmokeScript = "",
    [string]$OutDir = "logs/model_acceptance",
    [int]$TrainLookbackDays = 30,
    [int]$BacktestLookbackDays = 8,
    [bool]$TrainLookbackRampEnabled = $true,
    [string]$TrainLookbackRampMicroRoot = "data/parquet/micro_v1",
    [int]$TrainLookbackRampMinMarketsPerDate = 1,
    [string]$TrainDataQualityFloorDate = "",
    [string]$TrainStartFloorDate = "",
    [switch]$SplitPolicyHistoricalSelectorEnabled,
    [string]$SplitPolicyCandidateHoldoutDays = "",
    [int]$SplitPolicyMinHistoricalAnchors = 2,
    [int]$SplitPolicyMaxNewAnchorEvaluationsPerRun = 1,
    [int]$SplitPolicyHistoryBoosterSweepTrials = 1,
    [string]$SplitPolicyHistoryRunScope = "scheduled_split_policy_history",
    [string]$Tf = "5m",
    [string]$Quote = "KRW",
    [int]$TrainTopN = 50,
    [int]$FeatureParityTopN = 20,
    [int]$BacktestTopN = 20,
    [string]$ModelFamily = "train_v5_fusion",
    [string]$Trainer = "v5_fusion",
    [string[]]$DependencyTrainers = @(),
    [string]$FeatureSet = "v4",
    [string]$LabelSet = "v3",
    [string]$Task = "cls",
    [string]$RunScope = "scheduled_daily",
    [string]$CandidateModelRef = "latest_candidate",
    [string]$ChampionModelRef = "champion",
    [string]$ChampionModelFamily = "",
    [string]$StrategyName = "model_alpha_v1",
    [string]$ReportPrefix = "candidate_acceptance",
    [string]$ReportTitle = "Candidate Acceptance",
    [string]$LogTag = "candidate-accept",
    [int]$BoosterSweepTrials = 10,
    [int]$Seed = 42,
    [int]$NThread = 1,
    [double]$BacktestTopPct = 0.50,
    [double]$BacktestMinProb = 0.0,
    [int]$BacktestMinCandidatesPerTs = 1,
    [int]$HoldBars = 6,
    [bool]$BacktestRuntimeParityEnabled = $true,
    [double]$BacktestMinPayoffRatio = 0.75,
    [double]$BacktestMaxLossConcentration = 0.85,
    [int]$ExecutionStructureMinClosedTrades = 3,
    [int]$PaperSoakDurationSec = 10800,
    [string]$PaperMicroProvider = "live_ws",
    [string]$PaperFeatureProvider = "live_v4",
    [bool]$PaperUseLearnedRuntime = $true,
    [int]$PaperWarmupSec = 60,
    [int]$PaperWarmupMinTradeEventsPerMarket = 1,
    [double]$PaperMaxFallbackRatio = 0.20,
    [int]$PaperMinOrdersSubmitted = 1,
    [int]$PaperMinOrdersFilled = 2,
    [double]$PaperMinRealizedPnlQuote = 0.0,
    [double]$PaperMinMicroQualityScoreMean = 0.25,
    [int]$PaperMinActiveWindows = 1,
    [double]$PaperMinNonnegativeWindowRatio = 0.34,
    [double]$PaperMaxFillConcentrationRatio = 0.85,
    [double]$PaperMinPayoffRatio = 0.75,
    [double]$PaperMaxLossConcentration = 0.85,
    [double]$PaperEvidenceEdgeScore = 0.65,
    [double]$PaperEvidenceHoldScore = 0.50,
    [int]$PaperHistoryWindowRuns = 5,
    [int]$PaperHistoryMinCompletedRuns = 2,
    [double]$PaperHistoryMinNonnegativeRunRatio = 0.40,
    [double]$PaperHistoryMinPositiveRunRatio = 0.20,
    [double]$PaperHistoryMinMedianMicroQualityScore = 0.25,
    [int]$PaperMinTierCount = 1,
    [int]$PaperMinPolicyEvents = 0,
    [int]$BacktestMinOrdersFilled = 30,
    [double]$BacktestMinRealizedPnlQuote = 0.0,
    [double]$BacktestMinDeflatedSharpeRatio = 0.20,
    [double]$BacktestMinPnlDeltaVsChampion = 0.0,
    [ValidateSet("strict", "balanced_pareto", "conservative_pareto", "paper_final_balanced")]
    [string]$PromotionPolicy = "balanced_pareto",
    [ValidateSet("ignore", "informational", "required")]
    [string]$TrainerEvidenceMode = "ignore",
    [bool]$BacktestAllowStabilityOverride = $true,
    [double]$BacktestChampionPnlTolerancePct = 0.05,
    [double]$BacktestChampionMinDrawdownImprovementPct = 0.10,
    [double]$BacktestChampionMaxFillRateDegradation = 0.02,
    [double]$BacktestChampionMaxSlippageDeteriorationBps = 2.5,
    [double]$BacktestChampionMinUtilityEdgePct = 0.0,
    [string]$OverlayCalibrationArtifactPath = "logs/operational_overlay/latest.json",
    [int]$OverlayCalibrationWindowRuns = 20,
    [int]$OverlayCalibrationMinReports = 5,
    [string[]]$RestartUnits = @(),
    [string[]]$KnownRuntimeUnits = @("autobot-paper-v5.service", "autobot-live-alpha.service"),
    [bool]$AutoRestartKnownUnits = $true,
    [switch]$EnableVariantMatrixSelection,
    [switch]$SkipDailyPipeline,
    [switch]$SkipPaperSoak,
    [switch]$SkipReportRefresh,
    [switch]$SkipPromote,
    [switch]$SkipChampionCompare,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
$script:IsWindowsPlatform = [System.IO.Path]::DirectorySeparatorChar -eq '\'
. (Join-Path $PSScriptRoot "v4_candidate_state_helpers.ps1")

function Resolve-DefaultProjectRoot {
    return (Split-Path -Path $PSScriptRoot -Parent)
}

function Resolve-DefaultPythonExe {
    param([string]$Root)
    if ($script:IsWindowsPlatform) {
        return "C:\Python314\python.exe"
    }
    return (Join-Path $Root ".venv/bin/python")
}

function Resolve-DefaultDailyPipelineScript {
    param([string]$Root)
    if ($script:IsWindowsPlatform) {
        return (Join-Path $Root "scripts/daily_micro_pipeline.ps1")
    }
    return (Join-Path $Root "scripts/daily_micro_pipeline_for_server.ps1")
}

function Resolve-DefaultTrainSnapshotCloseReportPath {
    param([string]$Root)
    return (Join-Path $Root "data/collect/_meta/train_snapshot_close_latest.json")
}

function Resolve-DateToken {
    param([string]$DateText, [string]$LabelForError)
    if ([string]::IsNullOrWhiteSpace($DateText)) {
        throw "$LabelForError is empty"
    }
    try {
        $parsed = [DateTime]::ParseExact(
            $DateText,
            "yyyy-MM-dd",
            [System.Globalization.CultureInfo]::InvariantCulture,
            [System.Globalization.DateTimeStyles]::None
        )
        return $parsed.ToString("yyyy-MM-dd")
    } catch {
        throw "$LabelForError must be yyyy-MM-dd (actual='$DateText')"
    }
}

function Resolve-PathFromProjectRoot {
    param(
        [string]$Root,
        [string]$PathValue
    )
    if ([string]::IsNullOrWhiteSpace($PathValue)) {
        return ""
    }
    if ([System.IO.Path]::IsPathRooted($PathValue)) {
        return [System.IO.Path]::GetFullPath($PathValue)
    }
    return [System.IO.Path]::GetFullPath((Join-Path $Root $PathValue))
}

function Get-DataPlatformReadySnapshotId {
    param([string]$ProjectRoot)
    $pointerPath = Join-Path $ProjectRoot "data/_meta/data_platform_ready_snapshot.json"
    $payload = Load-JsonOrEmpty -PathValue $pointerPath
    $snapshotId = [string](Get-PropValue -ObjectValue $payload -Name "snapshot_id" -DefaultValue "")
    return $snapshotId.Trim()
}

function Resolve-DependencyTrainerModelFamily {
    param([string]$TrainerName)
    $normalized = [string]$TrainerName
    $normalized = $normalized.Trim().ToLowerInvariant()
    switch ($normalized) {
        "v5_panel_ensemble" { return "train_v5_panel_ensemble" }
        "v5_sequence" { return "train_v5_sequence" }
        "v5_lob" { return "train_v5_lob" }
        "v5_tradability" { return "train_v5_tradability" }
        "v5_fusion" { return "train_v5_fusion" }
        default { return "" }
    }
}

function Resolve-JsonObjectFromText {
    param([string]$TextValue)
    if ([string]::IsNullOrWhiteSpace($TextValue)) {
        return @{}
    }
    $trimmed = $TextValue.Trim()
    if ($trimmed.StartsWith("{") -and $trimmed.EndsWith("}")) {
        try {
            return ($trimmed | ConvertFrom-Json)
        } catch {
        }
    }
    $jsonMatch = [Regex]::Match($TextValue, '(?ms)\{.*\}')
    if ($jsonMatch.Success) {
        try {
            return ($jsonMatch.Value | ConvertFrom-Json)
        } catch {
        }
    }
    return @{}
}

function Get-VariantReportFilenameForTrainer {
    param([string]$TrainerName)
    switch (([string]$TrainerName).Trim().ToLowerInvariant()) {
        "v5_sequence" { return "sequence_variant_report.json" }
        "v5_lob" { return "lob_variant_report.json" }
        "v5_fusion" { return "fusion_variant_report.json" }
        default { return "" }
    }
}

function Resolve-RunVariantMetadata {
    param(
        [string]$RunDir,
        [string]$TrainerName
    )
    $result = [ordered]@{
        trainer = [string]$TrainerName
        run_dir = [string]$RunDir
        chosen_variant_name = ""
        variant_report_path = ""
        evaluated_variant_count = 0
        chosen_reason_code = ""
        baseline_kept_reason_code = ""
        sequence_variant_name = ""
        lob_variant_name = ""
        fusion_variant_name = ""
        fusion_stacker_family = ""
        fusion_gating_policy = ""
        source_mode = ""
    }
    if ([string]::IsNullOrWhiteSpace($RunDir) -or (-not (Test-Path $RunDir))) {
        return $result
    }
    $trainConfig = Load-JsonOrEmpty -PathValue (Join-Path $RunDir "train_config.yaml")
    $runtimeRecommendations = Load-JsonOrEmpty -PathValue (Join-Path $RunDir "runtime_recommendations.json")
    $variantReportFilename = Get-VariantReportFilenameForTrainer -TrainerName $TrainerName
    $variantReportPath = ""
    switch (([string]$TrainerName).Trim().ToLowerInvariant()) {
        "v5_sequence" {
            $result.chosen_variant_name = [string](Get-PropValue -ObjectValue $runtimeRecommendations -Name "sequence_variant_name" -DefaultValue (Get-PropValue -ObjectValue $trainConfig -Name "sequence_variant_name" -DefaultValue ""))
            $variantReportPath = [string](Get-PropValue -ObjectValue $runtimeRecommendations -Name "sequence_variant_report_path" -DefaultValue (Get-PropValue -ObjectValue $trainConfig -Name "sequence_variant_report_path" -DefaultValue ""))
            $result.sequence_variant_name = $result.chosen_variant_name
        }
        "v5_lob" {
            $result.chosen_variant_name = [string](Get-PropValue -ObjectValue $runtimeRecommendations -Name "lob_variant_name" -DefaultValue (Get-PropValue -ObjectValue $trainConfig -Name "lob_variant_name" -DefaultValue ""))
            $variantReportPath = [string](Get-PropValue -ObjectValue $runtimeRecommendations -Name "lob_variant_report_path" -DefaultValue (Get-PropValue -ObjectValue $trainConfig -Name "lob_variant_report_path" -DefaultValue ""))
            $result.lob_variant_name = $result.chosen_variant_name
        }
        "v5_fusion" {
            $result.chosen_variant_name = [string](Get-PropValue -ObjectValue $runtimeRecommendations -Name "fusion_variant_name" -DefaultValue (Get-PropValue -ObjectValue $trainConfig -Name "fusion_variant_name" -DefaultValue (Get-PropValue -ObjectValue $runtimeRecommendations -Name "fusion_stacker_family" -DefaultValue "")))
            $variantReportPath = [string](Get-PropValue -ObjectValue $runtimeRecommendations -Name "fusion_variant_report_path" -DefaultValue (Get-PropValue -ObjectValue $trainConfig -Name "fusion_variant_report_path" -DefaultValue ""))
            $result.sequence_variant_name = [string](Get-PropValue -ObjectValue $runtimeRecommendations -Name "sequence_variant_name" -DefaultValue (Get-PropValue -ObjectValue $trainConfig -Name "sequence_variant_name" -DefaultValue ""))
            $result.lob_variant_name = [string](Get-PropValue -ObjectValue $runtimeRecommendations -Name "lob_variant_name" -DefaultValue (Get-PropValue -ObjectValue $trainConfig -Name "lob_variant_name" -DefaultValue ""))
            $result.fusion_variant_name = $result.chosen_variant_name
            $result.fusion_stacker_family = [string](Get-PropValue -ObjectValue $runtimeRecommendations -Name "fusion_stacker_family" -DefaultValue "")
            $result.fusion_gating_policy = [string](Get-PropValue -ObjectValue $runtimeRecommendations -Name "fusion_gating_policy" -DefaultValue "")
        }
    }
    if ([string]::IsNullOrWhiteSpace($variantReportPath) -and (-not [string]::IsNullOrWhiteSpace($variantReportFilename))) {
        $candidatePath = Join-Path $RunDir $variantReportFilename
        if (Test-Path $candidatePath) {
            $variantReportPath = $candidatePath
        }
    }
    $result.variant_report_path = $variantReportPath
    if (-not [string]::IsNullOrWhiteSpace($variantReportPath) -and (Test-Path $variantReportPath)) {
        $variantReport = Load-JsonOrEmpty -PathValue $variantReportPath
        $result.evaluated_variant_count = [int](To-Int64 (Get-PropValue -ObjectValue $variantReport -Name "evaluated_variant_count" -DefaultValue 0) 0)
        $result.chosen_reason_code = [string](Get-PropValue -ObjectValue $variantReport -Name "chosen_reason_code" -DefaultValue "")
        $result.baseline_kept_reason_code = [string](Get-PropValue -ObjectValue $variantReport -Name "baseline_kept_reason_code" -DefaultValue "")
        if ([string]::IsNullOrWhiteSpace($result.chosen_variant_name)) {
            $result.chosen_variant_name = [string](Get-PropValue -ObjectValue $variantReport -Name "chosen_variant_name" -DefaultValue "")
        }
    }
    return $result
}

function Test-VariantSelectionStep {
    param(
        [string]$TrainerName,
        [Parameter(Mandatory = $false)]$ResultObject,
        [string]$FailureCode
    )
    $step = [ordered]@{
        attempted = $true
        artifact_path = ""
        evaluated_variant_count = 0
        chosen_variant_name = ""
        chosen_reason_code = ""
        baseline_kept_reason_code = ""
        pass = $false
        reasons = @()
    }
    $variantMetadata = Get-PropValue -ObjectValue $ResultObject -Name "variant_metadata" -DefaultValue @{}
    $runDir = [string](Get-PropValue -ObjectValue $ResultObject -Name "run_dir" -DefaultValue "")
    $step.artifact_path = [string](Get-PropValue -ObjectValue $variantMetadata -Name "variant_report_path" -DefaultValue "")
    $step.evaluated_variant_count = [int](To-Int64 (Get-PropValue -ObjectValue $variantMetadata -Name "evaluated_variant_count" -DefaultValue 0) 0)
    $step.chosen_variant_name = [string](Get-PropValue -ObjectValue $variantMetadata -Name "chosen_variant_name" -DefaultValue "")
    $step.chosen_reason_code = [string](Get-PropValue -ObjectValue $variantMetadata -Name "chosen_reason_code" -DefaultValue "")
    $step.baseline_kept_reason_code = [string](Get-PropValue -ObjectValue $variantMetadata -Name "baseline_kept_reason_code" -DefaultValue "")
    if ([string]::IsNullOrWhiteSpace($runDir)) {
        $step.reasons = @($FailureCode, "RUN_DIR_MISSING")
        return $step
    }
    if ([string]::IsNullOrWhiteSpace($step.artifact_path) -or (-not (Test-Path $step.artifact_path))) {
        $step.reasons = @($FailureCode, "VARIANT_REPORT_MISSING")
        return $step
    }
    if ([string]::IsNullOrWhiteSpace($step.chosen_variant_name)) {
        $step.reasons = @($FailureCode, "CHOSEN_VARIANT_MISSING")
        return $step
    }
    $trainConfig = Load-JsonOrEmpty -PathValue (Join-Path $runDir "train_config.yaml")
    $runtimeRecommendations = Load-JsonOrEmpty -PathValue (Join-Path $runDir "runtime_recommendations.json")
    $trainVariantName = switch (([string]$TrainerName).Trim().ToLowerInvariant()) {
        "v5_sequence" { [string](Get-PropValue -ObjectValue $trainConfig -Name "sequence_variant_name" -DefaultValue (Get-PropValue -ObjectValue $runtimeRecommendations -Name "sequence_variant_name" -DefaultValue "")) }
        "v5_lob" { [string](Get-PropValue -ObjectValue $trainConfig -Name "lob_variant_name" -DefaultValue (Get-PropValue -ObjectValue $runtimeRecommendations -Name "lob_variant_name" -DefaultValue "")) }
        "v5_fusion" { [string](Get-PropValue -ObjectValue $trainConfig -Name "fusion_variant_name" -DefaultValue (Get-PropValue -ObjectValue $runtimeRecommendations -Name "fusion_variant_name" -DefaultValue "")) }
        default { "" }
    }
    if ([string]::IsNullOrWhiteSpace($trainVariantName) -or ($trainVariantName -ne $step.chosen_variant_name)) {
        $step.reasons = @($FailureCode, "CHOSEN_VARIANT_METADATA_MISMATCH")
        return $step
    }
    if (([string]$TrainerName).Trim().ToLowerInvariant() -eq "v5_fusion") {
        $fusionStacker = [string](Get-PropValue -ObjectValue $runtimeRecommendations -Name "fusion_stacker_family" -DefaultValue "")
        if ([string]::IsNullOrWhiteSpace($fusionStacker) -or ($fusionStacker -ne $step.chosen_variant_name)) {
            $step.reasons = @("FUSION_VARIANT_PROVENANCE_MISMATCH")
            return $step
        }
    }
    $step.pass = $true
    $step.reasons = @()
    return $step
}

function Resolve-DependencyTradabilityInputPaths {
    param(
        [Parameter(Mandatory = $true)]
        [object[]]$DependencyResults
    )
    $required = [ordered]@{
        v5_panel_ensemble = "tradability_panel_input"
        v5_sequence = "tradability_sequence_input"
        v5_lob = "tradability_lob_input"
    }
    $resolved = [ordered]@{}
    foreach ($trainerName in $required.Keys) {
        $match = $null
        foreach ($item in @($DependencyResults)) {
            $candidateTrainer = [string](Get-PropValue -ObjectValue $item -Name "trainer" -DefaultValue "")
            if ($candidateTrainer -eq $trainerName) {
                $match = $item
                break
            }
        }
        if ($null -eq $match) {
            throw ("missing dependency trainer result: " + $trainerName)
        }
        $runDir = [string](Get-PropValue -ObjectValue $match -Name "run_dir" -DefaultValue "")
        if ([string]::IsNullOrWhiteSpace($runDir)) {
            throw ("dependency trainer run_dir missing: " + $trainerName)
        }
        $expertTablePath = Join-Path $runDir "expert_prediction_table.parquet"
        if ((-not $DryRun) -and (-not (Test-Path $expertTablePath))) {
            throw ("dependency expert_prediction_table missing: " + $expertTablePath)
        }
        $resolved[$required[$trainerName]] = $expertTablePath
    }
    return $resolved
}

function Resolve-LatestPairedPaperArtifact {
    param([string]$ProjectRoot)
    $default = [ordered]@{
        exists = $false
        latest_path = ""
        report_path = ""
        report = @{}
    }
    if ([string]::IsNullOrWhiteSpace($ProjectRoot)) {
        return $default
    }
    $latestPath = Join-Path $ProjectRoot "logs/paired_paper/latest.json"
    if (-not (Test-Path $latestPath)) {
        return $default
    }
    $latestPayload = Load-JsonOrEmpty -PathValue $latestPath
    $reportPath = [string](Get-PropValue -ObjectValue $latestPayload -Name "report_path" -DefaultValue "")
    $reportPayload = if (-not [string]::IsNullOrWhiteSpace($reportPath) -and (Test-Path $reportPath)) {
        Load-JsonOrEmpty -PathValue $reportPath
    } else {
        @{}
    }
    return [ordered]@{
        exists = $true
        latest_path = $latestPath
        report_path = $reportPath
        report = $reportPayload
    }
}

function Resolve-LatestCanaryConfidenceArtifact {
    param([string]$ProjectRoot)
    $default = [ordered]@{
        exists = $false
        path = ""
        payload = @{}
    }
    if ([string]::IsNullOrWhiteSpace($ProjectRoot)) {
        return $default
    }
    foreach ($slug in @("autobot_live_alpha_canary_service", "autobot_live_alpha_candidate_service", "canary")) {
        $candidatePath = Join-Path $ProjectRoot ("logs/canary_confidence_sequence/" + $slug + "/latest.json")
        if (Test-Path $candidatePath) {
            return [ordered]@{
                exists = $true
                path = $candidatePath
                payload = (Load-JsonOrEmpty -PathValue $candidatePath)
            }
        }
    }
    return $default
}

function Update-FusionVariantEvidenceArtifact {
    param(
        [string]$VariantReportPath,
        [string]$FusionVariantName,
        [string]$ReasonCode,
        [bool]$CandidateDefaultEligible,
        [Parameter(Mandatory = $false)]$BacktestGate = @{},
        [Parameter(Mandatory = $false)]$PaperGate = @{},
        [Parameter(Mandatory = $false)]$PairedArtifact = @{},
        [Parameter(Mandatory = $false)]$CanaryArtifact = @{}
    )
    if ([string]::IsNullOrWhiteSpace($VariantReportPath) -or (-not (Test-Path $VariantReportPath))) {
        return @{}
    }
    $payload = Load-JsonOrEmpty -PathValue $VariantReportPath
    $evaluatedVariants = @((Get-PropValue -ObjectValue $payload -Name "evaluated_variants" -DefaultValue @()))
    $selectedVariantName = [string](Get-PropValue -ObjectValue $payload -Name "chosen_variant_name" -DefaultValue "")
    $baselineVariant = $null
    $selectedVariant = $null
    foreach ($item in @($evaluatedVariants)) {
        $variantName = [string](Get-PropValue -ObjectValue $item -Name "variant_name" -DefaultValue "")
        if ($variantName -eq "linear") {
            $baselineVariant = $item
        }
        if ($variantName -eq $selectedVariantName) {
            $selectedVariant = $item
        }
    }
    $baselineUtility = To-Double (Get-PropValue -ObjectValue (Get-PropValue -ObjectValue $baselineVariant -Name "utility_summary" -DefaultValue @{}) -Name "test_ev_net_top5" -DefaultValue 0.0) 0.0
    $selectedUtility = To-Double (Get-PropValue -ObjectValue (Get-PropValue -ObjectValue $selectedVariant -Name "utility_summary" -DefaultValue @{}) -Name "test_ev_net_top5" -DefaultValue 0.0) 0.0
    $utilityEdge = $selectedUtility - $baselineUtility
    $executionNonRegression = if ([string]::Equals($FusionVariantName, "regime_moe", [System.StringComparison]::OrdinalIgnoreCase)) {
        $candidateExecutionStructureEvaluated = To-Bool (Get-PropValue -ObjectValue $BacktestGate -Name "candidate_execution_structure_evaluated" -DefaultValue $false) $false
        $backtestGatePass = To-Bool (Get-PropValue -ObjectValue $BacktestGate -Name "pass" -DefaultValue $false) $false
        ($candidateExecutionStructureEvaluated -and $backtestGatePass)
    } else {
        $true
    }
    $paperNonRegression = if (Test-IsEffectivelyEmptyObject -ObjectValue $PaperGate) {
        $null
    } else {
        To-Bool (Get-PropValue -ObjectValue $PaperGate -Name "pass" -DefaultValue $false) $false
    }
    $pairedReport = Get-PropValue -ObjectValue $PairedArtifact -Name "report" -DefaultValue @{}
    $pairedDeltas = Get-PropValue -ObjectValue $pairedReport -Name "paired_deltas" -DefaultValue @{}
    $pairedNonRegression = if (Test-IsEffectivelyEmptyObject -ObjectValue $pairedReport) {
        $null
    } else {
        ([int](To-Int64 (Get-PropValue -ObjectValue $pairedDeltas -Name "matched_no_trade_delta" -DefaultValue 0) 0) -ge -1) -and
        ([double](To-Double (Get-PropValue -ObjectValue $pairedDeltas -Name "matched_slippage_delta_bps" -DefaultValue 0.0) 0.0) -le 2.5)
    }
    $canaryPayload = Get-PropValue -ObjectValue $CanaryArtifact -Name "payload" -DefaultValue @{}
    $canaryDecision = Get-PropValue -ObjectValue (Get-PropValue -ObjectValue $canaryPayload -Name "decision" -DefaultValue @{}) -Name "status" -DefaultValue ""
    $canaryNonRegression = if (Test-IsEffectivelyEmptyObject -ObjectValue $canaryPayload) {
        $null
    } else {
        (-not [string]::Equals([string]$canaryDecision, "abort", [System.StringComparison]::OrdinalIgnoreCase))
    }
    $selectionEvidence = [ordered]@{
        utility_edge_vs_linear = [double]$utilityEdge
        execution_structure_non_regression = $executionNonRegression
        paper_non_regression = $paperNonRegression
        paired_non_regression = $pairedNonRegression
        canary_non_regression = $canaryNonRegression
        promotion_safe = [bool]$CandidateDefaultEligible
    }
    $defaultEligibleVariantName = if ($CandidateDefaultEligible) { $FusionVariantName } else { "linear" }
    $fusionEvidenceWinner = if ($CandidateDefaultEligible) { $FusionVariantName } else { "linear" }
    if ($payload -is [System.Collections.IDictionary]) {
        $payload["selection_evidence"] = $selectionEvidence
        $payload["offline_winner_variant_name"] = $selectedVariantName
        $payload["default_eligible_variant_name"] = $defaultEligibleVariantName
        $payload["default_eligible"] = [bool]$CandidateDefaultEligible
        $payload["fusion_candidate_default_eligible"] = [bool]$CandidateDefaultEligible
        $payload["fusion_evidence_winner"] = $fusionEvidenceWinner
        $payload["fusion_evidence_reason_code"] = [string]$ReasonCode
    } else {
        $payload | Add-Member -NotePropertyName "selection_evidence" -NotePropertyValue $selectionEvidence -Force
        $payload | Add-Member -NotePropertyName "offline_winner_variant_name" -NotePropertyValue $selectedVariantName -Force
        $payload | Add-Member -NotePropertyName "default_eligible_variant_name" -NotePropertyValue $defaultEligibleVariantName -Force
        $payload | Add-Member -NotePropertyName "default_eligible" -NotePropertyValue ([bool]$CandidateDefaultEligible) -Force
        $payload | Add-Member -NotePropertyName "fusion_candidate_default_eligible" -NotePropertyValue ([bool]$CandidateDefaultEligible) -Force
        $payload | Add-Member -NotePropertyName "fusion_evidence_winner" -NotePropertyValue $fusionEvidenceWinner -Force
        $payload | Add-Member -NotePropertyName "fusion_evidence_reason_code" -NotePropertyValue ([string]$ReasonCode) -Force
    }
    Write-JsonFile -PathValue $VariantReportPath -Payload $payload
    return $payload
}

function Resolve-DateTimeOffsetOrNull {
    param([string]$Value)
    if ([string]::IsNullOrWhiteSpace($Value)) {
        return $null
    }
    try {
        return [DateTimeOffset]::Parse($Value, [System.Globalization.CultureInfo]::InvariantCulture)
    } catch {
        return $null
    }
}

function Resolve-KstTimeZoneInfo {
    foreach ($timeZoneId in @("Asia/Seoul", "Korea Standard Time")) {
        try {
            return [System.TimeZoneInfo]::FindSystemTimeZoneById($timeZoneId)
        } catch {
        }
    }
    throw "unable to resolve Asia/Seoul timezone info"
}

function Convert-DateTokenToUnixMs {
    param(
        [string]$DateText,
        [switch]$EndOfDay
    )
    if ([string]::IsNullOrWhiteSpace($DateText)) {
        return [int64]0
    }
    $normalized = Resolve-DateToken -DateText $DateText -LabelForError "date_token"
    $parsedDate = [DateTime]::ParseExact(
        $normalized,
        "yyyy-MM-dd",
        [System.Globalization.CultureInfo]::InvariantCulture,
        [System.Globalization.DateTimeStyles]::None
    )
    $localDateTime = if ($EndOfDay) {
        $parsedDate.Date.AddDays(1).AddMilliseconds(-1)
    } else {
        $parsedDate.Date
    }
    $localDateTime = [DateTime]::SpecifyKind($localDateTime, [System.DateTimeKind]::Unspecified)
    $kstTimeZone = Resolve-KstTimeZoneInfo
    $offset = $kstTimeZone.GetUtcOffset($localDateTime)
    $dateTimeOffset = [DateTimeOffset]::new($localDateTime, $offset)
    return [int64]$dateTimeOffset.ToUnixTimeMilliseconds()
}

function Convert-UnixMsToOperatingDateToken {
    param([int64]$UnixMs)
    if ($UnixMs -le 0) {
        return ""
    }
    $utcDateTime = [DateTimeOffset]::FromUnixTimeMilliseconds([int64]$UnixMs)
    $kstTimeZone = Resolve-KstTimeZoneInfo
    return [System.TimeZoneInfo]::ConvertTime($utcDateTime, $kstTimeZone).ToString("yyyy-MM-dd")
}

function Get-DateTokenRangeInclusive {
    param(
        [string]$StartDate,
        [string]$EndDate
    )
    if ([string]::IsNullOrWhiteSpace($StartDate) -or [string]::IsNullOrWhiteSpace($EndDate)) {
        return @()
    }
    $startValue = Resolve-DateToken -DateText $StartDate -LabelForError "range_start"
    $endValue = Resolve-DateToken -DateText $EndDate -LabelForError "range_end"
    $startObj = [DateTime]::ParseExact($startValue, "yyyy-MM-dd", [System.Globalization.CultureInfo]::InvariantCulture)
    $endObj = [DateTime]::ParseExact($endValue, "yyyy-MM-dd", [System.Globalization.CultureInfo]::InvariantCulture)
    if ($endObj -lt $startObj) {
        return @()
    }
    $values = New-Object System.Collections.Generic.List[string]
    $cursor = $startObj
    while ($cursor -le $endObj) {
        $values.Add($cursor.ToString("yyyy-MM-dd")) | Out-Null
        $cursor = $cursor.AddDays(1)
    }
    return @($values.ToArray())
}

function Resolve-OperatingDateCoverageFields {
    param(
        [Parameter(Mandatory = $true)]
        $Payload
    )
    $coverageStartDate = [string](Get-PropValue -ObjectValue $Payload -Name "coverage_start_date" -DefaultValue "")
    $coverageEndDate = [string](Get-PropValue -ObjectValue $Payload -Name "coverage_end_date" -DefaultValue "")
    $coverageDates = @(
        @(Get-PropValue -ObjectValue $Payload -Name "coverage_dates" -DefaultValue @()) |
            ForEach-Object { [string]$_ } |
            Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    )
    $windowTimezone = [string](Get-PropValue -ObjectValue $Payload -Name "window_timezone" -DefaultValue "")
    $coverageStartTsMs = [int64](Get-PropValue -ObjectValue $Payload -Name "coverage_start_ts_ms" -DefaultValue 0)
    $coverageEndTsMs = [int64](Get-PropValue -ObjectValue $Payload -Name "coverage_end_ts_ms" -DefaultValue 0)
    if ([string]::IsNullOrWhiteSpace($coverageStartDate) -and ($coverageStartTsMs -gt 0)) {
        $coverageStartDate = Convert-UnixMsToOperatingDateToken -UnixMs $coverageStartTsMs
    }
    if ([string]::IsNullOrWhiteSpace($coverageEndDate) -and ($coverageEndTsMs -gt 0)) {
        $coverageEndDate = Convert-UnixMsToOperatingDateToken -UnixMs $coverageEndTsMs
    }
    if (@($coverageDates).Count -eq 0 -and (-not [string]::IsNullOrWhiteSpace($coverageStartDate)) -and (-not [string]::IsNullOrWhiteSpace($coverageEndDate))) {
        $coverageDates = @(Get-DateTokenRangeInclusive -StartDate $coverageStartDate -EndDate $coverageEndDate)
    }
    if ([string]::IsNullOrWhiteSpace($windowTimezone)) {
        $windowTimezone = "Asia/Seoul"
    }
    return [ordered]@{
        coverage_start_date = $coverageStartDate
        coverage_end_date = $coverageEndDate
        coverage_dates = @($coverageDates)
        window_timezone = $windowTimezone
    }
}

function Invoke-DependencyTrainerChain {
    param(
        [string]$PythonPath,
        [string]$TrainStartDate,
        [string]$TrainEndDate,
        [string]$ExecutionEvalStartDate,
        [string]$ExecutionEvalEndDate
    )
    $results = @()
    foreach ($trainerNameRaw in @($DependencyTrainers)) {
        $trainerName = [string]$trainerNameRaw
        $trainerName = $trainerName.Trim()
        if ([string]::IsNullOrWhiteSpace($trainerName)) {
            continue
        }
        $useVariantMatrix = $EnableVariantMatrixSelection.IsPresent -and ($trainerName -in @("v5_sequence", "v5_lob"))
        $dependencyModelFamily = Resolve-DependencyTrainerModelFamily -TrainerName $trainerName
        if ([string]::IsNullOrWhiteSpace($dependencyModelFamily)) {
            throw ("unsupported dependency trainer: " + $trainerName)
        }
        $dependencyRunScope = ([string]$RunScope).Trim() + "_dependency_" + $trainerName
        $reusableDependencyRun = Resolve-ReusableDependencyTrainerRun `
            -RegistryRoot $resolvedRegistryRoot `
            -TrainerName $trainerName `
            -ModelFamily $dependencyModelFamily `
            -TrainStartDate $TrainStartDate `
            -TrainEndDate $TrainEndDate `
            -ExecutionEvalStartDate $ExecutionEvalStartDate `
            -ExecutionEvalEndDate $ExecutionEvalEndDate `
            -ExpectedSnapshotId $script:dataPlatformReadySnapshotId
        if ($useVariantMatrix -and [bool](Get-PropValue -ObjectValue $reusableDependencyRun -Name "reusable" -DefaultValue $false)) {
            $reusableVariantMetadata = Resolve-RunVariantMetadata `
                -RunDir ([string](Get-PropValue -ObjectValue $reusableDependencyRun -Name "run_dir" -DefaultValue "")) `
                -TrainerName $trainerName
            if (
                [string]::IsNullOrWhiteSpace([string](Get-PropValue -ObjectValue $reusableVariantMetadata -Name "chosen_variant_name" -DefaultValue "")) -or
                [string]::IsNullOrWhiteSpace([string](Get-PropValue -ObjectValue $reusableVariantMetadata -Name "variant_report_path" -DefaultValue "")) -or
                (-not (Test-Path ([string](Get-PropValue -ObjectValue $reusableVariantMetadata -Name "variant_report_path" -DefaultValue ""))))
            ) {
                $reusableDependencyRun.reusable = $false
                $reusableDependencyRun.reason = "VARIANT_SELECTION_ARTIFACT_MISSING"
            } else {
                $reusableDependencyRun | Add-Member -NotePropertyName variant_metadata -NotePropertyValue $reusableVariantMetadata -Force
            }
        }
        if ([bool](Get-PropValue -ObjectValue $reusableDependencyRun -Name "reusable" -DefaultValue $false)) {
            $results += [ordered]@{
                trainer = $trainerName
                model_family = $dependencyModelFamily
                run_scope = $dependencyRunScope
                exit_code = 0
                command = ""
                output_preview = "REUSED_EXISTING_RUN"
                run_dir = [string](Get-PropValue -ObjectValue $reusableDependencyRun -Name "run_dir" -DefaultValue "")
                run_id = [string](Get-PropValue -ObjectValue $reusableDependencyRun -Name "run_id" -DefaultValue "")
                data_platform_ready_snapshot_id = [string](Get-PropValue -ObjectValue $reusableDependencyRun -Name "data_platform_ready_snapshot_id" -DefaultValue "")
                reused = $true
                source_mode = "existing_run"
                reuse_reason = [string](Get-PropValue -ObjectValue $reusableDependencyRun -Name "reason" -DefaultValue "")
                required_artifacts_complete = [bool](Get-PropValue -ObjectValue $reusableDependencyRun -Name "required_artifacts_complete" -DefaultValue $false)
                tail_mode = [string](Get-PropValue -ObjectValue $reusableDependencyRun -Name "tail_mode" -DefaultValue "")
                variant_matrix = $useVariantMatrix
                variant_metadata = (Get-PropValue -ObjectValue $reusableDependencyRun -Name "variant_metadata" -DefaultValue @{})
                chosen_variant_name = [string](Get-PropValue -ObjectValue (Get-PropValue -ObjectValue $reusableDependencyRun -Name "variant_metadata" -DefaultValue @{}) -Name "chosen_variant_name" -DefaultValue "")
                variant_report_path = [string](Get-PropValue -ObjectValue (Get-PropValue -ObjectValue $reusableDependencyRun -Name "variant_metadata" -DefaultValue @{}) -Name "variant_report_path" -DefaultValue "")
                evaluated_variant_count = [int](To-Int64 (Get-PropValue -ObjectValue (Get-PropValue -ObjectValue $reusableDependencyRun -Name "variant_metadata" -DefaultValue @{}) -Name "evaluated_variant_count" -DefaultValue 0) 0)
                baseline_kept_reason_code = [string](Get-PropValue -ObjectValue (Get-PropValue -ObjectValue $reusableDependencyRun -Name "variant_metadata" -DefaultValue @{}) -Name "baseline_kept_reason_code" -DefaultValue "")
                chosen_reason_code = [string](Get-PropValue -ObjectValue (Get-PropValue -ObjectValue $reusableDependencyRun -Name "variant_metadata" -DefaultValue @{}) -Name "chosen_reason_code" -DefaultValue "")
            }
            continue
        }
        $trainCommand = if ($useVariantMatrix) { "train-variant-matrix" } else { "train" }
        $trainArgs = @(
            "-m", "autobot.cli",
            "model", $trainCommand,
            "--trainer", $trainerName,
            "--model-family", $dependencyModelFamily,
            "--feature-set", $FeatureSet,
            "--label-set", $LabelSet,
            "--task", $Task,
            "--run-scope", $dependencyRunScope,
            "--tf", $Tf,
            "--quote", $Quote,
            "--top-n", $TrainTopN,
            "--start", $TrainStartDate,
            "--end", $TrainEndDate,
            "--booster-sweep-trials", $BoosterSweepTrials,
            "--seed", $Seed,
            "--nthread", $NThread
        )
        if ((-not [string]::IsNullOrWhiteSpace($ExecutionEvalStartDate)) -and (-not [string]::IsNullOrWhiteSpace($ExecutionEvalEndDate))) {
            $trainArgs += @(
                "--execution-eval-start", $ExecutionEvalStartDate,
                "--execution-eval-end", $ExecutionEvalEndDate
            )
        }
        if ([string]::Equals($trainerName, "v5_tradability", [System.StringComparison]::OrdinalIgnoreCase) -and @($results).Count -gt 0) {
            $tradabilityDependencyInputs = Resolve-DependencyTradabilityInputPaths -DependencyResults @($results)
            foreach ($entry in $tradabilityDependencyInputs.GetEnumerator()) {
                $argName = "--" + ([string]$entry.Key).Replace("_", "-")
                $trainArgs += @($argName, [string]$entry.Value)
            }
        }
        if ([string]::Equals($trainerName, "v5_panel_ensemble", [System.StringComparison]::OrdinalIgnoreCase)) {
            $trainArgs += "--dependency-expert-only"
        }
        $trainExec = Invoke-CommandCapture -Exe $PythonPath -ArgList $trainArgs
        $matrixPayload = if ($useVariantMatrix) { Resolve-JsonObjectFromText -TextValue ([string]$trainExec.Output) } else { @{} }
        $runDir = if ($DryRun) {
            ""
        } elseif ($useVariantMatrix) {
            [string](Get-PropValue -ObjectValue $matrixPayload -Name "run_dir" -DefaultValue "")
        } else {
            Resolve-RunDirFromText -TextValue ([string]$trainExec.Output)
        }
        $runId = if ($DryRun) {
            ""
        } elseif ($useVariantMatrix) {
            [string](Get-PropValue -ObjectValue $matrixPayload -Name "run_id" -DefaultValue "")
        } elseif ([string]::IsNullOrWhiteSpace($runDir)) {
            ""
        } else {
            Split-Path -Leaf $runDir
        }
        $trainConfigPath = if ([string]::IsNullOrWhiteSpace($runDir)) { "" } else { Join-Path $runDir "train_config.yaml" }
        $trainConfig = if ([string]::IsNullOrWhiteSpace($trainConfigPath)) { @{} } else { Load-JsonOrEmpty -PathValue $trainConfigPath }
        $snapshotId = [string](Get-PropValue -ObjectValue $trainConfig -Name "data_platform_ready_snapshot_id" -DefaultValue "")
        $variantMetadata = if ($useVariantMatrix -and (-not [string]::IsNullOrWhiteSpace($runDir))) {
            $metadata = Resolve-RunVariantMetadata -RunDir $runDir -TrainerName $trainerName
            if ($matrixPayload) {
                if ([string]::IsNullOrWhiteSpace([string](Get-PropValue -ObjectValue $metadata -Name "chosen_variant_name" -DefaultValue ""))) {
                    $metadata.chosen_variant_name = [string](Get-PropValue -ObjectValue $matrixPayload -Name "chosen_variant_name" -DefaultValue "")
                }
                if ([string]::IsNullOrWhiteSpace([string](Get-PropValue -ObjectValue $metadata -Name "variant_report_path" -DefaultValue ""))) {
                    $metadata.variant_report_path = [string](Get-PropValue -ObjectValue $matrixPayload -Name "variant_report_path" -DefaultValue "")
                }
                if ([int](To-Int64 (Get-PropValue -ObjectValue $metadata -Name "evaluated_variant_count" -DefaultValue 0) 0) -le 0) {
                    $metadata.evaluated_variant_count = [int](To-Int64 (Get-PropValue -ObjectValue $matrixPayload -Name "evaluated_variant_count" -DefaultValue 0) 0)
                }
                if ([string]::IsNullOrWhiteSpace([string](Get-PropValue -ObjectValue $metadata -Name "baseline_kept_reason_code" -DefaultValue ""))) {
                    $metadata.baseline_kept_reason_code = [string](Get-PropValue -ObjectValue $matrixPayload -Name "baseline_kept_reason_code" -DefaultValue "")
                }
                if ([string]::IsNullOrWhiteSpace([string](Get-PropValue -ObjectValue $metadata -Name "chosen_reason_code" -DefaultValue ""))) {
                    $metadata.chosen_reason_code = [string](Get-PropValue -ObjectValue $matrixPayload -Name "chosen_reason_code" -DefaultValue "")
                }
            }
            $metadata
        } else {
            @{}
        }
        $freshValidation = if ([string]::IsNullOrWhiteSpace($runDir)) {
            [ordered]@{
                required_artifacts_complete = $false
                tail_mode = ""
            }
        } else {
            Test-DependencyTrainerRunReusable `
                -RunDir $runDir `
                -TrainerName $trainerName `
                -ModelFamily $dependencyModelFamily `
                -TrainStartDate $TrainStartDate `
                -TrainEndDate $TrainEndDate `
                -ExecutionEvalStartDate $ExecutionEvalStartDate `
                -ExecutionEvalEndDate $ExecutionEvalEndDate `
                -ExpectedSnapshotId $script:dataPlatformReadySnapshotId
        }
        if ((-not $DryRun) -and ([int]$trainExec.ExitCode -ne 0)) {
            throw ("dependency trainer failed: " + $trainerName)
        }
        if ((-not $DryRun) -and [string]::IsNullOrWhiteSpace($runId)) {
            throw ("dependency trainer did not emit run_id: " + $trainerName)
        }
        $results += [ordered]@{
            trainer = $trainerName
            model_family = $dependencyModelFamily
            run_scope = $dependencyRunScope
            exit_code = [int]$trainExec.ExitCode
            command = $trainExec.Command
            output_preview = (Get-OutputPreview -Text ([string]$trainExec.Output))
            run_dir = $runDir
            run_id = $runId
            data_platform_ready_snapshot_id = $snapshotId
            reused = $false
            source_mode = "fresh_train"
            reuse_reason = [string](Get-PropValue -ObjectValue $reusableDependencyRun -Name "reason" -DefaultValue "")
            required_artifacts_complete = [bool](Get-PropValue -ObjectValue $freshValidation -Name "required_artifacts_complete" -DefaultValue $false)
            tail_mode = [string](Get-PropValue -ObjectValue $freshValidation -Name "tail_mode" -DefaultValue "")
            variant_matrix = $useVariantMatrix
            variant_metadata = $variantMetadata
            chosen_variant_name = [string](Get-PropValue -ObjectValue $variantMetadata -Name "chosen_variant_name" -DefaultValue "")
            variant_report_path = [string](Get-PropValue -ObjectValue $variantMetadata -Name "variant_report_path" -DefaultValue "")
            evaluated_variant_count = [int](To-Int64 (Get-PropValue -ObjectValue $variantMetadata -Name "evaluated_variant_count" -DefaultValue 0) 0)
            baseline_kept_reason_code = [string](Get-PropValue -ObjectValue $variantMetadata -Name "baseline_kept_reason_code" -DefaultValue "")
            chosen_reason_code = [string](Get-PropValue -ObjectValue $variantMetadata -Name "chosen_reason_code" -DefaultValue "")
        }
    }
    return @($results)
}

function Resolve-DependencyFusionInputPaths {
    param(
        [Parameter(Mandatory = $true)]
        [object[]]$DependencyResults
    )
    $required = [ordered]@{
        v5_panel_ensemble = "fusion_panel_input"
        v5_sequence = "fusion_sequence_input"
        v5_lob = "fusion_lob_input"
        v5_tradability = "fusion_tradability_input"
    }
    $resolved = [ordered]@{}
    foreach ($trainerName in $required.Keys) {
        $match = $null
        foreach ($item in @($DependencyResults)) {
            $candidateTrainer = [string](Get-PropValue -ObjectValue $item -Name "trainer" -DefaultValue "")
            if ($candidateTrainer -eq $trainerName) {
                $match = $item
                break
            }
        }
        if ($null -eq $match) {
            throw ("missing dependency trainer result: " + $trainerName)
        }
        $runDir = [string](Get-PropValue -ObjectValue $match -Name "run_dir" -DefaultValue "")
        if ($DryRun -and [string]::IsNullOrWhiteSpace($runDir)) {
            $modelFamily = [string](Get-PropValue -ObjectValue $match -Name "model_family" -DefaultValue "")
            if (-not [string]::IsNullOrWhiteSpace($modelFamily)) {
                $runDir = Join-Path (Join-Path $resolvedRegistryRoot $modelFamily) ("dry-run-" + $trainerName)
            }
        }
        if ([string]::IsNullOrWhiteSpace($runDir)) {
            throw ("dependency trainer run_dir missing: " + $trainerName)
        }
        $expertTablePath = Join-Path $runDir "expert_prediction_table.parquet"
        if ((-not $DryRun) -and (-not (Test-Path $expertTablePath))) {
            throw ("dependency expert_prediction_table missing: " + $expertTablePath)
        }
        $resolved[$required[$trainerName]] = $expertTablePath
    }
    return $resolved
}

function Test-DependencyRuntimeExportUsable {
    param(
        [Parameter(Mandatory = $true)]
        $ExportResult,
        [string]$ExpectedSnapshotId
    )
    $exportPath = [string](Get-PropValue -ObjectValue $ExportResult -Name "export_path" -DefaultValue "")
    $metadataPath = [string](Get-PropValue -ObjectValue $ExportResult -Name "metadata_path" -DefaultValue "")
    $snapshotId = [string](Get-PropValue -ObjectValue $ExportResult -Name "data_platform_ready_snapshot_id" -DefaultValue "")
    $rows = [int](Get-PropValue -ObjectValue $ExportResult -Name "rows" -DefaultValue 0)
    $coverageFields = Resolve-OperatingDateCoverageFields -Payload $ExportResult
    $coverageStartTsMs = [int64](Get-PropValue -ObjectValue $ExportResult -Name "coverage_start_ts_ms" -DefaultValue 0)
    $coverageEndTsMs = [int64](Get-PropValue -ObjectValue $ExportResult -Name "coverage_end_ts_ms" -DefaultValue 0)
    if ([string]::IsNullOrWhiteSpace($exportPath) -or [string]::IsNullOrWhiteSpace($metadataPath)) {
        return $false
    }
    if ((-not $DryRun) -and ((-not (Test-Path $exportPath)) -or (-not (Test-Path $metadataPath)))) {
        return $false
    }
    if ([string]::IsNullOrWhiteSpace($snapshotId) -or ($snapshotId -ne [string]$ExpectedSnapshotId)) {
        return $false
    }
    if ($DryRun) {
        return $true
    }
    return (
        ($rows -gt 0) -and
        ($coverageStartTsMs -gt 0) -and
        ($coverageEndTsMs -gt 0) -and
        (-not [string]::IsNullOrWhiteSpace([string](Get-PropValue -ObjectValue $coverageFields -Name "coverage_start_date" -DefaultValue ""))) -and
        (-not [string]::IsNullOrWhiteSpace([string](Get-PropValue -ObjectValue $coverageFields -Name "coverage_end_date" -DefaultValue ""))) -and
        ([string](Get-PropValue -ObjectValue $coverageFields -Name "window_timezone" -DefaultValue "") -eq "Asia/Seoul")
    )
}

function Normalize-MarketArray {
    param([Parameter(Mandatory = $false)]$Markets)
    return @(
        @($Markets) |
            ForEach-Object { ([string]$_).Trim().ToUpperInvariant() } |
            Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    )
}

function Test-StringArraySequenceEqual {
    param(
        [string[]]$Left = @(),
        [string[]]$Right = @()
    )
    $normalizedLeft = @(Normalize-MarketArray -Markets $Left)
    $normalizedRight = @(Normalize-MarketArray -Markets $Right)
    if ($normalizedLeft.Count -ne $normalizedRight.Count) {
        return $false
    }
    for ($index = 0; $index -lt $normalizedLeft.Count; $index += 1) {
        if ([string]$normalizedLeft[$index] -ne [string]$normalizedRight[$index]) {
            return $false
        }
    }
    return $true
}

function Get-DependencyRuntimeExportGapPrefix {
    param([string]$TrainerName)
    switch (([string]$TrainerName).Trim().ToLowerInvariant()) {
        "v5_panel_ensemble" { return "PANEL" }
        "v5_sequence" { return "SEQUENCE" }
        "v5_lob" { return "LOB" }
        "v5_tradability" { return "TRADABILITY" }
        default { return "DEPENDENCY" }
    }
}

function Get-DependencyRuntimeExportOrder {
    param([string]$TrainerName)
    switch (([string]$TrainerName).Trim().ToLowerInvariant()) {
        "v5_panel_ensemble" { return 0 }
        "v5_sequence" { return 1 }
        "v5_lob" { return 2 }
        "v5_tradability" { return 3 }
        default { return 100 }
    }
}

function New-CommonRuntimeUniverseArtifact {
    param(
        [Parameter(Mandatory = $true)]
        $DependencyRuntimeUniverse,
        [string]$CertificationStartDate,
        [string]$CertificationEndDate,
        [string]$ExpectedSnapshotId,
        [string]$ArtifactPath = ""
    )
    $commonMarkets = @(Normalize-MarketArray -Markets (Get-PropValue -ObjectValue $DependencyRuntimeUniverse -Name "common_markets" -DefaultValue @()))
    $identitySeed = [string]::Join(
        "|",
        @(
            [string]$ExpectedSnapshotId,
            [string]$CertificationStartDate,
            [string]$CertificationEndDate,
            [string]::Join(",", @($commonMarkets))
        )
    )
    $digest = Get-Sha256Hex -Text $identitySeed
    $universeId = "common_runtime_universe_" + $digest.Substring(0, [Math]::Min($digest.Length, 12))
    return [ordered]@{
        policy = "common_runtime_universe_v1"
        generated_at_utc = (Get-Date).ToUniversalTime().ToString("o")
        snapshot_id = [string]$ExpectedSnapshotId
        certification_window = [ordered]@{
            start = [string]$CertificationStartDate
            end = [string]$CertificationEndDate
        }
        resolution_policy = "panel_order_preserving_intersection"
        common_runtime_universe_id = $universeId
        common_markets = @($commonMarkets)
        common_market_count = @($commonMarkets).Count
        dependency_results = @((Get-PropValue -ObjectValue $DependencyRuntimeUniverse -Name "results" -DefaultValue @()))
        pass = [bool](Get-PropValue -ObjectValue $DependencyRuntimeUniverse -Name "pass" -DefaultValue $false)
        reason = [string](Get-PropValue -ObjectValue $DependencyRuntimeUniverse -Name "reason" -DefaultValue "")
        artifact_path = [string]$ArtifactPath
    }
}

function Test-DependencyRuntimeExportContractAlignment {
    param(
        [Parameter(Mandatory = $true)]
        [object[]]$ExportResults,
        [string]$CertificationStartDate,
        [string]$CertificationEndDate,
        [string]$ExpectedSnapshotId,
        [string[]]$CommonMarkets = @()
    )
    $reasons = @()
    $expectedCommonMarkets = @(Normalize-MarketArray -Markets $CommonMarkets)
    $expectedCoverageDates = @(Get-DateTokenRangeInclusive -StartDate $CertificationStartDate -EndDate $CertificationEndDate)
    $panelExport = @($ExportResults | Where-Object { [string](Get-PropValue -ObjectValue $_ -Name "trainer" -DefaultValue "") -eq "v5_panel_ensemble" } | Select-Object -First 1)
    $panelRowCount = if ($panelExport.Count -gt 0) { [int](Get-PropValue -ObjectValue $panelExport[0] -Name "rows" -DefaultValue 0) } else { 0 }
    $panelExportPath = if ($panelExport.Count -gt 0) { [string](Get-PropValue -ObjectValue $panelExport[0] -Name "export_path" -DefaultValue "") } else { "" }
    if (@($expectedCommonMarkets).Count -le 0) {
        $reasons += "COMMON_RUNTIME_UNIVERSE_EMPTY"
    }
    $details = @()
    foreach ($item in @($ExportResults)) {
        $trainerName = [string](Get-PropValue -ObjectValue $item -Name "trainer" -DefaultValue "")
        $prefix = Get-DependencyRuntimeExportGapPrefix -TrainerName $trainerName
        $snapshotId = [string](Get-PropValue -ObjectValue $item -Name "data_platform_ready_snapshot_id" -DefaultValue "")
        $startDate = [string](Get-PropValue -ObjectValue $item -Name "start" -DefaultValue "")
        $endDate = [string](Get-PropValue -ObjectValue $item -Name "end" -DefaultValue "")
        $coverageStartTsMs = [int64](Get-PropValue -ObjectValue $item -Name "coverage_start_ts_ms" -DefaultValue 0)
        $coverageEndTsMs = [int64](Get-PropValue -ObjectValue $item -Name "coverage_end_ts_ms" -DefaultValue 0)
        $coverageFields = Resolve-OperatingDateCoverageFields -Payload $item
        $coverageStartDate = [string](Get-PropValue -ObjectValue $coverageFields -Name "coverage_start_date" -DefaultValue "")
        $coverageEndDate = [string](Get-PropValue -ObjectValue $coverageFields -Name "coverage_end_date" -DefaultValue "")
        $coverageDates = @((Get-PropValue -ObjectValue $coverageFields -Name "coverage_dates" -DefaultValue @()))
        $windowTimezone = [string](Get-PropValue -ObjectValue $coverageFields -Name "window_timezone" -DefaultValue "")
        $anchorAlignmentComplete = [bool](Get-PropValue -ObjectValue $item -Name "anchor_alignment_complete" -DefaultValue $false)
        $anchorExportPath = [string](Get-PropValue -ObjectValue $item -Name "anchor_export_path" -DefaultValue "")
        $rows = [int](Get-PropValue -ObjectValue $item -Name "rows" -DefaultValue 0)
        $requestedSelectedMarkets = @(Normalize-MarketArray -Markets (Get-PropValue -ObjectValue $item -Name "requested_selected_markets" -DefaultValue @()))
        $selectedMarkets = @(Normalize-MarketArray -Markets (Get-PropValue -ObjectValue $item -Name "selected_markets" -DefaultValue @()))
        $selectedMarketsSource = [string](Get-PropValue -ObjectValue $item -Name "selected_markets_source" -DefaultValue "")
        $fallbackReason = [string](Get-PropValue -ObjectValue $item -Name "fallback_reason" -DefaultValue "")
        $missingCoverageDates = @($expectedCoverageDates | Where-Object { @($coverageDates) -notcontains [string]$_ })
        $allowTrailingSingleDayGap = $false
        if (
            (@($missingCoverageDates).Count -eq 1) -and
            ([string]$missingCoverageDates[0] -eq [string]$CertificationEndDate) -and
            (-not [string]::IsNullOrWhiteSpace($coverageEndDate))
        ) {
            try {
                $coverageEndObj = [DateTime]::ParseExact($coverageEndDate, "yyyy-MM-dd", [System.Globalization.CultureInfo]::InvariantCulture)
                $certificationEndObj = [DateTime]::ParseExact([string]$CertificationEndDate, "yyyy-MM-dd", [System.Globalization.CultureInfo]::InvariantCulture)
                $allowTrailingSingleDayGap = ($coverageEndObj.AddDays(1).ToString("yyyy-MM-dd") -eq $certificationEndObj.ToString("yyyy-MM-dd"))
            } catch {
                $allowTrailingSingleDayGap = $false
            }
        }
        $windowGap = (
            ($startDate -ne [string]$CertificationStartDate) -or
            ($endDate -ne [string]$CertificationEndDate) -or
            ($windowTimezone -ne "Asia/Seoul") -or
            [string]::IsNullOrWhiteSpace($coverageStartDate) -or
            [string]::IsNullOrWhiteSpace($coverageEndDate) -or
            ($coverageStartDate -gt [string]$CertificationStartDate) -or
            (($coverageEndDate -lt [string]$CertificationEndDate) -and (-not $allowTrailingSingleDayGap)) -or
            ((@($missingCoverageDates).Count -gt 0) -and (-not $allowTrailingSingleDayGap))
        )
        $anchorGap = (
            (([string]$trainerName -ne "v5_panel_ensemble")) -and
            (
                (-not $anchorAlignmentComplete) -or
                [string]::IsNullOrWhiteSpace($anchorExportPath) -or
                ($anchorExportPath -ne $panelExportPath) -or
                (($panelRowCount -gt 0) -and ($rows -ne $panelRowCount))
            )
        )
        $universeMismatch = (
            (-not (Test-StringArraySequenceEqual -Left $requestedSelectedMarkets -Right $expectedCommonMarkets)) -or
            (-not (Test-StringArraySequenceEqual -Left $selectedMarkets -Right $expectedCommonMarkets)) -or
            ($selectedMarketsSource -ne "acceptance_common_runtime_universe") -or
            (-not [string]::IsNullOrWhiteSpace($fallbackReason))
        )
        if ([string]::IsNullOrWhiteSpace($snapshotId) -or ($snapshotId -ne [string]$ExpectedSnapshotId)) {
            $reasons += ($prefix + "_RUNTIME_SNAPSHOT_MISMATCH")
        }
        if ($windowGap) {
            $reasons += ($prefix + "_RUNTIME_WINDOW_GAP")
        }
        if ($anchorGap) {
            $reasons += ($prefix + "_RUNTIME_ANCHOR_GAP")
        }
        if ($universeMismatch) {
            $reasons += ($prefix + "_RUNTIME_UNIVERSE_MISMATCH")
        }
        $details += [ordered]@{
            trainer = $trainerName
            data_platform_ready_snapshot_id = $snapshotId
            start = $startDate
            end = $endDate
            coverage_start_ts_ms = $coverageStartTsMs
            coverage_end_ts_ms = $coverageEndTsMs
            coverage_start_date = $coverageStartDate
            coverage_end_date = $coverageEndDate
            coverage_dates = @($coverageDates)
            window_timezone = $windowTimezone
            anchor_alignment_complete = $anchorAlignmentComplete
            anchor_export_path = $anchorExportPath
            rows = $rows
            requested_selected_markets = @($requestedSelectedMarkets)
            selected_markets = @($selectedMarkets)
            selected_markets_source = $selectedMarketsSource
            fallback_reason = $fallbackReason
            snapshot_match = (-not [string]::IsNullOrWhiteSpace($snapshotId)) -and ($snapshotId -eq [string]$ExpectedSnapshotId)
            window_match = (-not $windowGap)
            anchor_match = (-not $anchorGap)
            universe_match = (-not $universeMismatch)
        }
    }
    return [ordered]@{
        pass = (@($reasons).Count -eq 0)
        reasons = @($reasons | Select-Object -Unique)
        certification_window_start = $CertificationStartDate
        certification_window_end = $CertificationEndDate
        expected_snapshot_id = $ExpectedSnapshotId
        expected_common_markets = @($expectedCommonMarkets)
        details = @($details)
    }
}

function Test-ShouldAttemptDependencyRuntimeCoverage {
    param(
        [string]$TrainerName,
        [object[]]$DependencyResults,
        [string]$CertificationStartDate,
        [string]$CertificationEndDate,
        [string]$LaneMode
    )
    if (([string]$TrainerName).Trim().ToLowerInvariant() -ne "v5_fusion") {
        return $false
    }
    if (@($DependencyResults).Count -le 0) {
        return $false
    }
    if ([string]::Equals(([string]$LaneMode).Trim(), "bootstrap_latest_inclusive", [System.StringComparison]::OrdinalIgnoreCase)) {
        return $false
    }
    if ([string]::IsNullOrWhiteSpace($CertificationStartDate) -or [string]::IsNullOrWhiteSpace($CertificationEndDate)) {
        return $false
    }
    return $true
}

function Test-ShouldAttemptRuntimeDatasetCoveragePreflight {
    param(
        [string]$TrainerName,
        [string]$CertificationStartDate,
        [string]$CertificationEndDate,
        [string]$LaneMode
    )
    if (([string]$TrainerName).Trim().ToLowerInvariant() -ne "v5_fusion") {
        return $false
    }
    if ([string]::Equals(([string]$LaneMode).Trim(), "bootstrap_latest_inclusive", [System.StringComparison]::OrdinalIgnoreCase)) {
        return $false
    }
    if ([string]::IsNullOrWhiteSpace($CertificationStartDate) -or [string]::IsNullOrWhiteSpace($CertificationEndDate)) {
        return $false
    }
    return $true
}

function Invoke-DependencyRuntimeExportChain {
    param(
        [string]$PythonPath,
        [object[]]$DependencyResults,
        [string]$CertificationStartDate,
        [string]$CertificationEndDate,
        [string[]]$CommonMarkets = @()
    )
    $results = @()
    $panelRuntimeExportPath = ""
    $sequenceRuntimeExportPath = ""
    $lobRuntimeExportPath = ""
    $orderedDependencyResults = @(
        @($DependencyResults) |
            Sort-Object { Get-DependencyRuntimeExportOrder -TrainerName ([string](Get-PropValue -ObjectValue $_ -Name "trainer" -DefaultValue "")) }
    )
    foreach ($item in @($orderedDependencyResults)) {
        $trainerName = [string](Get-PropValue -ObjectValue $item -Name "trainer" -DefaultValue "")
        $runDir = [string](Get-PropValue -ObjectValue $item -Name "run_dir" -DefaultValue "")
        $runId = [string](Get-PropValue -ObjectValue $item -Name "run_id" -DefaultValue "")
        $modelFamily = [string](Get-PropValue -ObjectValue $item -Name "model_family" -DefaultValue "")
        $snapshotId = [string](Get-PropValue -ObjectValue $item -Name "data_platform_ready_snapshot_id" -DefaultValue "")
        if ($DryRun -and [string]::IsNullOrWhiteSpace($runDir)) {
            if (-not [string]::IsNullOrWhiteSpace($modelFamily)) {
                $runDir = Join-Path (Join-Path $resolvedRegistryRoot $modelFamily) ("dry-run-" + $trainerName)
            } else {
                $runDir = Join-Path $resolvedProjectRoot ("dry-run-" + $trainerName)
            }
        }
        if ($DryRun -and [string]::IsNullOrWhiteSpace($runId)) {
            $runId = "dry-run-" + $trainerName
        }
        if ([string]::IsNullOrWhiteSpace($trainerName) -or [string]::IsNullOrWhiteSpace($runDir)) {
            throw "dependency runtime export requires trainer and run_dir"
        }
        $exportRoot = Join-Path $runDir "_runtime_exports"
        $windowId = $CertificationStartDate + "__" + $CertificationEndDate
        $exportDir = Join-Path $exportRoot $windowId
        $coverageStartTsMs = Convert-DateTokenToUnixMs -DateText $CertificationStartDate
        $coverageEndTsMs = Convert-DateTokenToUnixMs -DateText $CertificationEndDate -EndOfDay
        $coverageDates = @(Get-DateTokenRangeInclusive -StartDate $CertificationStartDate -EndDate $CertificationEndDate)
        $syntheticPayload = [ordered]@{
            run_id = $runId
            trainer = $trainerName
            model_family = $modelFamily
            data_platform_ready_snapshot_id = $snapshotId
            start = $CertificationStartDate
            end = $CertificationEndDate
            coverage_start_ts_ms = $coverageStartTsMs
            coverage_end_ts_ms = $coverageEndTsMs
            coverage_start_date = $CertificationStartDate
            coverage_end_date = $CertificationEndDate
            coverage_dates = @($coverageDates)
            window_timezone = "Asia/Seoul"
            rows = 0
            requested_selected_markets = @($CommonMarkets)
            selected_markets = @($CommonMarkets)
            selected_markets_source = if (@($CommonMarkets).Count -gt 0) { "acceptance_common_runtime_universe" } else { "" }
            fallback_reason = ""
            export_path = Join-Path $exportDir "expert_prediction_table.parquet"
            metadata_path = Join-Path $exportDir "metadata.json"
            reused = $false
            source_mode = "dry_run"
        }
        if ($DryRun) {
            $exec = [PSCustomObject]@{
                ExitCode = 0
                Output = ($syntheticPayload | ConvertTo-Json -Depth 12 -Compress)
                Command = "[dry-run] autobot.cli model export-expert-table"
                DryRun = $true
            }
            $payload = $syntheticPayload
        } else {
            $args = @(
                "-m", "autobot.cli",
                "model", "export-expert-table",
                "--trainer", $trainerName,
                "--run-dir", $runDir,
                "--start", $CertificationStartDate,
                "--end", $CertificationEndDate
            )
            if (@($CommonMarkets).Count -gt 0) {
                $args += @("--markets", ([string]::Join(",", @(
                    @($CommonMarkets) |
                        ForEach-Object { ([string]$_).Trim() } |
                        Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
                ))))
            }
            if (
                ([string]$trainerName -ne "v5_panel_ensemble") -and
                (-not [string]::IsNullOrWhiteSpace($panelRuntimeExportPath))
            ) {
                $args += @("--anchor-export-path", $panelRuntimeExportPath)
            }
            if ([string]$trainerName -eq "v5_tradability") {
                if (
                    [string]::IsNullOrWhiteSpace($panelRuntimeExportPath) `
                    -or [string]::IsNullOrWhiteSpace($sequenceRuntimeExportPath) `
                    -or [string]::IsNullOrWhiteSpace($lobRuntimeExportPath)
                ) {
                    throw "tradability runtime export requires panel/sequence/lob runtime exports"
                }
                $args += @(
                    "--panel-runtime-input", $panelRuntimeExportPath,
                    "--sequence-runtime-input", $sequenceRuntimeExportPath,
                    "--lob-runtime-input", $lobRuntimeExportPath
                )
            }
            $exec = Invoke-CommandCapture -Exe $PythonPath -ArgList $args
            $payload = @{}
            try {
                $payload = Resolve-JsonPayloadFromText -TextValue ([string]$exec.Output) -ContextLabel ("dependency runtime export payload: " + $trainerName)
            } catch {
                throw ("dependency runtime export payload parse failed: " + $trainerName)
            }
        }
        if (-not (Test-DependencyRuntimeExportUsable -ExportResult $payload -ExpectedSnapshotId $snapshotId)) {
            throw ("dependency runtime export unusable: " + $trainerName)
        }
        $coverageFields = Resolve-OperatingDateCoverageFields -Payload $payload
        $results += [ordered]@{
            trainer = $trainerName
            model_family = $modelFamily
            run_dir = $runDir
            run_id = $runId
            command = $exec.Command
            output_preview = (Get-OutputPreview -Text ([string]$exec.Output))
            export_path = [string](Get-PropValue -ObjectValue $payload -Name "export_path" -DefaultValue "")
            metadata_path = [string](Get-PropValue -ObjectValue $payload -Name "metadata_path" -DefaultValue "")
            data_platform_ready_snapshot_id = [string](Get-PropValue -ObjectValue $payload -Name "data_platform_ready_snapshot_id" -DefaultValue "")
            start = [string](Get-PropValue -ObjectValue $payload -Name "start" -DefaultValue "")
            end = [string](Get-PropValue -ObjectValue $payload -Name "end" -DefaultValue "")
            coverage_start_ts_ms = [int64](Get-PropValue -ObjectValue $payload -Name "coverage_start_ts_ms" -DefaultValue 0)
            coverage_end_ts_ms = [int64](Get-PropValue -ObjectValue $payload -Name "coverage_end_ts_ms" -DefaultValue 0)
            coverage_start_date = [string](Get-PropValue -ObjectValue $coverageFields -Name "coverage_start_date" -DefaultValue "")
            coverage_end_date = [string](Get-PropValue -ObjectValue $coverageFields -Name "coverage_end_date" -DefaultValue "")
            coverage_dates = @((Get-PropValue -ObjectValue $coverageFields -Name "coverage_dates" -DefaultValue @()))
            window_timezone = [string](Get-PropValue -ObjectValue $coverageFields -Name "window_timezone" -DefaultValue "")
            anchor_alignment_complete = [bool](Get-PropValue -ObjectValue $payload -Name "anchor_alignment_complete" -DefaultValue $false)
            anchor_export_path = [string](Get-PropValue -ObjectValue $payload -Name "anchor_export_path" -DefaultValue "")
            rows = [int](Get-PropValue -ObjectValue $payload -Name "rows" -DefaultValue 0)
            requested_selected_markets = @((Get-PropValue -ObjectValue $payload -Name "requested_selected_markets" -DefaultValue @()))
            selected_markets = @((Get-PropValue -ObjectValue $payload -Name "selected_markets" -DefaultValue @()))
            selected_markets_source = [string](Get-PropValue -ObjectValue $payload -Name "selected_markets_source" -DefaultValue "")
            fallback_reason = [string](Get-PropValue -ObjectValue $payload -Name "fallback_reason" -DefaultValue "")
            reused = [bool](Get-PropValue -ObjectValue $payload -Name "reused" -DefaultValue $false)
            source_mode = [string](Get-PropValue -ObjectValue $payload -Name "source_mode" -DefaultValue "")
        }
        if ([string]$trainerName -eq "v5_panel_ensemble") {
            $panelRuntimeExportPath = [string](Get-PropValue -ObjectValue $payload -Name "export_path" -DefaultValue "")
        } elseif ([string]$trainerName -eq "v5_sequence") {
            $sequenceRuntimeExportPath = [string](Get-PropValue -ObjectValue $payload -Name "export_path" -DefaultValue "")
        } elseif ([string]$trainerName -eq "v5_lob") {
            $lobRuntimeExportPath = [string](Get-PropValue -ObjectValue $payload -Name "export_path" -DefaultValue "")
        }
    }
    return @($results)
}

function Resolve-DependencyRuntimeCommonUniverse {
    param(
        [string]$PythonPath,
        [object[]]$DependencyResults,
        [string]$CertificationStartDate,
        [string]$CertificationEndDate
    )
    $results = @()
    foreach ($item in @($DependencyResults)) {
        $trainerName = [string](Get-PropValue -ObjectValue $item -Name "trainer" -DefaultValue "")
        if ([string]::Equals($trainerName, "v5_tradability", [System.StringComparison]::OrdinalIgnoreCase)) {
            continue
        }
        $runDir = [string](Get-PropValue -ObjectValue $item -Name "run_dir" -DefaultValue "")
        $runId = [string](Get-PropValue -ObjectValue $item -Name "run_id" -DefaultValue "")
        $modelFamily = [string](Get-PropValue -ObjectValue $item -Name "model_family" -DefaultValue "")
        $snapshotId = [string](Get-PropValue -ObjectValue $item -Name "data_platform_ready_snapshot_id" -DefaultValue "")
        if ([string]::IsNullOrWhiteSpace($trainerName) -or [string]::IsNullOrWhiteSpace($runDir)) {
            throw "dependency runtime universe resolution requires trainer and run_dir"
        }
        if ($DryRun) {
            $payload = [ordered]@{
                run_id = $runId
                trainer = $trainerName
                model_family = $modelFamily
                data_platform_ready_snapshot_id = $snapshotId
                start = $CertificationStartDate
                end = $CertificationEndDate
                coverage_start_date = $CertificationStartDate
                coverage_end_date = $CertificationEndDate
                coverage_dates = @(Get-DateTokenRangeInclusive -StartDate $CertificationStartDate -EndDate $CertificationEndDate)
                window_timezone = "Asia/Seoul"
                rows = 0
                requested_selected_markets = @()
                selected_markets = @()
                selected_markets_source = "dry_run"
                fallback_reason = ""
                export_path = ""
                metadata_path = ""
                reused = $false
                source_mode = "resolve_markets_only"
            }
        } else {
            $args = @(
                "-m", "autobot.cli",
                "model", "export-expert-table",
                "--trainer", $trainerName,
                "--run-dir", $runDir,
                "--start", $CertificationStartDate,
                "--end", $CertificationEndDate,
                "--resolve-markets-only"
            )
            $exec = Invoke-CommandCapture -Exe $PythonPath -ArgList $args
            try {
                $payload = Resolve-JsonPayloadFromText -TextValue ([string]$exec.Output) -ContextLabel ("dependency runtime universe payload: " + $trainerName)
            } catch {
                throw ("dependency runtime universe payload parse failed: " + $trainerName)
            }
        }
        $results += [ordered]@{
            trainer = $trainerName
            model_family = $modelFamily
            run_dir = $runDir
            run_id = $runId
            data_platform_ready_snapshot_id = $snapshotId
            selected_markets = @((Get-PropValue -ObjectValue $payload -Name "selected_markets" -DefaultValue @()))
            selected_markets_source = [string](Get-PropValue -ObjectValue $payload -Name "selected_markets_source" -DefaultValue "")
            fallback_reason = [string](Get-PropValue -ObjectValue $payload -Name "fallback_reason" -DefaultValue "")
            requested_selected_markets = @((Get-PropValue -ObjectValue $payload -Name "requested_selected_markets" -DefaultValue @()))
        }
    }

    $orderedCommon = @()
    $panelResult = @($results | Where-Object { [string](Get-PropValue -ObjectValue $_ -Name "trainer" -DefaultValue "") -eq "v5_panel_ensemble" } | Select-Object -First 1)
    $orderedSource = if ($panelResult.Count -gt 0) {
        @((Get-PropValue -ObjectValue $panelResult[0] -Name "selected_markets" -DefaultValue @()))
    } elseif (@($results).Count -gt 0) {
        @((Get-PropValue -ObjectValue $results[0] -Name "selected_markets" -DefaultValue @()))
    } else {
        @()
    }
    $intersection = $null
    foreach ($result in @($results)) {
        $marketSet = New-Object 'System.Collections.Generic.HashSet[string]' ([System.StringComparer]::OrdinalIgnoreCase)
        foreach ($market in @((Get-PropValue -ObjectValue $result -Name "selected_markets" -DefaultValue @()))) {
            if (-not [string]::IsNullOrWhiteSpace([string]$market)) {
                [void]$marketSet.Add(([string]$market).Trim().ToUpperInvariant())
            }
        }
        if ($null -eq $intersection) {
            $intersection = $marketSet
        } else {
            $intersection.IntersectWith($marketSet)
        }
    }
    if ($null -ne $intersection) {
        foreach ($market in @($orderedSource)) {
            $normalized = ([string]$market).Trim().ToUpperInvariant()
            if ([string]::IsNullOrWhiteSpace($normalized)) {
                continue
            }
            if ($intersection.Contains($normalized) -and (-not ($orderedCommon -contains $normalized))) {
                $orderedCommon += $normalized
            }
        }
    }

    return [ordered]@{
        attempted = $true
        count = @($results).Count
        results = @($results)
        common_markets = @($orderedCommon)
        common_market_count = @($orderedCommon).Count
        pass = (@($orderedCommon).Count -gt 0)
        reason = if (@($orderedCommon).Count -gt 0) { "" } else { "COMMON_RUNTIME_UNIVERSE_EMPTY" }
    }
}

function Resolve-DependencyRuntimeFusionInputPaths {
    param(
        [Parameter(Mandatory = $true)]
        [object[]]$DependencyRuntimeExportResults
    )
    $required = [ordered]@{
        v5_panel_ensemble = "fusion_panel_runtime_input"
        v5_sequence = "fusion_sequence_runtime_input"
        v5_lob = "fusion_lob_runtime_input"
        v5_tradability = "fusion_tradability_runtime_input"
    }
    $resolved = [ordered]@{}
    foreach ($trainerName in $required.Keys) {
        $match = $null
        foreach ($item in @($DependencyRuntimeExportResults)) {
            if ([string](Get-PropValue -ObjectValue $item -Name "trainer" -DefaultValue "") -eq $trainerName) {
                $match = $item
                break
            }
        }
        if ($null -eq $match) {
            throw ("missing dependency runtime export result: " + $trainerName)
        }
        $resolved[$required[$trainerName]] = [string](Get-PropValue -ObjectValue $match -Name "export_path" -DefaultValue "")
    }
    return $resolved
}

function Get-DependencyTrainerRequiredArtifacts {
    param([string]$TrainerName)
    return @(
        "train_config.yaml",
        "artifact_status.json",
        "expert_prediction_table.parquet"
    )
}

function Get-DependencyTrainerProvenanceArtifacts {
    param([string]$TrainerName)
    switch (([string]$TrainerName).Trim().ToLowerInvariant()) {
        "v5_sequence" { return @("sequence_pretrain_contract.json", "sequence_pretrain_report.json", "domain_weighting_report.json") }
        "v5_lob" { return @("lob_backbone_contract.json", "lob_target_contract.json", "domain_weighting_report.json") }
        "v5_tradability" { return @("tradability_model_contract.json", "domain_weighting_report.json") }
        default { return @() }
    }
}

function Test-DependencyTrainerProvenanceArtifactContent {
    param(
        [string]$TrainerName,
        [string]$ArtifactName,
        [string]$ArtifactPath
    )
    $result = [ordered]@{
        pass = $true
        reasons = @()
        payload = @{}
    }
    if ([string]::IsNullOrWhiteSpace($ArtifactPath) -or (-not (Test-Path $ArtifactPath))) {
        $result.pass = $false
        $result.reasons = @("ARTIFACT_MISSING")
        return $result
    }
    $payload = Load-JsonOrEmpty -PathValue $ArtifactPath
    $result.payload = $payload
    switch (([string]$TrainerName).Trim().ToLowerInvariant()) {
        "v5_sequence" {
            if ([string]$ArtifactName -eq "sequence_pretrain_contract.json") {
                if ([string](Get-PropValue -ObjectValue $payload -Name "policy" -DefaultValue "") -ne "sequence_pretrain_contract_v1") {
                    $result.reasons += "SEQUENCE_PRETRAIN_POLICY_INVALID"
                }
                if ([string]::IsNullOrWhiteSpace([string](Get-PropValue -ObjectValue $payload -Name "backbone_family" -DefaultValue ""))) {
                    $result.reasons += "SEQUENCE_PRETRAIN_BACKBONE_MISSING"
                }
                if ([string]::IsNullOrWhiteSpace([string](Get-PropValue -ObjectValue $payload -Name "pretrain_method" -DefaultValue ""))) {
                    $result.reasons += "SEQUENCE_PRETRAIN_METHOD_MISSING"
                }
                if ([string]::IsNullOrWhiteSpace([string](Get-PropValue -ObjectValue $payload -Name "pretrain_impl_method" -DefaultValue ""))) {
                    $result.reasons += "SEQUENCE_PRETRAIN_IMPL_METHOD_MISSING"
                }
                $pretrainMethod = [string](Get-PropValue -ObjectValue $payload -Name "pretrain_method" -DefaultValue "")
                $pretrainReady = To-Bool (Get-PropValue -ObjectValue $payload -Name "pretrain_ready" -DefaultValue $false) $false
                $encoderArtifactPath = [string](Get-PropValue -ObjectValue $payload -Name "encoder_artifact_path" -DefaultValue "")
                if ([string]::Equals($pretrainMethod, "none", [System.StringComparison]::OrdinalIgnoreCase)) {
                    if ($pretrainReady) {
                        $result.reasons += "SEQUENCE_PRETRAIN_READY_INVALID"
                    }
                } else {
                    if (-not $pretrainReady) {
                        $result.reasons += "SEQUENCE_PRETRAIN_NOT_READY"
                    }
                    if ([string]::IsNullOrWhiteSpace($encoderArtifactPath) -or (-not (Test-Path $encoderArtifactPath))) {
                        $result.reasons += "SEQUENCE_PRETRAIN_ENCODER_ARTIFACT_MISSING"
                    }
                }
            } elseif ([string]$ArtifactName -eq "sequence_pretrain_report.json") {
                if ([string](Get-PropValue -ObjectValue $payload -Name "policy" -DefaultValue "") -ne "sequence_pretrain_report_v1") {
                    $result.reasons += "SEQUENCE_PRETRAIN_REPORT_POLICY_INVALID"
                }
                if ([string]::IsNullOrWhiteSpace([string](Get-PropValue -ObjectValue $payload -Name "objective_name" -DefaultValue ""))) {
                    $result.reasons += "SEQUENCE_PRETRAIN_OBJECTIVE_MISSING"
                }
                $status = [string](Get-PropValue -ObjectValue $payload -Name "status" -DefaultValue "")
                if ([string]::Equals($status, "enabled", [System.StringComparison]::OrdinalIgnoreCase)) {
                    if ((Get-PropValue -ObjectValue $payload -Name "final_loss" -DefaultValue $null) -eq $null) {
                        $result.reasons += "SEQUENCE_PRETRAIN_FINAL_LOSS_MISSING"
                    }
                    if ([int](To-Int64 (Get-PropValue -ObjectValue $payload -Name "best_epoch" -DefaultValue 0) 0) -le 0) {
                        $result.reasons += "SEQUENCE_PRETRAIN_BEST_EPOCH_INVALID"
                    }
                    if ([int](To-Int64 (Get-PropValue -ObjectValue $payload -Name "encoder_dim" -DefaultValue 0) 0) -le 0) {
                        $result.reasons += "SEQUENCE_PRETRAIN_ENCODER_DIM_INVALID"
                    }
                    if (Test-IsEffectivelyEmptyObject -ObjectValue (Get-PropValue -ObjectValue $payload -Name "final_component_values" -DefaultValue @{})) {
                        $result.reasons += "SEQUENCE_PRETRAIN_COMPONENT_VALUES_MISSING"
                    }
                    if (Test-IsEffectivelyEmptyObject -ObjectValue (Get-PropValue -ObjectValue $payload -Name "encoder_norm_summary" -DefaultValue @{})) {
                        $result.reasons += "SEQUENCE_PRETRAIN_ENCODER_NORMS_MISSING"
                    }
                }
            }
        }
        "v5_lob" {
            if ([string]$ArtifactName -eq "lob_backbone_contract.json") {
                if ([string](Get-PropValue -ObjectValue $payload -Name "policy" -DefaultValue "") -ne "lob_backbone_contract_v1") {
                    $result.reasons += "LOB_BACKBONE_POLICY_INVALID"
                }
                if ([string]::IsNullOrWhiteSpace([string](Get-PropValue -ObjectValue $payload -Name "backbone_family" -DefaultValue ""))) {
                    $result.reasons += "LOB_BACKBONE_NAME_MISSING"
                }
                if ([string](Get-PropValue -ObjectValue $payload -Name "uncertainty_head" -DefaultValue "") -ne "softplus_scalar") {
                    $result.reasons += "LOB_UNCERTAINTY_HEAD_INVALID"
                }
            } elseif ([string]$ArtifactName -eq "lob_target_contract.json") {
                if ([string](Get-PropValue -ObjectValue $payload -Name "policy" -DefaultValue "") -ne "lob_target_contract_v1") {
                    $result.reasons += "LOB_TARGET_POLICY_INVALID"
                }
                $primaryHorizon = [int](To-Int64 (Get-PropValue -ObjectValue $payload -Name "primary_horizon_seconds" -DefaultValue 0) 0)
                if ($primaryHorizon -ne 30) {
                    $result.reasons += "LOB_PRIMARY_HORIZON_INVALID"
                }
                $auxTargets = @((Get-PropValue -ObjectValue $payload -Name "auxiliary_targets" -DefaultValue @()))
                if (@($auxTargets | Where-Object { [string]$_ -eq "five_min_alpha" }).Count -le 0) {
                    $result.reasons += "LOB_AUXILIARY_TARGETS_INCOMPLETE"
                }
            }
        }
        "v5_tradability" {
            if ([string]$ArtifactName -eq "tradability_model_contract.json") {
                if ([string](Get-PropValue -ObjectValue $payload -Name "policy" -DefaultValue "") -ne "v5_tradability_v1") {
                    $result.reasons += "TRADABILITY_MODEL_POLICY_INVALID"
                }
                $inputExperts = Get-PropValue -ObjectValue $payload -Name "input_experts" -DefaultValue @{}
                foreach ($requiredExpert in @("panel", "sequence", "lob")) {
                    $expertPayload = Get-PropValue -ObjectValue $inputExperts -Name $requiredExpert -DefaultValue @{}
                    if (Test-IsEffectivelyEmptyObject -ObjectValue $expertPayload) {
                        $result.reasons += ("TRADABILITY_INPUT_" + $requiredExpert.ToUpperInvariant() + "_MISSING")
                    }
                }
            }
        }
    }
    if ([string]$ArtifactName -eq "domain_weighting_report.json") {
        if ([string](Get-PropValue -ObjectValue $payload -Name "policy" -DefaultValue "") -ne "v5_domain_weighting_v1") {
            $result.reasons += "DOMAIN_WEIGHTING_POLICY_INVALID"
        }
        $effectiveSummary = Get-PropValue -ObjectValue $payload -Name "effective_sample_weight_summary" -DefaultValue @{}
        if (Test-IsEffectivelyEmptyObject -ObjectValue $effectiveSummary) {
            $result.reasons += "DOMAIN_WEIGHTING_SUMMARY_MISSING"
        }
        $sourceKind = [string](Get-PropValue -ObjectValue (Get-PropValue -ObjectValue $payload -Name "domain_details" -DefaultValue @{}) -Name "source_kind" -DefaultValue "")
        if ([string]::IsNullOrWhiteSpace($sourceKind)) {
            $result.reasons += "DOMAIN_WEIGHTING_SOURCE_KIND_MISSING"
        } elseif (([string]$TrainerName).Trim().ToLowerInvariant() -in @("v5_sequence", "v5_lob", "v5_tradability") -and $sourceKind -ne "regime_inverse_frequency_v1") {
            $result.reasons += "DOMAIN_WEIGHTING_SOURCE_KIND_INVALID"
        }
    }
    $result.pass = (@($result.reasons).Count -eq 0)
    return $result
}

function Resolve-DependencyTrainerTailMode {
    param(
        [string]$TrainerName,
        [Parameter(Mandatory = $false)]$TrainConfig
    )
    $normalizedTrainer = ([string]$TrainerName).Trim().ToLowerInvariant()
    if ($normalizedTrainer -eq "v5_panel_ensemble") {
        $dependencyExpertOnly = To-Bool (Get-PropValue -ObjectValue $TrainConfig -Name "dependency_expert_only" -DefaultValue $false) $false
        if ($dependencyExpertOnly) {
            return "dependency_expert_only"
        }
        return "full"
    }
    if ($normalizedTrainer -in @("v5_sequence", "v5_lob", "v5_tradability", "v5_fusion")) {
        return "expert_tail"
    }
    return "standard"
}

function Test-DependencyTrainerRunReusable {
    param(
        [string]$RunDir,
        [string]$TrainerName,
        [string]$ModelFamily,
        [string]$TrainStartDate,
        [string]$TrainEndDate,
        [string]$ExecutionEvalStartDate,
        [string]$ExecutionEvalEndDate,
        [string]$ExpectedSnapshotId
    )
    $runId = if ([string]::IsNullOrWhiteSpace($RunDir)) { "" } else { Split-Path -Leaf $RunDir }
    $result = [ordered]@{
        reusable = $false
        reason = "RUN_DIR_MISSING"
        required_artifacts_complete = $false
        trainer = $TrainerName
        model_family = $ModelFamily
        run_dir = $RunDir
        run_id = $runId
        data_platform_ready_snapshot_id = ""
        tail_mode = "standard"
    }
    if ([string]::IsNullOrWhiteSpace($RunDir) -or (-not (Test-Path $RunDir))) {
        return $result
    }

    $requiredArtifactNames = @(Get-DependencyTrainerRequiredArtifacts -TrainerName $TrainerName)
    $requiredArtifactPaths = @{}
    $missingArtifacts = New-Object System.Collections.Generic.List[string]
    foreach ($artifactName in $requiredArtifactNames) {
        $artifactPath = Join-Path $RunDir $artifactName
        $requiredArtifactPaths[$artifactName] = $artifactPath
        if (-not (Test-Path $artifactPath)) {
            $missingArtifacts.Add($artifactName) | Out-Null
        }
    }
    $result.required_artifacts_complete = ($missingArtifacts.Count -eq 0)
    if (-not $result.required_artifacts_complete) {
        $result.reason = "REQUIRED_ARTIFACTS_MISSING:" + (($missingArtifacts.ToArray()) -join ",")
        return $result
    }

    $trainConfig = Load-JsonOrEmpty -PathValue ([string]$requiredArtifactPaths["train_config.yaml"])
    $artifactStatus = Load-JsonOrEmpty -PathValue ([string]$requiredArtifactPaths["artifact_status.json"])
    $result.data_platform_ready_snapshot_id = [string](Get-PropValue -ObjectValue $trainConfig -Name "data_platform_ready_snapshot_id" -DefaultValue "")
    $result.tail_mode = Resolve-DependencyTrainerTailMode -TrainerName $TrainerName -TrainConfig $trainConfig
    $expectedRunScope = ([string]$RunScope).Trim() + "_dependency_" + $TrainerName

    $checks = @(
        [ordered]@{ ok = ([string](Get-PropValue -ObjectValue $trainConfig -Name "trainer" -DefaultValue "") -eq [string]$TrainerName); reason = "TRAINER_MISMATCH" },
        [ordered]@{ ok = ([string](Get-PropValue -ObjectValue $trainConfig -Name "model_family" -DefaultValue "") -eq [string]$ModelFamily); reason = "MODEL_FAMILY_MISMATCH" },
        [ordered]@{ ok = ([string](Get-PropValue -ObjectValue $trainConfig -Name "feature_set" -DefaultValue "") -eq [string]$FeatureSet); reason = "FEATURE_SET_MISMATCH" },
        [ordered]@{ ok = ([string](Get-PropValue -ObjectValue $trainConfig -Name "label_set" -DefaultValue "") -eq [string]$LabelSet); reason = "LABEL_SET_MISMATCH" },
        [ordered]@{ ok = ([string](Get-PropValue -ObjectValue $trainConfig -Name "task" -DefaultValue "") -eq [string]$Task); reason = "TASK_MISMATCH" },
        [ordered]@{ ok = ([string](Get-PropValue -ObjectValue $trainConfig -Name "run_scope" -DefaultValue "") -eq $expectedRunScope); reason = "RUN_SCOPE_MISMATCH" },
        [ordered]@{ ok = ([string](Get-PropValue -ObjectValue $trainConfig -Name "tf" -DefaultValue "") -eq [string]$Tf); reason = "TF_MISMATCH" },
        [ordered]@{ ok = ([string](Get-PropValue -ObjectValue $trainConfig -Name "quote" -DefaultValue "") -eq [string]$Quote); reason = "QUOTE_MISMATCH" },
        [ordered]@{ ok = ([int](Get-PropValue -ObjectValue $trainConfig -Name "top_n" -DefaultValue 0) -eq [int]$TrainTopN); reason = "TOP_N_MISMATCH" },
        [ordered]@{ ok = ([string](Get-PropValue -ObjectValue $trainConfig -Name "start" -DefaultValue "") -eq [string]$TrainStartDate); reason = "TRAIN_START_MISMATCH" },
        [ordered]@{ ok = ([string](Get-PropValue -ObjectValue $trainConfig -Name "end" -DefaultValue "") -eq [string]$TrainEndDate); reason = "TRAIN_END_MISMATCH" },
        [ordered]@{ ok = ([string](Get-PropValue -ObjectValue $trainConfig -Name "execution_acceptance_eval_start" -DefaultValue "") -eq [string]$ExecutionEvalStartDate); reason = "EXECUTION_EVAL_START_MISMATCH" },
        [ordered]@{ ok = ([string](Get-PropValue -ObjectValue $trainConfig -Name "execution_acceptance_eval_end" -DefaultValue "") -eq [string]$ExecutionEvalEndDate); reason = "EXECUTION_EVAL_END_MISMATCH" },
        [ordered]@{ ok = ([int](Get-PropValue -ObjectValue $trainConfig -Name "seed" -DefaultValue 0) -eq [int]$Seed); reason = "SEED_MISMATCH" },
        [ordered]@{ ok = ([string](Get-PropValue -ObjectValue $trainConfig -Name "data_platform_ready_snapshot_id" -DefaultValue "") -eq [string]$ExpectedSnapshotId); reason = "SNAPSHOT_ID_MISMATCH" },
        [ordered]@{ ok = (To-Bool (Get-PropValue -ObjectValue $artifactStatus -Name "core_saved" -DefaultValue $false) $false); reason = "ARTIFACT_STATUS_CORE_SAVED_MISSING" },
        [ordered]@{ ok = (To-Bool (Get-PropValue -ObjectValue $artifactStatus -Name "support_artifacts_written" -DefaultValue $false) $false); reason = "ARTIFACT_STATUS_SUPPORT_ARTIFACTS_MISSING" },
        [ordered]@{ ok = (To-Bool (Get-PropValue -ObjectValue $artifactStatus -Name "expert_prediction_table_complete" -DefaultValue $false) $false); reason = "ARTIFACT_STATUS_EXPERT_TABLE_MISSING" }
    )
    foreach ($check in $checks) {
        if (-not [bool](Get-PropValue -ObjectValue $check -Name "ok" -DefaultValue $false)) {
            $result.reason = [string](Get-PropValue -ObjectValue $check -Name "reason" -DefaultValue "MISMATCH")
            return $result
        }
    }

    if (([string]$TrainerName).Trim().ToLowerInvariant() -eq "v5_panel_ensemble") {
        $dependencyExpertOnly = To-Bool (Get-PropValue -ObjectValue $trainConfig -Name "dependency_expert_only" -DefaultValue $false) $false
        if (-not $dependencyExpertOnly) {
            $result.reason = "PANEL_DEPENDENCY_EXPERT_ONLY_REQUIRED"
            return $result
        }
    }

    $result.reusable = $true
    $result.reason = "MATCHING_RUN_REUSED"
    return $result
}

function Resolve-ReusableDependencyTrainerRun {
    param(
        [string]$RegistryRoot,
        [string]$TrainerName,
        [string]$ModelFamily,
        [string]$TrainStartDate,
        [string]$TrainEndDate,
        [string]$ExecutionEvalStartDate,
        [string]$ExecutionEvalEndDate,
        [string]$ExpectedSnapshotId
    )
    $familyRoot = Join-Path $RegistryRoot $ModelFamily
    $default = [ordered]@{
        reusable = $false
        reason = "NO_REUSABLE_RUN_FOUND"
        required_artifacts_complete = $false
        trainer = $TrainerName
        model_family = $ModelFamily
        run_dir = ""
        run_id = ""
        data_platform_ready_snapshot_id = ""
        tail_mode = "standard"
    }
    if ([string]::IsNullOrWhiteSpace($RegistryRoot) -or (-not (Test-Path $familyRoot))) {
        return $default
    }

    $candidateRunDirs = New-Object System.Collections.Generic.List[string]
    $latestPath = Join-Path $familyRoot "latest.json"
    if (Test-Path $latestPath) {
        $latestPayload = Load-JsonOrEmpty -PathValue $latestPath
        $latestRunId = [string](Get-PropValue -ObjectValue $latestPayload -Name "run_id" -DefaultValue "")
        if (-not [string]::IsNullOrWhiteSpace($latestRunId)) {
            $latestRunDir = Join-Path $familyRoot $latestRunId
            if (Test-Path $latestRunDir) {
                $candidateRunDirs.Add($latestRunDir) | Out-Null
            }
        }
    }

    foreach ($runDir in @(Get-ChildItem -Path $familyRoot -Directory | Sort-Object Name -Descending | Select-Object -ExpandProperty FullName)) {
        if (-not $candidateRunDirs.Contains([string]$runDir)) {
            $candidateRunDirs.Add([string]$runDir) | Out-Null
        }
    }

    $lastChecked = $default
    foreach ($candidateRunDir in $candidateRunDirs.ToArray()) {
        $checked = Test-DependencyTrainerRunReusable `
            -RunDir $candidateRunDir `
            -TrainerName $TrainerName `
            -ModelFamily $ModelFamily `
            -TrainStartDate $TrainStartDate `
            -TrainEndDate $TrainEndDate `
            -ExecutionEvalStartDate $ExecutionEvalStartDate `
            -ExecutionEvalEndDate $ExecutionEvalEndDate `
            -ExpectedSnapshotId $ExpectedSnapshotId
        if ([bool](Get-PropValue -ObjectValue $checked -Name "reusable" -DefaultValue $false)) {
            return $checked
        }
        $lastChecked = $checked
    }
    if ([string]::IsNullOrWhiteSpace([string](Get-PropValue -ObjectValue $lastChecked -Name "reason" -DefaultValue ""))) {
        return $default
    }
    return $lastChecked
}

function Get-MicroDateCoverageCounts {
    param(
        [string]$MicroRoot,
        [string]$Tf
    )
    $counts = @{}
    if ([string]::IsNullOrWhiteSpace($MicroRoot) -or (-not (Test-Path $MicroRoot))) {
        return $counts
    }
    $tfRoot = Join-Path $MicroRoot ("tf=" + $Tf)
    if (-not (Test-Path $tfRoot)) {
        return $counts
    }
    $marketDirs = @(Get-ChildItem -Path $tfRoot -Directory -Filter "market=*")
    foreach ($marketDir in $marketDirs) {
        foreach ($dateDir in @(Get-ChildItem -Path $marketDir.FullName -Directory -Filter "date=*")) {
            if ($dateDir.Name -notmatch "^date=(\d{4}-\d{2}-\d{2})$") {
                continue
            }
            $dateValue = [string]$Matches[1]
            if (-not $counts.ContainsKey($dateValue)) {
                $counts[$dateValue] = 0
            }
            $counts[$dateValue] = [int]$counts[$dateValue] + 1
        }
    }
    return $counts
}

function Convert-ToStringIntMap {
    param([Parameter(Mandatory = $false)]$Value)
    $result = @{}
    if ($null -eq $Value) {
        return $result
    }
    if ($Value -is [System.Collections.IDictionary]) {
        foreach ($key in @($Value.Keys)) {
            $name = [string]$key
            if ([string]::IsNullOrWhiteSpace($name)) {
                continue
            }
            try {
                $result[$name] = [int]$Value[$key]
            } catch {
            }
        }
        return $result
    }
    if ($Value.PSObject) {
        foreach ($prop in @($Value.PSObject.Properties)) {
            $name = [string]$prop.Name
            if ([string]::IsNullOrWhiteSpace($name)) {
                continue
            }
            try {
                $result[$name] = [int]$prop.Value
            } catch {
            }
        }
    }
    return $result
}

function Resolve-TrainSnapshotCloseContract {
    param(
        [string]$ProjectRoot,
        [string]$ReportPath,
        [string]$ExpectedBatchDate,
        [string]$ExpectedSnapshotId
    )
    $resolvedReportPath = if ([string]::IsNullOrWhiteSpace($ReportPath)) {
        Resolve-DefaultTrainSnapshotCloseReportPath -Root $ProjectRoot
    } else {
        Resolve-PathFromProjectRoot -Root $ProjectRoot -PathValue $ReportPath
    }
    $payload = Load-JsonOrEmpty -PathValue $resolvedReportPath
    $exists = (Test-Path $resolvedReportPath) -and (-not (Test-IsEffectivelyEmptyObject -ObjectValue $payload))
    $reportBatchDate = [string](Get-PropValue -ObjectValue $payload -Name "batch_date" -DefaultValue "")
    $reportSnapshotId = [string](Get-PropValue -ObjectValue $payload -Name "snapshot_id" -DefaultValue "")
    $overallPass = To-Bool (Get-PropValue -ObjectValue $payload -Name "overall_pass" -DefaultValue $false) $false
    $deadlineMet = To-Bool (Get-PropValue -ObjectValue $payload -Name "deadline_met" -DefaultValue $false) $false
    $publishedAtUtc = [string](Get-PropValue -ObjectValue $payload -Name "published_at_utc" -DefaultValue "")
    $generatedAtUtc = [string](Get-PropValue -ObjectValue $payload -Name "generated_at_utc" -DefaultValue "")
    $snapshotRoot = [string](Get-PropValue -ObjectValue $payload -Name "snapshot_root" -DefaultValue "")
    $microCoverageCounts = Convert-ToStringIntMap -Value (Get-PropValue -ObjectValue $payload -Name "micro_date_coverage_counts" -DefaultValue @{})
    $microRoot = [string](Get-PropValue -ObjectValue $payload -Name "micro_root" -DefaultValue "")
    $failureReasons = @(
        @(Get-PropValue -ObjectValue $payload -Name "failure_reasons" -DefaultValue @()) |
            ForEach-Object { [string]$_ } |
            Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    )
    $sourceFreshness = Get-PropValue -ObjectValue $payload -Name "source_freshness" -DefaultValue @{}

    $reasons = @()
    if (-not $exists) {
        $reasons += "TRAIN_SNAPSHOT_CLOSE_REPORT_MISSING"
    }
    if ($exists -and ([string]$reportBatchDate).Trim() -ne ([string]$ExpectedBatchDate).Trim()) {
        $reasons += "TRAIN_SNAPSHOT_CLOSE_BATCH_DATE_MISMATCH"
    }
    if ($exists -and (-not $overallPass)) {
        $reasons += "TRAIN_SNAPSHOT_CLOSE_FAILED"
    }
    if ($exists -and ([string]$reportSnapshotId).Trim() -ne ([string]$ExpectedSnapshotId).Trim()) {
        $reasons += "TRAIN_SNAPSHOT_CLOSE_SNAPSHOT_ID_MISMATCH"
    }
    if ($exists -and (-not $deadlineMet)) {
        $reasons += "TRAIN_SNAPSHOT_CLOSE_DEADLINE_MISSED"
    }

    $coverageWindowPayload = Get-PropValue -ObjectValue $payload -Name "coverage_window" -DefaultValue @{}
    $trainWindowPayload = Get-PropValue -ObjectValue $payload -Name "train_window" -DefaultValue @{}
    $certificationWindowPayload = Get-PropValue -ObjectValue $payload -Name "certification_window" -DefaultValue @{}
    $coverageStartDate = [string](Get-PropValue -ObjectValue $coverageWindowPayload -Name "start" -DefaultValue (Get-PropValue -ObjectValue $payload -Name "training_critical_start_date" -DefaultValue ""))
    $coverageEndDate = [string](Get-PropValue -ObjectValue $coverageWindowPayload -Name "end" -DefaultValue (Get-PropValue -ObjectValue $payload -Name "training_critical_end_date" -DefaultValue ""))
    $featuresV4EffectiveEnd = [string](Get-PropValue -ObjectValue $payload -Name "features_v4_effective_end" -DefaultValue "")
    if ([string]::IsNullOrWhiteSpace($featuresV4EffectiveEnd)) {
        $featureRefresh = Get-PropValue -ObjectValue $payload -Name "feature_contract_refresh" -DefaultValue @{}
        $featuresV4EffectiveEnd = [string](Get-PropValue -ObjectValue $featureRefresh -Name "features_v4_effective_end" -DefaultValue "")
    }

    return [ordered]@{
        attempted = $true
        report_path = $resolvedReportPath
        exists = $exists
        batch_date = $reportBatchDate
        expected_batch_date = $ExpectedBatchDate
        snapshot_id = $reportSnapshotId
        expected_snapshot_id = $ExpectedSnapshotId
        snapshot_root = $snapshotRoot
        generated_at_utc = $generatedAtUtc
        published_at_utc = $publishedAtUtc
        overall_pass = $overallPass
        deadline_met = $deadlineMet
        source_freshness = $sourceFreshness
        micro_root = $microRoot
        micro_date_coverage_counts = $microCoverageCounts
        coverage_window = [ordered]@{
            start = $coverageStartDate
            end = $coverageEndDate
        }
        train_window = [ordered]@{
            start = [string](Get-PropValue -ObjectValue $trainWindowPayload -Name "start" -DefaultValue "")
            end = [string](Get-PropValue -ObjectValue $trainWindowPayload -Name "end" -DefaultValue "")
        }
        certification_window = [ordered]@{
            start = [string](Get-PropValue -ObjectValue $certificationWindowPayload -Name "start" -DefaultValue "")
            end = [string](Get-PropValue -ObjectValue $certificationWindowPayload -Name "end" -DefaultValue "")
        }
        training_critical_start_date = $coverageStartDate
        training_critical_end_date = $coverageEndDate
        training_critical_refresh = Get-PropValue -ObjectValue $payload -Name "training_critical_refresh" -DefaultValue @{}
        coverage_window_source = [string](Get-PropValue -ObjectValue $payload -Name "coverage_window_source" -DefaultValue "")
        refresh_argument_mode = [string](Get-PropValue -ObjectValue $payload -Name "refresh_argument_mode" -DefaultValue "")
        features_v4_effective_end = $featuresV4EffectiveEnd
        failure_reasons = @($failureReasons)
        pass = (@($reasons).Count -eq 0)
        reasons = @($reasons)
    }
}

function Test-DateWindowContains {
    param(
        [string]$OuterStartDate,
        [string]$OuterEndDate,
        [string]$InnerStartDate,
        [string]$InnerEndDate
    )
    if (
        [string]::IsNullOrWhiteSpace($OuterStartDate) -or
        [string]::IsNullOrWhiteSpace($OuterEndDate) -or
        [string]::IsNullOrWhiteSpace($InnerStartDate) -or
        [string]::IsNullOrWhiteSpace($InnerEndDate)
    ) {
        return $false
    }
    $outerStartObj = [DateTime]::ParseExact($OuterStartDate, "yyyy-MM-dd", [System.Globalization.CultureInfo]::InvariantCulture)
    $outerEndObj = [DateTime]::ParseExact($OuterEndDate, "yyyy-MM-dd", [System.Globalization.CultureInfo]::InvariantCulture)
    $innerStartObj = [DateTime]::ParseExact($InnerStartDate, "yyyy-MM-dd", [System.Globalization.CultureInfo]::InvariantCulture)
    $innerEndObj = [DateTime]::ParseExact($InnerEndDate, "yyyy-MM-dd", [System.Globalization.CultureInfo]::InvariantCulture)
    return (($outerStartObj -le $innerStartObj) -and ($outerEndObj -ge $innerEndObj))
}

function Assert-TrainingCriticalCoverageWindow {
    param(
        [Parameter(Mandatory = $true)]$TrainSnapshotCloseContract,
        [string]$TrainWindowStart,
        [string]$TrainWindowEnd,
        [string]$CertificationWindowStart,
        [string]$CertificationWindowEnd
    )
    $coverageWindow = Get-PropValue -ObjectValue $TrainSnapshotCloseContract -Name "coverage_window" -DefaultValue @{}
    $coverageStartDate = [string](Get-PropValue -ObjectValue $coverageWindow -Name "start" -DefaultValue (Get-PropValue -ObjectValue $TrainSnapshotCloseContract -Name "training_critical_start_date" -DefaultValue ""))
    $coverageEndDate = [string](Get-PropValue -ObjectValue $coverageWindow -Name "end" -DefaultValue (Get-PropValue -ObjectValue $TrainSnapshotCloseContract -Name "training_critical_end_date" -DefaultValue ""))
    $reasons = @()
    $trainWindowCovered = $true
    $certificationWindowCovered = $true

    if ([string]::IsNullOrWhiteSpace($coverageStartDate) -or [string]::IsNullOrWhiteSpace($coverageEndDate)) {
        $trainWindowCovered = $false
        $certificationWindowCovered = $false
        $reasons += "TRAIN_SNAPSHOT_CLOSE_COVERAGE_WINDOW_MISSING"
    } else {
        if (-not [string]::IsNullOrWhiteSpace($TrainWindowStart)) {
            $trainWindowCovered = Test-DateWindowContains `
                -OuterStartDate $coverageStartDate `
                -OuterEndDate $coverageEndDate `
                -InnerStartDate $TrainWindowStart `
                -InnerEndDate $TrainWindowEnd
            if (-not $trainWindowCovered) {
                $reasons += "TRAIN_SNAPSHOT_CLOSE_TRAIN_WINDOW_OUTSIDE_COVERAGE"
            }
        }
        if (-not [string]::IsNullOrWhiteSpace($CertificationWindowStart)) {
            $certificationWindowCovered = Test-DateWindowContains `
                -OuterStartDate $coverageStartDate `
                -OuterEndDate $coverageEndDate `
                -InnerStartDate $CertificationWindowStart `
                -InnerEndDate $CertificationWindowEnd
            if (-not $certificationWindowCovered) {
                $reasons += "TRAIN_SNAPSHOT_CLOSE_CERTIFICATION_WINDOW_OUTSIDE_COVERAGE"
            }
        }
    }

    return [ordered]@{
        pass = (@($reasons).Count -eq 0)
        coverage_start_date = $coverageStartDate
        coverage_end_date = $coverageEndDate
        train_window_start = $TrainWindowStart
        train_window_end = $TrainWindowEnd
        certification_window_start = $CertificationWindowStart
        certification_window_end = $CertificationWindowEnd
        train_window_covered = $trainWindowCovered
        certification_window_covered = $certificationWindowCovered
        reasons = @($reasons)
    }
}

function Resolve-TrainWindowRamp {
    param(
        [string]$ProjectRoot,
        [string]$BatchDate,
        [string]$Tf,
        [int]$RequestedTrainLookbackDays,
        [int]$RequestedBacktestLookbackDays,
        [bool]$RampEnabled,
        [string]$MicroRoot,
        [int]$MinMarketsPerDate,
        [string]$TrainDataQualityFloorDate,
        [string]$TrainStartFloorDate,
        [Parameter(Mandatory = $false)]$CoverageCounts = $null
    )
    $resolvedMicroRoot = Resolve-PathFromProjectRoot -Root $ProjectRoot -PathValue $MicroRoot
    $batchDateValue = Resolve-DateToken -DateText $BatchDate -LabelForError "batch_date"
    $batchDateObj = [DateTime]::ParseExact($batchDateValue, "yyyy-MM-dd", [System.Globalization.CultureInfo]::InvariantCulture)
    $trainDataQualityFloorInput = if ([string]::IsNullOrWhiteSpace($TrainDataQualityFloorDate)) {
        $TrainStartFloorDate
    } else {
        $TrainDataQualityFloorDate
    }
    $trainDataQualityFloorDateValue = if ([string]::IsNullOrWhiteSpace($trainDataQualityFloorInput)) {
        ""
    } else {
        Resolve-DateToken -DateText $trainDataQualityFloorInput -LabelForError "train_data_quality_floor_date"
    }
    $trainDataQualityFloorObj = if ([string]::IsNullOrWhiteSpace($trainDataQualityFloorDateValue)) {
        $null
    } else {
        [DateTime]::ParseExact($trainDataQualityFloorDateValue, "yyyy-MM-dd", [System.Globalization.CultureInfo]::InvariantCulture)
    }
    $requestedTrainDays = [Math]::Max([int]$RequestedTrainLookbackDays, 1)
    $requestedBacktestDays = [Math]::Max([int]$RequestedBacktestLookbackDays, 1)
    $minMarkets = [Math]::Max([int]$MinMarketsPerDate, 1)
    $coverageCounts = Convert-ToStringIntMap -Value $CoverageCounts
    $coverageSource = "train_snapshot_close_report"
    if ($coverageCounts.Count -eq 0) {
        $coverageCounts = Get-MicroDateCoverageCounts -MicroRoot $resolvedMicroRoot -Tf $Tf
        $coverageSource = "mutable_micro_root"
    }
    $availableDates = @(
        $coverageCounts.Keys |
            Sort-Object
    )
    $coveragePresent = $availableDates.Count -gt 0
    $availableContiguousDays = 0
    if ($coveragePresent) {
        $cursor = $batchDateObj
        while ($true) {
            $cursorText = $cursor.ToString("yyyy-MM-dd")
            if ((-not $coverageCounts.ContainsKey($cursorText)) -or ([int]$coverageCounts[$cursorText] -lt $minMarkets)) {
                break
            }
            $availableContiguousDays += 1
            $cursor = $cursor.AddDays(-1)
        }
    }
    $effectiveTrainDays = $requestedTrainDays
    $reason = "DISABLED"
    if (-not $RampEnabled) {
        $reason = "DISABLED"
    } elseif (-not $coveragePresent) {
        $reason = "MICRO_COVERAGE_UNAVAILABLE_FALLBACK"
    } else {
        $effectiveTrainDays = [Math]::Min(
            $requestedTrainDays,
            [Math]::Max($availableContiguousDays - $requestedBacktestDays, 0)
        )
        if ($effectiveTrainDays -le 0) {
            $reason = "INSUFFICIENT_CONTIGUOUS_MICRO_HISTORY"
        } elseif ($effectiveTrainDays -lt $requestedTrainDays) {
            $reason = "RAMP_ACTIVE"
        } else {
            $reason = "TARGET_REACHED"
        }
    }
    $certificationStartDate = $batchDateObj.AddDays(-1 * [Math]::Max($requestedBacktestDays - 1, 0)).ToString("yyyy-MM-dd")
    $trainEndDate = $batchDateObj.AddDays(-1 * [Math]::Max($requestedBacktestDays, 0)).ToString("yyyy-MM-dd")
    $trainStartDate = ""
    $trainDataQualityFloorApplied = $false
    if ($effectiveTrainDays -gt 0) {
        $trainEndObj = [DateTime]::ParseExact($trainEndDate, "yyyy-MM-dd", [System.Globalization.CultureInfo]::InvariantCulture)
        $trainStartDate = $trainEndObj.AddDays(-1 * [Math]::Max($effectiveTrainDays - 1, 0)).ToString("yyyy-MM-dd")
        if ($null -ne $trainDataQualityFloorObj) {
            $trainStartObj = [DateTime]::ParseExact($trainStartDate, "yyyy-MM-dd", [System.Globalization.CultureInfo]::InvariantCulture)
            if ($trainStartObj -lt $trainDataQualityFloorObj) {
                $trainDataQualityFloorApplied = $true
                if ($trainEndObj -lt $trainDataQualityFloorObj) {
                    $trainStartDate = ""
                    $effectiveTrainDays = 0
                    $reason = "TRAIN_DATA_QUALITY_FLOOR_AFTER_TRAIN_END"
                } else {
                    $trainStartDate = $trainDataQualityFloorDateValue
                    $effectiveTrainDays = [int](($trainEndObj - $trainDataQualityFloorObj).TotalDays) + 1
                    $reason = "TRAIN_DATA_QUALITY_FLOOR_ACTIVE"
                }
            }
        }
    }
    return [ordered]@{
        enabled = [bool]$RampEnabled
        micro_root = $resolvedMicroRoot
        micro_coverage_source = $coverageSource
        micro_coverage_present = $coveragePresent
        requested_train_lookback_days = [int]$requestedTrainDays
        requested_backtest_lookback_days = [int]$requestedBacktestDays
        effective_train_lookback_days = [int]$effectiveTrainDays
        min_markets_per_date = [int]$minMarkets
        available_contiguous_micro_days = [int]$availableContiguousDays
        available_market_dates = @($availableDates)
        first_available_micro_date = if ($availableDates.Count -gt 0) { [string]$availableDates[0] } else { "" }
        last_available_micro_date = if ($availableDates.Count -gt 0) { [string]$availableDates[-1] } else { "" }
        ramp_active = (([bool]$RampEnabled) -and ($coveragePresent) -and ($effectiveTrainDays -lt $requestedTrainDays)) -or $trainDataQualityFloorApplied
        comparable_window_available = ($effectiveTrainDays -gt 0)
        reason = $reason
        train_start_date = $trainStartDate
        train_end_date = $trainEndDate
        certification_start_date = $certificationStartDate
        certification_end_date = $batchDateValue
        train_data_quality_floor_date = $trainDataQualityFloorDateValue
        train_data_quality_floor_applied = $trainDataQualityFloorApplied
        train_start_floor_date = $trainDataQualityFloorDateValue
        train_start_floor_applied = $trainDataQualityFloorApplied
    }
}

function Load-JsonOrEmpty {
    param([string]$PathValue)
    if ([string]::IsNullOrWhiteSpace($PathValue) -or (-not (Test-Path $PathValue))) {
        return @{}
    }
    $raw = Get-Content -Path $PathValue -Raw -Encoding UTF8
    if ([string]::IsNullOrWhiteSpace($raw)) {
        return @{}
    }
    return $raw | ConvertFrom-Json
}

function Write-JsonFile {
    param(
        [string]$PathValue,
        [Parameter(Mandatory = $false)]$Payload,
        [int]$Depth = 12
    )
    if ([string]::IsNullOrWhiteSpace($PathValue)) {
        return
    }
    $parent = Split-Path -Parent $PathValue
    if (-not [string]::IsNullOrWhiteSpace($parent)) {
        New-Item -ItemType Directory -Path $parent -Force | Out-Null
    }
    ($Payload | ConvertTo-Json -Depth $Depth) | Set-Content -Path $PathValue -Encoding UTF8
}

function Test-IsEffectivelyEmptyObject {
    param([Parameter(Mandatory = $false)]$ObjectValue)
    if ($null -eq $ObjectValue) {
        return $true
    }
    if ($ObjectValue -is [System.Collections.IDictionary]) {
        return $ObjectValue.Count -eq 0
    }
    if ($ObjectValue.PSObject) {
        $ownProperties = @(
            $ObjectValue.PSObject.Properties |
                Where-Object { $_.MemberType -in @("NoteProperty", "Property") }
        )
        return $ownProperties.Count -eq 0
    }
    return $false
}

function Get-PropValue {
    param(
        [Parameter(Mandatory = $false)]$ObjectValue,
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $false)]$DefaultValue = $null
    )
    if ($null -eq $ObjectValue) {
        return $DefaultValue
    }
    if ($ObjectValue -is [System.Collections.IDictionary]) {
        if ($ObjectValue.Contains($Name)) {
            return $ObjectValue[$Name]
        }
        return $DefaultValue
    }
    $propertyNames = @(
        $ObjectValue.PSObject.Properties |
            Where-Object { $null -ne $_ -and $null -ne $_.Name } |
            ForEach-Object { [string]$_.Name }
    )
    if ($propertyNames -contains $Name) {
        return $ObjectValue.$Name
    }
    return $DefaultValue
}

function Convert-ToStringArray {
    param([Parameter(Mandatory = $false)]$Value)
    if ($null -eq $Value) {
        Write-Output -NoEnumerate @()
        return
    }
    if ($Value -is [string]) {
        if ([string]::IsNullOrWhiteSpace($Value)) {
            Write-Output -NoEnumerate @()
            return
        }
        Write-Output -NoEnumerate @($Value.Trim())
        return
    }
    $items = @()
    if ($Value -is [System.Array] -or $Value -is [System.Collections.IEnumerable]) {
        foreach ($item in $Value) {
            $text = [string]$item
            if (-not [string]::IsNullOrWhiteSpace($text)) {
                $items += $text.Trim()
            }
        }
        Write-Output -NoEnumerate @($items)
        return
    }
    $text = [string]$Value
    if ([string]::IsNullOrWhiteSpace($text)) {
        Write-Output -NoEnumerate @()
        return
    }
    Write-Output -NoEnumerate @($text.Trim())
}

function To-Double {
    param([Parameter(Mandatory = $false)]$Value, [double]$DefaultValue = 0.0)
    try {
        if ($null -eq $Value) {
            return $DefaultValue
        }
        return [double]$Value
    } catch {
        return $DefaultValue
    }
}

function To-Int64 {
    param([Parameter(Mandatory = $false)]$Value, [long]$DefaultValue = 0)
    try {
        if ($null -eq $Value) {
            return $DefaultValue
        }
        return [long]$Value
    } catch {
        return $DefaultValue
    }
}

function To-Bool {
    param([Parameter(Mandatory = $false)]$Value, [bool]$DefaultValue = $false)
    if ($null -eq $Value) {
        return $DefaultValue
    }
    try {
        return [bool]$Value
    } catch {
        return $DefaultValue
    }
}

function Get-NullableDouble {
    param([Parameter(Mandatory = $false)]$Value)
    if ($null -eq $Value) {
        return $null
    }
    try {
        return [double]$Value
    } catch {
        return $null
    }
}

function Get-ArtifactHashInfo {
    param([string]$PathValue)
    $info = [ordered]@{
        path = $PathValue
        exists = $false
        size_bytes = $null
        sha256 = ""
    }
    if ([string]::IsNullOrWhiteSpace($PathValue) -or (-not (Test-Path -LiteralPath $PathValue -PathType Leaf))) {
        return $info
    }
    $item = Get-Item -LiteralPath $PathValue
    $info.exists = $true
    $info.size_bytes = [int64]$item.Length
    try {
        $hash = Get-FileHash -LiteralPath $PathValue -Algorithm SHA256
        $info.sha256 = [string]$hash.Hash
    } catch {
        $info.sha256 = ""
    }
    return $info
}

function Resolve-DuplicateCandidateArtifacts {
    param(
        [string]$CandidateRunDir,
        [string]$ChampionRunDir
    )
    $result = [ordered]@{
        evaluated = $false
        duplicate = $false
        basis = "model_bin_and_thresholds_sha256"
        candidate = [ordered]@{
            model_bin = (Get-ArtifactHashInfo -PathValue "")
            thresholds = (Get-ArtifactHashInfo -PathValue "")
        }
        champion = [ordered]@{
            model_bin = (Get-ArtifactHashInfo -PathValue "")
            thresholds = (Get-ArtifactHashInfo -PathValue "")
        }
        reasons = @()
    }
    if ([string]::IsNullOrWhiteSpace($CandidateRunDir)) {
        $result.reasons += "MISSING_CANDIDATE_RUN_DIR"
        return $result
    }
    if ([string]::IsNullOrWhiteSpace($ChampionRunDir)) {
        $result.reasons += "MISSING_CHAMPION_RUN_DIR"
        return $result
    }

    $candidateModelPath = Join-Path $CandidateRunDir "model.bin"
    $candidateThresholdsPath = Join-Path $CandidateRunDir "thresholds.json"
    $championModelPath = Join-Path $ChampionRunDir "model.bin"
    $championThresholdsPath = Join-Path $ChampionRunDir "thresholds.json"

    $result.candidate.model_bin = Get-ArtifactHashInfo -PathValue $candidateModelPath
    $result.candidate.thresholds = Get-ArtifactHashInfo -PathValue $candidateThresholdsPath
    $result.champion.model_bin = Get-ArtifactHashInfo -PathValue $championModelPath
    $result.champion.thresholds = Get-ArtifactHashInfo -PathValue $championThresholdsPath

    foreach ($entry in @(
        @{ prefix = "CANDIDATE"; label = "MODEL_BIN"; node = $result.candidate.model_bin },
        @{ prefix = "CANDIDATE"; label = "THRESHOLDS"; node = $result.candidate.thresholds },
        @{ prefix = "CHAMPION"; label = "MODEL_BIN"; node = $result.champion.model_bin },
        @{ prefix = "CHAMPION"; label = "THRESHOLDS"; node = $result.champion.thresholds }
    )) {
        if (-not (To-Bool (Get-PropValue -ObjectValue $entry.node -Name "exists" -DefaultValue $false) $false)) {
            $result.reasons += ("MISSING_" + [string]$entry.prefix + "_" + [string]$entry.label)
        }
    }

    $candidateModelHash = [string](Get-PropValue -ObjectValue $result.candidate.model_bin -Name "sha256" -DefaultValue "")
    $candidateThresholdHash = [string](Get-PropValue -ObjectValue $result.candidate.thresholds -Name "sha256" -DefaultValue "")
    $championModelHash = [string](Get-PropValue -ObjectValue $result.champion.model_bin -Name "sha256" -DefaultValue "")
    $championThresholdHash = [string](Get-PropValue -ObjectValue $result.champion.thresholds -Name "sha256" -DefaultValue "")
    $result.evaluated = ($result.reasons.Count -eq 0)
    if ($result.evaluated) {
        $result.duplicate = (
            (-not [string]::IsNullOrWhiteSpace($candidateModelHash)) -and
            ($candidateModelHash -eq $championModelHash) -and
            (-not [string]::IsNullOrWhiteSpace($candidateThresholdHash)) -and
            ($candidateThresholdHash -eq $championThresholdHash)
        )
        if ($result.duplicate) {
            $result.reasons += "ARTIFACT_HASH_MATCH"
        }
    }
    return $result
}

function Get-CalmarLikeScore {
    param(
        [double]$RealizedPnlQuote,
        [double]$MaxDrawdownPct
    )
    if ($MaxDrawdownPct -lt 0.0) {
        return $null
    }
    if ($RealizedPnlQuote -lt 0.0) {
        if ($MaxDrawdownPct -gt 0.0) {
            return $RealizedPnlQuote / $MaxDrawdownPct
        }
        return $RealizedPnlQuote
    }
    if ($MaxDrawdownPct -gt 0.0) {
        return $RealizedPnlQuote / $MaxDrawdownPct
    }
    if ($RealizedPnlQuote -gt 0.0) {
        return [double]::PositiveInfinity
    }
    return 0.0
}

function Get-ExecutionStructureMetrics {
    param([object]$Summary)
    $payload = Get-PropValue -ObjectValue $Summary -Name "execution_structure" -DefaultValue @{}
    return [ordered]@{
        closed_trade_count = [int](To-Int64 (Get-PropValue -ObjectValue $payload -Name "closed_trade_count" -DefaultValue 0) 0)
        payoff_ratio = [double](To-Double (Get-PropValue -ObjectValue $payload -Name "payoff_ratio" -DefaultValue 0.0) 0.0)
        market_loss_concentration = [double](To-Double (Get-PropValue -ObjectValue $payload -Name "market_loss_concentration" -DefaultValue 0.0) 0.0)
        tp_exit_share = [double](To-Double (Get-PropValue -ObjectValue $payload -Name "tp_exit_share" -DefaultValue 0.0) 0.0)
        sl_exit_share = [double](To-Double (Get-PropValue -ObjectValue $payload -Name "sl_exit_share" -DefaultValue 0.0) 0.0)
        timeout_exit_share = [double](To-Double (Get-PropValue -ObjectValue $payload -Name "timeout_exit_share" -DefaultValue 0.0) 0.0)
        wins = [int](To-Int64 (Get-PropValue -ObjectValue $payload -Name "wins" -DefaultValue 0) 0)
        losses = [int](To-Int64 (Get-PropValue -ObjectValue $payload -Name "losses" -DefaultValue 0) 0)
        payload = $payload
    }
}

function Test-ExecutionStructureGate {
    param(
        [object]$Metrics,
        [double]$MinPayoffRatio,
        [double]$MaxLossConcentration,
        [int]$MinClosedTrades
    )
    $closedTradeCount = [int](To-Int64 (Get-PropValue -ObjectValue $Metrics -Name "closed_trade_count" -DefaultValue 0) 0)
    $evaluated = $closedTradeCount -ge [Math]::Max([int]$MinClosedTrades, 1)
    $payoffRatio = [double](To-Double (Get-PropValue -ObjectValue $Metrics -Name "payoff_ratio" -DefaultValue 0.0) 0.0)
    $lossConcentration = [double](To-Double (Get-PropValue -ObjectValue $Metrics -Name "market_loss_concentration" -DefaultValue 0.0) 0.0)
    $payoffPass = (-not $evaluated) -or ($payoffRatio -ge [double]$MinPayoffRatio)
    $lossConcentrationPass = (-not $evaluated) -or ($lossConcentration -le [double]$MaxLossConcentration)
    $reasons = @()
    if ($evaluated -and (-not $payoffPass)) {
        $reasons += "PAYOFF_RATIO_TOO_LOW"
    }
    if ($evaluated -and (-not $lossConcentrationPass)) {
        $reasons += "LOSS_CONCENTRATION_TOO_HIGH"
    }
    return [ordered]@{
        evaluated = $evaluated
        pass = ($payoffPass -and $lossConcentrationPass)
        min_closed_trades = [int][Math]::Max([int]$MinClosedTrades, 1)
        closed_trade_count = $closedTradeCount
        payoff_ratio = $payoffRatio
        payoff_ratio_pass = $payoffPass
        payoff_ratio_threshold = [double]$MinPayoffRatio
        market_loss_concentration = $lossConcentration
        market_loss_concentration_pass = $lossConcentrationPass
        market_loss_concentration_threshold = [double]$MaxLossConcentration
        reasons = @($reasons)
    }
}

function Resolve-PromotionPolicyConfig {
    param(
        [string]$PolicyName,
        [System.Collections.IDictionary]$BoundParams,
        [object]$EconomicObjectiveProfile = @{}
    )
    $promotionCompareProfile = if (Test-IsEffectivelyEmptyObject -ObjectValue $EconomicObjectiveProfile) {
        @{}
    } else {
        Get-PropValue -ObjectValue $EconomicObjectiveProfile -Name "promotion_compare" -DefaultValue @{}
    }
    $thresholdDefaults = Get-PropValue -ObjectValue $promotionCompareProfile -Name "threshold_defaults" -DefaultValue @{}
    $policyVariants = Get-PropValue -ObjectValue $promotionCompareProfile -Name "policy_variants" -DefaultValue @{}
    $profileApplied = -not (Test-IsEffectivelyEmptyObject -ObjectValue $promotionCompareProfile)
    $resolved = [ordered]@{
        name = $PolicyName
        profile_id = if ($profileApplied) { [string](Get-PropValue -ObjectValue $EconomicObjectiveProfile -Name "profile_id" -DefaultValue "") } else { "" }
        threshold_source = if ($profileApplied) { "economic_objective_profile" } else { "script_defaults" }
        cli_override_keys = @()
        backtest_min_orders_filled = 30
        backtest_min_realized_pnl_quote = 0.0
        backtest_min_deflated_sharpe_ratio = 0.20
        backtest_min_pnl_delta_vs_champion = 0.0
        backtest_champion_min_drawdown_improvement_pct = 0.10
        backtest_allow_stability_override = $true
        backtest_champion_pnl_tolerance_pct = 0.05
        backtest_champion_max_fill_rate_degradation = 0.02
        backtest_champion_max_slippage_deterioration_bps = 2.5
        backtest_champion_min_utility_edge_pct = 0.0
        use_pareto = $true
        use_utility_tie_break = $true
        backtest_compare_required = $true
        paper_final_gate = $false
    }
    switch ($PolicyName) {
        "strict" {
            $resolved.backtest_allow_stability_override = $false
            $resolved.backtest_champion_pnl_tolerance_pct = 0.0
            $resolved.backtest_champion_max_fill_rate_degradation = 0.0
            $resolved.backtest_champion_max_slippage_deterioration_bps = 0.0
            $resolved.backtest_champion_min_utility_edge_pct = 0.0
            $resolved.use_pareto = $false
            $resolved.use_utility_tie_break = $false
        }
        "conservative_pareto" {
            $resolved.backtest_allow_stability_override = $true
            $resolved.backtest_champion_pnl_tolerance_pct = 0.02
            $resolved.backtest_champion_max_fill_rate_degradation = 0.01
            $resolved.backtest_champion_max_slippage_deterioration_bps = 1.0
            $resolved.backtest_champion_min_utility_edge_pct = 0.05
            $resolved.use_pareto = $true
            $resolved.use_utility_tie_break = $true
        }
        "paper_final_balanced" {
            $resolved.backtest_allow_stability_override = $true
            $resolved.backtest_champion_pnl_tolerance_pct = 0.05
            $resolved.backtest_champion_max_fill_rate_degradation = 0.02
            $resolved.backtest_champion_max_slippage_deterioration_bps = 2.5
            $resolved.backtest_champion_min_utility_edge_pct = 0.0
            $resolved.use_pareto = $true
            $resolved.use_utility_tie_break = $true
            $resolved.backtest_compare_required = $false
            $resolved.paper_final_gate = $true
        }
    }
    $resolved.backtest_min_orders_filled = To-Int64 (Get-PropValue -ObjectValue $thresholdDefaults -Name "candidate_min_orders_filled" -DefaultValue $resolved.backtest_min_orders_filled) $resolved.backtest_min_orders_filled
    $resolved.backtest_min_realized_pnl_quote = To-Double (Get-PropValue -ObjectValue $thresholdDefaults -Name "candidate_min_realized_pnl_quote" -DefaultValue $resolved.backtest_min_realized_pnl_quote) $resolved.backtest_min_realized_pnl_quote
    $resolved.backtest_min_deflated_sharpe_ratio = To-Double (Get-PropValue -ObjectValue $thresholdDefaults -Name "candidate_min_deflated_sharpe_ratio" -DefaultValue $resolved.backtest_min_deflated_sharpe_ratio) $resolved.backtest_min_deflated_sharpe_ratio
    $resolved.backtest_min_pnl_delta_vs_champion = To-Double (Get-PropValue -ObjectValue $thresholdDefaults -Name "candidate_min_pnl_delta_vs_champion" -DefaultValue $resolved.backtest_min_pnl_delta_vs_champion) $resolved.backtest_min_pnl_delta_vs_champion
    $resolved.backtest_champion_min_drawdown_improvement_pct = To-Double (Get-PropValue -ObjectValue $thresholdDefaults -Name "champion_min_drawdown_improvement_pct" -DefaultValue $resolved.backtest_champion_min_drawdown_improvement_pct) $resolved.backtest_champion_min_drawdown_improvement_pct
    $profilePolicy = Get-PropValue -ObjectValue $policyVariants -Name $PolicyName -DefaultValue @{}
    if (Test-IsEffectivelyEmptyObject -ObjectValue $profilePolicy) {
        $profilePolicy = Get-PropValue -ObjectValue $policyVariants -Name "balanced_pareto" -DefaultValue @{}
    }
    if (-not (Test-IsEffectivelyEmptyObject -ObjectValue $profilePolicy)) {
        $resolved.backtest_allow_stability_override = To-Bool (Get-PropValue -ObjectValue $profilePolicy -Name "allow_stability_override" -DefaultValue $resolved.backtest_allow_stability_override) $resolved.backtest_allow_stability_override
        $resolved.backtest_champion_pnl_tolerance_pct = To-Double (Get-PropValue -ObjectValue $profilePolicy -Name "champion_pnl_tolerance_pct" -DefaultValue $resolved.backtest_champion_pnl_tolerance_pct) $resolved.backtest_champion_pnl_tolerance_pct
        $resolved.backtest_champion_max_fill_rate_degradation = To-Double (Get-PropValue -ObjectValue $profilePolicy -Name "champion_max_fill_rate_degradation" -DefaultValue $resolved.backtest_champion_max_fill_rate_degradation) $resolved.backtest_champion_max_fill_rate_degradation
        $resolved.backtest_champion_max_slippage_deterioration_bps = To-Double (Get-PropValue -ObjectValue $profilePolicy -Name "champion_max_slippage_deterioration_bps" -DefaultValue $resolved.backtest_champion_max_slippage_deterioration_bps) $resolved.backtest_champion_max_slippage_deterioration_bps
        $resolved.backtest_champion_min_utility_edge_pct = To-Double (Get-PropValue -ObjectValue $profilePolicy -Name "champion_min_utility_edge_pct" -DefaultValue $resolved.backtest_champion_min_utility_edge_pct) $resolved.backtest_champion_min_utility_edge_pct
        $resolved.use_pareto = To-Bool (Get-PropValue -ObjectValue $profilePolicy -Name "use_pareto" -DefaultValue $resolved.use_pareto) $resolved.use_pareto
        $resolved.use_utility_tie_break = To-Bool (Get-PropValue -ObjectValue $profilePolicy -Name "use_utility_tie_break" -DefaultValue $resolved.use_utility_tie_break) $resolved.use_utility_tie_break
        $resolved.backtest_compare_required = To-Bool (Get-PropValue -ObjectValue $profilePolicy -Name "backtest_compare_required" -DefaultValue $resolved.backtest_compare_required) $resolved.backtest_compare_required
        $resolved.paper_final_gate = To-Bool (Get-PropValue -ObjectValue $profilePolicy -Name "paper_final_gate" -DefaultValue $resolved.paper_final_gate) $resolved.paper_final_gate
    }
    $overrideKeys = New-Object System.Collections.Generic.List[string]
    if ($BoundParams.ContainsKey("BacktestMinOrdersFilled")) {
        $resolved.backtest_min_orders_filled = [int]$BacktestMinOrdersFilled
        $overrideKeys.Add("backtest_min_orders_filled") | Out-Null
    }
    if ($BoundParams.ContainsKey("BacktestMinRealizedPnlQuote")) {
        $resolved.backtest_min_realized_pnl_quote = [double]$BacktestMinRealizedPnlQuote
        $overrideKeys.Add("backtest_min_realized_pnl_quote") | Out-Null
    }
    if ($BoundParams.ContainsKey("BacktestMinDeflatedSharpeRatio")) {
        $resolved.backtest_min_deflated_sharpe_ratio = [double]$BacktestMinDeflatedSharpeRatio
        $overrideKeys.Add("backtest_min_deflated_sharpe_ratio") | Out-Null
    }
    if ($BoundParams.ContainsKey("BacktestMinPnlDeltaVsChampion")) {
        $resolved.backtest_min_pnl_delta_vs_champion = [double]$BacktestMinPnlDeltaVsChampion
        $overrideKeys.Add("backtest_min_pnl_delta_vs_champion") | Out-Null
    }
    if ($BoundParams.ContainsKey("BacktestChampionMinDrawdownImprovementPct")) {
        $resolved.backtest_champion_min_drawdown_improvement_pct = [double]$BacktestChampionMinDrawdownImprovementPct
        $overrideKeys.Add("backtest_champion_min_drawdown_improvement_pct") | Out-Null
    }
    if ($BoundParams.ContainsKey("BacktestAllowStabilityOverride")) {
        $resolved.backtest_allow_stability_override = [bool]$BacktestAllowStabilityOverride
        $overrideKeys.Add("backtest_allow_stability_override") | Out-Null
    }
    if ($BoundParams.ContainsKey("BacktestChampionPnlTolerancePct")) {
        $resolved.backtest_champion_pnl_tolerance_pct = [double]$BacktestChampionPnlTolerancePct
        $overrideKeys.Add("backtest_champion_pnl_tolerance_pct") | Out-Null
    }
    if ($BoundParams.ContainsKey("BacktestChampionMaxFillRateDegradation")) {
        $resolved.backtest_champion_max_fill_rate_degradation = [double]$BacktestChampionMaxFillRateDegradation
        $overrideKeys.Add("backtest_champion_max_fill_rate_degradation") | Out-Null
    }
    if ($BoundParams.ContainsKey("BacktestChampionMaxSlippageDeteriorationBps")) {
        $resolved.backtest_champion_max_slippage_deterioration_bps = [double]$BacktestChampionMaxSlippageDeteriorationBps
        $overrideKeys.Add("backtest_champion_max_slippage_deterioration_bps") | Out-Null
    }
    if ($BoundParams.ContainsKey("BacktestChampionMinUtilityEdgePct")) {
        $resolved.backtest_champion_min_utility_edge_pct = [double]$BacktestChampionMinUtilityEdgePct
        $overrideKeys.Add("backtest_champion_min_utility_edge_pct") | Out-Null
    }
    $resolved.cli_override_keys = @($overrideKeys)
    return $resolved
}

function Sync-PromotionPolicyConfigToReport {
    param(
        [object]$ReportValue,
        [object]$PromotionPolicyConfig
    )
    if (($null -eq $ReportValue) -or ($null -eq $PromotionPolicyConfig)) {
        return
    }
    $config = Get-PropValue -ObjectValue $ReportValue -Name "config" -DefaultValue $null
    if ($null -eq $config) {
        return
    }
    $config.backtest_min_orders_filled = [int](Get-PropValue -ObjectValue $PromotionPolicyConfig -Name "backtest_min_orders_filled" -DefaultValue 30)
    $config.backtest_min_realized_pnl_quote = [double](Get-PropValue -ObjectValue $PromotionPolicyConfig -Name "backtest_min_realized_pnl_quote" -DefaultValue 0.0)
    $config.backtest_min_pnl_delta_vs_champion = [double](Get-PropValue -ObjectValue $PromotionPolicyConfig -Name "backtest_min_pnl_delta_vs_champion" -DefaultValue 0.0)
    $config.backtest_min_deflated_sharpe_ratio = [double](Get-PropValue -ObjectValue $PromotionPolicyConfig -Name "backtest_min_deflated_sharpe_ratio" -DefaultValue 0.20)
    $config.promotion_policy = [string](Get-PropValue -ObjectValue $PromotionPolicyConfig -Name "name" -DefaultValue "")
    $config.backtest_allow_stability_override = [bool](Get-PropValue -ObjectValue $PromotionPolicyConfig -Name "backtest_allow_stability_override" -DefaultValue $true)
    $config.backtest_champion_pnl_tolerance_pct = [double](Get-PropValue -ObjectValue $PromotionPolicyConfig -Name "backtest_champion_pnl_tolerance_pct" -DefaultValue 0.05)
    $config.backtest_champion_min_drawdown_improvement_pct = [double](Get-PropValue -ObjectValue $PromotionPolicyConfig -Name "backtest_champion_min_drawdown_improvement_pct" -DefaultValue 0.10)
    $config.backtest_champion_max_fill_rate_degradation = [double](Get-PropValue -ObjectValue $PromotionPolicyConfig -Name "backtest_champion_max_fill_rate_degradation" -DefaultValue 0.02)
    $config.backtest_champion_max_slippage_deterioration_bps = [double](Get-PropValue -ObjectValue $PromotionPolicyConfig -Name "backtest_champion_max_slippage_deterioration_bps" -DefaultValue 2.5)
    $config.backtest_champion_min_utility_edge_pct = [double](Get-PropValue -ObjectValue $PromotionPolicyConfig -Name "backtest_champion_min_utility_edge_pct" -DefaultValue 0.0)
    $config.backtest_use_pareto = [bool](Get-PropValue -ObjectValue $PromotionPolicyConfig -Name "use_pareto" -DefaultValue $true)
    $config.backtest_use_utility_tie_break = [bool](Get-PropValue -ObjectValue $PromotionPolicyConfig -Name "use_utility_tie_break" -DefaultValue $true)
    $config.promotion_policy_contract_source = [string](Get-PropValue -ObjectValue $PromotionPolicyConfig -Name "threshold_source" -DefaultValue "script_defaults")
    $config.promotion_policy_contract_profile_id = [string](Get-PropValue -ObjectValue $PromotionPolicyConfig -Name "profile_id" -DefaultValue "")
    $config.promotion_policy_cli_override_keys = @((Get-PropValue -ObjectValue $PromotionPolicyConfig -Name "cli_override_keys" -DefaultValue @()))
}

function New-DateWindowRecord {
    param(
        [string]$Name,
        [string]$Start,
        [string]$End,
        [string]$Source = ""
    )
    $reasonPrefix = ([string]$Name).Trim().ToUpperInvariant().Replace("-", "_")
    $window = [ordered]@{
        name = $Name
        start = ""
        end = ""
        source = $Source
        valid = $false
        day_count = $null
        reasons = @()
    }
    if ([string]::IsNullOrWhiteSpace($Start) -or [string]::IsNullOrWhiteSpace($End)) {
        $window.reasons = @("MISSING_${reasonPrefix}_WINDOW")
        return $window
    }
    try {
        $normalizedStart = Resolve-DateToken -DateText $Start -LabelForError ("{0}_window_start" -f $Name)
        $normalizedEnd = Resolve-DateToken -DateText $End -LabelForError ("{0}_window_end" -f $Name)
        $startObj = [DateTime]::ParseExact($normalizedStart, "yyyy-MM-dd", [System.Globalization.CultureInfo]::InvariantCulture)
        $endObj = [DateTime]::ParseExact($normalizedEnd, "yyyy-MM-dd", [System.Globalization.CultureInfo]::InvariantCulture)
        $window.start = $normalizedStart
        $window.end = $normalizedEnd
        if ($endObj -lt $startObj) {
            $window.reasons = @("INVALID_${reasonPrefix}_WINDOW_RANGE")
            return $window
        }
        $window.valid = $true
        $window.day_count = [int](($endObj - $startObj).TotalDays + 1)
        return $window
    } catch {
        $window.start = [string]$Start
        $window.end = [string]$End
        $window.reasons = @("INVALID_${reasonPrefix}_WINDOW_DATE")
        return $window
    }
}

function New-TrainabilityAttemptRecord {
    param(
        $Probe,
        [string]$StartDate,
        [string]$EndDate
    )
    return [ordered]@{
        start = $StartDate
        end = $EndDate
        exit_code = [int]$Probe.Exec.ExitCode
        report_path = [string]$Probe.ReportPath
        rows_final = [int]$Probe.RowsFinal
        min_rows_for_train = [int]$Probe.MinRowsForTrain
        status = [string]$Probe.Status
        effective_start = [string](Get-PropValue -ObjectValue $Probe.Report -Name "effective_start" -DefaultValue "")
        effective_end = [string](Get-PropValue -ObjectValue $Probe.Report -Name "effective_end" -DefaultValue "")
        error_message = [string]$Probe.ErrorMessage
        usable = [bool]$Probe.Usable
        source_mode = [string](Get-PropValue -ObjectValue $Probe -Name "SourceMode" -DefaultValue "")
        command = $Probe.Exec.Command
        output_preview = (Get-OutputPreview -Text ([string]$Probe.Exec.Output))
    }
}

function Build-SplitPolicyDecisionRecord {
    param(
        [string]$PolicyId,
        [string]$LaneMode,
        [bool]$PromotionEligible,
        [string]$SelectedBy,
        [int]$RequestedHoldoutDays,
        [int]$SelectedHoldoutDays,
        [string]$QualityFloorDate,
        [string[]]$ReasonCodes,
        $StrictTrainability,
        $BootstrapTrainability,
        [string]$TrainWindowStart,
        [string]$TrainWindowEnd,
        [string]$CertificationWindowStart,
        [string]$CertificationWindowEnd,
        [string]$BootstrapWindowStart,
        [string]$BootstrapWindowEnd,
        [int]$HistoricalAnchorCount = 0,
        [string]$ArtifactPath = "",
        [string]$CandidateRunId = "",
        [string]$CandidateRunDir = ""
    )
    return [ordered]@{
        version = 1
        policy_id = $PolicyId
        lane_mode = $LaneMode
        promotion_eligible = [bool]$PromotionEligible
        selected_by = $SelectedBy
        requested_holdout_days = [int]$RequestedHoldoutDays
        selected_holdout_days = [int]$SelectedHoldoutDays
        train_data_quality_floor_date = $QualityFloorDate
        historical_anchor_count = [int]$HistoricalAnchorCount
        reason_codes = @($ReasonCodes)
        current_batch_windows = [ordered]@{
            train = New-DateWindowRecord -Name "train" -Start $TrainWindowStart -End $TrainWindowEnd -Source "split_policy.current_train"
            certification = New-DateWindowRecord -Name "certification" -Start $CertificationWindowStart -End $CertificationWindowEnd -Source "split_policy.current_certification"
            backtest = New-DateWindowRecord -Name "backtest" -Start $CertificationWindowStart -End $CertificationWindowEnd -Source "split_policy.current_backtest"
            bootstrap = New-DateWindowRecord -Name "bootstrap" -Start $BootstrapWindowStart -End $BootstrapWindowEnd -Source "split_policy.bootstrap_window"
        }
        strict_trainability = if ($null -eq $StrictTrainability) { @{} } else { $StrictTrainability }
        bootstrap_trainability = if ($null -eq $BootstrapTrainability) { @{} } else { $BootstrapTrainability }
        artifact_path = $ArtifactPath
        candidate_run_id = $CandidateRunId
        candidate_run_dir = $CandidateRunDir
        candidate_holdout_days = @()
        historical_anchor_min_required = 0
        selection_summary = @()
        history_path = ""
        new_evaluations = @()
    }
}

function Write-SplitPolicyDecisionArtifact {
    param(
        [string]$CandidateRunDir,
        $SplitPolicyDecision
    )
    if ([string]::IsNullOrWhiteSpace($CandidateRunDir) -or (-not (Test-Path $CandidateRunDir))) {
        return ""
    }
    $artifactPath = Join-Path $CandidateRunDir "split_policy_decision.json"
    Write-JsonFile -PathValue $artifactPath -Payload $SplitPolicyDecision
    return $artifactPath
}

function Resolve-SplitPolicySelectorHistoryPath {
    param(
        [string]$RegistryRoot,
        [string]$ModelFamilyName,
        [string]$TaskName
    )
    $taskSlug = ([string]$TaskName).Trim().ToLowerInvariant()
    if ([string]::IsNullOrWhiteSpace($taskSlug)) {
        $taskSlug = "cls"
    }
    $taskSlug = ($taskSlug -replace '[^a-z0-9]+', '_').Trim('_')
    if ([string]::IsNullOrWhiteSpace($taskSlug)) {
        $taskSlug = "cls"
    }
    return (Join-Path (Join-Path $RegistryRoot $ModelFamilyName) ("split_policy_selector_history." + $taskSlug + ".jsonl"))
}

function Get-SplitPolicyHistoryRecordKey {
    param(
        [string]$TaskName,
        [int]$HoldoutDays,
        [string]$AnchorDate
    )
    return (([string]$TaskName).Trim().ToLowerInvariant() + "|" + [string]([int]$HoldoutDays) + "|" + ([string]$AnchorDate).Trim())
}

function Load-SplitPolicySelectorHistoryRecords {
    param([string]$PathValue)
    if ([string]::IsNullOrWhiteSpace($PathValue) -or (-not (Test-Path $PathValue))) {
        Write-Output -NoEnumerate @()
        return
    }
    $rawLines = Get-Content -Path $PathValue -Encoding UTF8
    $recordsByKey = @{}
    $orderedKeys = New-Object System.Collections.Generic.List[string]
    foreach ($line in $rawLines) {
        $text = [string]$line
        if ([string]::IsNullOrWhiteSpace($text)) {
            continue
        }
        try {
            $record = $text | ConvertFrom-Json
        } catch {
            continue
        }
        $taskName = [string](Get-PropValue -ObjectValue $record -Name "task" -DefaultValue "")
        $holdoutDays = [int](To-Int64 (Get-PropValue -ObjectValue $record -Name "holdout_days" -DefaultValue 0) 0)
        $anchorDate = [string](Get-PropValue -ObjectValue $record -Name "anchor_date" -DefaultValue "")
        if ([string]::IsNullOrWhiteSpace($taskName) -or ($holdoutDays -le 0) -or [string]::IsNullOrWhiteSpace($anchorDate)) {
            continue
        }
        $key = Get-SplitPolicyHistoryRecordKey -TaskName $taskName -HoldoutDays $holdoutDays -AnchorDate $anchorDate
        if (-not $recordsByKey.ContainsKey($key)) {
            $orderedKeys.Add($key) | Out-Null
        }
        $recordsByKey[$key] = $record
    }
    $records = New-Object System.Collections.Generic.List[object]
    foreach ($key in $orderedKeys) {
        $records.Add($recordsByKey[$key]) | Out-Null
    }
    Write-Output -NoEnumerate ($records.ToArray())
}

function Save-SplitPolicySelectorHistoryRecords {
    param(
        [string]$PathValue,
        [object[]]$Records
    )
    if ([string]::IsNullOrWhiteSpace($PathValue)) {
        return ""
    }
    $parent = Split-Path -Parent $PathValue
    if (-not [string]::IsNullOrWhiteSpace($parent)) {
        New-Item -ItemType Directory -Force -Path $parent | Out-Null
    }
    $deduped = @{}
    foreach ($record in @($Records)) {
        $taskName = [string](Get-PropValue -ObjectValue $record -Name "task" -DefaultValue "")
        $holdoutDays = [int](To-Int64 (Get-PropValue -ObjectValue $record -Name "holdout_days" -DefaultValue 0) 0)
        $anchorDate = [string](Get-PropValue -ObjectValue $record -Name "anchor_date" -DefaultValue "")
        if ([string]::IsNullOrWhiteSpace($taskName) -or ($holdoutDays -le 0) -or [string]::IsNullOrWhiteSpace($anchorDate)) {
            continue
        }
        $key = Get-SplitPolicyHistoryRecordKey -TaskName $taskName -HoldoutDays $holdoutDays -AnchorDate $anchorDate
        $deduped[$key] = $record
    }
    $sortedRecords = @(
        $deduped.Values |
            Sort-Object `
                @{ Expression = { [int](To-Int64 (Get-PropValue -ObjectValue $_ -Name "holdout_days" -DefaultValue 0) 0) } }, `
                @{ Expression = { [string](Get-PropValue -ObjectValue $_ -Name "anchor_date" -DefaultValue "") } }
    )
    $lines = New-Object System.Collections.Generic.List[string]
    foreach ($record in $sortedRecords) {
        $lines.Add(($record | ConvertTo-Json -Compress -Depth 12)) | Out-Null
    }
    $content = if ($lines.Count -gt 0) { ($lines -join [Environment]::NewLine) + [Environment]::NewLine } else { "" }
    Set-Content -Path $PathValue -Value $content -Encoding UTF8
    return $PathValue
}

function Resolve-SplitPolicyCandidateHoldoutDays {
    param(
        [int]$RequestedBacktestLookbackDays,
        [string]$OverrideText
    )
    $resolved = New-Object System.Collections.Generic.List[int]
    $tokens = New-Object System.Collections.Generic.List[string]
    $seen = @{}
    $hasTokens = $false
    if (-not [string]::IsNullOrWhiteSpace($OverrideText)) {
        foreach ($token in @($OverrideText -split '[,\s]+')) {
            $tokenText = [string]$token
            if ([string]::IsNullOrWhiteSpace($tokenText)) {
                continue
            }
            $tokens.Add($tokenText) | Out-Null
            $hasTokens = $true
        }
    }
    if (-not $hasTokens) {
        foreach ($value in 1..([Math]::Max([int]$RequestedBacktestLookbackDays, 1))) {
            if (-not $seen.ContainsKey($value)) {
                $seen[$value] = $true
                $resolved.Add([int]$value) | Out-Null
            }
        }
    } else {
        foreach ($token in @($tokens.ToArray())) {
            $parsed = [int](To-Int64 ([string]$token) 0)
            if ($parsed -le 0) {
                continue
            }
            if ($seen.ContainsKey($parsed)) {
                continue
            }
            $seen[$parsed] = $true
            $resolved.Add($parsed) | Out-Null
        }
    }
    Write-Output -NoEnumerate ($resolved.ToArray())
}

function Resolve-SplitPolicyHoldoutWindows {
    param(
        [string]$BatchDateValue,
        [int]$HoldoutDays,
        [string]$QualityFloorDate
    )
    $qualityFloorText = [string]$QualityFloorDate
    $result = [ordered]@{
        holdout_days = [int]$HoldoutDays
        valid_for_strict = $false
        reasons = @()
        train_start = ""
        train_end = ""
        certification_start = ""
        certification_end = ""
        bootstrap_start = $qualityFloorText
        bootstrap_end = $BatchDateValue
    }
    if ([string]::IsNullOrWhiteSpace($qualityFloorText)) {
        $result.reasons = @("MISSING_TRAIN_DATA_QUALITY_FLOOR")
        return $result
    }
    $batchObj = [DateTime]::ParseExact($BatchDateValue, "yyyy-MM-dd", [System.Globalization.CultureInfo]::InvariantCulture)
    $qualityFloorObj = [DateTime]::ParseExact($qualityFloorText, "yyyy-MM-dd", [System.Globalization.CultureInfo]::InvariantCulture)
    $holdoutValue = [Math]::Max([int]$HoldoutDays, 1)
    $certificationEndObj = $batchObj
    $certificationStartObj = $batchObj.AddDays(-1 * [Math]::Max($holdoutValue - 1, 0))
    $trainEndObj = $batchObj.AddDays(-1 * $holdoutValue)
    $result.certification_start = $certificationStartObj.ToString("yyyy-MM-dd")
    $result.certification_end = $certificationEndObj.ToString("yyyy-MM-dd")
    if ($trainEndObj -lt $qualityFloorObj) {
        $result.reasons = @("TRAIN_WINDOW_BEFORE_QUALITY_FLOOR")
        return $result
    }
    $result.train_start = $qualityFloorText
    $result.train_end = $trainEndObj.ToString("yyyy-MM-dd")
    $result.valid_for_strict = $true
    return $result
}

function Get-DateWindowInclusiveDayCount {
    param(
        [string]$StartDate,
        [string]$EndDate
    )
    if ([string]::IsNullOrWhiteSpace($StartDate) -or [string]::IsNullOrWhiteSpace($EndDate)) {
        return 0
    }
    try {
        $startObj = [DateTime]::ParseExact($StartDate, "yyyy-MM-dd", [System.Globalization.CultureInfo]::InvariantCulture)
        $endObj = [DateTime]::ParseExact($EndDate, "yyyy-MM-dd", [System.Globalization.CultureInfo]::InvariantCulture)
    } catch {
        return 0
    }
    if ($endObj -lt $startObj) {
        return 0
    }
    return [int](($endObj - $startObj).TotalDays + 1)
}

function Test-SplitPolicyHistoryRecordUsable {
    param(
        [Parameter(Mandatory = $false)]$Record,
        [string]$QualityFloorDate
    )
    if ([string]::IsNullOrWhiteSpace($QualityFloorDate)) {
        return $true
    }
    if (Test-IsEffectivelyEmptyObject -ObjectValue $Record) {
        return $false
    }
    $holdoutDays = [int](To-Int64 (Get-PropValue -ObjectValue $Record -Name "holdout_days" -DefaultValue 0) 0)
    $anchorDate = [string](Get-PropValue -ObjectValue $Record -Name "anchor_date" -DefaultValue "")
    if (($holdoutDays -le 0) -or [string]::IsNullOrWhiteSpace($anchorDate)) {
        return $false
    }
    $windowSpec = Resolve-SplitPolicyHoldoutWindows -BatchDateValue $anchorDate -HoldoutDays $holdoutDays -QualityFloorDate $QualityFloorDate
    return (To-Bool (Get-PropValue -ObjectValue $windowSpec -Name "valid_for_strict" -DefaultValue $false) $false)
}

function Remove-DirectoryIfExists {
    param([string]$PathValue)
    if ([string]::IsNullOrWhiteSpace($PathValue)) {
        return
    }
    if (Test-Path $PathValue) {
        Remove-Item -Path $PathValue -Recurse -Force -ErrorAction SilentlyContinue
    }
}

function Get-SplitPolicyUtilityStatistics {
    param(
        [double[]]$Values,
        [double]$ZScore = 1.96
    )
    $validValues = @(
        @($Values) |
            Where-Object { $null -ne $_ -and (-not [double]::IsNaN([double]$_)) -and (-not [double]::IsInfinity([double]$_)) }
    )
    $positiveInfinityCount = @($Values | Where-Object { $null -ne $_ -and [double]::IsPositiveInfinity([double]$_) }).Count
    if (($validValues.Count -eq 0) -and ($positiveInfinityCount -gt 0)) {
        return [ordered]@{
            count = [int]$positiveInfinityCount
            mean_utility = [double]::PositiveInfinity
            standard_error = 0.0
            lower_confidence_bound = [double]::PositiveInfinity
            method = "lag1_hac"
        }
    }
    if ($validValues.Count -eq 0) {
        return [ordered]@{
            count = 0
            mean_utility = $null
            standard_error = $null
            lower_confidence_bound = $null
            method = "lag1_hac"
        }
    }
    $valuesArray = @($validValues | ForEach-Object { [double]$_ })
    $n = [int]$valuesArray.Count
    $meanValue = ($valuesArray | Measure-Object -Average).Average
    if ($n -eq 1) {
        return [ordered]@{
            count = 1
            mean_utility = [double]$meanValue
            standard_error = 0.0
            lower_confidence_bound = [double]$meanValue
            method = "lag1_hac"
        }
    }
    $demeaned = @()
    foreach ($value in $valuesArray) {
        $demeaned += ([double]$value - [double]$meanValue)
    }
    $gamma0 = 0.0
    foreach ($value in $demeaned) {
        $gamma0 += ([double]$value * [double]$value)
    }
    $gamma0 = $gamma0 / [double]$n
    $gamma1 = 0.0
    for ($index = 1; $index -lt $demeaned.Count; $index++) {
        $gamma1 += ([double]$demeaned[$index] * [double]$demeaned[$index - 1])
    }
    $gamma1 = $gamma1 / [double]$n
    $varianceOfMean = ([double]$gamma0 + [double]$gamma1) / [double]$n
    if ($varianceOfMean -lt 0.0) {
        $varianceOfMean = 0.0
    }
    $standardError = [Math]::Sqrt($varianceOfMean)
    return [ordered]@{
        count = [int]$n
        mean_utility = [double]$meanValue
        standard_error = [double]$standardError
        lower_confidence_bound = ([double]$meanValue - ([double]$ZScore * [double]$standardError))
        method = "lag1_hac"
    }
}

function Invoke-SplitPolicyHistoricalAnchorEvaluation {
    param(
        [string]$PythonPath,
        [string]$Root,
        [string]$RegistryRoot,
        [string]$AnchorDate,
        [int]$HoldoutDays,
        [string]$QualityFloorDate
    )
    $windowSpec = Resolve-SplitPolicyHoldoutWindows -BatchDateValue $AnchorDate -HoldoutDays $HoldoutDays -QualityFloorDate $QualityFloorDate
    $record = [ordered]@{
        version = 1
        policy = "v4_split_policy_forward_validation_lcb_v1"
        task = $Task
        holdout_days = [int]$HoldoutDays
        anchor_date = $AnchorDate
        evaluated_at = (Get-Date).ToString("o")
        train_window = New-DateWindowRecord -Name "train" -Start ([string](Get-PropValue -ObjectValue $windowSpec -Name "train_start" -DefaultValue "")) -End ([string](Get-PropValue -ObjectValue $windowSpec -Name "train_end" -DefaultValue "")) -Source "split_policy.history_train"
        certification_window = New-DateWindowRecord -Name "certification" -Start ([string](Get-PropValue -ObjectValue $windowSpec -Name "certification_start" -DefaultValue "")) -End ([string](Get-PropValue -ObjectValue $windowSpec -Name "certification_end" -DefaultValue "")) -Source "split_policy.history_certification"
        status = "pending"
        reasons = @((Get-PropValue -ObjectValue $windowSpec -Name "reasons" -DefaultValue @()))
        utility_metric = "calmar_like"
        utility_score = $null
        candidate_run_id = ""
        candidate_run_dir = ""
        backtest_run_dir = ""
        trainability = @{}
    }
    if (-not (To-Bool (Get-PropValue -ObjectValue $windowSpec -Name "valid_for_strict" -DefaultValue $false) $false)) {
        $record.status = "INVALID_WINDOW"
        return $record
    }
    $trainStartDate = [string](Get-PropValue -ObjectValue $windowSpec -Name "train_start" -DefaultValue "")
    $trainEndDate = [string](Get-PropValue -ObjectValue $windowSpec -Name "train_end" -DefaultValue "")
    $certificationStartDate = [string](Get-PropValue -ObjectValue $windowSpec -Name "certification_start" -DefaultValue "")
    $certificationEndDate = [string](Get-PropValue -ObjectValue $windowSpec -Name "certification_end" -DefaultValue "")
    $probe = Invoke-FeaturesBuildAndLoadReport -PythonPath $PythonPath -StartDate $trainStartDate -EndDate $trainEndDate
    $record.trainability = New-TrainabilityAttemptRecord -Probe $probe -StartDate $trainStartDate -EndDate $trainEndDate
    if (-not $probe.Usable) {
        $record.status = "INSUFFICIENT_TRAINABLE_ROWS"
        $record.reasons = Merge-UniqueStringArray -First @($record.reasons) -Second @("INSUFFICIENT_TRAINABLE_ROWS")
        return $record
    }
    $historyRunDir = ""
    $backtestRunDir = ""
    try {
        $trainArgs = @(
            "-m", "autobot.cli",
            "model", "train",
            "--trainer", $Trainer,
            "--model-family", $ModelFamily,
            "--feature-set", $FeatureSet,
            "--label-set", $LabelSet,
            "--task", $Task,
            "--run-scope", $SplitPolicyHistoryRunScope,
            "--tf", $Tf,
            "--quote", $Quote,
            "--top-n", $TrainTopN,
            "--start", $trainStartDate,
            "--end", $trainEndDate,
            "--booster-sweep-trials", $SplitPolicyHistoryBoosterSweepTrials,
            "--seed", $Seed,
            "--nthread", $NThread,
            "--execution-acceptance-top-n", $BacktestTopN,
            "--execution-acceptance-top-pct", $BacktestTopPct,
            "--execution-acceptance-min-prob", $BacktestMinProb,
            "--execution-acceptance-min-cands-per-ts", $BacktestMinCandidatesPerTs,
            "--execution-acceptance-hold-bars", $HoldBars
        )
        $trainExec = Invoke-CommandCapture -Exe $PythonPath -ArgList $trainArgs -AllowFailure
        $historyRunDir = Resolve-RunDirFromText -TextValue ([string]$trainExec.Output)
        $record.train_exec = [ordered]@{
            exit_code = [int]$trainExec.ExitCode
            command = $trainExec.Command
            output_preview = (Get-OutputPreview -Text ([string]$trainExec.Output))
        }
        if ($trainExec.ExitCode -ne 0) {
            $record.status = "TRAIN_FAILED"
            $record.reasons = Merge-UniqueStringArray -First @($record.reasons) -Second @("TRAIN_FAILED")
            return $record
        }
        $historyRunId = if ([string]::IsNullOrWhiteSpace($historyRunDir)) { "" } else { Split-Path -Leaf $historyRunDir }
        $record.candidate_run_id = $historyRunId
        $record.candidate_run_dir = $historyRunDir
        $backtest = Invoke-BacktestAndLoadSummary `
            -PythonPath $PythonPath `
            -Root $Root `
            -ModelRef $historyRunId `
            -ModelFamilyName $ModelFamily `
            -StartDate $certificationStartDate `
            -EndDate $certificationEndDate
        $backtestRunDir = $backtest.RunDir
        $summary = $backtest.Summary
        $ordersFilled = [int64](To-Int64 (Get-PropValue -ObjectValue $summary -Name "orders_filled" -DefaultValue 0) 0)
        $realizedPnl = To-Double (Get-PropValue -ObjectValue $summary -Name "realized_pnl_quote" -DefaultValue 0.0) 0.0
        $maxDrawdownPct = To-Double (Get-PropValue -ObjectValue $summary -Name "max_drawdown_pct" -DefaultValue -1.0) -1.0
        $utilityScore = Get-CalmarLikeScore -RealizedPnlQuote $realizedPnl -MaxDrawdownPct $maxDrawdownPct
        $record.backtest = [ordered]@{
            start = $certificationStartDate
            end = $certificationEndDate
            run_dir = $backtestRunDir
            summary_path = $backtest.SummaryPath
            orders_filled = $ordersFilled
            realized_pnl_quote = [double]$realizedPnl
            max_drawdown_pct = [double]$maxDrawdownPct
            fill_rate = To-Double (Get-PropValue -ObjectValue $summary -Name "fill_rate" -DefaultValue -1.0) -1.0
            slippage_bps_mean = Get-NullableDouble (Get-PropValue -ObjectValue $summary -Name "slippage_bps_mean" -DefaultValue $null)
            utility_score = $utilityScore
        }
        $record.utility_score = $utilityScore
        $record.status = "EVALUATED"
        return $record
    } catch {
        $record.status = "UNHANDLED_EXCEPTION"
        $record.reasons = Merge-UniqueStringArray -First @($record.reasons) -Second @("UNHANDLED_EXCEPTION")
        $record.exception = [ordered]@{ message = $_.Exception.Message }
        return $record
    } finally {
        Remove-DirectoryIfExists -PathValue $historyRunDir
        Remove-DirectoryIfExists -PathValue $backtestRunDir
    }
}

function Resolve-SplitPolicySelection {
    param(
        [string]$PythonPath,
        [string]$Root,
        [string]$RegistryRoot,
        [string]$BatchDateValue,
        [string]$QualityFloorDate
    )
    $historyPath = Resolve-SplitPolicySelectorHistoryPath -RegistryRoot $RegistryRoot -ModelFamilyName $ModelFamily -TaskName $Task
    $candidateHoldoutDays = Resolve-SplitPolicyCandidateHoldoutDays -RequestedBacktestLookbackDays $BacktestLookbackDays -OverrideText $SplitPolicyCandidateHoldoutDays
    $historyRecords = Load-SplitPolicySelectorHistoryRecords -PathValue $historyPath
    if (-not [string]::IsNullOrWhiteSpace($QualityFloorDate)) {
        $historyRecords = @(
            $historyRecords |
                Where-Object { Test-SplitPolicyHistoryRecordUsable -Record $_ -QualityFloorDate $QualityFloorDate }
        )
    }
    $newEvaluations = New-Object System.Collections.Generic.List[object]
    $remainingEvaluationBudget = [Math]::Max([int]$SplitPolicyMaxNewAnchorEvaluationsPerRun, 0)

    if ($SplitPolicyHistoricalSelectorEnabled -and ($remainingEvaluationBudget -gt 0) -and (-not [string]::IsNullOrWhiteSpace($QualityFloorDate))) {
        $batchObj = [DateTime]::ParseExact($BatchDateValue, "yyyy-MM-dd", [System.Globalization.CultureInfo]::InvariantCulture)
        $qualityFloorObj = [DateTime]::ParseExact($QualityFloorDate, "yyyy-MM-dd", [System.Globalization.CultureInfo]::InvariantCulture)
        foreach ($holdoutDays in $candidateHoldoutDays) {
            $anchorCursor = $qualityFloorObj.AddDays([double]$holdoutDays)
            while (($anchorCursor -lt $batchObj) -and ($remainingEvaluationBudget -gt 0)) {
                $anchorDate = $anchorCursor.ToString("yyyy-MM-dd")
                $recordKey = Get-SplitPolicyHistoryRecordKey -TaskName $Task -HoldoutDays $holdoutDays -AnchorDate $anchorDate
                $alreadyExists = $false
                foreach ($existingRecord in $historyRecords) {
                    $existingTask = [string](Get-PropValue -ObjectValue $existingRecord -Name "task" -DefaultValue "")
                    $existingHoldoutDays = [int](To-Int64 (Get-PropValue -ObjectValue $existingRecord -Name "holdout_days" -DefaultValue 0) 0)
                    $existingAnchorDate = [string](Get-PropValue -ObjectValue $existingRecord -Name "anchor_date" -DefaultValue "")
                    if ($recordKey -eq (Get-SplitPolicyHistoryRecordKey -TaskName $existingTask -HoldoutDays $existingHoldoutDays -AnchorDate $existingAnchorDate)) {
                        $alreadyExists = $true
                        break
                    }
                }
                if (-not $alreadyExists) {
                    $newRecord = Invoke-SplitPolicyHistoricalAnchorEvaluation `
                        -PythonPath $PythonPath `
                        -Root $Root `
                        -RegistryRoot $RegistryRoot `
                        -AnchorDate $anchorDate `
                        -HoldoutDays $holdoutDays `
                        -QualityFloorDate $QualityFloorDate
                    $historyRecords += $newRecord
                    $newEvaluations.Add($newRecord) | Out-Null
                    $remainingEvaluationBudget -= 1
                }
                $anchorCursor = $anchorCursor.AddDays(1)
            }
        }
        if ($newEvaluations.Count -gt 0) {
            Save-SplitPolicySelectorHistoryRecords -PathValue $historyPath -Records $historyRecords | Out-Null
        }
    }

    $selectionSummary = New-Object System.Collections.Generic.List[object]
    $selectedHoldout = $null
    $selectedSummary = $null
    $bestLcb = $null
    $bootstrapProbe = $null
    if ((-not [string]::IsNullOrWhiteSpace($QualityFloorDate)) -and ($BatchDateValue -ge $QualityFloorDate)) {
        $bootstrapProbe = Invoke-FeaturesBuildAndLoadReport -PythonPath $PythonPath -StartDate $QualityFloorDate -EndDate $BatchDateValue
    }
    $bootstrapAttempt = if ($null -eq $bootstrapProbe) { @{} } else { New-TrainabilityAttemptRecord -Probe $bootstrapProbe -StartDate $QualityFloorDate -EndDate $BatchDateValue }

    foreach ($holdoutDays in $candidateHoldoutDays) {
        $windowSpec = Resolve-SplitPolicyHoldoutWindows -BatchDateValue $BatchDateValue -HoldoutDays $holdoutDays -QualityFloorDate $QualityFloorDate
        $currentAttempt = $null
        if (To-Bool (Get-PropValue -ObjectValue $windowSpec -Name "valid_for_strict" -DefaultValue $false) $false) {
            $currentProbe = Invoke-FeaturesBuildAndLoadReport `
                -PythonPath $PythonPath `
                -StartDate ([string](Get-PropValue -ObjectValue $windowSpec -Name "train_start" -DefaultValue "")) `
                -EndDate ([string](Get-PropValue -ObjectValue $windowSpec -Name "train_end" -DefaultValue ""))
            $currentAttempt = New-TrainabilityAttemptRecord `
                -Probe $currentProbe `
                -StartDate ([string](Get-PropValue -ObjectValue $windowSpec -Name "train_start" -DefaultValue "")) `
                -EndDate ([string](Get-PropValue -ObjectValue $windowSpec -Name "train_end" -DefaultValue ""))
        } else {
            $currentAttempt = [ordered]@{
                start = [string](Get-PropValue -ObjectValue $windowSpec -Name "train_start" -DefaultValue "")
                end = [string](Get-PropValue -ObjectValue $windowSpec -Name "train_end" -DefaultValue "")
                exit_code = 2
                report_path = ""
                rows_final = 0
                min_rows_for_train = 0
                status = "INVALID_WINDOW"
                effective_start = ""
                effective_end = ""
                error_message = ""
                usable = $false
                command = ""
                output_preview = ""
            }
        }
        $historyForHoldout = @(
            $historyRecords |
                Where-Object {
                    ([string](Get-PropValue -ObjectValue $_ -Name "task" -DefaultValue "") -eq $Task) -and
                    ([int](To-Int64 (Get-PropValue -ObjectValue $_ -Name "holdout_days" -DefaultValue 0) 0) -eq [int]$holdoutDays)
                }
        )
        $evaluatedHistory = @(
            $historyForHoldout |
                Where-Object { [string](Get-PropValue -ObjectValue $_ -Name "status" -DefaultValue "") -eq "EVALUATED" }
        )
        $utilityValues = @()
        foreach ($historyRecord in $evaluatedHistory) {
            $utilityValue = Get-PropValue -ObjectValue $historyRecord -Name "utility_score" -DefaultValue $null
            if ($null -ne $utilityValue) {
                $utilityValues += [double]$utilityValue
            }
        }
        $stats = Get-SplitPolicyUtilityStatistics -Values $utilityValues
        $admissibilityReasons = @()
        if (-not (To-Bool (Get-PropValue -ObjectValue $windowSpec -Name "valid_for_strict" -DefaultValue $false) $false)) {
            $admissibilityReasons += @((Get-PropValue -ObjectValue $windowSpec -Name "reasons" -DefaultValue @()))
        }
        if (-not (To-Bool (Get-PropValue -ObjectValue $currentAttempt -Name "usable" -DefaultValue $false) $false)) {
            $admissibilityReasons += "CURRENT_BATCH_INSUFFICIENT_TRAINABLE_ROWS"
        }
        if ([int](Get-PropValue -ObjectValue $stats -Name "count" -DefaultValue 0) -lt [int]$SplitPolicyMinHistoricalAnchors) {
            $admissibilityReasons += "HISTORICAL_ANCHOR_COUNT_LT_MIN"
        }
        $admissibilityReasons = Merge-UniqueStringArray -First @($admissibilityReasons) -Second @()
        $admissibilityReasonCount = if ($null -eq $admissibilityReasons) { 0 } else { @($admissibilityReasons).Count }
        $admissible = ($admissibilityReasonCount -eq 0)
        $summaryItem = [ordered]@{
            holdout_days = [int]$holdoutDays
            admissible = [bool]$admissible
            reasons = @($admissibilityReasons)
            current_windows = $windowSpec
            current_trainability = $currentAttempt
            historical_anchor_count = [int](Get-PropValue -ObjectValue $stats -Name "count" -DefaultValue 0)
            utility_stats = $stats
            history_records = @($evaluatedHistory)
        }
        $selectionSummary.Add($summaryItem) | Out-Null
        if ($admissible) {
            $lcb = Get-PropValue -ObjectValue $stats -Name "lower_confidence_bound" -DefaultValue $null
            if (($null -eq $bestLcb) -or ($lcb -gt $bestLcb)) {
                $bestLcb = $lcb
                $selectedHoldout = [int]$holdoutDays
                $selectedSummary = $summaryItem
            }
        }
    }

    if ($null -ne $selectedSummary) {
        return [ordered]@{
            policy_id = "v4_split_policy_forward_validation_lcb_v1"
            lane_mode = "promotion_strict"
            promotion_eligible = $true
            selected_by = "forward_validation_lcb"
            selected_holdout_days = [int]$selectedHoldout
            candidate_holdout_days = @($candidateHoldoutDays)
            historical_anchor_count = [int](Get-PropValue -ObjectValue $selectedSummary -Name "historical_anchor_count" -DefaultValue 0)
            reason_codes = @("FORWARD_VALIDATION_LCB")
            selection_summary = @($selectionSummary.ToArray())
            history_path = $historyPath
            new_evaluations = @($newEvaluations.ToArray())
            bootstrap_attempt = $bootstrapAttempt
            selected_summary = $selectedSummary
            current_windows = Get-PropValue -ObjectValue $selectedSummary -Name "current_windows" -DefaultValue @{}
        }
    }

    $bootstrapReasons = @("BOOTSTRAP_ONLY_POLICY")
    if ((Get-PropValue -ObjectValue $selectionSummary -Name "Count" -DefaultValue 0) -eq 0) {
        $bootstrapReasons += "NO_HOLDOUT_CANDIDATES"
    } else {
        $bootstrapReasons += "NO_ADMISSIBLE_FORWARD_VALIDATION_HOLDOUT"
    }
    return [ordered]@{
        policy_id = "v4_split_policy_forward_validation_lcb_v1"
        lane_mode = "bootstrap_latest_inclusive"
        promotion_eligible = $false
        selected_by = "bootstrap_latest_inclusive_fallback"
        selected_holdout_days = 0
        candidate_holdout_days = @($candidateHoldoutDays)
        historical_anchor_count = 0
        reason_codes = @($bootstrapReasons)
        selection_summary = @($selectionSummary.ToArray())
        history_path = $historyPath
        new_evaluations = @($newEvaluations.ToArray())
        bootstrap_attempt = $bootstrapAttempt
        selected_summary = $null
        current_windows = [ordered]@{
            train_start = $QualityFloorDate
            train_end = $BatchDateValue
            certification_start = ""
            certification_end = ""
            bootstrap_start = $QualityFloorDate
            bootstrap_end = $BatchDateValue
        }
    }
}

function Compare-DateWindowRecords {
    param(
        [Parameter(Mandatory = $false)]$LeftWindow,
        [Parameter(Mandatory = $false)]$RightWindow
    )
    $comparison = [ordered]@{
        comparable = $false
        overlap = $false
        left_ends_before_right_starts = $false
        gap_days = $null
    }
    if ((-not (To-Bool (Get-PropValue -ObjectValue $LeftWindow -Name "valid" -DefaultValue $false) $false)) -or
        (-not (To-Bool (Get-PropValue -ObjectValue $RightWindow -Name "valid" -DefaultValue $false) $false))) {
        return $comparison
    }
    $leftStart = [DateTime]::ParseExact([string](Get-PropValue -ObjectValue $LeftWindow -Name "start" -DefaultValue ""), "yyyy-MM-dd", [System.Globalization.CultureInfo]::InvariantCulture)
    $leftEnd = [DateTime]::ParseExact([string](Get-PropValue -ObjectValue $LeftWindow -Name "end" -DefaultValue ""), "yyyy-MM-dd", [System.Globalization.CultureInfo]::InvariantCulture)
    $rightStart = [DateTime]::ParseExact([string](Get-PropValue -ObjectValue $RightWindow -Name "start" -DefaultValue ""), "yyyy-MM-dd", [System.Globalization.CultureInfo]::InvariantCulture)
    $rightEnd = [DateTime]::ParseExact([string](Get-PropValue -ObjectValue $RightWindow -Name "end" -DefaultValue ""), "yyyy-MM-dd", [System.Globalization.CultureInfo]::InvariantCulture)
    $comparison.comparable = $true
    $comparison.overlap = ($leftStart -le $rightEnd) -and ($rightStart -le $leftEnd)
    $comparison.left_ends_before_right_starts = $leftEnd -lt $rightStart
    if ($comparison.left_ends_before_right_starts) {
        $comparison.gap_days = [int](($rightStart - $leftEnd).TotalDays - 1)
    }
    return $comparison
}

function Resolve-ResearchWindowFromDecisionSurface {
    param(
        [Parameter(Mandatory = $false)]$DecisionSurface,
        [string]$FallbackStart,
        [string]$FallbackEnd
    )
    $trainerEntrypoint = Get-PropValue -ObjectValue $DecisionSurface -Name "trainer_entrypoint" -DefaultValue @{}
    $datasetWindow = Get-PropValue -ObjectValue $trainerEntrypoint -Name "dataset_window" -DefaultValue @{}
    $researchStart = [string](Get-PropValue -ObjectValue $datasetWindow -Name "start" -DefaultValue "")
    $researchEnd = [string](Get-PropValue -ObjectValue $datasetWindow -Name "end" -DefaultValue "")
    if ([string]::IsNullOrWhiteSpace($researchStart) -or [string]::IsNullOrWhiteSpace($researchEnd)) {
        return [ordered]@{
            start = $FallbackStart
            end = $FallbackEnd
            source = "candidate_acceptance_train_window_fallback"
            fallback = $true
        }
    }
    return [ordered]@{
        start = $researchStart
        end = $researchEnd
        source = "decision_surface.trainer_entrypoint.dataset_window"
        fallback = $false
    }
}

function New-CertificationArtifact {
    param(
        [string]$CandidateRunId,
        [string]$CandidateRunDir,
        [string]$PromotionDecisionPath,
        [string]$ResearchEvidencePath,
        [string]$EconomicObjectiveProfilePath,
        [string]$DecisionSurfacePath,
        [string]$TrainStartDate,
        [string]$TrainEndDate,
        [string]$CertificationStartDate,
        [string]$CertificationEndDate,
        [string]$TrainerEvidenceMode,
        [Parameter(Mandatory = $false)]$TrainerResearchPrior
    )
    $decisionSurface = Load-JsonOrEmpty -PathValue $DecisionSurfacePath
    $decisionSurfacePresent = -not (Test-IsEffectivelyEmptyObject -ObjectValue $decisionSurface)
    $economicObjectiveProfile = Load-JsonOrEmpty -PathValue $EconomicObjectiveProfilePath
    $economicObjectiveProfilePresent = -not (Test-IsEffectivelyEmptyObject -ObjectValue $economicObjectiveProfile)
    $researchWindowSpec = Resolve-ResearchWindowFromDecisionSurface `
        -DecisionSurface $decisionSurface `
        -FallbackStart $TrainStartDate `
        -FallbackEnd $TrainEndDate
    $trainWindow = New-DateWindowRecord -Name "train" -Start $TrainStartDate -End $TrainEndDate -Source "candidate_acceptance.train_args"
    $researchWindow = New-DateWindowRecord `
        -Name "research" `
        -Start ([string](Get-PropValue -ObjectValue $researchWindowSpec -Name "start" -DefaultValue "")) `
        -End ([string](Get-PropValue -ObjectValue $researchWindowSpec -Name "end" -DefaultValue "")) `
        -Source ([string](Get-PropValue -ObjectValue $researchWindowSpec -Name "source" -DefaultValue ""))
    $certificationWindow = New-DateWindowRecord `
        -Name "certification" `
        -Start $CertificationStartDate `
        -End $CertificationEndDate `
        -Source "candidate_acceptance.backtest_window"
    $trainVsResearch = Compare-DateWindowRecords -LeftWindow $trainWindow -RightWindow $researchWindow
    $trainVsCertification = Compare-DateWindowRecords -LeftWindow $trainWindow -RightWindow $certificationWindow
    $researchVsCertification = Compare-DateWindowRecords -LeftWindow $researchWindow -RightWindow $certificationWindow
    $reasons = @()
    if (-not $decisionSurfacePresent) {
        $reasons += "MISSING_DECISION_SURFACE"
    }
    foreach ($window in @($trainWindow, $researchWindow, $certificationWindow)) {
        $reasons = Merge-UniqueStringArray -First $reasons -Second @(Get-PropValue -ObjectValue $window -Name "reasons" -DefaultValue @())
    }
    if (To-Bool (Get-PropValue -ObjectValue $trainVsCertification -Name "overlap" -DefaultValue $false) $false) {
        $reasons += "TRAIN_CERTIFICATION_WINDOW_OVERLAP"
    }
    if (To-Bool (Get-PropValue -ObjectValue $researchVsCertification -Name "overlap" -DefaultValue $false) $false) {
        $reasons += "RESEARCH_CERTIFICATION_WINDOW_OVERLAP"
    }
    $reasons = Merge-UniqueStringArray -First $reasons -Second @()
    return [ordered]@{
        version = 1
        policy = "candidate_acceptance_certification_v1"
        generated_at = (Get-Date).ToString("o")
        candidate_run_id = $CandidateRunId
        candidate_run_dir = $CandidateRunDir
        status = "pending"
        provenance = [ordered]@{
            promotion_decision_path = $PromotionDecisionPath
            promotion_decision_present = (Test-Path $PromotionDecisionPath)
            trainer_research_prior_path = $ResearchEvidencePath
            trainer_research_prior_present = (Test-Path $ResearchEvidencePath)
            economic_objective_profile_path = $EconomicObjectiveProfilePath
            economic_objective_profile_present = $economicObjectiveProfilePresent
            economic_objective_profile_id = [string](Get-PropValue -ObjectValue $economicObjectiveProfile -Name "profile_id" -DefaultValue "")
            decision_surface_path = $DecisionSurfacePath
            decision_surface_present = $decisionSurfacePresent
            trainer_evidence_mode = $TrainerEvidenceMode
            trainer_evidence_source = "certification_artifact"
            research_evidence_source = "certification_lane_backtest"
            trainer_research_prior_source = "trainer_research_evidence_artifact"
        }
        windows = [ordered]@{
            train_window = $trainWindow
            research_window = $researchWindow
            certification_window = $certificationWindow
        }
        overlap_checks = [ordered]@{
            train_vs_research = $trainVsResearch
            train_vs_certification = $trainVsCertification
            research_vs_certification = $researchVsCertification
        }
        valid_window_contract = ($reasons.Count -eq 0)
        reasons = @($reasons)
        trainer_research_prior = $TrainerResearchPrior
        research_evidence = @{}
        certification = [ordered]@{
            evaluated = $false
        }
    }
}

function Resolve-ResearchEvidenceFromPromotionDecision {
    param(
        [Parameter(Mandatory = $false)]$PromotionDecision,
        [string]$Mode
    )
    $resolved = [ordered]@{
        mode = $Mode
        available = $false
        required = ($Mode -eq "required")
        source = "promotion_decision"
        pass = $null
        offline_pass = $false
        execution_pass = $true
        reasons = @()
        checks = [ordered]@{
            existing_champion_present = $false
            walk_forward_present = $false
            walk_forward_windows_run = 0
            offline_comparable = $false
            offline_candidate_edge = $false
            spa_like_present = $false
            spa_like_comparable = $false
            spa_like_candidate_edge = $false
            white_rc_present = $false
            white_rc_comparable = $false
            white_rc_candidate_edge = $false
            hansen_spa_present = $false
            hansen_spa_comparable = $false
            hansen_spa_candidate_edge = $false
            execution_acceptance_enabled = $false
            execution_acceptance_present = $false
            execution_comparable = $false
            execution_candidate_edge = $false
        }
        offline = [ordered]@{
            policy = ""
            decision = ""
            comparable = $false
        }
        spa_like = [ordered]@{
            policy = ""
            decision = ""
            comparable = $false
        }
        white_rc = [ordered]@{
            policy = ""
            decision = ""
            comparable = $false
        }
        hansen_spa = [ordered]@{
            policy = ""
            decision = ""
            comparable = $false
        }
        execution = [ordered]@{
            status = ""
            policy = ""
            decision = ""
            comparable = $false
        }
    }
    if ($Mode -eq "ignore") {
        $resolved.reasons = @("IGNORED_BY_POLICY")
        return $resolved
    }
    if (Test-IsEffectivelyEmptyObject -ObjectValue $PromotionDecision) {
        $resolved.pass = $false
        $resolved.reasons = @("MISSING_PROMOTION_DECISION")
        return $resolved
    }

    $checks = Get-PropValue -ObjectValue $PromotionDecision -Name "checks" -DefaultValue @{}
    $research = Get-PropValue -ObjectValue $PromotionDecision -Name "research_acceptance" -DefaultValue @{}
    $offlineCompare = Get-PropValue -ObjectValue $research -Name "compare_to_champion" -DefaultValue @{}
    $spaLikeDoc = Get-PropValue -ObjectValue $research -Name "spa_like_window_test" -DefaultValue @{}
    $whiteRcDoc = Get-PropValue -ObjectValue $research -Name "white_reality_check" -DefaultValue @{}
    $hansenSpaDoc = Get-PropValue -ObjectValue $research -Name "hansen_spa" -DefaultValue @{}
    $walkSummary = Get-PropValue -ObjectValue $research -Name "walk_forward_summary" -DefaultValue @{}
    $executionDoc = Get-PropValue -ObjectValue $PromotionDecision -Name "execution_acceptance" -DefaultValue @{}
    $executionCompare = Get-PropValue -ObjectValue $executionDoc -Name "compare_to_champion" -DefaultValue @{}

    $existingChampionPresent = To-Bool (Get-PropValue -ObjectValue $checks -Name "existing_champion_present" -DefaultValue $false) $false
    $walkForwardPresent = To-Bool (Get-PropValue -ObjectValue $checks -Name "walk_forward_present" -DefaultValue $false) $false
    $walkForwardWindowsRun = [int](To-Int64 (Get-PropValue -ObjectValue $checks -Name "walk_forward_windows_run" -DefaultValue 0) 0)
    $offlineComparable = To-Bool (Get-PropValue -ObjectValue $checks -Name "balanced_pareto_comparable" -DefaultValue $false) $false
    $offlineCandidateEdge = To-Bool (Get-PropValue -ObjectValue $checks -Name "balanced_pareto_candidate_edge" -DefaultValue $false) $false
    $spaLikePresent = To-Bool (Get-PropValue -ObjectValue $checks -Name "spa_like_present" -DefaultValue $false) $false
    $spaLikeComparable = To-Bool (Get-PropValue -ObjectValue $checks -Name "spa_like_comparable" -DefaultValue $false) $false
    $spaLikeCandidateEdge = To-Bool (Get-PropValue -ObjectValue $checks -Name "spa_like_candidate_edge" -DefaultValue $false) $false
    $whiteRcPresent = To-Bool (Get-PropValue -ObjectValue $checks -Name "white_rc_present" -DefaultValue $false) $false
    $whiteRcComparable = To-Bool (Get-PropValue -ObjectValue $checks -Name "white_rc_comparable" -DefaultValue $false) $false
    $whiteRcCandidateEdge = To-Bool (Get-PropValue -ObjectValue $checks -Name "white_rc_candidate_edge" -DefaultValue $false) $false
    $hansenSpaPresent = To-Bool (Get-PropValue -ObjectValue $checks -Name "hansen_spa_present" -DefaultValue $false) $false
    $hansenSpaComparable = To-Bool (Get-PropValue -ObjectValue $checks -Name "hansen_spa_comparable" -DefaultValue $false) $false
    $hansenSpaCandidateEdge = To-Bool (Get-PropValue -ObjectValue $checks -Name "hansen_spa_candidate_edge" -DefaultValue $false) $false
    $executionEnabled = To-Bool (Get-PropValue -ObjectValue $checks -Name "execution_acceptance_enabled" -DefaultValue $false) $false
    $executionPresent = To-Bool (Get-PropValue -ObjectValue $checks -Name "execution_acceptance_present" -DefaultValue $false) $false
    $executionComparable = To-Bool (Get-PropValue -ObjectValue $checks -Name "execution_balanced_pareto_comparable" -DefaultValue $false) $false
    $executionCandidateEdge = To-Bool (Get-PropValue -ObjectValue $checks -Name "execution_balanced_pareto_candidate_edge" -DefaultValue $false) $false

    $offlineDecision = [string](Get-PropValue -ObjectValue $offlineCompare -Name "decision" -DefaultValue "")
    $offlinePolicy = [string](Get-PropValue -ObjectValue $research -Name "policy" -DefaultValue "")
    $spaLikeDecision = [string](Get-PropValue -ObjectValue $spaLikeDoc -Name "decision" -DefaultValue "")
    $spaLikePolicy = [string](Get-PropValue -ObjectValue $spaLikeDoc -Name "policy" -DefaultValue "")
    $whiteRcDecision = [string](Get-PropValue -ObjectValue $whiteRcDoc -Name "decision" -DefaultValue "")
    $whiteRcPolicy = [string](Get-PropValue -ObjectValue $whiteRcDoc -Name "policy" -DefaultValue "")
    $hansenSpaDecision = [string](Get-PropValue -ObjectValue $hansenSpaDoc -Name "decision" -DefaultValue "")
    $hansenSpaPolicy = [string](Get-PropValue -ObjectValue $hansenSpaDoc -Name "policy" -DefaultValue "")
    $executionStatus = [string](Get-PropValue -ObjectValue $executionDoc -Name "status" -DefaultValue "")
    $executionDecision = [string](Get-PropValue -ObjectValue $executionCompare -Name "decision" -DefaultValue "")
    $executionPolicy = [string](Get-PropValue -ObjectValue $executionCompare -Name "policy" -DefaultValue "")

    $resolved.available = $walkForwardPresent -or $executionPresent -or (-not [string]::IsNullOrWhiteSpace($offlineDecision)) -or (-not [string]::IsNullOrWhiteSpace($executionDecision))
    $resolved.checks.existing_champion_present = $existingChampionPresent
    $resolved.checks.walk_forward_present = $walkForwardPresent
    $resolved.checks.walk_forward_windows_run = $walkForwardWindowsRun
    $resolved.checks.offline_comparable = $offlineComparable
    $resolved.checks.offline_candidate_edge = $offlineCandidateEdge
    $resolved.checks.spa_like_present = $spaLikePresent
    $resolved.checks.spa_like_comparable = $spaLikeComparable
    $resolved.checks.spa_like_candidate_edge = $spaLikeCandidateEdge
    $resolved.checks.white_rc_present = $whiteRcPresent
    $resolved.checks.white_rc_comparable = $whiteRcComparable
    $resolved.checks.white_rc_candidate_edge = $whiteRcCandidateEdge
    $resolved.checks.hansen_spa_present = $hansenSpaPresent
    $resolved.checks.hansen_spa_comparable = $hansenSpaComparable
    $resolved.checks.hansen_spa_candidate_edge = $hansenSpaCandidateEdge
    $resolved.checks.execution_acceptance_enabled = $executionEnabled
    $resolved.checks.execution_acceptance_present = $executionPresent
    $resolved.checks.execution_comparable = $executionComparable
    $resolved.checks.execution_candidate_edge = $executionCandidateEdge
    $resolved.offline.policy = $offlinePolicy
    $resolved.offline.decision = $offlineDecision
    $resolved.offline.comparable = $offlineComparable
    $resolved.spa_like.policy = $spaLikePolicy
    $resolved.spa_like.decision = $spaLikeDecision
    $resolved.spa_like.comparable = $spaLikeComparable
    $resolved.white_rc.policy = $whiteRcPolicy
    $resolved.white_rc.decision = $whiteRcDecision
    $resolved.white_rc.comparable = $whiteRcComparable
    $resolved.hansen_spa.policy = $hansenSpaPolicy
    $resolved.hansen_spa.decision = $hansenSpaDecision
    $resolved.hansen_spa.comparable = $hansenSpaComparable
    $resolved.execution.status = $executionStatus
    $resolved.execution.policy = $executionPolicy
    $resolved.execution.decision = $executionDecision
    $resolved.execution.comparable = $executionComparable

    if (-not $walkForwardPresent) {
        $resolved.reasons += "NO_WALK_FORWARD_EVIDENCE"
    } elseif ($existingChampionPresent) {
        if (-not $offlineComparable) {
            $resolved.reasons += "OFFLINE_NOT_COMPARABLE"
        } elseif (-not $offlineCandidateEdge) {
            $resolved.reasons += "OFFLINE_NOT_CANDIDATE_EDGE"
        }
        if ($spaLikePresent) {
            if (-not $spaLikeComparable) {
                $resolved.reasons += "SPA_LIKE_NOT_COMPARABLE"
            } elseif (-not $spaLikeCandidateEdge) {
                $resolved.reasons += "SPA_LIKE_NOT_CANDIDATE_EDGE"
            }
        }
        if ($whiteRcPresent) {
            if (-not $whiteRcComparable) {
                $resolved.reasons += "WHITE_RC_NOT_COMPARABLE"
            } elseif (-not $whiteRcCandidateEdge) {
                $resolved.reasons += "WHITE_RC_NOT_CANDIDATE_EDGE"
            }
        }
        if ($hansenSpaPresent) {
            if (-not $hansenSpaComparable) {
                $resolved.reasons += "HANSEN_SPA_NOT_COMPARABLE"
            } elseif (-not $hansenSpaCandidateEdge) {
                $resolved.reasons += "HANSEN_SPA_NOT_CANDIDATE_EDGE"
            }
        }
    }

    $offlinePass = $walkForwardPresent -and (
        (-not $existingChampionPresent) -or (
            $offlineComparable -and
            $offlineCandidateEdge -and
            ((-not $spaLikePresent) -or ($spaLikeComparable -and $spaLikeCandidateEdge)) -and
            ((-not $whiteRcPresent) -or ($whiteRcComparable -and $whiteRcCandidateEdge)) -and
            ((-not $hansenSpaPresent) -or ($hansenSpaComparable -and $hansenSpaCandidateEdge))
        )
    )
    $executionPass = $true
    if ($executionEnabled) {
        if (-not $executionPresent) {
            $executionPass = $false
            $resolved.reasons += "NO_EXECUTION_EVIDENCE"
        } elseif ($existingChampionPresent) {
            if (-not $executionComparable) {
                $executionPass = $false
                $resolved.reasons += "EXECUTION_NOT_COMPARABLE"
            } elseif (-not $executionCandidateEdge) {
                $executionPass = $false
                $resolved.reasons += "EXECUTION_NOT_CANDIDATE_EDGE"
            }
        }
    }
    $resolved.offline_pass = $offlinePass
    $resolved.execution_pass = $executionPass
    $resolved.pass = $offlinePass -and $executionPass
    if ($resolved.available -and $resolved.reasons.Count -eq 0) {
        $resolved.reasons = @("TRAINER_EVIDENCE_PASS")
    }
    return $resolved
}

function Resolve-ResearchEvidenceFromArtifact {
    param(
        [Parameter(Mandatory = $false)]$ResearchEvidenceArtifact,
        [string]$Mode
    )
    $resolved = [ordered]@{
        mode = $Mode
        available = $false
        required = ($Mode -eq "required")
        source = "trainer_research_evidence_artifact"
        pass = $null
        offline_pass = $false
        execution_pass = $true
        reasons = @()
        checks = [ordered]@{}
        offline = [ordered]@{}
        spa_like = [ordered]@{}
        white_rc = [ordered]@{}
        hansen_spa = [ordered]@{}
        execution = [ordered]@{}
        support_lane = [ordered]@{}
    }
    if ($Mode -eq "ignore") {
        $resolved.reasons = @("IGNORED_BY_POLICY")
        return $resolved
    }
    if (Test-IsEffectivelyEmptyObject -ObjectValue $ResearchEvidenceArtifact) {
        $resolved.pass = $false
        $resolved.reasons = @("MISSING_TRAINER_RESEARCH_EVIDENCE_ARTIFACT")
        return $resolved
    }
    $resolved.available = To-Bool (Get-PropValue -ObjectValue $ResearchEvidenceArtifact -Name "available" -DefaultValue $false) $false
    $resolved.pass = To-Bool (Get-PropValue -ObjectValue $ResearchEvidenceArtifact -Name "pass" -DefaultValue $false) $false
    $resolved.offline_pass = To-Bool (Get-PropValue -ObjectValue $ResearchEvidenceArtifact -Name "offline_pass" -DefaultValue $false) $false
    $resolved.execution_pass = To-Bool (Get-PropValue -ObjectValue $ResearchEvidenceArtifact -Name "execution_pass" -DefaultValue $true) $true
    $resolved.reasons = @((Get-PropValue -ObjectValue $ResearchEvidenceArtifact -Name "reasons" -DefaultValue @()))
    $resolved.checks = (Get-PropValue -ObjectValue $ResearchEvidenceArtifact -Name "checks" -DefaultValue @{})
    $resolved.offline = (Get-PropValue -ObjectValue $ResearchEvidenceArtifact -Name "offline" -DefaultValue @{})
    $resolved.spa_like = (Get-PropValue -ObjectValue $ResearchEvidenceArtifact -Name "spa_like" -DefaultValue @{})
    $resolved.white_rc = (Get-PropValue -ObjectValue $ResearchEvidenceArtifact -Name "white_rc" -DefaultValue @{})
    $resolved.hansen_spa = (Get-PropValue -ObjectValue $ResearchEvidenceArtifact -Name "hansen_spa" -DefaultValue @{})
    $resolved.execution = (Get-PropValue -ObjectValue $ResearchEvidenceArtifact -Name "execution" -DefaultValue @{})
    $resolved.support_lane = (Get-PropValue -ObjectValue $ResearchEvidenceArtifact -Name "support_lane" -DefaultValue @{})
    if ($resolved.available -and $resolved.reasons.Count -eq 0) {
        $resolved.reasons = @("TRAINER_EVIDENCE_PASS")
    }
    return $resolved
}

function New-CertificationResearchEvidence {
    param(
        [Parameter(Mandatory = $false)]$CertificationArtifact,
        [Parameter(Mandatory = $false)]$TrainerResearchPrior,
        [string]$CertificationStartDate,
        [string]$CertificationEndDate,
        [string]$PromotionPolicyName,
        [string]$EconomicObjectiveProfileId,
        [bool]$CandidateBacktestPresent,
        [int64]$CandidateOrdersFilled,
        [double]$CandidateRealizedPnl,
        [bool]$CandidateMinOrdersPass,
        [int]$CandidateMinOrdersThreshold,
        [bool]$CandidateMinRealizedPnlPass,
        [double]$CandidateMinRealizedPnlThreshold,
        [bool]$CandidateDsrEvaluated,
        [double]$CandidateDeflatedSharpeRatio,
        [double]$CandidateMinDeflatedSharpeRatio,
        [bool]$CandidateDeflatedSharpePass,
        [bool]$CandidateBacktestPass,
        [bool]$CompareRequired,
        [bool]$ChampionPresent,
        [bool]$ChampionCompareEvaluated,
        [bool]$ChampionComparePass,
        [bool]$CandidateParetoDominates,
        [bool]$ChampionParetoDominates,
        [bool]$ParetoIncomparable,
        [string]$DecisionBasis
    )
    $artifactReasons = @((Get-PropValue -ObjectValue $CertificationArtifact -Name "reasons" -DefaultValue @()))
    $windowValid = To-Bool (Get-PropValue -ObjectValue $CertificationArtifact -Name "valid_window_contract" -DefaultValue $false) $false
    $artifactProvenance = Get-PropValue -ObjectValue $CertificationArtifact -Name "provenance" -DefaultValue @{}
    $trainerResearchPriorPresent = To-Bool (Get-PropValue -ObjectValue $artifactProvenance -Name "trainer_research_prior_present" -DefaultValue $false) $false
    $trainerSupportLane = Get-PropValue -ObjectValue $TrainerResearchPrior -Name "support_lane" -DefaultValue @{}
    $trainerResearchPriorPass = if ($trainerResearchPriorPresent) {
        To-Bool (Get-PropValue -ObjectValue $TrainerResearchPrior -Name "pass" -DefaultValue $false) $false
    } else {
        $false
    }
    if (Test-IsEffectivelyEmptyObject -ObjectValue $trainerSupportLane) {
        $supportLaneReasons = if ($trainerResearchPriorPresent) { @("MISSING_TRAINER_SUPPORT_LANE") } else { @("TRAINER_RESEARCH_PRIOR_NOT_PRESENT") }
        $trainerSupportLane = [ordered]@{
            policy = "v4_certification_support_lane_v1"
            source = "trainer_research_prior"
            support_only = $true
            summary = [ordered]@{
                status = if ($trainerResearchPriorPresent) { "missing" } else { "missing_prior" }
                windows_run = 0
                multiple_testing_supported = $false
                cpcv_lite_status = "missing"
                reasons = @($supportLaneReasons)
            }
            multiple_testing_panel_diagnostics = [ordered]@{}
            spa_like = [ordered]@{}
            white_rc = [ordered]@{}
            hansen_spa = [ordered]@{}
            cpcv_lite = [ordered]@{}
        }
    }

    $reasons = @()
    if (-not $windowValid) {
        $reasons += "CERTIFICATION_WINDOW_CONTRACT_INVALID"
        $reasons = Merge-UniqueStringArray -First $reasons -Second $artifactReasons
    }
    if (-not $CandidateBacktestPresent) {
        $reasons += "NO_CERTIFICATION_CANDIDATE_BACKTEST"
    }
    if (-not $CandidateMinOrdersPass) {
        $reasons += "CERTIFICATION_MIN_ORDERS_NOT_MET"
    }
    if (-not $CandidateMinRealizedPnlPass) {
        $reasons += "CERTIFICATION_MIN_REALIZED_PNL_NOT_MET"
    }
    if (-not $CandidateDeflatedSharpePass) {
        $reasons += "CERTIFICATION_MIN_DSR_NOT_MET"
    }
    if ($CompareRequired) {
        if (-not $ChampionPresent) {
            $reasons += "NO_CERTIFICATION_CHAMPION"
        } elseif (-not $ChampionCompareEvaluated) {
            $reasons += "NO_CERTIFICATION_CHAMPION_COMPARE"
        } elseif (-not $ChampionComparePass) {
            if ($ChampionParetoDominates) {
                $reasons += "CERTIFICATION_CHAMPION_PARETO_DOMINANCE"
            } elseif ($ParetoIncomparable) {
                $reasons += "CERTIFICATION_COMPARE_INCOMPARABLE"
            } else {
                $reasons += "CERTIFICATION_EXECUTION_NOT_CANDIDATE_EDGE"
            }
        }
    }

    $offlinePass = $windowValid -and $CandidateBacktestPass
    $executionPass = if ($CompareRequired) {
        $ChampionPresent -and $ChampionCompareEvaluated -and $ChampionComparePass
    } else {
        $true
    }
    $passed = $offlinePass -and $executionPass
    if ($passed -and $reasons.Count -eq 0) {
        $reasons = @("CERTIFICATION_EVIDENCE_PASS")
    }

    $offlineDecision = if (-not $CandidateBacktestPresent) {
        "missing"
    } elseif ($offlinePass) {
        "candidate_pass"
    } else {
        "candidate_fail"
    }
    $executionDecision = if (-not $CompareRequired) {
        "sanity_only"
    } elseif (-not $ChampionPresent) {
        "missing_champion"
    } elseif (-not $ChampionCompareEvaluated) {
        "not_evaluated"
    } elseif ($ChampionComparePass) {
        "candidate_edge"
    } elseif ($ChampionParetoDominates) {
        "champion_edge"
    } elseif ($ParetoIncomparable) {
        "incomparable"
    } else {
        "candidate_fail"
    }

    return [ordered]@{
        version = 1
        policy = "candidate_acceptance_certification_research_evidence_v1"
        source = "certification_lane_backtest"
        available = ($windowValid -and $CandidateBacktestPresent)
        pass = $passed
        offline_pass = $offlinePass
        execution_pass = $executionPass
        reasons = @($reasons)
        checks = [ordered]@{
            certification_window_valid = $windowValid
            candidate_backtest_present = $CandidateBacktestPresent
            certification_window_start = $CertificationStartDate
            certification_window_end = $CertificationEndDate
            compare_required = $CompareRequired
            champion_present = $ChampionPresent
            champion_compare_evaluated = $ChampionCompareEvaluated
            champion_compare_pass = $ChampionComparePass
            candidate_min_orders_threshold = $CandidateMinOrdersThreshold
            candidate_orders_filled = [int64]$CandidateOrdersFilled
            candidate_min_orders_pass = $CandidateMinOrdersPass
            candidate_min_realized_pnl_threshold = [double]$CandidateMinRealizedPnlThreshold
            candidate_realized_pnl_quote = [double]$CandidateRealizedPnl
            candidate_min_realized_pnl_pass = $CandidateMinRealizedPnlPass
            candidate_dsr_evaluated = $CandidateDsrEvaluated
            candidate_min_deflated_sharpe_ratio = [double]$CandidateMinDeflatedSharpeRatio
            candidate_deflated_sharpe_ratio_est = [double]$CandidateDeflatedSharpeRatio
            candidate_deflated_sharpe_pass = $CandidateDeflatedSharpePass
            candidate_backtest_pass = $CandidateBacktestPass
            candidate_pareto_dominates = $CandidateParetoDominates
            champion_pareto_dominates = $ChampionParetoDominates
            pareto_incomparable = $ParetoIncomparable
            decision_basis = $DecisionBasis
            promotion_policy = $PromotionPolicyName
            economic_objective_profile_id = $EconomicObjectiveProfileId
            trainer_research_prior_present = $trainerResearchPriorPresent
            trainer_research_prior_pass = $trainerResearchPriorPass
            support_lane_present = (-not (Test-IsEffectivelyEmptyObject -ObjectValue $trainerSupportLane))
            support_lane_status = [string](Get-PropValue -ObjectValue (Get-PropValue -ObjectValue $trainerSupportLane -Name "summary" -DefaultValue @{}) -Name "status" -DefaultValue "")
        }
        offline = [ordered]@{
            policy = "certification_candidate_sanity_v1"
            decision = $offlineDecision
            comparable = $CandidateBacktestPresent
        }
        spa_like = [ordered]@{
            policy = "not_run_in_certification_lane"
            decision = "not_evaluated"
            comparable = $false
        }
        white_rc = [ordered]@{
            policy = "not_run_in_certification_lane"
            decision = "not_evaluated"
            comparable = $false
        }
        hansen_spa = [ordered]@{
            policy = "not_run_in_certification_lane"
            decision = "not_evaluated"
            comparable = $false
        }
        execution = [ordered]@{
            status = if ($CompareRequired) { "compared" } else { "candidate_only" }
            policy = "certification_backtest_compare_v1"
            decision = $executionDecision
            comparable = ($ChampionPresent -and $ChampionCompareEvaluated)
        }
        trainer_research_prior = [ordered]@{
            present = $trainerResearchPriorPresent
            pass = $trainerResearchPriorPass
            source = [string](Get-PropValue -ObjectValue $TrainerResearchPrior -Name "source" -DefaultValue "")
        }
        support_lane = $trainerSupportLane
    }
}

function Resolve-TrainerEvidenceFromCertificationArtifact {
    param(
        [Parameter(Mandatory = $false)]$CertificationArtifact,
        [string]$Mode
    )
    $resolved = [ordered]@{
        mode = $Mode
        available = $false
        required = ($Mode -eq "required")
        source = "certification_artifact"
        pass = $null
        offline_pass = $false
        execution_pass = $true
        reasons = @()
        checks = [ordered]@{}
        offline = [ordered]@{}
        spa_like = [ordered]@{}
        white_rc = [ordered]@{}
        hansen_spa = [ordered]@{}
        execution = [ordered]@{}
        support_lane = [ordered]@{}
        certification_window_valid = $false
        certification_window_reasons = @()
    }
    if ($Mode -eq "ignore") {
        $resolved.reasons = @("IGNORED_BY_POLICY")
        return $resolved
    }
    if (Test-IsEffectivelyEmptyObject -ObjectValue $CertificationArtifact) {
        $resolved.pass = $false
        $resolved.reasons = @("MISSING_CERTIFICATION_ARTIFACT")
        return $resolved
    }
    $researchEvidence = Get-PropValue -ObjectValue $CertificationArtifact -Name "research_evidence" -DefaultValue @{}
    $artifactReasons = @(Get-PropValue -ObjectValue $CertificationArtifact -Name "reasons" -DefaultValue @())
    $windowValid = To-Bool (Get-PropValue -ObjectValue $CertificationArtifact -Name "valid_window_contract" -DefaultValue $false) $false
    if (Test-IsEffectivelyEmptyObject -ObjectValue $researchEvidence) {
        $resolved.pass = $false
        $resolved.certification_window_valid = $windowValid
        $resolved.certification_window_reasons = @($artifactReasons)
        $resolved.reasons = Merge-UniqueStringArray -First @("MISSING_CERTIFICATION_RESEARCH_EVIDENCE") -Second $artifactReasons
        return $resolved
    }
    $resolved.available = To-Bool (Get-PropValue -ObjectValue $researchEvidence -Name "available" -DefaultValue $false) $false
    $resolved.pass = To-Bool (Get-PropValue -ObjectValue $researchEvidence -Name "pass" -DefaultValue $false) $false
    $resolved.offline_pass = To-Bool (Get-PropValue -ObjectValue $researchEvidence -Name "offline_pass" -DefaultValue $false) $false
    $resolved.execution_pass = To-Bool (Get-PropValue -ObjectValue $researchEvidence -Name "execution_pass" -DefaultValue $true) $true
    $resolved.reasons = @((Get-PropValue -ObjectValue $researchEvidence -Name "reasons" -DefaultValue @()))
    $resolved.checks = (Get-PropValue -ObjectValue $researchEvidence -Name "checks" -DefaultValue @{})
    $resolved.offline = (Get-PropValue -ObjectValue $researchEvidence -Name "offline" -DefaultValue @{})
    $resolved.spa_like = (Get-PropValue -ObjectValue $researchEvidence -Name "spa_like" -DefaultValue @{})
    $resolved.white_rc = (Get-PropValue -ObjectValue $researchEvidence -Name "white_rc" -DefaultValue @{})
    $resolved.hansen_spa = (Get-PropValue -ObjectValue $researchEvidence -Name "hansen_spa" -DefaultValue @{})
    $resolved.execution = (Get-PropValue -ObjectValue $researchEvidence -Name "execution" -DefaultValue @{})
    $resolved.support_lane = (Get-PropValue -ObjectValue $researchEvidence -Name "support_lane" -DefaultValue @{})
    $resolved.certification_window_valid = $windowValid
    $resolved.certification_window_reasons = @($artifactReasons)
    if (-not $windowValid) {
        $resolved.pass = $false
        $resolved.reasons = Merge-UniqueStringArray -First $resolved.reasons -Second $artifactReasons
    }
    if ($resolved.available -and $resolved.reasons.Count -eq 0) {
        $resolved.reasons = @("TRAINER_EVIDENCE_PASS")
    }
    return $resolved
}

function Get-OutputPreview {
    param([string]$Text, [int]$MaxLength = 400)
    if ([string]::IsNullOrWhiteSpace($Text)) {
        return ""
    }
    $preview = $Text.Trim() -replace "\r?\n", " | "
    if ($preview.Length -le $MaxLength) {
        return $preview
    }
    return $preview.Substring(0, $MaxLength)
}

function Format-CommandText {
    param([string]$Exe, [string[]]$ArgList)
    return ($Exe + " " + ($ArgList -join " ")).Trim()
}

function Invoke-CommandCapture {
    param(
        [string]$Exe,
        [string[]]$ArgList,
        [switch]$AllowFailure
    )
    $commandText = Format-CommandText -Exe $Exe -ArgList $ArgList
    if ($DryRun) {
        return [PSCustomObject]@{
            ExitCode = 0
            Output = "[dry-run] $commandText"
            Command = $commandText
            DryRun = $true
        }
    }
    $snapshotEnvName = "AUTOBOT_DATA_PLATFORM_READY_SNAPSHOT_ID"
    $hadSnapshotEnv = Test-Path ("Env:" + $snapshotEnvName)
    $previousSnapshotEnv = if ($hadSnapshotEnv) { [string](Get-Item ("Env:" + $snapshotEnvName)).Value } else { "" }
    $effectiveSnapshotEnv = [string]$script:dataPlatformReadySnapshotId
    $hadNativeErrorPreference = Test-Path Variable:PSNativeCommandUseErrorActionPreference
    $previousNativeErrorPreference = if ($hadNativeErrorPreference) { $global:PSNativeCommandUseErrorActionPreference } else { $null }
    $stdoutPath = ""
    $stderrPath = ""
    try {
        if ([string]::IsNullOrWhiteSpace($effectiveSnapshotEnv)) {
            Remove-Item ("Env:" + $snapshotEnvName) -ErrorAction SilentlyContinue
        } else {
            $env:AUTOBOT_DATA_PLATFORM_READY_SNAPSHOT_ID = $effectiveSnapshotEnv
        }
        $global:PSNativeCommandUseErrorActionPreference = $false
        $stdoutPath = [System.IO.Path]::GetTempFileName()
        $stderrPath = [System.IO.Path]::GetTempFileName()
        & $Exe @ArgList 1> $stdoutPath 2> $stderrPath
        $exitCode = [int]$LASTEXITCODE
        $stdoutText = if (Test-Path $stdoutPath) { Get-Content -Path $stdoutPath -Raw -ErrorAction SilentlyContinue } else { "" }
        $stderrText = if (Test-Path $stderrPath) { Get-Content -Path $stderrPath -Raw -ErrorAction SilentlyContinue } else { "" }
        $outputParts = New-Object System.Collections.Generic.List[string]
        if (-not [string]::IsNullOrWhiteSpace([string]$stdoutText)) {
            $outputParts.Add([string]$stdoutText) | Out-Null
        }
        if (-not [string]::IsNullOrWhiteSpace([string]$stderrText)) {
            $outputParts.Add([string]$stderrText) | Out-Null
        }
        $output = @($outputParts.ToArray())
    } finally {
        if ($hadSnapshotEnv) {
            $env:AUTOBOT_DATA_PLATFORM_READY_SNAPSHOT_ID = $previousSnapshotEnv
        } else {
            Remove-Item ("Env:" + $snapshotEnvName) -ErrorAction SilentlyContinue
        }
        if ($hadNativeErrorPreference) {
            $global:PSNativeCommandUseErrorActionPreference = $previousNativeErrorPreference
        } else {
            Remove-Variable -Name PSNativeCommandUseErrorActionPreference -Scope Global -ErrorAction SilentlyContinue
        }
        if (-not [string]::IsNullOrWhiteSpace($stdoutPath)) {
            Remove-Item -LiteralPath $stdoutPath -ErrorAction SilentlyContinue
        }
        if (-not [string]::IsNullOrWhiteSpace($stderrPath)) {
            Remove-Item -LiteralPath $stderrPath -ErrorAction SilentlyContinue
        }
    }
    if ((-not $AllowFailure) -and $exitCode -ne 0) {
        $outputText = ($output -join "`n")
        throw "Command failed (exit=$exitCode): $commandText`n$outputText"
    }
    return [PSCustomObject]@{
        ExitCode = $exitCode
        Output = ($output -join "`n")
        Command = $commandText
        DryRun = $false
    }
}

function Get-MedianDouble {
    param([double[]]$Values)
    if ($null -eq $Values -or $Values.Count -le 0) {
        return 0.0
    }
    $ordered = @($Values | Sort-Object)
    $count = $ordered.Count
    $middle = [int]([math]::Floor($count / 2))
    if (($count % 2) -eq 1) {
        return [double]$ordered[$middle]
    }
    return ([double]$ordered[$middle - 1] + [double]$ordered[$middle]) / 2.0
}

function Invoke-BacktestStatValidation {
    param(
        [string]$PythonPath,
        [string]$Root,
        [string]$RunDir,
        [int]$TrialCount,
        [string]$ModelRunDir = ""
    )
    if ([string]::IsNullOrWhiteSpace($RunDir)) {
        return @{}
    }
    $args = @(
        "-m", "autobot.models.stat_validation",
        "--run-dir", $RunDir,
        "--trial-count", ([string]([Math]::Max([int]$TrialCount, 1)))
    )
    if (-not [string]::IsNullOrWhiteSpace($ModelRunDir)) {
        $args += @("--model-run-dir", $ModelRunDir)
    }
    $exec = Invoke-CommandCapture -Exe $PythonPath -ArgList $args -AllowFailure
    if ($exec.ExitCode -ne 0) {
        return @{
            comparable = $false
            reasons = @("STAT_VALIDATION_COMMAND_FAILED")
            exit_code = [int]$exec.ExitCode
            output_preview = (Get-OutputPreview -Text ([string]$exec.Output))
        }
    }
    try {
        $parsed = [string]$exec.Output | ConvertFrom-Json
        if ($parsed -is [System.Collections.IDictionary]) {
            return $parsed
        }
        return $parsed
    } catch {
        return @{
            comparable = $false
            reasons = @("STAT_VALIDATION_JSON_PARSE_FAILED")
            output_preview = (Get-OutputPreview -Text ([string]$exec.Output))
        }
    }
}

function Get-Sha256Hex {
    param([string]$Text)
    $value = [string]$Text
    $bytes = [System.Text.Encoding]::UTF8.GetBytes($value)
    $sha = [System.Security.Cryptography.SHA256]::Create()
    try {
        $hashBytes = $sha.ComputeHash($bytes)
    } finally {
        $sha.Dispose()
    }
    return ([System.BitConverter]::ToString($hashBytes)).Replace("-", "").ToLowerInvariant()
}

function Resolve-ModelRunSnapshotId {
    param(
        [string]$ModelRunDir,
        [string]$DefaultSnapshotId = ""
    )
    if ([string]::IsNullOrWhiteSpace($ModelRunDir)) {
        return [string]$DefaultSnapshotId
    }
    $trainConfigPath = Join-Path $ModelRunDir "train_config.yaml"
    if (-not (Test-Path $trainConfigPath)) {
        return [string]$DefaultSnapshotId
    }
    $trainConfig = Load-JsonOrEmpty -PathValue $trainConfigPath
    $resolved = [string](Get-PropValue -ObjectValue $trainConfig -Name "data_platform_ready_snapshot_id" -DefaultValue "")
    if ([string]::IsNullOrWhiteSpace($resolved)) {
        return [string]$DefaultSnapshotId
    }
    return $resolved
}

function New-AcceptanceBacktestContract {
    param(
        [string]$StepName,
        [string]$ModelRef,
        [string]$ModelFamilyName,
        [string]$ModelRunDir,
        [string]$StartDate,
        [string]$EndDate,
        [ValidateSet("acceptance", "runtime_parity")]
        [string]$Preset
    )
    $resolvedSnapshotId = Resolve-ModelRunSnapshotId -ModelRunDir $ModelRunDir -DefaultSnapshotId $script:dataPlatformReadySnapshotId
    $candidateOrChampion = if ([string]$StepName -match "champion") { "champion" } else { "candidate" }
    return [ordered]@{
        version = 1
        policy = "candidate_acceptance_backtest_contract_v1"
        step_name = [string]$StepName
        candidate_or_champion = $candidateOrChampion
        model_ref = [string]$ModelRef
        model_family = [string]$ModelFamilyName
        model_run_dir = [string]$ModelRunDir
        preset = [string]$Preset
        feature_set = [string]$FeatureSet
        tf = [string]$Tf
        quote = [string]$Quote
        top_n = [int]$BacktestTopN
        start = [string]$StartDate
        end = [string]$EndDate
        top_pct = [double]$BacktestTopPct
        min_prob = [double]$BacktestMinProb
        min_candidates_per_ts = [int]$BacktestMinCandidatesPerTs
        hold_bars = [int]$HoldBars
        backtest_runtime_parity_enabled = [bool]$BacktestRuntimeParityEnabled
        compare_required = [bool]$promotionPolicyConfig.backtest_compare_required
        compare_mode = [string]$promotionPolicyConfig.name
        feature_dataset_snapshot_id = [string]$resolvedSnapshotId
    }
}

function Get-AcceptanceBacktestCacheRoot {
    param(
        [string]$RegistryRoot,
        [string]$ModelFamilyName
    )
    return (Join-Path (Join-Path $RegistryRoot $ModelFamilyName) "_acceptance_backtest_cache")
}

function Get-AcceptanceBacktestCacheKey {
    param($Contract)
    $contractJson = $Contract | ConvertTo-Json -Depth 20 -Compress
    return (Get-Sha256Hex -Text $contractJson)
}

function Get-AcceptanceBacktestCacheEntryPaths {
    param(
        [string]$RegistryRoot,
        [string]$ModelFamilyName,
        $Contract
    )
    $cacheRoot = Get-AcceptanceBacktestCacheRoot -RegistryRoot $RegistryRoot -ModelFamilyName $ModelFamilyName
    $cacheKey = Get-AcceptanceBacktestCacheKey -Contract $Contract
    $entryRoot = Join-Path $cacheRoot $cacheKey
    return [ordered]@{
        cache_root = $cacheRoot
        cache_key = $cacheKey
        entry_root = $entryRoot
        contract_path = (Join-Path $entryRoot "contract.json")
        summary_path = (Join-Path $entryRoot "summary.json")
        stat_validation_path = (Join-Path $entryRoot "stat_validation.json")
        metadata_path = (Join-Path $entryRoot "metadata.json")
    }
}

function Resolve-AcceptanceBacktestCacheHit {
    param(
        [string]$RegistryRoot,
        [string]$ModelFamilyName,
        $Contract
    )
    $paths = Get-AcceptanceBacktestCacheEntryPaths -RegistryRoot $RegistryRoot -ModelFamilyName $ModelFamilyName -Contract $Contract
    $contractPath = [string](Get-PropValue -ObjectValue $paths -Name "contract_path" -DefaultValue "")
    $summaryPath = [string](Get-PropValue -ObjectValue $paths -Name "summary_path" -DefaultValue "")
    $statValidationPath = [string](Get-PropValue -ObjectValue $paths -Name "stat_validation_path" -DefaultValue "")
    $metadataPath = [string](Get-PropValue -ObjectValue $paths -Name "metadata_path" -DefaultValue "")
    if ((-not (Test-Path $contractPath)) -or (-not (Test-Path $summaryPath)) -or (-not (Test-Path $statValidationPath)) -or (-not (Test-Path $metadataPath))) {
        return [ordered]@{
            hit = $false
            reason = "CACHE_ENTRY_MISSING"
            paths = $paths
        }
    }
    $cachedContract = Load-JsonOrEmpty -PathValue $contractPath
    $expectedJson = $Contract | ConvertTo-Json -Depth 20 -Compress
    $cachedJson = $cachedContract | ConvertTo-Json -Depth 20 -Compress
    if ($cachedJson -ne $expectedJson) {
        return [ordered]@{
            hit = $false
            reason = "CACHE_CONTRACT_MISMATCH"
            paths = $paths
        }
    }
    return [ordered]@{
        hit = $true
        reason = "CACHE_HIT"
        paths = $paths
        contract = $cachedContract
        summary = Load-JsonOrEmpty -PathValue $summaryPath
        stat_validation = Load-JsonOrEmpty -PathValue $statValidationPath
        metadata = Load-JsonOrEmpty -PathValue $metadataPath
    }
}

function Write-AcceptanceBacktestCacheEntry {
    param(
        [string]$RegistryRoot,
        [string]$ModelFamilyName,
        $Contract,
        $Summary,
        $StatValidation,
        [string]$SourceBacktestRunDir
    )
    if ($DryRun) {
        return (Get-AcceptanceBacktestCacheEntryPaths -RegistryRoot $RegistryRoot -ModelFamilyName $ModelFamilyName -Contract $Contract)
    }
    $paths = Get-AcceptanceBacktestCacheEntryPaths -RegistryRoot $RegistryRoot -ModelFamilyName $ModelFamilyName -Contract $Contract
    $entryRoot = [string](Get-PropValue -ObjectValue $paths -Name "entry_root" -DefaultValue "")
    New-Item -ItemType Directory -Force -Path $entryRoot | Out-Null
    Write-JsonFile -PathValue ([string](Get-PropValue -ObjectValue $paths -Name "contract_path" -DefaultValue "")) -Payload $Contract
    Write-JsonFile -PathValue ([string](Get-PropValue -ObjectValue $paths -Name "summary_path" -DefaultValue "")) -Payload $Summary
    Write-JsonFile -PathValue ([string](Get-PropValue -ObjectValue $paths -Name "stat_validation_path" -DefaultValue "")) -Payload $StatValidation
    Write-JsonFile -PathValue ([string](Get-PropValue -ObjectValue $paths -Name "metadata_path" -DefaultValue "")) -Payload ([ordered]@{
        version = 1
        policy = "candidate_acceptance_backtest_cache_metadata_v1"
        cache_key = [string](Get-PropValue -ObjectValue $paths -Name "cache_key" -DefaultValue "")
        step_name = [string](Get-PropValue -ObjectValue $Contract -Name "step_name" -DefaultValue "")
        candidate_or_champion = [string](Get-PropValue -ObjectValue $Contract -Name "candidate_or_champion" -DefaultValue "")
        preset = [string](Get-PropValue -ObjectValue $Contract -Name "preset" -DefaultValue "")
        model_ref = [string](Get-PropValue -ObjectValue $Contract -Name "model_ref" -DefaultValue "")
        model_family = [string](Get-PropValue -ObjectValue $Contract -Name "model_family" -DefaultValue "")
        source_backtest_run_dir = [string]$SourceBacktestRunDir
        created_at_utc = (Get-Date).ToUniversalTime().ToString("o")
    })
    return $paths
}

function Invoke-OrReuse-AcceptanceBacktest {
    param(
        [string]$PythonPath,
        [string]$Root,
        [string]$RegistryRoot,
        [string]$StepName,
        [string]$ModelRef,
        [string]$ModelFamilyName,
        [string]$ModelRunDir,
        [string]$StartDate,
        [string]$EndDate,
        [ValidateSet("acceptance", "runtime_parity")]
        [string]$Preset
    )
    $contract = New-AcceptanceBacktestContract `
        -StepName $StepName `
        -ModelRef $ModelRef `
        -ModelFamilyName $ModelFamilyName `
        -ModelRunDir $ModelRunDir `
        -StartDate $StartDate `
        -EndDate $EndDate `
        -Preset $Preset
    $cacheHit = Resolve-AcceptanceBacktestCacheHit -RegistryRoot $RegistryRoot -ModelFamilyName $ModelFamilyName -Contract $contract
    $paths = Get-PropValue -ObjectValue $cacheHit -Name "paths" -DefaultValue @{}
    if ([bool](Get-PropValue -ObjectValue $cacheHit -Name "hit" -DefaultValue $false)) {
        $metadata = Get-PropValue -ObjectValue $cacheHit -Name "metadata" -DefaultValue @{}
        return [PSCustomObject]@{
            Exec = [PSCustomObject]@{
                ExitCode = 0
                Output = "[cache-hit]"
                Command = ""
                DryRun = $false
            }
            RunDir = [string](Get-PropValue -ObjectValue $metadata -Name "source_backtest_run_dir" -DefaultValue "")
            SummaryPath = [string](Get-PropValue -ObjectValue $paths -Name "summary_path" -DefaultValue "")
            Summary = (Get-PropValue -ObjectValue $cacheHit -Name "summary" -DefaultValue @{})
            Preset = $Preset
            Reused = $true
            SourceMode = "cached_result"
            SourceBacktestRunDir = [string](Get-PropValue -ObjectValue $metadata -Name "source_backtest_run_dir" -DefaultValue "")
            CacheKey = [string](Get-PropValue -ObjectValue $paths -Name "cache_key" -DefaultValue "")
            CacheContractPath = [string](Get-PropValue -ObjectValue $paths -Name "contract_path" -DefaultValue "")
            CacheSummaryPath = [string](Get-PropValue -ObjectValue $paths -Name "summary_path" -DefaultValue "")
            CacheStatValidationPath = [string](Get-PropValue -ObjectValue $paths -Name "stat_validation_path" -DefaultValue "")
            CacheMetadataPath = [string](Get-PropValue -ObjectValue $paths -Name "metadata_path" -DefaultValue "")
            Contract = $contract
        }
    }
    $backtest = Invoke-BacktestAndLoadSummary `
        -PythonPath $PythonPath `
        -Root $Root `
        -ModelRef $ModelRef `
        -ModelFamilyName $ModelFamilyName `
        -StartDate $StartDate `
        -EndDate $EndDate `
        -Preset $Preset
    return [PSCustomObject]@{
        Exec = $backtest.Exec
        RunDir = $backtest.RunDir
        SummaryPath = $backtest.SummaryPath
        Summary = $backtest.Summary
        Preset = $Preset
        Reused = $false
        SourceMode = "fresh_run"
        SourceBacktestRunDir = $backtest.RunDir
        CacheKey = [string](Get-PropValue -ObjectValue $paths -Name "cache_key" -DefaultValue "")
        CacheContractPath = [string](Get-PropValue -ObjectValue $paths -Name "contract_path" -DefaultValue "")
        CacheSummaryPath = [string](Get-PropValue -ObjectValue $paths -Name "summary_path" -DefaultValue "")
        CacheStatValidationPath = [string](Get-PropValue -ObjectValue $paths -Name "stat_validation_path" -DefaultValue "")
        CacheMetadataPath = [string](Get-PropValue -ObjectValue $paths -Name "metadata_path" -DefaultValue "")
        Contract = $contract
    }
}

function Invoke-OrReuse-AcceptanceStatValidation {
    param(
        $BacktestValue,
        [string]$PythonPath,
        [string]$Root,
        [int]$TrialCount,
        [string]$ModelRunDir = ""
    )
    if ([bool](Get-PropValue -ObjectValue $BacktestValue -Name "Reused" -DefaultValue $false)) {
        $cachedStatValidation = Load-JsonOrEmpty -PathValue ([string](Get-PropValue -ObjectValue $BacktestValue -Name "CacheStatValidationPath" -DefaultValue ""))
        if (-not (Test-IsEffectivelyEmptyObject -ObjectValue $cachedStatValidation)) {
            return $cachedStatValidation
        }
    }
    $runDir = [string](Get-PropValue -ObjectValue $BacktestValue -Name "RunDir" -DefaultValue "")
    $validation = Invoke-BacktestStatValidation `
        -PythonPath $PythonPath `
        -Root $Root `
        -RunDir $runDir `
        -TrialCount $TrialCount `
        -ModelRunDir $ModelRunDir
    $summary = Get-PropValue -ObjectValue $BacktestValue -Name "Summary" -DefaultValue @{}
    $contract = Get-PropValue -ObjectValue $BacktestValue -Name "Contract" -DefaultValue @{}
    $modelFamilyName = [string](Get-PropValue -ObjectValue $contract -Name "model_family" -DefaultValue "")
    if (-not [string]::IsNullOrWhiteSpace($modelFamilyName)) {
        (
            Write-AcceptanceBacktestCacheEntry `
                -RegistryRoot $resolvedRegistryRoot `
                -ModelFamilyName $modelFamilyName `
                -Contract $contract `
                -Summary $summary `
                -StatValidation $validation `
                -SourceBacktestRunDir ([string](Get-PropValue -ObjectValue $BacktestValue -Name "SourceBacktestRunDir" -DefaultValue ""))
        ) | Out-Null
    }
    return $validation
}

function Build-BacktestEvidenceFromSummary {
    param(
        [Parameter(Mandatory = $false)]$Summary,
        [Parameter(Mandatory = $false)]$StatValidation
    )
    $ordersSubmitted = [int64](To-Int64 (Get-PropValue -ObjectValue $Summary -Name "orders_submitted" -DefaultValue 0) 0)
    $ordersFilled = [int64](To-Int64 (Get-PropValue -ObjectValue $Summary -Name "orders_filled" -DefaultValue 0) 0)
    $realizedPnl = To-Double (Get-PropValue -ObjectValue $Summary -Name "realized_pnl_quote" -DefaultValue 0.0) 0.0
    $fillRate = To-Double (Get-PropValue -ObjectValue $Summary -Name "fill_rate" -DefaultValue -1.0) -1.0
    $maxDrawdownPct = To-Double (Get-PropValue -ObjectValue $Summary -Name "max_drawdown_pct" -DefaultValue -1.0) -1.0
    $slippageBpsMean = Get-NullableDouble (Get-PropValue -ObjectValue $Summary -Name "slippage_bps_mean" -DefaultValue $null)
    $candidatesAbortedByPolicy = [int64](To-Int64 (Get-PropValue -ObjectValue $Summary -Name "candidates_aborted_by_policy" -DefaultValue 0) 0)
    $executionPolicyVetoFailure = ($ordersSubmitted -le 0) -and ($candidatesAbortedByPolicy -gt 0)
    $calmarLikeScore = Get-CalmarLikeScore -RealizedPnlQuote $realizedPnl -MaxDrawdownPct $maxDrawdownPct
    $deflatedSharpeRatio = To-Double (Get-PropValue -ObjectValue $StatValidation -Name "deflated_sharpe_ratio_est" -DefaultValue 0.0) 0.0
    $probabilisticSharpeRatio = To-Double (Get-PropValue -ObjectValue $StatValidation -Name "probabilistic_sharpe_ratio" -DefaultValue 0.0) 0.0
    $statComparable = To-Bool (Get-PropValue -ObjectValue $StatValidation -Name "comparable" -DefaultValue $false) $false
    return [ordered]@{
        orders_submitted = [int64]$ordersSubmitted
        orders_filled = [int64]$ordersFilled
        realized_pnl_quote = [double]$realizedPnl
        fill_rate = [double]$fillRate
        max_drawdown_pct = [double]$maxDrawdownPct
        slippage_bps_mean = $slippageBpsMean
        candidates_aborted_by_policy = [int64]$candidatesAbortedByPolicy
        execution_policy_veto_failure = $executionPolicyVetoFailure
        calmar_like_score = $calmarLikeScore
        stat_validation = $StatValidation
        deflated_sharpe_ratio_est = [double]$deflatedSharpeRatio
        probabilistic_sharpe_ratio = [double]$probabilisticSharpeRatio
        stat_comparable = $statComparable
    }
}

function Build-BacktestStepReport {
    param(
        [string]$StepName,
        $BacktestValue,
        [string]$StartDate,
        [string]$EndDate,
        [string]$ModelRefRequested,
        [string]$ModelRefUsed,
        [ValidateSet("acceptance", "runtime_parity")]
        [string]$Preset,
        $Evidence,
        [hashtable]$ExtraFields = @{}
    )
    $doc = [ordered]@{
        exit_code = [int](Get-PropValue -ObjectValue (Get-PropValue -ObjectValue $BacktestValue -Name "Exec" -DefaultValue @{}) -Name "ExitCode" -DefaultValue 0)
        command = [string](Get-PropValue -ObjectValue (Get-PropValue -ObjectValue $BacktestValue -Name "Exec" -DefaultValue @{}) -Name "Command" -DefaultValue "")
        output_preview = (Get-OutputPreview -Text ([string](Get-PropValue -ObjectValue (Get-PropValue -ObjectValue $BacktestValue -Name "Exec" -DefaultValue @{}) -Name "Output" -DefaultValue "")))
        start = $StartDate
        end = $EndDate
        model_ref_requested = $ModelRefRequested
        model_ref_used = $ModelRefUsed
        run_dir = [string](Get-PropValue -ObjectValue $BacktestValue -Name "RunDir" -DefaultValue "")
        summary_path = [string](Get-PropValue -ObjectValue $BacktestValue -Name "SummaryPath" -DefaultValue "")
        preset = [string]$Preset
        reused = [bool](Get-PropValue -ObjectValue $BacktestValue -Name "Reused" -DefaultValue $false)
        source_mode = [string](Get-PropValue -ObjectValue $BacktestValue -Name "SourceMode" -DefaultValue "")
        cache_key = [string](Get-PropValue -ObjectValue $BacktestValue -Name "CacheKey" -DefaultValue "")
        cache_contract_path = [string](Get-PropValue -ObjectValue $BacktestValue -Name "CacheContractPath" -DefaultValue "")
        cache_summary_path = [string](Get-PropValue -ObjectValue $BacktestValue -Name "CacheSummaryPath" -DefaultValue "")
        cache_stat_validation_path = [string](Get-PropValue -ObjectValue $BacktestValue -Name "CacheStatValidationPath" -DefaultValue "")
        source_backtest_run_dir = [string](Get-PropValue -ObjectValue $BacktestValue -Name "SourceBacktestRunDir" -DefaultValue "")
        evaluation_contract_id = [string](Get-PropValue -ObjectValue (Get-PropValue -ObjectValue $BacktestValue -Name "Summary" -DefaultValue @{}) -Name "evaluation_contract_id" -DefaultValue "")
        evaluation_contract_role = [string](Get-PropValue -ObjectValue (Get-PropValue -ObjectValue $BacktestValue -Name "Summary" -DefaultValue @{}) -Name "evaluation_contract_role" -DefaultValue "")
        orders_submitted = [int64](Get-PropValue -ObjectValue $Evidence -Name "orders_submitted" -DefaultValue 0)
        orders_filled = [int64](Get-PropValue -ObjectValue $Evidence -Name "orders_filled" -DefaultValue 0)
        realized_pnl_quote = [double](Get-PropValue -ObjectValue $Evidence -Name "realized_pnl_quote" -DefaultValue 0.0)
        fill_rate = [double](Get-PropValue -ObjectValue $Evidence -Name "fill_rate" -DefaultValue -1.0)
        max_drawdown_pct = [double](Get-PropValue -ObjectValue $Evidence -Name "max_drawdown_pct" -DefaultValue -1.0)
        slippage_bps_mean = (Get-PropValue -ObjectValue $Evidence -Name "slippage_bps_mean" -DefaultValue $null)
        calmar_like_score = (Get-PropValue -ObjectValue $Evidence -Name "calmar_like_score" -DefaultValue $null)
        deflated_sharpe_ratio_est = [double](Get-PropValue -ObjectValue $Evidence -Name "deflated_sharpe_ratio_est" -DefaultValue 0.0)
        probabilistic_sharpe_ratio = [double](Get-PropValue -ObjectValue $Evidence -Name "probabilistic_sharpe_ratio" -DefaultValue 0.0)
        stat_validation = (Get-PropValue -ObjectValue $Evidence -Name "stat_validation" -DefaultValue @{})
        candidates_aborted_by_policy = [int64](Get-PropValue -ObjectValue $Evidence -Name "candidates_aborted_by_policy" -DefaultValue 0)
        execution_policy_veto_failure = [bool](Get-PropValue -ObjectValue $Evidence -Name "execution_policy_veto_failure" -DefaultValue $false)
    }
    foreach ($entry in $ExtraFields.GetEnumerator()) {
        $doc[[string]$entry.Key] = $entry.Value
    }
    return $doc
}

function Build-BacktestCompareMetrics {
    param($Evidence)
    return @{
        realized_pnl_quote = [double](Get-PropValue -ObjectValue $Evidence -Name "realized_pnl_quote" -DefaultValue 0.0)
        fill_rate = if ([double](Get-PropValue -ObjectValue $Evidence -Name "fill_rate" -DefaultValue -1.0) -ge 0.0) { [double](Get-PropValue -ObjectValue $Evidence -Name "fill_rate" -DefaultValue -1.0) } else { $null }
        max_drawdown_pct = if ([double](Get-PropValue -ObjectValue $Evidence -Name "max_drawdown_pct" -DefaultValue -1.0) -ge 0.0) { [double](Get-PropValue -ObjectValue $Evidence -Name "max_drawdown_pct" -DefaultValue -1.0) } else { $null }
        slippage_bps_mean = Get-PropValue -ObjectValue $Evidence -Name "slippage_bps_mean" -DefaultValue $null
        calmar_like = Get-PropValue -ObjectValue $Evidence -Name "calmar_like_score" -DefaultValue $null
    }
}

function Resolve-CandidateRuntimeDatasetCoverage {
    param(
        [string]$PythonPath,
        [string]$CandidateRunDir
    )
    if ([string]::IsNullOrWhiteSpace($CandidateRunDir)) {
        return @{}
    }
    $runtimeContractPath = Join-Path $CandidateRunDir "fusion_runtime_input_contract.json"
    $runtimeContract = Load-JsonOrEmpty -PathValue $runtimeContractPath
    if (Test-IsEffectivelyEmptyObject -ObjectValue $runtimeContract) {
        return @{}
    }
    $runtimeWindow = Get-PropValue -ObjectValue $runtimeContract -Name "runtime_window" -DefaultValue @{}
    $datasetRoot = [string](Get-PropValue -ObjectValue $runtimeContract -Name "runtime_dataset_root" -DefaultValue "")
    $datasetSummary = @{}
    $inspectCommand = ""
    $inspectOutputPreview = ""
    if ((-not [string]::IsNullOrWhiteSpace($PythonPath)) -and (-not [string]::IsNullOrWhiteSpace($datasetRoot))) {
        $inspectArgs = @(
            "-m", "autobot.cli",
            "model", "inspect-runtime-dataset",
            "--dataset-root", $datasetRoot
        )
        $inspectExec = Invoke-CommandCapture -Exe $PythonPath -ArgList $inspectArgs
        $inspectCommand = [string]$inspectExec.Command
        $inspectOutputPreview = Get-OutputPreview -Text ([string]$inspectExec.Output)
        try {
            $datasetSummary = Resolve-JsonPayloadFromText -TextValue ([string]$inspectExec.Output) -ContextLabel "runtime dataset summary payload"
        } catch {
            throw "runtime dataset summary payload parse failed"
        }
    }
    return [ordered]@{
        contract_path = $runtimeContractPath
        dataset_root = $datasetRoot
        rows = [int](Get-PropValue -ObjectValue $runtimeContract -Name "runtime_rows_after_date_filter" -DefaultValue 0)
        coverage_start_ts_ms = [int64](Get-PropValue -ObjectValue $runtimeContract -Name "coverage_start_ts_ms" -DefaultValue 0)
        coverage_end_ts_ms = [int64](Get-PropValue -ObjectValue $runtimeContract -Name "coverage_end_ts_ms" -DefaultValue 0)
        coverage_start_date = [string](Get-PropValue -ObjectValue $runtimeContract -Name "coverage_start_date" -DefaultValue "")
        coverage_end_date = [string](Get-PropValue -ObjectValue $runtimeContract -Name "coverage_end_date" -DefaultValue "")
        coverage_dates = @((Get-PropValue -ObjectValue $runtimeContract -Name "coverage_dates" -DefaultValue @()))
        window_timezone = [string](Get-PropValue -ObjectValue $runtimeContract -Name "window_timezone" -DefaultValue "")
        requested_start_ts_ms = [int64](Get-PropValue -ObjectValue $runtimeWindow -Name "start_ts_ms" -DefaultValue 0)
        requested_end_ts_ms = [int64](Get-PropValue -ObjectValue $runtimeWindow -Name "end_ts_ms" -DefaultValue 0)
        requested_start = [string](Get-PropValue -ObjectValue $runtimeWindow -Name "start" -DefaultValue "")
        requested_end = [string](Get-PropValue -ObjectValue $runtimeWindow -Name "end" -DefaultValue "")
        data_platform_ready_snapshot_id = [string](Get-PropValue -ObjectValue $runtimeContract -Name "snapshot_id" -DefaultValue "")
        inspect_command = $inspectCommand
        inspect_output_preview = $inspectOutputPreview
        actual_dataset_exists = [bool](Get-PropValue -ObjectValue $datasetSummary -Name "exists" -DefaultValue $false)
        actual_dataset_rows = [int](Get-PropValue -ObjectValue $datasetSummary -Name "rows" -DefaultValue 0)
        actual_dataset_min_ts_ms = (Get-PropValue -ObjectValue $datasetSummary -Name "min_ts_ms" -DefaultValue $null)
        actual_dataset_max_ts_ms = (Get-PropValue -ObjectValue $datasetSummary -Name "max_ts_ms" -DefaultValue $null)
        manifest_path = [string](Get-PropValue -ObjectValue $datasetSummary -Name "manifest_path" -DefaultValue "")
        manifest_exists = [bool](Get-PropValue -ObjectValue $datasetSummary -Name "manifest_exists" -DefaultValue $false)
        data_file_count = [int](Get-PropValue -ObjectValue $datasetSummary -Name "data_file_count" -DefaultValue 0)
        markets = @((Get-PropValue -ObjectValue $datasetSummary -Name "markets" -DefaultValue @()))
    }
}

function Test-CandidateRuntimeDatasetCertificationCoverage {
    param(
        [Parameter(Mandatory = $true)]
        $Coverage,
        [string]$CertificationStartDate,
        [string]$CertificationEndDate
    )
    if (Test-IsEffectivelyEmptyObject -ObjectValue $Coverage) {
        return [ordered]@{
            pass = $false
            reason = "CANDIDATE_RUNTIME_DATASET_CERTIFICATION_WINDOW_EMPTY"
        }
    }
    $datasetExists = [bool](Get-PropValue -ObjectValue $Coverage -Name "actual_dataset_exists" -DefaultValue $false)
    $manifestExists = [bool](Get-PropValue -ObjectValue $Coverage -Name "manifest_exists" -DefaultValue $false)
    $dataFileCount = [int](Get-PropValue -ObjectValue $Coverage -Name "data_file_count" -DefaultValue 0)
    $rows = [int](Get-PropValue -ObjectValue $Coverage -Name "actual_dataset_rows" -DefaultValue 0)
    if ((-not $datasetExists) -or (-not $manifestExists) -or ($dataFileCount -le 0) -or ($rows -le 0)) {
        return [ordered]@{
            pass = $false
            reason = "CANDIDATE_RUNTIME_DATASET_CERTIFICATION_WINDOW_EMPTY"
        }
    }
    $coverageFields = Resolve-OperatingDateCoverageFields -Payload $Coverage
    $coverageStartDate = [string](Get-PropValue -ObjectValue $coverageFields -Name "coverage_start_date" -DefaultValue "")
    $coverageEndDate = [string](Get-PropValue -ObjectValue $coverageFields -Name "coverage_end_date" -DefaultValue "")
    $coverageDates = @((Get-PropValue -ObjectValue $coverageFields -Name "coverage_dates" -DefaultValue @()))
    $windowTimezone = [string](Get-PropValue -ObjectValue $coverageFields -Name "window_timezone" -DefaultValue "")
    $expectedCoverageDates = @(Get-DateTokenRangeInclusive -StartDate $CertificationStartDate -EndDate $CertificationEndDate)
    $missingCoverageDates = @($expectedCoverageDates | Where-Object { @($coverageDates) -notcontains [string]$_ })
    $allowTrailingSingleDayGap = $false
    if (
        (@($missingCoverageDates).Count -eq 1) -and
        ([string]$missingCoverageDates[0] -eq [string]$CertificationEndDate) -and
        (-not [string]::IsNullOrWhiteSpace($coverageEndDate))
    ) {
        try {
            $coverageEndObj = [DateTime]::ParseExact($coverageEndDate, "yyyy-MM-dd", [System.Globalization.CultureInfo]::InvariantCulture)
            $certificationEndObj = [DateTime]::ParseExact([string]$CertificationEndDate, "yyyy-MM-dd", [System.Globalization.CultureInfo]::InvariantCulture)
            $allowTrailingSingleDayGap = ($coverageEndObj.AddDays(1).ToString("yyyy-MM-dd") -eq $certificationEndObj.ToString("yyyy-MM-dd"))
        } catch {
            $allowTrailingSingleDayGap = $false
        }
    }
    if (($windowTimezone -ne "Asia/Seoul") -or [string]::IsNullOrWhiteSpace($coverageStartDate) -or [string]::IsNullOrWhiteSpace($coverageEndDate)) {
        return [ordered]@{
            pass = $false
            reason = "CANDIDATE_RUNTIME_DATASET_CERTIFICATION_WINDOW_GAP"
        }
    }
    if (($coverageStartDate -gt $CertificationStartDate) -or (($coverageEndDate -lt $CertificationEndDate) -and (-not $allowTrailingSingleDayGap))) {
        return [ordered]@{
            pass = $false
            reason = "CANDIDATE_RUNTIME_DATASET_CERTIFICATION_WINDOW_GAP"
        }
    }
    if ((@($missingCoverageDates).Count -gt 0) -and (-not $allowTrailingSingleDayGap)) {
        return [ordered]@{
            pass = $false
            reason = "CANDIDATE_RUNTIME_DATASET_CERTIFICATION_WINDOW_GAP"
        }
    }
    return [ordered]@{
        pass = $true
        reason = "CANDIDATE_RUNTIME_DATASET_CERTIFICATION_WINDOW_READY"
    }
}

function Resolve-CandidateRuntimeViabilityArtifact {
    param(
        [string]$CandidateRunDir
    )
    if ([string]::IsNullOrWhiteSpace($CandidateRunDir)) {
        return @{}
    }
    $reportPath = Join-Path $CandidateRunDir "runtime_viability_report.json"
    $payload = Load-JsonOrEmpty -PathValue $reportPath
    $exists = Test-Path $reportPath
    return [ordered]@{
        report_path = $reportPath
        exists = $exists
        payload = $payload
        pass = To-Bool (Get-PropValue -ObjectValue $payload -Name "pass" -DefaultValue $false) $false
        alpha_lcb_floor = To-Double (Get-PropValue -ObjectValue $payload -Name "alpha_lcb_floor" -DefaultValue 0.0) 0.0
        runtime_rows_total = To-Int64 (Get-PropValue -ObjectValue $payload -Name "runtime_rows_total" -DefaultValue 0) 0
        mean_final_expected_return = To-Double (Get-PropValue -ObjectValue $payload -Name "mean_final_expected_return" -DefaultValue 0.0) 0.0
        mean_final_expected_es = To-Double (Get-PropValue -ObjectValue $payload -Name "mean_final_expected_es" -DefaultValue 0.0) 0.0
        mean_final_uncertainty = To-Double (Get-PropValue -ObjectValue $payload -Name "mean_final_uncertainty" -DefaultValue 0.0) 0.0
        mean_final_alpha_lcb = To-Double (Get-PropValue -ObjectValue $payload -Name "mean_final_alpha_lcb" -DefaultValue 0.0) 0.0
        alpha_lcb_positive_count = To-Int64 (Get-PropValue -ObjectValue $payload -Name "alpha_lcb_positive_count" -DefaultValue 0) 0
        rows_above_alpha_floor = To-Int64 (Get-PropValue -ObjectValue $payload -Name "rows_above_alpha_floor" -DefaultValue 0) 0
        rows_above_alpha_floor_ratio = To-Double (Get-PropValue -ObjectValue $payload -Name "rows_above_alpha_floor_ratio" -DefaultValue 0.0) 0.0
        expected_return_positive_count = To-Int64 (Get-PropValue -ObjectValue $payload -Name "expected_return_positive_count" -DefaultValue 0) 0
        entry_gate_allowed_count = To-Int64 (Get-PropValue -ObjectValue $payload -Name "entry_gate_allowed_count" -DefaultValue 0) 0
        entry_gate_allowed_ratio = To-Double (Get-PropValue -ObjectValue $payload -Name "entry_gate_allowed_ratio" -DefaultValue 0.0) 0.0
        estimated_intent_candidate_count = To-Int64 (Get-PropValue -ObjectValue $payload -Name "estimated_intent_candidate_count" -DefaultValue 0) 0
        primary_reason_code = [string](Get-PropValue -ObjectValue $payload -Name "primary_reason_code" -DefaultValue "")
        top_entry_gate_reason_codes = @((Get-PropValue -ObjectValue $payload -Name "top_entry_gate_reason_codes" -DefaultValue @()))
        sample_rows = @((Get-PropValue -ObjectValue $payload -Name "sample_rows" -DefaultValue @()))
    }
}

function Test-CandidateRuntimeViability {
    param(
        $Viability
    )
    if (Test-IsEffectivelyEmptyObject -ObjectValue $Viability) {
        return [ordered]@{
            pass = $false
            reason = "FUSION_RUNTIME_VIABILITY_REPORT_MISSING"
        }
    }
    if (-not [bool](Get-PropValue -ObjectValue $Viability -Name "exists" -DefaultValue $false)) {
        return [ordered]@{
            pass = $false
            reason = "FUSION_RUNTIME_VIABILITY_REPORT_MISSING"
        }
    }
    $rowsAboveAlphaFloor = To-Int64 (Get-PropValue -ObjectValue $Viability -Name "rows_above_alpha_floor" -DefaultValue 0) 0
    if ($rowsAboveAlphaFloor -le 0) {
        return [ordered]@{
            pass = $false
            reason = "FUSION_RUNTIME_ALPHA_LCB_ZERO_VIABILITY"
        }
    }
    $entryGateAllowedCount = To-Int64 (Get-PropValue -ObjectValue $Viability -Name "entry_gate_allowed_count" -DefaultValue 0) 0
    if ($entryGateAllowedCount -le 0) {
        return [ordered]@{
            pass = $false
            reason = "FUSION_RUNTIME_ENTRY_GATE_ZERO_VIABILITY"
        }
    }
    return [ordered]@{
        pass = $true
        reason = "FUSION_RUNTIME_VIABILITY_READY"
    }
}

function Get-PaperHistoryEvidence {
    param(
        [string]$DirectoryPath,
        [int]$WindowRuns,
        [int]$MinCompletedRuns
    )
    $result = [ordered]@{
        completed_runs = 0
        sufficient_history = $false
        nonnegative_run_ratio = 0.0
        positive_run_ratio = 0.0
        median_micro_quality_score_mean = 0.0
        median_fallback_ratio = 0.0
        reports = @()
    }
    if ([string]::IsNullOrWhiteSpace($DirectoryPath) -or (-not (Test-Path $DirectoryPath))) {
        return $result
    }
    $files = @(
        Get-ChildItem -Path $DirectoryPath -Filter "paper_micro_smoke_*.json" -File -ErrorAction SilentlyContinue |
            Sort-Object LastWriteTimeUtc -Descending |
            Select-Object -First ([Math]::Max([int]$WindowRuns, 1))
    )
    if ($files.Count -le 0) {
        return $result
    }
    $docs = @()
    foreach ($file in $files) {
        try {
            $doc = Load-JsonOrEmpty -PathValue $file.FullName
        } catch {
            continue
        }
        if ($null -eq $doc) {
            continue
        }
        $runId = [string](Get-PropValue -ObjectValue $doc -Name "run_id" -DefaultValue "")
        if ([string]::IsNullOrWhiteSpace($runId)) {
            continue
        }
        $docs += $doc
    }
    if ($docs.Count -le 0) {
        return $result
    }
    $nonnegativeCount = 0
    $positiveCount = 0
    $microQualityValues = @()
    $fallbackValues = @()
    foreach ($doc in $docs) {
        $pnl = [double](To-Double (Get-PropValue -ObjectValue $doc -Name "realized_pnl_quote" -DefaultValue 0.0) 0.0)
        if ($pnl -ge 0.0) {
            $nonnegativeCount += 1
        }
        if ($pnl -gt 0.0) {
            $positiveCount += 1
        }
        $microQualityValues += [double](To-Double (Get-PropValue -ObjectValue $doc -Name "micro_quality_score_mean" -DefaultValue 0.0) 0.0)
        $fallbackValues += [double](To-Double (Get-PropValue -ObjectValue $doc -Name "micro_missing_fallback_ratio" -DefaultValue 0.0) 0.0)
    }
    $completedRuns = $docs.Count
    $result.completed_runs = [int]$completedRuns
    $result.sufficient_history = ($completedRuns -ge ([Math]::Max([int]$MinCompletedRuns, 1)))
    $result.nonnegative_run_ratio = [double]($nonnegativeCount / [double]$completedRuns)
    $result.positive_run_ratio = [double]($positiveCount / [double]$completedRuns)
    $result.median_micro_quality_score_mean = [double](Get-MedianDouble -Values $microQualityValues)
    $result.median_fallback_ratio = [double](Get-MedianDouble -Values $fallbackValues)
    $result.reports = @(
        foreach ($doc in $docs) {
            [ordered]@{
                run_id = [string](Get-PropValue -ObjectValue $doc -Name "run_id" -DefaultValue "")
                realized_pnl_quote = [double](To-Double (Get-PropValue -ObjectValue $doc -Name "realized_pnl_quote" -DefaultValue 0.0) 0.0)
                micro_quality_score_mean = [double](To-Double (Get-PropValue -ObjectValue $doc -Name "micro_quality_score_mean" -DefaultValue 0.0) 0.0)
                micro_missing_fallback_ratio = [double](To-Double (Get-PropValue -ObjectValue $doc -Name "micro_missing_fallback_ratio" -DefaultValue 0.0) 0.0)
                generated_at = [string](Get-PropValue -ObjectValue $doc -Name "generated_at" -DefaultValue "")
            }
        }
    )
    return $result
}

function Clamp01 {
    param([double]$Value)
    if ($Value -lt 0.0) { return 0.0 }
    if ($Value -gt 1.0) { return 1.0 }
    return [double]$Value
}

function Get-ScaledPositiveScore {
    param(
        [double]$Value,
        [double]$Pivot,
        [double]$Scale
    )
    $safeScale = [Math]::Max([double]$Scale, 1e-9)
    $shifted = 0.5 + (($Value - $Pivot) / (2.0 * $safeScale))
    return Clamp01 -Value $shifted
}

function Resolve-PaperEvidenceDecision {
    param(
        [bool]$T15GatePass,
        [int64]$OrdersFilled,
        [double]$RealizedPnlQuote,
        [double]$MicroQualityScoreMean,
        [int64]$ActiveWindows,
        [double]$NonnegativeWindowRatio,
        [double]$FillConcentrationRatio,
        [bool]$HistorySufficient,
        [double]$HistoryNonnegativeRunRatio,
        [double]$HistoryPositiveRunRatio,
        [double]$HistoryMedianMicroQualityScore,
        [double]$HistoryMedianFallbackRatio,
        [int]$MinOrdersFilled,
        [double]$MinRealizedPnlQuote,
        [double]$MinMicroQualityScoreMean,
        [int]$MinActiveWindows,
        [double]$MinNonnegativeWindowRatio,
        [double]$MaxFillConcentrationRatio,
        [double]$HistoryMinNonnegativeRunRatio,
        [double]$HistoryMinPositiveRunRatio,
        [double]$HistoryMinMedianMicroQualityScore,
        [double]$EvidenceEdgeScore,
        [double]$EvidenceHoldScore
    )
    $hardFailures = @()
    if (-not $T15GatePass) {
        $hardFailures += "T15_GATE_FAILED"
    }
    if ($OrdersFilled -lt $MinOrdersFilled) {
        $hardFailures += "MIN_ORDERS_FILLED"
    }
    if ($ActiveWindows -lt $MinActiveWindows) {
        $hardFailures += "MIN_ACTIVE_WINDOWS"
    }
    if ($MicroQualityScoreMean -lt ([double]$MinMicroQualityScoreMean * 0.50)) {
        $hardFailures += "MICRO_QUALITY_CATASTROPHIC"
    }
    $catastrophicFillConcentrationThreshold = [Math]::Max([double]$MaxFillConcentrationRatio + 0.10, 0.95)
    if ($FillConcentrationRatio -gt $catastrophicFillConcentrationThreshold) {
        $hardFailures += "FILL_CONCENTRATION_CATASTROPHIC"
    }

    $currentPnlScore = Get-ScaledPositiveScore `
        -Value $RealizedPnlQuote `
        -Pivot $MinRealizedPnlQuote `
        -Scale ([Math]::Max([Math]::Abs([double]$MinRealizedPnlQuote), 50.0))
    $fillsScore = Clamp01 -Value ([double]$OrdersFilled / [double][Math]::Max(($MinOrdersFilled * 3), 1))
    $microScore = Clamp01 -Value $MicroQualityScoreMean
    $windowScore = Clamp01 -Value $NonnegativeWindowRatio
    $concentrationScore = Clamp01 -Value (1.0 - $FillConcentrationRatio)
    $historyNonnegativeScore = if ($HistorySufficient) { Clamp01 -Value $HistoryNonnegativeRunRatio } else { 0.50 }
    $historyPositiveScore = if ($HistorySufficient) { Clamp01 -Value $HistoryPositiveRunRatio } else { 0.50 }
    $historyMicroScore = if ($HistorySufficient) { Clamp01 -Value $HistoryMedianMicroQualityScore } else { 0.50 }
    $historyFallbackScore = if ($HistorySufficient) { Clamp01 -Value (1.0 - $HistoryMedianFallbackRatio) } else { 0.50 }

    $evidenceScore = `
        (0.16 * $currentPnlScore) + `
        (0.12 * $fillsScore) + `
        (0.16 * $microScore) + `
        (0.14 * $windowScore) + `
        (0.08 * $concentrationScore) + `
        (0.12 * $historyNonnegativeScore) + `
        (0.08 * $historyPositiveScore) + `
        (0.08 * $historyMicroScore) + `
        (0.06 * $historyFallbackScore)

    $softFailures = @()
    if ($RealizedPnlQuote -lt $MinRealizedPnlQuote) {
        $softFailures += "MIN_REALIZED_PNL"
    }
    if ($MicroQualityScoreMean -lt $MinMicroQualityScoreMean) {
        $softFailures += "MIN_MICRO_QUALITY"
    }
    if ($NonnegativeWindowRatio -lt $MinNonnegativeWindowRatio) {
        $softFailures += "MIN_NONNEGATIVE_WINDOW_RATIO"
    }
    if ($FillConcentrationRatio -gt $MaxFillConcentrationRatio) {
        $softFailures += "MAX_FILL_CONCENTRATION"
    }
    if ($HistorySufficient -and ($HistoryNonnegativeRunRatio -lt $HistoryMinNonnegativeRunRatio)) {
        $softFailures += "MIN_HISTORY_NONNEGATIVE_RUN_RATIO"
    }
    if ($HistorySufficient -and ($HistoryPositiveRunRatio -lt $HistoryMinPositiveRunRatio)) {
        $softFailures += "MIN_HISTORY_POSITIVE_RUN_RATIO"
    }
    if ($HistorySufficient -and ($HistoryMedianMicroQualityScore -lt $HistoryMinMedianMicroQualityScore)) {
        $softFailures += "MIN_HISTORY_MEDIAN_MICRO_QUALITY"
    }

    $decision = "candidate_edge"
    $pass = $true
    $basis = "EVIDENCE_SCORE_EDGE"
    if ($hardFailures.Count -gt 0) {
        $decision = "hard_fail"
        $pass = $false
        $basis = "HARD_FAILURE"
    } elseif ($evidenceScore -lt $EvidenceHoldScore) {
        $decision = "insufficient_evidence"
        $pass = $false
        $basis = "EVIDENCE_SCORE_TOO_LOW"
    } elseif (($softFailures.Count -gt 0) -or ($evidenceScore -lt $EvidenceEdgeScore)) {
        $decision = "statistical_hold"
        $pass = $false
        $basis = "STATISTICAL_HOLD"
    }

    return [ordered]@{
        pass = $pass
        decision = $decision
        final_decision_basis = $basis
        hard_failures = @($hardFailures)
        soft_failures = @($softFailures)
        evidence_score = [double]$evidenceScore
        evidence_components = [ordered]@{
            current_pnl_score = [double]$currentPnlScore
            fills_score = [double]$fillsScore
            micro_quality_score = [double]$microScore
            nonnegative_window_score = [double]$windowScore
            fill_concentration_score = [double]$concentrationScore
            history_nonnegative_score = [double]$historyNonnegativeScore
            history_positive_score = [double]$historyPositiveScore
            history_micro_quality_score = [double]$historyMicroScore
            history_fallback_score = [double]$historyFallbackScore
        }
    }
}

function Get-PowerShellExe {
    if ($script:IsWindowsPlatform) {
        return "powershell.exe"
    }
    $cmd = Get-Command pwsh -ErrorAction SilentlyContinue
    if ($null -ne $cmd -and -not [string]::IsNullOrWhiteSpace($cmd.Source)) {
        $resolved = [string]$cmd.Source
        if (-not $resolved.StartsWith("/snap/")) {
            return $resolved
        }
    }
    foreach ($candidatePath in @(
        "/usr/bin/pwsh",
        "/usr/local/bin/pwsh",
        "/opt/microsoft/powershell/7/pwsh"
    )) {
        if (Test-Path $candidatePath) {
            return $candidatePath
        }
    }
    if ($null -ne $cmd -and -not [string]::IsNullOrWhiteSpace($cmd.Source)) {
        return [string]$cmd.Source
    }
    return "pwsh"
}

function Resolve-RegistryPointerPath {
    param(
        [string]$RegistryRoot,
        [string]$Family,
        [string]$PointerName
    )
    return (Resolve-V4RegistryPointerPath -RegistryRoot $RegistryRoot -Family $Family -PointerName $PointerName)
}

function Update-LatestCandidatePointers {
    param(
        [string]$RegistryRoot,
        [string]$Family,
        [string]$RunId
    )
    return (Update-V4LatestCandidatePointers -RegistryRoot $RegistryRoot -Family $Family -RunId $RunId -DryRun:$DryRun)
}

function Resolve-ArtifactStatusPath {
    param([string]$RunDir)
    return (Resolve-V4ArtifactStatusPath -RunDir $RunDir)
}

function Resolve-OptionalBool {
    param(
        [AllowNull()][Nullable[bool]]$Value,
        [bool]$DefaultValue = $false
    )
    return (Resolve-V4OptionalBool -Value $Value -DefaultValue $DefaultValue)
}

function Update-RunArtifactStatus {
    param(
        [string]$RunDir,
        [string]$RunId,
        [string]$Status = "",
        [AllowNull()][Nullable[bool]]$AcceptanceCompleted = $null,
        [AllowNull()][Nullable[bool]]$CandidateAdoptable = $null,
        [AllowNull()][Nullable[bool]]$CandidateAdopted = $null,
        [AllowNull()][Nullable[bool]]$Promoted = $null
    )
    return (
        Update-V4RunArtifactStatus `
            -RunDir $RunDir `
            -RunId $RunId `
            -Status $Status `
            -AcceptanceCompleted $AcceptanceCompleted `
            -CandidateAdoptable $CandidateAdoptable `
            -CandidateAdopted $CandidateAdopted `
            -Promoted $Promoted `
            -DryRun:$DryRun
    )
}

function Invoke-BacktestAndLoadSummary {
    param(
        [string]$PythonPath,
        [string]$Root,
        [string]$ModelRef,
        [string]$ModelFamilyName,
        [string]$StartDate,
        [string]$EndDate,
        [ValidateSet("acceptance", "runtime_parity")]
        [string]$Preset = "runtime_parity"
    )
    $args = @(
        "-m", "autobot.cli",
        "backtest", "alpha",
        "--preset", $Preset,
        "--model-ref", $ModelRef,
        "--model-family", $ModelFamilyName,
        "--feature-set", $FeatureSet,
        "--tf", $Tf,
        "--quote", $Quote,
        "--top-n", $BacktestTopN,
        "--start", $StartDate,
        "--end", $EndDate
    )
    if ($Preset -eq "acceptance") {
        $args += @(
            "--evaluation-contract-id", "acceptance_frozen_compare_v1",
            "--evaluation-contract-role", "frozen_compare",
            "--selection-policy-mode", "raw_threshold",
            "--no-use_learned_selection_recommendations",
            "--no-use_learned_exit_mode",
            "--no-use_learned_hold_bars",
            "--no-use_learned_risk_recommendations",
            "--no-use_trade_level_action_policy",
            "--no-use_learned_execution_recommendations",
            "--micro-order-policy", "off",
            "--top-pct", $BacktestTopPct,
            "--min-prob", $BacktestMinProb,
            "--min-cands-per-ts", $BacktestMinCandidatesPerTs,
            "--exit-mode", "hold",
            "--hold-bars", $HoldBars
        )
    } elseif ($Preset -eq "runtime_parity") {
        $args += @(
            "--evaluation-contract-id", "runtime_deploy_contract_v1",
            "--evaluation-contract-role", "deploy_runtime",
            "--selection-policy-mode", "auto",
            "--use_learned_selection_recommendations",
            "--use_learned_exit_mode",
            "--use_learned_hold_bars",
            "--use_learned_risk_recommendations",
            "--use_trade_level_action_policy",
            "--use_learned_execution_recommendations",
            "--micro-order-policy", "on",
            "--micro-order-policy-mode", "trade_only",
            "--micro-order-policy-on-missing", "static_fallback"
        )
    }
    $exec = Invoke-CommandCapture -Exe $PythonPath -ArgList $args
    $runDir = if ($DryRun) { "" } else { Resolve-RunDirFromText -TextValue ([string]$exec.Output) }
    if ((-not $DryRun) -and [string]::IsNullOrWhiteSpace($runDir)) {
        throw "backtest run completed but run_dir was not reported by CLI stdout"
    }
    if ((-not $DryRun) -and (-not (Test-Path $runDir))) {
        throw "backtest run_dir does not exist: $runDir"
    }
    $summaryPath = if ([string]::IsNullOrWhiteSpace($runDir)) { "" } else { Join-Path $runDir "summary.json" }
    if ((-not $DryRun) -and (-not [string]::IsNullOrWhiteSpace($summaryPath)) -and (-not (Test-Path $summaryPath))) {
        throw "backtest summary.json does not exist: $summaryPath"
    }
    $summary = if ([string]::IsNullOrWhiteSpace($summaryPath)) { @{} } else { Load-JsonOrEmpty -PathValue $summaryPath }
    return [PSCustomObject]@{
        Exec = $exec
        RunDir = $runDir
        SummaryPath = $summaryPath
        Summary = $summary
    }
}

function Invoke-RestartUnits {
    param([string[]]$UnitsToRestart)
    $results = @()
    foreach ($unit in $UnitsToRestart) {
        $trimmed = [string]$unit
        if ([string]::IsNullOrWhiteSpace($trimmed)) {
            continue
        }
        $trimmed = $trimmed.Trim()
        if ($script:IsWindowsPlatform) {
            $results += [ordered]@{
                unit = $trimmed
                attempted = $false
                active = $false
                reason = "WINDOWS_SYSTEMCTL_UNAVAILABLE"
            }
            continue
        }
        $restartExec = Invoke-CommandCapture -Exe "sudo" -ArgList @("systemctl", "restart", $trimmed) -AllowFailure
        $activeExec = Invoke-CommandCapture -Exe "systemctl" -ArgList @("is-active", $trimmed) -AllowFailure
        $results += [ordered]@{
            unit = $trimmed
            attempted = $true
            restart_exit_code = [int]$restartExec.ExitCode
            restart_command = $restartExec.Command
            restart_output_preview = (Get-OutputPreview -Text ([string]$restartExec.Output))
            active = ($activeExec.ExitCode -eq 0) -and (([string]$activeExec.Output).Trim() -eq "active")
            active_command = $activeExec.Command
            active_output_preview = (Get-OutputPreview -Text ([string]$activeExec.Output))
        }
    }
    Write-Output -NoEnumerate @($results)
}

function Get-UnitStates {
    param([string[]]$Units)
    $results = @()
    foreach ($unit in $Units) {
        $trimmed = [string]$unit
        if ([string]::IsNullOrWhiteSpace($trimmed)) {
            continue
        }
        $trimmed = $trimmed.Trim()
        if ($script:IsWindowsPlatform) {
            $results += [ordered]@{
                unit = $trimmed
                attempted = $false
                active = $false
                reason = "WINDOWS_SYSTEMCTL_UNAVAILABLE"
            }
            continue
        }
        $activeExec = Invoke-CommandCapture -Exe "systemctl" -ArgList @("is-active", $trimmed) -AllowFailure
        $enabledExec = Invoke-CommandCapture -Exe "systemctl" -ArgList @("is-enabled", $trimmed) -AllowFailure
        $results += [ordered]@{
            unit = $trimmed
            attempted = $true
            active = ($activeExec.ExitCode -eq 0) -and (([string]$activeExec.Output).Trim() -eq "active")
            enabled = ($enabledExec.ExitCode -eq 0)
            active_output_preview = (Get-OutputPreview -Text ([string]$activeExec.Output))
            enabled_output_preview = (Get-OutputPreview -Text ([string]$enabledExec.Output))
        }
    }
    Write-Output -NoEnumerate @($results)
}

function Merge-UniqueStringArray {
    param(
        [string[]]$First = @(),
        [string[]]$Second = @()
    )
    $seen = @{}
    $values = New-Object System.Collections.Generic.List[string]
    foreach ($item in @($First) + @($Second)) {
        $text = [string]$item
        if ([string]::IsNullOrWhiteSpace($text)) {
            continue
        }
        $text = $text.Trim()
        if ($seen.ContainsKey($text)) {
            continue
        }
        $seen[$text] = $true
        $values.Add($text) | Out-Null
    }
    Write-Output -NoEnumerate ($values.ToArray())
}

function Build-ReportMarkdown {
    param($ReportValue)
    $lines = New-Object System.Collections.Generic.List[string]
    $generatedAt = [string](Get-PropValue -ObjectValue $ReportValue -Name "generated_at" -DefaultValue "")
    $batchDateValue = [string](Get-PropValue -ObjectValue $ReportValue -Name "batch_date" -DefaultValue "")
    $candidate = Get-PropValue -ObjectValue $ReportValue -Name "candidate" -DefaultValue @{}
    $steps = Get-PropValue -ObjectValue $ReportValue -Name "steps" -DefaultValue @{}
    $gates = Get-PropValue -ObjectValue $ReportValue -Name "gates" -DefaultValue @{}
    $reasons = @(Get-PropValue -ObjectValue $ReportValue -Name "reasons" -DefaultValue @())
    $runtimeBefore = @(Get-PropValue -ObjectValue $ReportValue -Name "runtime_units_before" -DefaultValue @())
    $restartTargets = @(Get-PropValue -ObjectValue $ReportValue -Name "restart_targets" -DefaultValue @())

    $lines.Add("# $ReportTitle") | Out-Null
    $lines.Add("") | Out-Null
    if (-not [string]::IsNullOrWhiteSpace($generatedAt)) {
        $lines.Add("- generated_at: $generatedAt") | Out-Null
    }
    if (-not [string]::IsNullOrWhiteSpace($batchDateValue)) {
        $lines.Add("- batch_date: $batchDateValue") | Out-Null
    }
    $lines.Add("- overall_pass: $([string](Get-PropValue -ObjectValue $gates -Name 'overall_pass' -DefaultValue ''))") | Out-Null
    $lines.Add("- candidate_run_id: $([string](Get-PropValue -ObjectValue $candidate -Name 'run_id' -DefaultValue ''))") | Out-Null
    $lines.Add("- champion_before_run_id: $([string](Get-PropValue -ObjectValue $candidate -Name 'champion_before_run_id' -DefaultValue ''))") | Out-Null
    $lines.Add("- champion_after_run_id: $([string](Get-PropValue -ObjectValue $candidate -Name 'champion_after_run_id' -DefaultValue ''))") | Out-Null
    $lines.Add("- candidate_model_ref_requested: $([string](Get-PropValue -ObjectValue $candidate -Name 'candidate_model_ref_requested' -DefaultValue ''))") | Out-Null
    $lines.Add("- candidate_run_id_used_for_backtest: $([string](Get-PropValue -ObjectValue $candidate -Name 'candidate_run_id_used_for_backtest' -DefaultValue ''))") | Out-Null
    $lines.Add("- candidate_run_id_used_for_paper: $([string](Get-PropValue -ObjectValue $candidate -Name 'candidate_run_id_used_for_paper' -DefaultValue ''))") | Out-Null
    $lines.Add("- champion_model_ref_requested: $([string](Get-PropValue -ObjectValue $candidate -Name 'champion_model_ref_requested' -DefaultValue ''))") | Out-Null
    $lines.Add("- champion_run_id_used_for_backtest: $([string](Get-PropValue -ObjectValue $candidate -Name 'champion_run_id_used_for_backtest' -DefaultValue ''))") | Out-Null
    $lines.Add("- research_evidence_path: $([string](Get-PropValue -ObjectValue $candidate -Name 'research_evidence_path' -DefaultValue ''))") | Out-Null
    $lines.Add("- search_budget_decision_path: $([string](Get-PropValue -ObjectValue $candidate -Name 'search_budget_decision_path' -DefaultValue ''))") | Out-Null
    $lines.Add("- economic_objective_profile_path: $([string](Get-PropValue -ObjectValue $candidate -Name 'economic_objective_profile_path' -DefaultValue ''))") | Out-Null
    $lines.Add("- economic_objective_profile_id: $([string](Get-PropValue -ObjectValue $candidate -Name 'economic_objective_profile_id' -DefaultValue ''))") | Out-Null
    $lines.Add("- lane_id: $([string](Get-PropValue -ObjectValue $candidate -Name 'lane_id' -DefaultValue ''))") | Out-Null
    $lines.Add("- lane_role: $([string](Get-PropValue -ObjectValue $candidate -Name 'lane_role' -DefaultValue ''))") | Out-Null
    $lines.Add("- lane_shadow_only: $([string](Get-PropValue -ObjectValue $candidate -Name 'lane_shadow_only' -DefaultValue ''))") | Out-Null
    $lines.Add("- certification_artifact_path: $([string](Get-PropValue -ObjectValue $candidate -Name 'certification_artifact_path' -DefaultValue ''))") | Out-Null
    $lines.Add("- duplicate_candidate: $([string](Get-PropValue -ObjectValue $candidate -Name 'duplicate_candidate' -DefaultValue ''))") | Out-Null
    $lines.Add("") | Out-Null

    $lines.Add("## Gates") | Out-Null
    foreach ($gateName in @("backtest", "runtime_parity", "paper")) {
        $gateValue = Get-PropValue -ObjectValue $gates -Name $gateName -DefaultValue @{}
        $lines.Add("- $gateName.pass: $([string](Get-PropValue -ObjectValue $gateValue -Name 'pass' -DefaultValue ''))") | Out-Null
    }
    if ($reasons.Count -gt 0) {
        $lines.Add("") | Out-Null
        $lines.Add("## Reasons") | Out-Null
        foreach ($reason in $reasons) {
            $lines.Add("- $([string]$reason)") | Out-Null
        }
    }

    $backtestCandidate = Get-PropValue -ObjectValue $steps -Name "backtest_candidate" -DefaultValue @{}
    $paperCandidate = Get-PropValue -ObjectValue $steps -Name "paper_candidate" -DefaultValue @{}
    $promoteStep = Get-PropValue -ObjectValue $steps -Name "promote" -DefaultValue @{}
    $trainStep = Get-PropValue -ObjectValue $steps -Name "train" -DefaultValue @{}
    $trainerEvidence = Get-PropValue -ObjectValue $trainStep -Name "trainer_evidence" -DefaultValue @{}
    $config = Get-PropValue -ObjectValue $ReportValue -Name "config" -DefaultValue @{}
    $lines.Add("") | Out-Null
    $lines.Add("## Candidate Metrics") | Out-Null
    $lines.Add("- trainer: $([string](Get-PropValue -ObjectValue $config -Name 'trainer' -DefaultValue ''))") | Out-Null
    $lines.Add("- feature_set: $([string](Get-PropValue -ObjectValue $config -Name 'feature_set' -DefaultValue ''))") | Out-Null
    $lines.Add("- label_set: $([string](Get-PropValue -ObjectValue $config -Name 'label_set' -DefaultValue ''))") | Out-Null
    $lines.Add("- promotion_policy: $([string](Get-PropValue -ObjectValue $config -Name 'promotion_policy' -DefaultValue ''))") | Out-Null
    $lines.Add("- trainer_evidence_mode: $([string](Get-PropValue -ObjectValue $config -Name 'trainer_evidence_mode' -DefaultValue ''))") | Out-Null
    $lines.Add("- lane_id: $([string](Get-PropValue -ObjectValue $config -Name 'lane_id' -DefaultValue ''))") | Out-Null
    $lines.Add("- lane_role: $([string](Get-PropValue -ObjectValue $config -Name 'lane_role' -DefaultValue ''))") | Out-Null
    $lines.Add("- backtest_orders_submitted: $([string](Get-PropValue -ObjectValue $backtestCandidate -Name 'orders_submitted' -DefaultValue ''))") | Out-Null
    $lines.Add("- backtest_orders_filled: $([string](Get-PropValue -ObjectValue $backtestCandidate -Name 'orders_filled' -DefaultValue ''))") | Out-Null
    $lines.Add("- backtest_candidates_aborted_by_policy: $([string](Get-PropValue -ObjectValue $backtestCandidate -Name 'candidates_aborted_by_policy' -DefaultValue ''))") | Out-Null
    $lines.Add("- backtest_execution_policy_veto_failure: $([string](Get-PropValue -ObjectValue $backtestCandidate -Name 'execution_policy_veto_failure' -DefaultValue ''))") | Out-Null
    $lines.Add("- backtest_realized_pnl_quote: $([string](Get-PropValue -ObjectValue $backtestCandidate -Name 'realized_pnl_quote' -DefaultValue ''))") | Out-Null
    $lines.Add("- backtest_fill_rate: $([string](Get-PropValue -ObjectValue $backtestCandidate -Name 'fill_rate' -DefaultValue ''))") | Out-Null
    $lines.Add("- backtest_max_drawdown_pct: $([string](Get-PropValue -ObjectValue $backtestCandidate -Name 'max_drawdown_pct' -DefaultValue ''))") | Out-Null
    $lines.Add("- backtest_slippage_bps_mean: $([string](Get-PropValue -ObjectValue $backtestCandidate -Name 'slippage_bps_mean' -DefaultValue ''))") | Out-Null
    $lines.Add("- backtest_calmar_like_score: $([string](Get-PropValue -ObjectValue $backtestCandidate -Name 'calmar_like_score' -DefaultValue ''))") | Out-Null
    $lines.Add("- backtest_evaluation_contract_id: $([string](Get-PropValue -ObjectValue $backtestCandidate -Name 'evaluation_contract_id' -DefaultValue ''))") | Out-Null
    $lines.Add("- runtime_parity_evaluation_contract_id: $([string](Get-PropValue -ObjectValue (Get-PropValue -ObjectValue $steps -Name 'backtest_runtime_parity_candidate' -DefaultValue @{}) -Name 'evaluation_contract_id' -DefaultValue ''))") | Out-Null
    $lines.Add("- paper_orders_submitted: $([string](Get-PropValue -ObjectValue $paperCandidate -Name 'orders_submitted' -DefaultValue ''))") | Out-Null
    $lines.Add("- paper_orders_filled: $([string](Get-PropValue -ObjectValue $paperCandidate -Name 'orders_filled' -DefaultValue ''))") | Out-Null
    $lines.Add("- paper_realized_pnl_quote: $([string](Get-PropValue -ObjectValue $paperCandidate -Name 'realized_pnl_quote' -DefaultValue ''))") | Out-Null
    $lines.Add("- paper_slippage_bps_mean: $([string](Get-PropValue -ObjectValue $paperCandidate -Name 'slippage_bps_mean' -DefaultValue ''))") | Out-Null
    $lines.Add("- paper_t15_gate_pass: $([string](Get-PropValue -ObjectValue $paperCandidate -Name 't15_gate_pass' -DefaultValue ''))") | Out-Null
    $lines.Add("- paper_evaluation_contract_id: $([string](Get-PropValue -ObjectValue $paperCandidate -Name 'evaluation_contract_id' -DefaultValue ''))") | Out-Null
    $lines.Add("- promoted: $([string](Get-PropValue -ObjectValue $promoteStep -Name 'promoted' -DefaultValue ''))") | Out-Null

    $lines.Add("") | Out-Null
    $lines.Add("## Trainer Evidence") | Out-Null
    $lines.Add("- available: $([string](Get-PropValue -ObjectValue $trainerEvidence -Name 'available' -DefaultValue ''))") | Out-Null
    $lines.Add("- required: $([string](Get-PropValue -ObjectValue $trainerEvidence -Name 'required' -DefaultValue ''))") | Out-Null
    $lines.Add("- pass: $([string](Get-PropValue -ObjectValue $trainerEvidence -Name 'pass' -DefaultValue ''))") | Out-Null
    $lines.Add("- offline_pass: $([string](Get-PropValue -ObjectValue $trainerEvidence -Name 'offline_pass' -DefaultValue ''))") | Out-Null
    $lines.Add("- execution_pass: $([string](Get-PropValue -ObjectValue $trainerEvidence -Name 'execution_pass' -DefaultValue ''))") | Out-Null
    $lines.Add("- certification_window_valid: $([string](Get-PropValue -ObjectValue $trainerEvidence -Name 'certification_window_valid' -DefaultValue ''))") | Out-Null
    $trainerEvidenceReasons = @(Get-PropValue -ObjectValue $trainerEvidence -Name "reasons" -DefaultValue @())
    $lines.Add("- reasons: $([string]($trainerEvidenceReasons -join ','))") | Out-Null

    $backtestGate = Get-PropValue -ObjectValue $gates -Name "backtest" -DefaultValue @{}
    $lines.Add("") | Out-Null
    $lines.Add("## Champion Compare") | Out-Null
    $lines.Add("- strict_pnl_pass: $([string](Get-PropValue -ObjectValue $backtestGate -Name 'vs_champion_strict_pnl_pass' -DefaultValue ''))") | Out-Null
    $lines.Add("- pareto_candidate_dominates: $([string](Get-PropValue -ObjectValue $backtestGate -Name 'vs_champion_pareto_candidate_dominates' -DefaultValue ''))") | Out-Null
    $lines.Add("- pareto_champion_dominates: $([string](Get-PropValue -ObjectValue $backtestGate -Name 'vs_champion_pareto_champion_dominates' -DefaultValue ''))") | Out-Null
    $lines.Add("- pareto_incomparable: $([string](Get-PropValue -ObjectValue $backtestGate -Name 'vs_champion_pareto_incomparable' -DefaultValue ''))") | Out-Null
    $lines.Add("- pnl_delta_quote: $([string](Get-PropValue -ObjectValue $backtestGate -Name 'vs_champion_pnl_delta_quote' -DefaultValue ''))") | Out-Null
    $lines.Add("- pnl_delta_pct: $([string](Get-PropValue -ObjectValue $backtestGate -Name 'vs_champion_pnl_delta_pct' -DefaultValue ''))") | Out-Null
    $lines.Add("- pnl_within_tolerance_pass: $([string](Get-PropValue -ObjectValue $backtestGate -Name 'vs_champion_pnl_within_tolerance_pass' -DefaultValue ''))") | Out-Null
    $lines.Add("- drawdown_improvement_pct: $([string](Get-PropValue -ObjectValue $backtestGate -Name 'vs_champion_drawdown_improvement_pct' -DefaultValue ''))") | Out-Null
    $lines.Add("- utility_metric: $([string](Get-PropValue -ObjectValue $backtestGate -Name 'utility_metric' -DefaultValue ''))") | Out-Null
    $lines.Add("- utility_candidate_score: $([string](Get-PropValue -ObjectValue $backtestGate -Name 'vs_champion_utility_candidate_score' -DefaultValue ''))") | Out-Null
    $lines.Add("- utility_champion_score: $([string](Get-PropValue -ObjectValue $backtestGate -Name 'vs_champion_utility_champion_score' -DefaultValue ''))") | Out-Null
    $lines.Add("- utility_delta_pct: $([string](Get-PropValue -ObjectValue $backtestGate -Name 'vs_champion_utility_delta_pct' -DefaultValue ''))") | Out-Null
    $lines.Add("- utility_tie_break_pass: $([string](Get-PropValue -ObjectValue $backtestGate -Name 'vs_champion_utility_tie_break_pass' -DefaultValue ''))") | Out-Null
    $lines.Add("- fill_rate_degradation: $([string](Get-PropValue -ObjectValue $backtestGate -Name 'vs_champion_fill_rate_degradation' -DefaultValue ''))") | Out-Null
    $lines.Add("- fill_rate_guard_pass: $([string](Get-PropValue -ObjectValue $backtestGate -Name 'vs_champion_fill_rate_guard_pass' -DefaultValue ''))") | Out-Null
    $lines.Add("- slippage_deterioration_bps: $([string](Get-PropValue -ObjectValue $backtestGate -Name 'vs_champion_slippage_deterioration_bps' -DefaultValue ''))") | Out-Null
    $lines.Add("- slippage_guard_pass: $([string](Get-PropValue -ObjectValue $backtestGate -Name 'vs_champion_slippage_guard_pass' -DefaultValue ''))") | Out-Null
    $lines.Add("- stability_override_pass: $([string](Get-PropValue -ObjectValue $backtestGate -Name 'vs_champion_stability_override_pass' -DefaultValue ''))") | Out-Null
    $lines.Add("- trainer_evidence_applied: $([string](Get-PropValue -ObjectValue $backtestGate -Name 'trainer_evidence_applied' -DefaultValue ''))") | Out-Null
    $lines.Add("- trainer_evidence_available: $([string](Get-PropValue -ObjectValue $backtestGate -Name 'trainer_evidence_available' -DefaultValue ''))") | Out-Null
    $lines.Add("- trainer_evidence_pass: $([string](Get-PropValue -ObjectValue $backtestGate -Name 'trainer_evidence_pass' -DefaultValue ''))") | Out-Null
    $lines.Add("- trainer_evidence_gate_pass: $([string](Get-PropValue -ObjectValue $backtestGate -Name 'trainer_evidence_gate_pass' -DefaultValue ''))") | Out-Null
    $lines.Add("- trainer_evidence_offline_pass: $([string](Get-PropValue -ObjectValue $backtestGate -Name 'trainer_evidence_offline_pass' -DefaultValue ''))") | Out-Null
    $lines.Add("- trainer_evidence_execution_pass: $([string](Get-PropValue -ObjectValue $backtestGate -Name 'trainer_evidence_execution_pass' -DefaultValue ''))") | Out-Null
    $lines.Add("- candidate_orders_submitted: $([string](Get-PropValue -ObjectValue $backtestGate -Name 'candidate_orders_submitted' -DefaultValue ''))") | Out-Null
    $lines.Add("- candidate_candidates_aborted_by_policy: $([string](Get-PropValue -ObjectValue $backtestGate -Name 'candidate_candidates_aborted_by_policy' -DefaultValue ''))") | Out-Null
    $lines.Add("- candidate_execution_policy_veto_failure: $([string](Get-PropValue -ObjectValue $backtestGate -Name 'candidate_execution_policy_veto_failure' -DefaultValue ''))") | Out-Null
    $lines.Add("- budget_contract_gate_pass: $([string](Get-PropValue -ObjectValue $backtestGate -Name 'budget_contract_gate_pass' -DefaultValue ''))") | Out-Null
    $lines.Add("- budget_lane_class_effective: $([string](Get-PropValue -ObjectValue $backtestGate -Name 'budget_lane_class_effective' -DefaultValue ''))") | Out-Null
    $lines.Add("- lane_shadow_only: $([string](Get-PropValue -ObjectValue $backtestGate -Name 'lane_shadow_only' -DefaultValue ''))") | Out-Null
    $lines.Add("- economic_objective_profile_id: $([string](Get-PropValue -ObjectValue $backtestGate -Name 'economic_objective_profile_id' -DefaultValue ''))") | Out-Null
    $lines.Add("- certification_window_start: $([string](Get-PropValue -ObjectValue $backtestGate -Name 'certification_window_start' -DefaultValue ''))") | Out-Null
    $lines.Add("- certification_window_end: $([string](Get-PropValue -ObjectValue $backtestGate -Name 'certification_window_end' -DefaultValue ''))") | Out-Null
    $lines.Add("- certification_window_valid: $([string](Get-PropValue -ObjectValue $backtestGate -Name 'certification_window_valid' -DefaultValue ''))") | Out-Null
    $lines.Add("- decision_basis: $([string](Get-PropValue -ObjectValue $backtestGate -Name 'decision_basis' -DefaultValue ''))") | Out-Null
    $lines.Add("- compare_mode: $([string](Get-PropValue -ObjectValue $backtestGate -Name 'compare_mode' -DefaultValue ''))") | Out-Null

    if ($runtimeBefore.Count -gt 0) {
        $lines.Add("") | Out-Null
        $lines.Add("## Runtime Units Before") | Out-Null
        foreach ($item in $runtimeBefore) {
            $unitName = [string](Get-PropValue -ObjectValue $item -Name "unit" -DefaultValue "")
            $activeValue = [string](Get-PropValue -ObjectValue $item -Name "active" -DefaultValue "")
            $enabledValue = [string](Get-PropValue -ObjectValue $item -Name "enabled" -DefaultValue "")
            if (-not [string]::IsNullOrWhiteSpace($unitName)) {
                $lines.Add("- $unitName active=$activeValue enabled=$enabledValue") | Out-Null
            }
        }
    }
    if ($restartTargets.Count -gt 0) {
        $lines.Add("") | Out-Null
        $lines.Add("## Restart Targets") | Out-Null
        foreach ($item in $restartTargets) {
            $lines.Add("- $([string]$item)") | Out-Null
        }
    }
    return ($lines -join "`n") + "`n"
}

$resolvedProjectRoot = if ([string]::IsNullOrWhiteSpace($ProjectRoot)) { Resolve-DefaultProjectRoot } else { $ProjectRoot }
$resolvedProjectRoot = [System.IO.Path]::GetFullPath($resolvedProjectRoot)
$resolvedPythonExe = if ([string]::IsNullOrWhiteSpace($PythonExe)) { Resolve-DefaultPythonExe -Root $resolvedProjectRoot } else { $PythonExe }
$resolvedDailyPipelineScript = if ([string]::IsNullOrWhiteSpace($DailyPipelineScript)) { Resolve-DefaultDailyPipelineScript -Root $resolvedProjectRoot } else { $DailyPipelineScript }
$resolvedTrainSnapshotCloseReportPath = if ([string]::IsNullOrWhiteSpace($TrainSnapshotCloseReportPath)) {
    Resolve-DefaultTrainSnapshotCloseReportPath -Root $resolvedProjectRoot
} else {
    Resolve-PathFromProjectRoot -Root $resolvedProjectRoot -PathValue $TrainSnapshotCloseReportPath
}
$resolvedPaperSmokeScript = if ([string]::IsNullOrWhiteSpace($PaperSmokeScript)) { Join-Path $resolvedProjectRoot "scripts/paper_micro_smoke.ps1" } else { $PaperSmokeScript }
$resolvedOutDir = if ([System.IO.Path]::IsPathRooted($OutDir)) { $OutDir } else { Join-Path $resolvedProjectRoot $OutDir }
$resolvedPaperSmokeOutDir = Join-Path $resolvedOutDir "paper_smoke"
$resolvedRegistryRoot = Join-Path $resolvedProjectRoot "models/registry"
$resolvedChampionModelFamily = if ([string]::IsNullOrWhiteSpace($ChampionModelFamily)) { $ModelFamily } else { $ChampionModelFamily.Trim() }
$psExe = Get-PowerShellExe

Set-Location $resolvedProjectRoot

$vendorSitePackages = Join-Path $resolvedProjectRoot "python/site-packages"
if ($script:IsWindowsPlatform -and (Test-Path $vendorSitePackages)) {
    if ([string]::IsNullOrWhiteSpace($env:PYTHONPATH)) {
        $env:PYTHONPATH = $vendorSitePackages
    } elseif ($env:PYTHONPATH -notlike "*$vendorSitePackages*") {
        $env:PYTHONPATH = "$vendorSitePackages;$($env:PYTHONPATH)"
    }
}

New-Item -ItemType Directory -Path $resolvedOutDir -Force | Out-Null
New-Item -ItemType Directory -Path $resolvedPaperSmokeOutDir -Force | Out-Null

$effectiveBatchDate = if ([string]::IsNullOrWhiteSpace($BatchDate)) { (Get-Date).Date.AddDays(-1).ToString("yyyy-MM-dd") } else { $BatchDate }
$effectiveBatchDate = Resolve-DateToken -DateText $effectiveBatchDate -LabelForError "batch_date"
$dataPlatformReadySnapshotId = Get-DataPlatformReadySnapshotId -ProjectRoot $resolvedProjectRoot
$script:dataPlatformReadySnapshotId = $dataPlatformReadySnapshotId
$trainSnapshotCloseContract = Resolve-TrainSnapshotCloseContract `
    -ProjectRoot $resolvedProjectRoot `
    -ReportPath $resolvedTrainSnapshotCloseReportPath `
    -ExpectedBatchDate $effectiveBatchDate `
    -ExpectedSnapshotId $dataPlatformReadySnapshotId
$windowRamp = Resolve-TrainWindowRamp `
    -ProjectRoot $resolvedProjectRoot `
    -BatchDate $effectiveBatchDate `
    -Tf $Tf `
    -RequestedTrainLookbackDays $TrainLookbackDays `
    -RequestedBacktestLookbackDays $BacktestLookbackDays `
    -RampEnabled $TrainLookbackRampEnabled `
    -MicroRoot $TrainLookbackRampMicroRoot `
    -MinMarketsPerDate $TrainLookbackRampMinMarketsPerDate `
    -TrainDataQualityFloorDate $TrainDataQualityFloorDate `
    -TrainStartFloorDate $TrainStartFloorDate `
    -CoverageCounts (Get-PropValue -ObjectValue $trainSnapshotCloseContract -Name "micro_date_coverage_counts" -DefaultValue @{})
$certificationStartDate = [string](Get-PropValue -ObjectValue $windowRamp -Name "certification_start_date" -DefaultValue "")
$trainEndDate = [string](Get-PropValue -ObjectValue $windowRamp -Name "train_end_date" -DefaultValue "")
$trainStartDate = [string](Get-PropValue -ObjectValue $windowRamp -Name "train_start_date" -DefaultValue "")
$promotionPolicyConfig = Resolve-PromotionPolicyConfig -PolicyName $PromotionPolicy -BoundParams $PSBoundParameters -EconomicObjectiveProfile @{}

$latestPointerPath = Resolve-RegistryPointerPath -RegistryRoot $resolvedRegistryRoot -Family $ModelFamily -PointerName "latest"
$candidatePointerPath = Resolve-RegistryPointerPath -RegistryRoot $resolvedRegistryRoot -Family $ModelFamily -PointerName "latest_candidate"
$championPointerPath = Resolve-RegistryPointerPath -RegistryRoot $resolvedRegistryRoot -Family $resolvedChampionModelFamily -PointerName "champion"
$championBefore = Load-JsonOrEmpty -PathValue $championPointerPath

$report = [ordered]@{
    generated_at = (Get-Date).ToString("o")
    dry_run = [bool]$DryRun
    project_root = $resolvedProjectRoot
    python_exe = $resolvedPythonExe
    batch_date = $effectiveBatchDate
    model_family = $ModelFamily
    run_scope = $RunScope
    windows = [bool]$script:IsWindowsPlatform
    config = [ordered]@{
        train_lookback_days = [int](Get-PropValue -ObjectValue $windowRamp -Name "effective_train_lookback_days" -DefaultValue $TrainLookbackDays)
        train_lookback_days_requested = [int]$TrainLookbackDays
        train_lookback_days_effective = [int](Get-PropValue -ObjectValue $windowRamp -Name "effective_train_lookback_days" -DefaultValue $TrainLookbackDays)
        backtest_lookback_days = [int]$BacktestLookbackDays
        train_window_ramp_enabled = [bool](Get-PropValue -ObjectValue $windowRamp -Name "enabled" -DefaultValue $false)
        train_window_ramp_active = [bool](Get-PropValue -ObjectValue $windowRamp -Name "ramp_active" -DefaultValue $false)
        train_window_ramp_reason = [string](Get-PropValue -ObjectValue $windowRamp -Name "reason" -DefaultValue "")
        train_window_ramp_micro_root = [string](Get-PropValue -ObjectValue $windowRamp -Name "micro_root" -DefaultValue "")
        train_window_ramp_min_markets_per_date = [int](Get-PropValue -ObjectValue $windowRamp -Name "min_markets_per_date" -DefaultValue 1)
        train_window_ramp_available_contiguous_micro_days = [int](Get-PropValue -ObjectValue $windowRamp -Name "available_contiguous_micro_days" -DefaultValue 0)
        train_window_ramp_first_available_micro_date = [string](Get-PropValue -ObjectValue $windowRamp -Name "first_available_micro_date" -DefaultValue "")
        train_window_ramp_last_available_micro_date = [string](Get-PropValue -ObjectValue $windowRamp -Name "last_available_micro_date" -DefaultValue "")
        train_data_quality_floor_date = [string](Get-PropValue -ObjectValue $windowRamp -Name "train_data_quality_floor_date" -DefaultValue "")
        train_data_quality_floor_applied = [bool](Get-PropValue -ObjectValue $windowRamp -Name "train_data_quality_floor_applied" -DefaultValue $false)
        train_start_floor_date = [string](Get-PropValue -ObjectValue $windowRamp -Name "train_start_floor_date" -DefaultValue "")
        train_start_floor_applied = [bool](Get-PropValue -ObjectValue $windowRamp -Name "train_start_floor_applied" -DefaultValue $false)
        tf = $Tf
        quote = $Quote
        trainer = $Trainer
        feature_set = $FeatureSet
        label_set = $LabelSet
        task = $Task
        run_scope = $RunScope
        strategy = $StrategyName
        candidate_model_ref = $CandidateModelRef
        champion_model_ref = $ChampionModelRef
        champion_model_family = $resolvedChampionModelFamily
        trainer_evidence_mode = $TrainerEvidenceMode
        train_top_n = [int]$TrainTopN
        backtest_top_n = [int]$BacktestTopN
        booster_sweep_trials = [int]$BoosterSweepTrials
        backtest_top_pct = [double]$BacktestTopPct
        backtest_min_prob = [double]$BacktestMinProb
        backtest_min_candidates_per_ts = [int]$BacktestMinCandidatesPerTs
        hold_bars = [int]$HoldBars
        backtest_runtime_parity_enabled = [bool]$BacktestRuntimeParityEnabled
        skip_champion_compare = [bool]$SkipChampionCompare
        paper_soak_duration_sec = [int]$PaperSoakDurationSec
        paper_micro_provider = $PaperMicroProvider
        paper_feature_provider = $PaperFeatureProvider
        paper_use_learned_runtime = [bool]$PaperUseLearnedRuntime
        paper_min_active_windows = [int]$PaperMinActiveWindows
        paper_min_nonnegative_window_ratio = [double]$PaperMinNonnegativeWindowRatio
        paper_max_fill_concentration_ratio = [double]$PaperMaxFillConcentrationRatio
        paper_min_payoff_ratio = [double]$PaperMinPayoffRatio
        paper_max_loss_concentration = [double]$PaperMaxLossConcentration
        execution_structure_min_closed_trades = [int]$ExecutionStructureMinClosedTrades
        backtest_min_orders_filled = [int]$promotionPolicyConfig.backtest_min_orders_filled
        backtest_min_realized_pnl_quote = [double]$promotionPolicyConfig.backtest_min_realized_pnl_quote
        backtest_min_deflated_sharpe_ratio = [double]$promotionPolicyConfig.backtest_min_deflated_sharpe_ratio
        backtest_min_pnl_delta_vs_champion = [double]$promotionPolicyConfig.backtest_min_pnl_delta_vs_champion
        backtest_min_payoff_ratio = [double]$BacktestMinPayoffRatio
        backtest_max_loss_concentration = [double]$BacktestMaxLossConcentration
        promotion_policy = $promotionPolicyConfig.name
        backtest_allow_stability_override = [bool]$promotionPolicyConfig.backtest_allow_stability_override
        backtest_champion_pnl_tolerance_pct = [double]$promotionPolicyConfig.backtest_champion_pnl_tolerance_pct
        backtest_champion_min_drawdown_improvement_pct = [double]$promotionPolicyConfig.backtest_champion_min_drawdown_improvement_pct
        backtest_champion_max_fill_rate_degradation = [double]$promotionPolicyConfig.backtest_champion_max_fill_rate_degradation
        backtest_champion_max_slippage_deterioration_bps = [double]$promotionPolicyConfig.backtest_champion_max_slippage_deterioration_bps
        backtest_champion_min_utility_edge_pct = [double]$promotionPolicyConfig.backtest_champion_min_utility_edge_pct
        backtest_use_pareto = [bool]$promotionPolicyConfig.use_pareto
        backtest_use_utility_tie_break = [bool]$promotionPolicyConfig.use_utility_tie_break
        promotion_policy_contract_source = [string]$promotionPolicyConfig.threshold_source
        promotion_policy_contract_profile_id = [string]$promotionPolicyConfig.profile_id
        promotion_policy_cli_override_keys = @($promotionPolicyConfig.cli_override_keys)
        skip_paper_soak = [bool]$SkipPaperSoak
        restart_units = @($RestartUnits)
        known_runtime_units = @($KnownRuntimeUnits)
        auto_restart_known_units = [bool]$AutoRestartKnownUnits
        data_platform_ready_snapshot_id = $dataPlatformReadySnapshotId
        train_snapshot_close_report_path = $resolvedTrainSnapshotCloseReportPath
    }
    windows_by_step = [ordered]@{
        train = [ordered]@{ start = $trainStartDate; end = $trainEndDate }
        research = [ordered]@{ start = $trainStartDate; end = $trainEndDate; source = "train_command_window" }
        certification = [ordered]@{ start = $certificationStartDate; end = $effectiveBatchDate }
        backtest = [ordered]@{ start = $certificationStartDate; end = $effectiveBatchDate; alias_of = "certification" }
        bootstrap = [ordered]@{
            start = [string](Get-PropValue -ObjectValue $windowRamp -Name "train_data_quality_floor_date" -DefaultValue "")
            end = $effectiveBatchDate
            source = "bootstrap_latest_inclusive_candidate"
        }
    }
    window_ramp = $windowRamp
    split_policy = [ordered]@{}
    steps = [ordered]@{}
    candidate = [ordered]@{}
    gates = [ordered]@{}
    reasons = @()
    failure_stage = ""
    failure_code = ""
    failure_report_path = ""
}

$report.steps.train_snapshot_close_preflight = [ordered]@{
    attempted = $true
    report_path = [string](Get-PropValue -ObjectValue $trainSnapshotCloseContract -Name "report_path" -DefaultValue "")
    exists = [bool](Get-PropValue -ObjectValue $trainSnapshotCloseContract -Name "exists" -DefaultValue $false)
    batch_date = [string](Get-PropValue -ObjectValue $trainSnapshotCloseContract -Name "batch_date" -DefaultValue "")
    expected_batch_date = [string](Get-PropValue -ObjectValue $trainSnapshotCloseContract -Name "expected_batch_date" -DefaultValue "")
    snapshot_id = [string](Get-PropValue -ObjectValue $trainSnapshotCloseContract -Name "snapshot_id" -DefaultValue "")
    expected_snapshot_id = [string](Get-PropValue -ObjectValue $trainSnapshotCloseContract -Name "expected_snapshot_id" -DefaultValue "")
    snapshot_root = [string](Get-PropValue -ObjectValue $trainSnapshotCloseContract -Name "snapshot_root" -DefaultValue "")
    generated_at_utc = [string](Get-PropValue -ObjectValue $trainSnapshotCloseContract -Name "generated_at_utc" -DefaultValue "")
    published_at_utc = [string](Get-PropValue -ObjectValue $trainSnapshotCloseContract -Name "published_at_utc" -DefaultValue "")
    overall_pass = [bool](Get-PropValue -ObjectValue $trainSnapshotCloseContract -Name "overall_pass" -DefaultValue $false)
    deadline_met = [bool](Get-PropValue -ObjectValue $trainSnapshotCloseContract -Name "deadline_met" -DefaultValue $false)
    source_freshness = (Get-PropValue -ObjectValue $trainSnapshotCloseContract -Name "source_freshness" -DefaultValue @{})
    coverage_window = (Get-PropValue -ObjectValue $trainSnapshotCloseContract -Name "coverage_window" -DefaultValue @{})
    train_window = (Get-PropValue -ObjectValue $trainSnapshotCloseContract -Name "train_window" -DefaultValue @{})
    certification_window = (Get-PropValue -ObjectValue $trainSnapshotCloseContract -Name "certification_window" -DefaultValue @{})
    training_critical_start_date = [string](Get-PropValue -ObjectValue $trainSnapshotCloseContract -Name "training_critical_start_date" -DefaultValue "")
    training_critical_end_date = [string](Get-PropValue -ObjectValue $trainSnapshotCloseContract -Name "training_critical_end_date" -DefaultValue "")
    features_v4_effective_end = [string](Get-PropValue -ObjectValue $trainSnapshotCloseContract -Name "features_v4_effective_end" -DefaultValue "")
    training_critical_refresh = (Get-PropValue -ObjectValue $trainSnapshotCloseContract -Name "training_critical_refresh" -DefaultValue @{})
    actual_dataset_rows = $null
    actual_dataset_min_ts_ms = $null
    actual_dataset_max_ts_ms = $null
    manifest_path = ""
    data_file_count = 0
    pass = [bool](Get-PropValue -ObjectValue $trainSnapshotCloseContract -Name "pass" -DefaultValue $false)
    reasons = @(
        @(Get-PropValue -ObjectValue $trainSnapshotCloseContract -Name "reasons" -DefaultValue @()) |
            ForEach-Object { [string]$_ } |
            Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    )
}

$runtimeUnitsBefore = if ($DryRun) { @() } else { @(Get-UnitStates -Units $KnownRuntimeUnits) }
$activeKnownUnits = @(
    $runtimeUnitsBefore |
        Where-Object { $true -eq (Get-PropValue -ObjectValue $_ -Name "active" -DefaultValue $false) } |
        ForEach-Object { [string](Get-PropValue -ObjectValue $_ -Name "unit" -DefaultValue "") }
)
$effectiveRestartUnits = if ($AutoRestartKnownUnits) {
    Merge-UniqueStringArray -First $RestartUnits -Second $activeKnownUnits
} else {
    Merge-UniqueStringArray -First $RestartUnits -Second @()
}
$report.runtime_units_before = @($runtimeUnitsBefore)
$report.restart_targets = @($effectiveRestartUnits)
$candidateRunId = ""
$candidateRunDir = ""

function Set-ReportFailure {
    param(
        [string]$Stage,
        [string]$Code,
        [string]$ReportPath = ""
    )
    if (-not [string]::IsNullOrWhiteSpace($Stage)) {
        $report.failure_stage = $Stage
    }
    if (-not [string]::IsNullOrWhiteSpace($Code)) {
        $report.failure_code = $Code
    }
    if (-not [string]::IsNullOrWhiteSpace($ReportPath)) {
        $report.failure_report_path = $ReportPath
    }
}

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

Sync-WindowRampState -WindowRampValue $windowRamp

$trainingCriticalCoverage = Assert-TrainingCriticalCoverageWindow `
    -TrainSnapshotCloseContract $trainSnapshotCloseContract `
    -TrainWindowStart $script:trainStartDate `
    -TrainWindowEnd $script:trainEndDate `
    -CertificationWindowStart $script:certificationStartDate `
    -CertificationWindowEnd $effectiveBatchDate
$report.steps.train_snapshot_close_preflight.train_window_start = [string](Get-PropValue -ObjectValue $trainingCriticalCoverage -Name "train_window_start" -DefaultValue "")
$report.steps.train_snapshot_close_preflight.train_window_end = [string](Get-PropValue -ObjectValue $trainingCriticalCoverage -Name "train_window_end" -DefaultValue "")
$report.steps.train_snapshot_close_preflight.certification_window_start = [string](Get-PropValue -ObjectValue $trainingCriticalCoverage -Name "certification_window_start" -DefaultValue "")
$report.steps.train_snapshot_close_preflight.certification_window_end = [string](Get-PropValue -ObjectValue $trainingCriticalCoverage -Name "certification_window_end" -DefaultValue "")
$report.steps.train_snapshot_close_preflight.coverage_window_start = [string](Get-PropValue -ObjectValue $trainingCriticalCoverage -Name "coverage_start_date" -DefaultValue "")
$report.steps.train_snapshot_close_preflight.coverage_window_end = [string](Get-PropValue -ObjectValue $trainingCriticalCoverage -Name "coverage_end_date" -DefaultValue "")
$report.steps.train_snapshot_close_preflight.train_window_covered = [bool](Get-PropValue -ObjectValue $trainingCriticalCoverage -Name "train_window_covered" -DefaultValue $false)
$report.steps.train_snapshot_close_preflight.certification_window_covered = [bool](Get-PropValue -ObjectValue $trainingCriticalCoverage -Name "certification_window_covered" -DefaultValue $false)
$report.steps.train_snapshot_close_preflight.coverage_window_source = [string](Get-PropValue -ObjectValue $trainSnapshotCloseContract -Name "coverage_window_source" -DefaultValue "")
$report.steps.train_snapshot_close_preflight.refresh_argument_mode = [string](Get-PropValue -ObjectValue $trainSnapshotCloseContract -Name "refresh_argument_mode" -DefaultValue "")
$existingTrainSnapshotCloseReasons = @(
    @(Get-PropValue -ObjectValue $report.steps.train_snapshot_close_preflight -Name "reasons" -DefaultValue @()) |
        ForEach-Object { [string]$_ } |
        Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
)
$report.steps.train_snapshot_close_preflight.reasons = @(
    Merge-UniqueStringArray `
        -First @($existingTrainSnapshotCloseReasons) `
        -Second @((Get-PropValue -ObjectValue $trainingCriticalCoverage -Name "reasons" -DefaultValue @()))
)
$report.steps.train_snapshot_close_preflight.pass = (
    (To-Bool (Get-PropValue -ObjectValue $report.steps.train_snapshot_close_preflight -Name "pass" -DefaultValue $false) $false) -and
    (To-Bool (Get-PropValue -ObjectValue $trainingCriticalCoverage -Name "pass" -DefaultValue $false) $false)
)

$script:trainSnapshotClosePreflightFailed = -not (To-Bool (Get-PropValue -ObjectValue $report.steps.train_snapshot_close_preflight -Name "pass" -DefaultValue $false) $false)
$script:trainSnapshotClosePreflightReasons = @(
    @(Get-PropValue -ObjectValue $report.steps.train_snapshot_close_preflight -Name "reasons" -DefaultValue @()) |
        ForEach-Object { [string]$_ } |
        Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
)

$script:splitPolicyId = "v4_split_policy_forward_validation_lcb_v1"
$script:splitPolicyLaneMode = "promotion_strict"
$script:splitPolicyPromotionEligible = $true
$script:splitPolicySelectedBy = "strict_trainability"
$script:splitPolicySelectedHoldoutDays = [int]$BacktestLookbackDays
$script:splitPolicyReasonCodes = @()
$script:splitPolicyStrictTrainability = @{}
$script:splitPolicyBootstrapTrainability = @{}
$script:splitPolicyCandidateHoldoutDays = @()
$script:splitPolicyHistoricalAnchorCount = 0
$script:splitPolicySelectionSummary = @()
$script:splitPolicyHistoryPath = ""
$script:splitPolicyNewEvaluations = @()
$script:splitPolicyArtifactPath = ""
$script:bootstrapWindowStart = [string](Get-PropValue -ObjectValue $windowRamp -Name "train_data_quality_floor_date" -DefaultValue "")
$script:bootstrapWindowEnd = $effectiveBatchDate

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
    $actualTrainDays = Get-DateWindowInclusiveDayCount -StartDate $script:trainStartDate -EndDate $script:trainEndDate
    if ($actualTrainDays -gt 0) {
        $report.config.train_lookback_days = [int]$actualTrainDays
        $report.config.train_lookback_days_effective = [int]$actualTrainDays
    }
}

Sync-SplitPolicyState

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

if ($script:trainSnapshotClosePreflightFailed) {
    $report.reasons = @($script:trainSnapshotClosePreflightReasons)
    $report.gates.overall_pass = $false
    $trainSnapshotFailureCode = if (@($script:trainSnapshotClosePreflightReasons).Count -gt 0) {
        [string]$script:trainSnapshotClosePreflightReasons[0]
    } else {
        "TRAIN_SNAPSHOT_CLOSE_PRECHECK_FAILED"
    }
    Set-ReportFailure `
        -Stage "data_close" `
        -Code $trainSnapshotFailureCode `
        -ReportPath ([string](Get-PropValue -ObjectValue $report.steps.train_snapshot_close_preflight -Name "report_path" -DefaultValue ""))
    $paths = Save-Report
    Write-ReportPointers -LogTag $LogTag -Paths $paths -OverallPass $false
    exit 2
}

function Resolve-ReportedJsonPathFromText {
    param(
        [string]$TextValue,
        [string]$LogTag
    )
    if ([string]::IsNullOrWhiteSpace($TextValue) -or [string]::IsNullOrWhiteSpace($LogTag)) {
        return ""
    }
    $pattern = '(?m)^\[' + [Regex]::Escape($LogTag) + '\] report=(.+)$'
    $match = [Regex]::Match($TextValue, $pattern)
    if (-not $match.Success) {
        return ""
    }
    $reportedPath = [string]$match.Groups[1].Value
    if ([string]::IsNullOrWhiteSpace($reportedPath)) {
        return ""
    }
    return $reportedPath.Trim()
}

function Resolve-FeaturesBuildReportPathFromText {
    param([string]$TextValue)
    if ([string]::IsNullOrWhiteSpace($TextValue)) {
        return ""
    }
    $pattern = '(?m)^\[features\]\[build\]\[[^\]]+\]\s+report=(.+)$'
    $match = [Regex]::Match($TextValue, $pattern)
    if (-not $match.Success) {
        return ""
    }
    $reportedPath = [string]$match.Groups[1].Value
    if ([string]::IsNullOrWhiteSpace($reportedPath)) {
        return ""
    }
    return $reportedPath.Trim()
}

function Resolve-JsonPayloadFromText {
    param(
        [string]$TextValue,
        [string]$ContextLabel = "json_payload"
    )
    if ([string]::IsNullOrWhiteSpace($TextValue)) {
        throw ($ContextLabel + " output is empty")
    }
    try {
        return [string]$TextValue | ConvertFrom-Json
    } catch {
    }
    $lines = @(
        $TextValue -split "`r?`n" |
            ForEach-Object { ([string]$_).Trim() } |
            Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    )
    foreach ($line in @($lines)) {
        if (-not ($line.StartsWith("{") -or $line.StartsWith("["))) {
            continue
        }
        try {
            return [string]$line | ConvertFrom-Json
        } catch {
        }
    }
    throw ($ContextLabel + " json parse failed")
}

function Resolve-FeaturesValidateReportPathFromText {
    param([string]$TextValue)
    if ([string]::IsNullOrWhiteSpace($TextValue)) {
        return ""
    }
    $pattern = '(?m)^\[features\]\[validate\]\[[^\]]+\]\s+report=(.+)$'
    $match = [Regex]::Match($TextValue, $pattern)
    if (-not $match.Success) {
        return ""
    }
    $reportedPath = [string]$match.Groups[1].Value
    if ([string]::IsNullOrWhiteSpace($reportedPath)) {
        return ""
    }
    return $reportedPath.Trim()
}

function Resolve-DataContractRegistryPathFromText {
    param([string]$TextValue)
    if ([string]::IsNullOrWhiteSpace($TextValue)) {
        return ""
    }
    $pattern = '(?m)^\[ops\]\[data-contract-registry\]\s+path=(.+)$'
    $match = [Regex]::Match($TextValue, $pattern)
    if (-not $match.Success) {
        return ""
    }
    $reportedPath = [string]$match.Groups[1].Value
    if ([string]::IsNullOrWhiteSpace($reportedPath)) {
        return ""
    }
    return $reportedPath.Trim()
}

function Resolve-LiveFeatureParityReportPathFromText {
    param([string]$TextValue)
    if ([string]::IsNullOrWhiteSpace($TextValue)) {
        return ""
    }
    $pattern = '(?m)^\[ops\]\[live-feature-parity\]\s+path=(.+)$'
    $match = [Regex]::Match($TextValue, $pattern)
    if (-not $match.Success) {
        return ""
    }
    $reportedPath = [string]$match.Groups[1].Value
    if ([string]::IsNullOrWhiteSpace($reportedPath)) {
        return ""
    }
    return $reportedPath.Trim()
}

function Resolve-FeatureDatasetCertificationPathFromText {
    param([string]$TextValue)
    if ([string]::IsNullOrWhiteSpace($TextValue)) {
        return ""
    }
    $pattern = '(?m)^\[ops\]\[feature-dataset-certification\]\s+path=(.+)$'
    $match = [Regex]::Match($TextValue, $pattern)
    if (-not $match.Success) {
        return ""
    }
    $reportedPath = [string]$match.Groups[1].Value
    if ([string]::IsNullOrWhiteSpace($reportedPath)) {
        return ""
    }
    return $reportedPath.Trim()
}

function Resolve-PrivateExecutionLabelStoreBuildPathFromText {
    param([string]$TextValue)
    if ([string]::IsNullOrWhiteSpace($TextValue)) {
        return ""
    }
    $match = [Regex]::Match($TextValue, '"build_report_path"\s*:\s*"([^"]+)"')
    if (-not $match.Success) {
        return ""
    }
    $reportedPath = [string]$match.Groups[1].Value
    if ([string]::IsNullOrWhiteSpace($reportedPath)) {
        return ""
    }
    return $reportedPath.Trim()
}

function Get-DateRangeAscending {
    param(
        [string]$StartDate,
        [string]$EndDate
    )
    if ([string]::IsNullOrWhiteSpace($StartDate) -or [string]::IsNullOrWhiteSpace($EndDate)) {
        Write-Output -NoEnumerate @()
        return
    }
    $startObj = [DateTime]::ParseExact($StartDate, "yyyy-MM-dd", [System.Globalization.CultureInfo]::InvariantCulture)
    $endObj = [DateTime]::ParseExact($EndDate, "yyyy-MM-dd", [System.Globalization.CultureInfo]::InvariantCulture)
    if ($endObj -lt $startObj) {
        Write-Output -NoEnumerate @()
        return
    }
    $values = New-Object System.Collections.Generic.List[string]
    $cursor = $startObj
    while ($cursor -le $endObj) {
        $values.Add($cursor.ToString("yyyy-MM-dd")) | Out-Null
        $cursor = $cursor.AddDays(1)
    }
    Write-Output -NoEnumerate ($values.ToArray())
}

function Invoke-FeaturesBuildAndLoadReport {
    param(
        [string]$PythonPath,
        [string]$StartDate,
        [string]$EndDate
    )
    $reportPath = Join-Path $resolvedProjectRoot ("data/features/features_" + $FeatureSet + "/_meta/build_report.json")
    $reportDoc = if (Test-Path $reportPath) { Load-JsonOrEmpty -PathValue $reportPath } else { @{} }
    $closeFeaturesEffectiveEnd = [string](Get-PropValue -ObjectValue $trainSnapshotCloseContract -Name "features_v4_effective_end" -DefaultValue "")
    $useFrozenCloseFeatures = (
        (([string]$Trainer).Trim().ToLowerInvariant() -eq "v5_fusion") -and
        (To-Bool (Get-PropValue -ObjectValue $trainSnapshotCloseContract -Name "pass" -DefaultValue $false) $false) -and
        (-not [string]::IsNullOrWhiteSpace($closeFeaturesEffectiveEnd))
    )
    if ($useFrozenCloseFeatures) {
        $rowsFinal = [int](To-Int64 (Get-PropValue -ObjectValue $reportDoc -Name "rows_final" -DefaultValue 0) 0)
        $minRows = [int](To-Int64 (Get-PropValue -ObjectValue $reportDoc -Name "min_rows_for_train" -DefaultValue 0) 0)
        $status = [string](Get-PropValue -ObjectValue $reportDoc -Name "status" -DefaultValue "")
        $effectiveStart = [string](Get-PropValue -ObjectValue $reportDoc -Name "effective_start" -DefaultValue "")
        $effectiveEnd = [string](Get-PropValue -ObjectValue $reportDoc -Name "effective_end" -DefaultValue "")
        $windowCovered = Test-DateWindowContains `
            -OuterStartDate $effectiveStart `
            -OuterEndDate $effectiveEnd `
            -InnerStartDate $StartDate `
            -InnerEndDate $EndDate
        $usable = ($windowCovered -and ($rowsFinal -ge $minRows) -and ($status -eq "PASS"))
        return [PSCustomObject]@{
            Exec = [PSCustomObject]@{
                ExitCode = 0
                Output = ""
                Command = "[frozen-close-contract] features_v4"
            }
            ReportPath = $reportPath
            Report = $reportDoc
            RowsFinal = $rowsFinal
            MinRowsForTrain = $minRows
            Status = $status
            ErrorMessage = if ($windowCovered) { "" } else { "FEATURES_V4_WINDOW_NOT_COVERED_BY_CLOSE" }
            Usable = [bool]$usable
            SourceMode = "train_snapshot_close_frozen_features"
        }
    }
    $args = @(
        "-m", "autobot.cli",
        "features", "build",
        "--feature-set", $FeatureSet,
        "--label-set", $LabelSet,
        "--tf", $Tf,
        "--quote", $Quote,
        "--top-n", $TrainTopN,
        "--start", $StartDate,
        "--end", $EndDate
    )
    $commandText = ($PythonPath + " " + (($args | ForEach-Object { [string]$_ }) -join " "))
    $exec = $null
    try {
        $exec = Invoke-CommandCapture -Exe $PythonPath -ArgList $args -AllowFailure
    } catch {
        $exec = [PSCustomObject]@{
            ExitCode = 2
            Output = [string]$_.Exception.Message
            Command = $commandText
        }
    }
    $reportPath = if ($DryRun) { "" } else { Resolve-FeaturesBuildReportPathFromText -TextValue ([string]$exec.Output) }
    if ([string]::IsNullOrWhiteSpace($reportPath) -and (-not $DryRun)) {
        $defaultReportPath = Join-Path $resolvedProjectRoot ("data/features/features_" + $FeatureSet + "/_meta/build_report.json")
        if (Test-Path $defaultReportPath) {
            $reportPath = $defaultReportPath
        }
    }
    $reportDoc = if ([string]::IsNullOrWhiteSpace($reportPath)) { @{} } else { Load-JsonOrEmpty -PathValue $reportPath }
    $rowsFinal = [int](To-Int64 (Get-PropValue -ObjectValue $reportDoc -Name "rows_final" -DefaultValue 0) 0)
    $minRows = [int](To-Int64 (Get-PropValue -ObjectValue $reportDoc -Name "min_rows_for_train" -DefaultValue 0) 0)
    $status = [string](Get-PropValue -ObjectValue $reportDoc -Name "status" -DefaultValue "")
    $errorMessage = [string](Get-PropValue -ObjectValue $reportDoc -Name "error_message" -DefaultValue "")
    if (($minRows -le 0) -and (-not [string]::IsNullOrWhiteSpace($errorMessage))) {
        $minRowsMatch = [Regex]::Match($errorMessage, 'min_rows_for_train=(\d+)')
        if ($minRowsMatch.Success) {
            $minRows = [int](To-Int64 ([string]$minRowsMatch.Groups[1].Value) 0)
        }
    }
    $usable = ($exec.ExitCode -eq 0) -or (
        (-not [string]::IsNullOrWhiteSpace($status)) `
        -and ($status -eq "PASS") `
        -and ($rowsFinal -ge $minRows)
    )
    return [PSCustomObject]@{
        Exec = $exec
        ReportPath = $reportPath
        Report = $reportDoc
        RowsFinal = $rowsFinal
        MinRowsForTrain = $minRows
        Status = $status
        ErrorMessage = $errorMessage
        Usable = [bool]$usable
        SourceMode = "mutable_features_build"
    }
}

function Invoke-FeaturesValidateAndLoadReport {
    param(
        [string]$PythonPath,
        [string]$StartDate,
        [string]$EndDate
    )
    $args = @(
        "-m", "autobot.cli",
        "features", "validate",
        "--feature-set", $FeatureSet,
        "--tf", $Tf,
        "--quote", $Quote,
        "--top-n", $TrainTopN,
        "--start", $StartDate,
        "--end", $EndDate
    )
    $commandText = ($PythonPath + " " + (($args | ForEach-Object { [string]$_ }) -join " "))
    $exec = $null
    try {
        $exec = Invoke-CommandCapture -Exe $PythonPath -ArgList $args -AllowFailure
    } catch {
        $exec = [PSCustomObject]@{
            ExitCode = 2
            Output = [string]$_.Exception.Message
            Command = $commandText
        }
    }
    $reportPath = if ($DryRun) { "" } else { Resolve-FeaturesValidateReportPathFromText -TextValue ([string]$exec.Output) }
    if ([string]::IsNullOrWhiteSpace($reportPath) -and (-not $DryRun)) {
        $defaultReportPath = Join-Path $resolvedProjectRoot ("data/features/features_" + $FeatureSet + "/_meta/validate_report.json")
        if (Test-Path $defaultReportPath) {
            $reportPath = $defaultReportPath
        }
    }
    $reportDoc = if ([string]::IsNullOrWhiteSpace($reportPath)) { @{} } else { Load-JsonOrEmpty -PathValue $reportPath }
    $checkedFiles = [int](To-Int64 (Get-PropValue -ObjectValue $reportDoc -Name "checked_files" -DefaultValue 0) 0)
    $okFiles = [int](To-Int64 (Get-PropValue -ObjectValue $reportDoc -Name "ok_files" -DefaultValue 0) 0)
    $warnFiles = [int](To-Int64 (Get-PropValue -ObjectValue $reportDoc -Name "warn_files" -DefaultValue 0) 0)
    $failFiles = [int](To-Int64 (Get-PropValue -ObjectValue $reportDoc -Name "fail_files" -DefaultValue 0) 0)
    $schemaOk = To-Bool (Get-PropValue -ObjectValue $reportDoc -Name "schema_ok" -DefaultValue $false) $false
    $leakageSmoke = [string](Get-PropValue -ObjectValue $reportDoc -Name "leakage_smoke" -DefaultValue "")
    $usable = ($exec.ExitCode -eq 0) -or (
        ($checkedFiles -gt 0) `
        -and ($failFiles -eq 0) `
        -and ($schemaOk) `
        -and ($leakageSmoke -eq "PASS")
    )
    return [PSCustomObject]@{
        Exec = $exec
        ReportPath = $reportPath
        Report = $reportDoc
        CheckedFiles = $checkedFiles
        OkFiles = $okFiles
        WarnFiles = $warnFiles
        FailFiles = $failFiles
        SchemaOk = [bool]$schemaOk
        LeakageSmoke = $leakageSmoke
        Usable = [bool]$usable
    }
}

function Invoke-DataContractRegistryAndLoadReport {
    param([string]$PythonPath)
    $args = @(
        "-m", "autobot.ops.data_contract_registry",
        "--project-root", $resolvedProjectRoot
    )
    $commandText = ($PythonPath + " " + (($args | ForEach-Object { [string]$_ }) -join " "))
    $exec = $null
    try {
        $exec = Invoke-CommandCapture -Exe $PythonPath -ArgList $args -AllowFailure
    } catch {
        $exec = [PSCustomObject]@{
            ExitCode = 2
            Output = [string]$_.Exception.Message
            Command = $commandText
        }
    }
    $registryPath = if ($DryRun) { "" } else { Resolve-DataContractRegistryPathFromText -TextValue ([string]$exec.Output) }
    if ([string]::IsNullOrWhiteSpace($registryPath) -and (-not $DryRun)) {
        $defaultRegistryPath = Join-Path $resolvedProjectRoot "data/_meta/data_contract_registry.json"
        if (Test-Path $defaultRegistryPath) {
            $registryPath = $defaultRegistryPath
        }
    }
    $registryDoc = if ([string]::IsNullOrWhiteSpace($registryPath)) { @{} } else { Load-JsonOrEmpty -PathValue $registryPath }
    $entries = @(Get-PropValue -ObjectValue $registryDoc -Name "entries" -DefaultValue @())
    $entryCount = $entries.Count
    $summary = Get-PropValue -ObjectValue $registryDoc -Name "summary" -DefaultValue @{}
    $contractCount = [int](To-Int64 (Get-PropValue -ObjectValue $summary -Name "contract_count" -DefaultValue 0) 0)
    if ($contractCount -le 0) {
        $contractCount = [int](To-Int64 $entryCount 0)
    }
    $usable = ($exec.ExitCode -eq 0) -and (-not [string]::IsNullOrWhiteSpace($registryPath)) -and ($contractCount -gt 0)
    return [PSCustomObject]@{
        Exec = $exec
        RegistryPath = $registryPath
        Registry = $registryDoc
        ContractCount = $contractCount
        Usable = [bool]$usable
    }
}

function Invoke-LiveFeatureParityAndLoadReport {
    param([string]$PythonPath)
    $args = @(
        "-m", "autobot.ops.live_feature_parity_report",
        "--project-root", $resolvedProjectRoot,
        "--feature-set", $FeatureSet,
        "--tf", $Tf,
        "--quote", $Quote,
        "--top-n", ([string]([Math]::Max([int]$FeatureParityTopN, 1)))
    )
    $commandText = ($PythonPath + " " + (($args | ForEach-Object { [string]$_ }) -join " "))
    $exec = $null
    try {
        $exec = Invoke-CommandCapture -Exe $PythonPath -ArgList $args -AllowFailure
    } catch {
        $exec = [PSCustomObject]@{
            ExitCode = 2
            Output = [string]$_.Exception.Message
            Command = $commandText
        }
    }
    $reportPath = if ($DryRun) { "" } else { Resolve-LiveFeatureParityReportPathFromText -TextValue ([string]$exec.Output) }
    if ([string]::IsNullOrWhiteSpace($reportPath) -and (-not $DryRun)) {
        $defaultReportPath = Join-Path $resolvedProjectRoot ("data/features/features_" + $FeatureSet + "/_meta/live_feature_parity_report.json")
        if (Test-Path $defaultReportPath) {
            $reportPath = $defaultReportPath
        }
    }
    $reportDoc = if ([string]::IsNullOrWhiteSpace($reportPath)) { @{} } else { Load-JsonOrEmpty -PathValue $reportPath }
    $sampledPairs = [int](To-Int64 (Get-PropValue -ObjectValue $reportDoc -Name "sampled_pairs" -DefaultValue 0) 0)
    $comparedPairs = [int](To-Int64 (Get-PropValue -ObjectValue $reportDoc -Name "compared_pairs" -DefaultValue 0) 0)
    $passingPairs = [int](To-Int64 (Get-PropValue -ObjectValue $reportDoc -Name "passing_pairs" -DefaultValue 0) 0)
    $acceptable = To-Bool (Get-PropValue -ObjectValue $reportDoc -Name "acceptable" -DefaultValue $false) $false
    $status = [string](Get-PropValue -ObjectValue $reportDoc -Name "status" -DefaultValue "")
    $usable = ($exec.ExitCode -eq 0) -and (-not [string]::IsNullOrWhiteSpace($reportPath)) -and ($sampledPairs -gt 0) -and ($acceptable) -and ($status -eq "PASS")
    return [PSCustomObject]@{
        Exec = $exec
        ReportPath = $reportPath
        Report = $reportDoc
        SampledPairs = $sampledPairs
        ComparedPairs = $comparedPairs
        PassingPairs = $passingPairs
        Acceptable = [bool]$acceptable
        Status = $status
        Usable = [bool]$usable
    }
}

function Invoke-FeatureDatasetCertificationAndLoadReport {
    param([string]$PythonPath)
    $args = @(
        "-m", "autobot.ops.feature_dataset_certification",
        "--project-root", $resolvedProjectRoot,
        "--feature-set", $FeatureSet
    )
    $commandText = ($PythonPath + " " + (($args | ForEach-Object { [string]$_ }) -join " "))
    $exec = $null
    try {
        $exec = Invoke-CommandCapture -Exe $PythonPath -ArgList $args -AllowFailure
    } catch {
        $exec = [PSCustomObject]@{
            ExitCode = 2
            Output = [string]$_.Exception.Message
            Command = $commandText
        }
    }
    $reportPath = if ($DryRun) { "" } else { Resolve-FeatureDatasetCertificationPathFromText -TextValue ([string]$exec.Output) }
    if ([string]::IsNullOrWhiteSpace($reportPath) -and (-not $DryRun)) {
        $defaultReportPath = Join-Path $resolvedProjectRoot ("data/features/features_" + $FeatureSet + "/_meta/feature_dataset_certification.json")
        if (Test-Path $defaultReportPath) {
            $reportPath = $defaultReportPath
        }
    }
    $reportDoc = if ([string]::IsNullOrWhiteSpace($reportPath)) { @{} } else { Load-JsonOrEmpty -PathValue $reportPath }
    $status = [string](Get-PropValue -ObjectValue $reportDoc -Name "status" -DefaultValue "")
    $pass = To-Bool (Get-PropValue -ObjectValue $reportDoc -Name "pass" -DefaultValue $false) $false
    $reasons = @((Get-PropValue -ObjectValue $reportDoc -Name "reasons" -DefaultValue @()))
    $usable = ($exec.ExitCode -eq 0) -and (-not [string]::IsNullOrWhiteSpace($reportPath)) -and ($pass) -and ($status -eq "PASS")
    return [PSCustomObject]@{
        Exec = $exec
        ReportPath = $reportPath
        Report = $reportDoc
        Status = $status
        Pass = [bool]$pass
        Reasons = @($reasons)
        Usable = [bool]$usable
    }
}

function Invoke-PrivateExecutionLabelStoreAndLoadReport {
    param([string]$PythonPath)
    $args = @(
        "-m", "autobot.ops.private_execution_label_store",
        "--project-root", $resolvedProjectRoot
    )
    $commandText = ($PythonPath + " " + (($args | ForEach-Object { [string]$_ }) -join " "))
    $exec = $null
    try {
        $exec = Invoke-CommandCapture -Exe $PythonPath -ArgList $args -AllowFailure
    } catch {
        $exec = [PSCustomObject]@{
            ExitCode = 2
            Output = [string]$_.Exception.Message
            Command = $commandText
        }
    }
    $buildReportPath = if ($DryRun) { "" } else { Resolve-PrivateExecutionLabelStoreBuildPathFromText -TextValue ([string]$exec.Output) }
    if ([string]::IsNullOrWhiteSpace($buildReportPath) -and (-not $DryRun)) {
        $defaultBuildReportPath = Join-Path $resolvedProjectRoot "data/parquet/private_execution_v1/_meta/build_report.json"
        if (Test-Path $defaultBuildReportPath) {
            $buildReportPath = $defaultBuildReportPath
        }
    }
    $buildReport = if ([string]::IsNullOrWhiteSpace($buildReportPath)) { @{} } else { Load-JsonOrEmpty -PathValue $buildReportPath }
    $validateReportPath = if ([string]::IsNullOrWhiteSpace($buildReportPath)) { "" } else { Join-Path (Split-Path -Parent $buildReportPath) "validate_report.json" }
    $validateReport = if ([string]::IsNullOrWhiteSpace($validateReportPath)) { @{} } else { Load-JsonOrEmpty -PathValue $validateReportPath }
    $rowsWrittenTotal = [int](To-Int64 (Get-PropValue -ObjectValue $buildReport -Name "rows_written_total" -DefaultValue 0) 0)
    $status = [string](Get-PropValue -ObjectValue $validateReport -Name "status" -DefaultValue "")
    $pass = To-Bool (Get-PropValue -ObjectValue $validateReport -Name "pass" -DefaultValue $false) $false
    $reasons = @((Get-PropValue -ObjectValue $validateReport -Name "reasons" -DefaultValue @()))
    $usable = ($exec.ExitCode -eq 0) -and (-not [string]::IsNullOrWhiteSpace($buildReportPath)) -and ($rowsWrittenTotal -gt 0) -and ($pass) -and ($status -eq "PASS")
    return [PSCustomObject]@{
        Exec = $exec
        BuildReportPath = $buildReportPath
        BuildReport = $buildReport
        ValidateReportPath = $validateReportPath
        ValidateReport = $validateReport
        RowsWrittenTotal = [int]$rowsWrittenTotal
        Status = $status
        Pass = [bool]$pass
        Reasons = @($reasons)
        Usable = [bool]$usable
    }
}

function Resolve-TrainWindowByTrainableRows {
    param(
        [string]$PythonPath,
        [string]$InitialTrainStartDate,
        [string]$TrainEndDate
    )
    $attempts = @()
    $bestAttempt = $null
    foreach ($candidateStartDate in (Get-DateRangeAscending -StartDate $InitialTrainStartDate -EndDate $TrainEndDate)) {
        $probe = Invoke-FeaturesBuildAndLoadReport `
            -PythonPath $PythonPath `
            -StartDate $candidateStartDate `
            -EndDate $TrainEndDate
        $attempt = New-TrainabilityAttemptRecord -Probe $probe -StartDate $candidateStartDate -EndDate $TrainEndDate
        $attempts += $attempt
        if (($null -eq $bestAttempt) -or ([int]$attempt.rows_final -gt [int](Get-PropValue -ObjectValue $bestAttempt -Name "rows_final" -DefaultValue 0))) {
            $bestAttempt = $attempt
        }
        if ($probe.Usable) {
            return [ordered]@{
                status = "PASS"
                selected_start = $candidateStartDate
                selected_end = $TrainEndDate
                attempts = @($attempts)
                best_attempt = $bestAttempt
                features_build = $attempt
            }
        }
    }
    return [ordered]@{
        status = "INSUFFICIENT_TRAINABLE_ROWS"
        selected_start = ""
        selected_end = $TrainEndDate
        attempts = @($attempts)
        best_attempt = $bestAttempt
        features_build = $null
    }
}

function Resolve-RunDirFromText {
    param([string]$TextValue)
    if ([string]::IsNullOrWhiteSpace($TextValue)) {
        return ""
    }

    $jsonMatch = [Regex]::Match($TextValue, '(?ms)"run_dir"\s*:\s*"((?:\\.|[^"])*)"')
    if ($jsonMatch.Success) {
        $encodedPath = [string]$jsonMatch.Groups[1].Value
        try {
            return [string](('"' + $encodedPath + '"') | ConvertFrom-Json)
        } catch {
        }
    }

    $lineMatch = [Regex]::Match($TextValue, '(?m)^(?:\[[^\]]+\])+\s+run_dir=(.+)$')
    if ($lineMatch.Success) {
        return ([string]$lineMatch.Groups[1].Value).Trim()
    }

    $plainMatch = [Regex]::Match($TextValue, '(?m)^run_dir=(.+)$')
    if ($plainMatch.Success) {
        return ([string]$plainMatch.Groups[1].Value).Trim()
    }
    return ""
}

try {
    if (-not $SkipDailyPipeline) {
        $report.steps.daily_pipeline = [ordered]@{
            attempted = $false
            reason = "REPLACED_BY_TRAIN_SNAPSHOT_CLOSE"
            script_path = $resolvedDailyPipelineScript
        }
        $report.steps.window_ramp_recomputed_after_pipeline = [ordered]@{
            attempted = $false
            reason = "REPLACED_BY_TRAIN_SNAPSHOT_CLOSE"
        }
    } else {
        $report.steps.daily_pipeline = [ordered]@{ attempted = $false; reason = "SKIPPED_BY_FLAG"; script_path = $resolvedDailyPipelineScript }
        $report.steps.window_ramp_recomputed_after_pipeline = [ordered]@{ attempted = $false; reason = "SKIPPED_BY_FLAG" }
    }

    if ($SplitPolicyHistoricalSelectorEnabled) {
        $splitPolicyResolution = Resolve-SplitPolicySelection `
            -PythonPath $resolvedPythonExe `
            -Root $resolvedProjectRoot `
            -RegistryRoot $resolvedRegistryRoot `
            -BatchDateValue $effectiveBatchDate `
            -QualityFloorDate $script:bootstrapWindowStart
        $script:splitPolicyId = [string](Get-PropValue -ObjectValue $splitPolicyResolution -Name "policy_id" -DefaultValue $script:splitPolicyId)
        $script:splitPolicyLaneMode = [string](Get-PropValue -ObjectValue $splitPolicyResolution -Name "lane_mode" -DefaultValue "bootstrap_latest_inclusive")
        $script:splitPolicyPromotionEligible = [bool](Get-PropValue -ObjectValue $splitPolicyResolution -Name "promotion_eligible" -DefaultValue $false)
        $script:splitPolicySelectedBy = [string](Get-PropValue -ObjectValue $splitPolicyResolution -Name "selected_by" -DefaultValue "")
        $script:splitPolicySelectedHoldoutDays = [int](To-Int64 (Get-PropValue -ObjectValue $splitPolicyResolution -Name "selected_holdout_days" -DefaultValue 0) 0)
        $script:splitPolicyReasonCodes = @((Get-PropValue -ObjectValue $splitPolicyResolution -Name "reason_codes" -DefaultValue @()))
        $script:splitPolicyCandidateHoldoutDays = @((Get-PropValue -ObjectValue $splitPolicyResolution -Name "candidate_holdout_days" -DefaultValue @()))
        $script:splitPolicyHistoricalAnchorCount = [int](To-Int64 (Get-PropValue -ObjectValue $splitPolicyResolution -Name "historical_anchor_count" -DefaultValue 0) 0)
        $script:splitPolicySelectionSummary = @((Get-PropValue -ObjectValue $splitPolicyResolution -Name "selection_summary" -DefaultValue @()))
        $script:splitPolicyHistoryPath = [string](Get-PropValue -ObjectValue $splitPolicyResolution -Name "history_path" -DefaultValue "")
        $script:splitPolicyNewEvaluations = @((Get-PropValue -ObjectValue $splitPolicyResolution -Name "new_evaluations" -DefaultValue @()))
        $script:splitPolicyBootstrapTrainability = Get-PropValue -ObjectValue $splitPolicyResolution -Name "bootstrap_attempt" -DefaultValue @{}
        $report.steps.split_policy_selector = [ordered]@{
            attempted = $true
            policy_id = $script:splitPolicyId
            lane_mode = $script:splitPolicyLaneMode
            promotion_eligible = $script:splitPolicyPromotionEligible
            selected_by = $script:splitPolicySelectedBy
            selected_holdout_days = $script:splitPolicySelectedHoldoutDays
            candidate_holdout_days = @($script:splitPolicyCandidateHoldoutDays)
            historical_anchor_count = $script:splitPolicyHistoricalAnchorCount
            history_path = $script:splitPolicyHistoryPath
            new_evaluation_count = @($script:splitPolicyNewEvaluations).Count
            reason_codes = @($script:splitPolicyReasonCodes)
            selection_summary = @($script:splitPolicySelectionSummary)
        }
        if ($script:splitPolicyLaneMode -eq "promotion_strict") {
            $selectedSummary = Get-PropValue -ObjectValue $splitPolicyResolution -Name "selected_summary" -DefaultValue @{}
            $selectedWindows = Get-PropValue -ObjectValue $splitPolicyResolution -Name "current_windows" -DefaultValue @{}
            $selectedHistoricalAttempt = Get-PropValue -ObjectValue $selectedSummary -Name "current_trainability" -DefaultValue @{}
            $script:splitPolicyStrictTrainability = $selectedSummary
            $script:certificationStartDate = [string](Get-PropValue -ObjectValue $selectedWindows -Name "certification_start" -DefaultValue $certificationStartDate)
            $script:trainStartDate = [string](Get-PropValue -ObjectValue $selectedWindows -Name "train_start" -DefaultValue $trainStartDate)
            $script:trainEndDate = [string](Get-PropValue -ObjectValue $selectedWindows -Name "train_end" -DefaultValue $trainEndDate)
            $certificationStartDate = $script:certificationStartDate
            $trainStartDate = $script:trainStartDate
            $trainEndDate = $script:trainEndDate
            $selectedHoldoutProbe = Invoke-FeaturesBuildAndLoadReport `
                -PythonPath $resolvedPythonExe `
                -StartDate $trainStartDate `
                -EndDate $trainEndDate
            $selectedAttempt = New-TrainabilityAttemptRecord `
                -Probe $selectedHoldoutProbe `
                -StartDate $trainStartDate `
                -EndDate $trainEndDate
            if (-not $selectedHoldoutProbe.Usable) {
                $report.steps.features_build = [ordered]@{
                    attempted = $true
                    feature_set = $FeatureSet
                    label_set = $LabelSet
                    start = $trainStartDate
                    end = $trainEndDate
                    resolution_status = "SELECTED_HOLDOUT_REBUILD_FAILED"
                    selected_holdout_days = $script:splitPolicySelectedHoldoutDays
                    selection_method = "forward_validation_lcb"
                    current_trainability = $selectedAttempt
                    historical_trainability = $selectedHistoricalAttempt
                }
                $report.reasons = @("INSUFFICIENT_TRAINABLE_V4_ROWS")
                $report.gates.overall_pass = $false
                Sync-SplitPolicyState
                $paths = Save-Report
                Write-ReportPointers -LogTag $LogTag -Paths $paths -OverallPass $false
                exit 2
            }
            $report.steps.features_build = [ordered]@{
                attempted = $true
                feature_set = $FeatureSet
                label_set = $LabelSet
                start = $trainStartDate
                end = $trainEndDate
                resolution_status = "PASS"
                selected_holdout_days = $script:splitPolicySelectedHoldoutDays
                selection_method = "forward_validation_lcb"
                current_trainability = $selectedAttempt
                historical_trainability = $selectedHistoricalAttempt
                selected_holdout_rebuild = $selectedAttempt
            }
            $report.steps.features_build.exit_code = [int](Get-PropValue -ObjectValue $selectedAttempt -Name "exit_code" -DefaultValue 0)
            $report.steps.features_build.command = [string](Get-PropValue -ObjectValue $selectedAttempt -Name "command" -DefaultValue "")
            $report.steps.features_build.output_preview = [string](Get-PropValue -ObjectValue $selectedAttempt -Name "output_preview" -DefaultValue "")
            $report.steps.features_build.report_path = [string](Get-PropValue -ObjectValue $selectedAttempt -Name "report_path" -DefaultValue "")
            $report.steps.features_build.rows_final = [int](Get-PropValue -ObjectValue $selectedAttempt -Name "rows_final" -DefaultValue 0)
            $report.steps.features_build.min_rows_for_train = [int](Get-PropValue -ObjectValue $selectedAttempt -Name "min_rows_for_train" -DefaultValue 0)
            $report.steps.features_build.effective_start = [string](Get-PropValue -ObjectValue $selectedAttempt -Name "effective_start" -DefaultValue "")
            $report.steps.features_build.effective_end = [string](Get-PropValue -ObjectValue $selectedAttempt -Name "effective_end" -DefaultValue "")
            $report.windows_by_step.train = [ordered]@{ start = $trainStartDate; end = $trainEndDate }
            $report.windows_by_step.research = [ordered]@{ start = $trainStartDate; end = $trainEndDate; source = "train_command_window" }
            $report.windows_by_step.certification = [ordered]@{ start = $certificationStartDate; end = $effectiveBatchDate }
            $report.windows_by_step.backtest = [ordered]@{ start = $certificationStartDate; end = $effectiveBatchDate; alias_of = "certification" }
            Sync-SplitPolicyState
        } else {
            $bootstrapAttempt = Get-PropValue -ObjectValue $splitPolicyResolution -Name "bootstrap_attempt" -DefaultValue @{}
            if (-not (To-Bool (Get-PropValue -ObjectValue $bootstrapAttempt -Name "usable" -DefaultValue $false) $false)) {
                $report.steps.features_build = [ordered]@{
                    attempted = $true
                    feature_set = $FeatureSet
                    label_set = $LabelSet
                    start = $script:bootstrapWindowStart
                    end = $script:bootstrapWindowEnd
                    resolution_status = "INSUFFICIENT_TRAINABLE_ROWS"
                    bootstrap_attempt = $bootstrapAttempt
                }
                $report.steps.train = [ordered]@{
                    attempted = $false
                    reason = "INSUFFICIENT_TRAINABLE_V4_ROWS"
                    start = $script:bootstrapWindowStart
                    end = $script:bootstrapWindowEnd
                }
                Sync-SplitPolicyState
                $report.reasons = @("INSUFFICIENT_TRAINABLE_V4_ROWS")
                $report.gates.overall_pass = $false
                $paths = Save-Report
                Write-ReportPointers -LogTag $LogTag -Paths $paths -OverallPass $false
                exit 2
            }
            $trainStartDate = $script:bootstrapWindowStart
            $trainEndDate = $script:bootstrapWindowEnd
            $certificationStartDate = ""
            $report.steps.features_build = [ordered]@{
                attempted = $true
                feature_set = $FeatureSet
                label_set = $LabelSet
                start = $trainStartDate
                end = $trainEndDate
                resolution_status = "BOOTSTRAP_ONLY_POLICY"
                selected_holdout_days = 0
                selection_method = [string](Get-PropValue -ObjectValue $splitPolicyResolution -Name "selected_by" -DefaultValue "")
                bootstrap_attempt = $bootstrapAttempt
            }
            $report.steps.features_build.exit_code = [int](Get-PropValue -ObjectValue $bootstrapAttempt -Name "exit_code" -DefaultValue 0)
            $report.steps.features_build.command = [string](Get-PropValue -ObjectValue $bootstrapAttempt -Name "command" -DefaultValue "")
            $report.steps.features_build.output_preview = [string](Get-PropValue -ObjectValue $bootstrapAttempt -Name "output_preview" -DefaultValue "")
            $report.steps.features_build.report_path = [string](Get-PropValue -ObjectValue $bootstrapAttempt -Name "report_path" -DefaultValue "")
            $report.steps.features_build.rows_final = [int](Get-PropValue -ObjectValue $bootstrapAttempt -Name "rows_final" -DefaultValue 0)
            $report.steps.features_build.min_rows_for_train = [int](Get-PropValue -ObjectValue $bootstrapAttempt -Name "min_rows_for_train" -DefaultValue 0)
            $report.steps.features_build.effective_start = [string](Get-PropValue -ObjectValue $bootstrapAttempt -Name "effective_start" -DefaultValue "")
            $report.steps.features_build.effective_end = [string](Get-PropValue -ObjectValue $bootstrapAttempt -Name "effective_end" -DefaultValue "")
            $report.windows_by_step.train = [ordered]@{ start = $trainStartDate; end = $trainEndDate }
            $report.windows_by_step.research = [ordered]@{ start = $trainStartDate; end = $trainEndDate; source = "bootstrap_latest_inclusive_fallback" }
            $report.windows_by_step.bootstrap = [ordered]@{ start = $trainStartDate; end = $trainEndDate; source = "bootstrap_latest_inclusive_fallback" }
            $report.windows_by_step.certification = [ordered]@{ start = ""; end = ""; source = "not_applicable_bootstrap_lane" }
            $report.windows_by_step.backtest = [ordered]@{ start = ""; end = ""; alias_of = "certification" }
            Sync-SplitPolicyState
        }
    } else {
        if (
            (To-Bool (Get-PropValue -ObjectValue $windowRamp -Name "enabled" -DefaultValue $false) $false) `
            -and (To-Bool (Get-PropValue -ObjectValue $windowRamp -Name "micro_coverage_present" -DefaultValue $false) $false) `
            -and (-not (To-Bool (Get-PropValue -ObjectValue $windowRamp -Name "comparable_window_available" -DefaultValue $false) $false))
        ) {
            $windowRampReason = [string](Get-PropValue -ObjectValue $windowRamp -Name "reason" -DefaultValue "INSUFFICIENT_CONTIGUOUS_MICRO_HISTORY")
            if ([string]::IsNullOrWhiteSpace($windowRampReason)) {
                $windowRampReason = "INSUFFICIENT_CONTIGUOUS_MICRO_HISTORY"
            }
            $report.reasons = @($windowRampReason)
            $report.gates.overall_pass = $false
            $report.steps.features_build = [ordered]@{
                attempted = $false
                reason = $windowRampReason
            }
            $report.steps.train = [ordered]@{
                attempted = $false
                reason = $windowRampReason
                start = $trainStartDate
                end = $trainEndDate
            }
            $paths = Save-Report
            Write-ReportPointers -LogTag $LogTag -Paths $paths -OverallPass $false
            exit 2
        }

        if (-not $SkipDailyPipeline) {
            $trainabilityResolution = Resolve-TrainWindowByTrainableRows `
                -PythonPath $resolvedPythonExe `
                -InitialTrainStartDate $trainStartDate `
                -TrainEndDate $trainEndDate
        $script:splitPolicyStrictTrainability = [ordered]@{
            status = [string](Get-PropValue -ObjectValue $trainabilityResolution -Name "status" -DefaultValue "")
            selected_start = [string](Get-PropValue -ObjectValue $trainabilityResolution -Name "selected_start" -DefaultValue "")
            selected_end = [string](Get-PropValue -ObjectValue $trainabilityResolution -Name "selected_end" -DefaultValue "")
            best_attempt = Get-PropValue -ObjectValue $trainabilityResolution -Name "best_attempt" -DefaultValue @{}
            attempts = @((Get-PropValue -ObjectValue $trainabilityResolution -Name "attempts" -DefaultValue @()))
        }
        $report.steps.features_build = [ordered]@{
            attempted = $true
            feature_set = $FeatureSet
            label_set = $LabelSet
            start = [string](Get-PropValue -ObjectValue $trainabilityResolution -Name "selected_start" -DefaultValue $trainStartDate)
            end = $trainEndDate
            resolution_status = [string](Get-PropValue -ObjectValue $trainabilityResolution -Name "status" -DefaultValue "")
            best_attempt = Get-PropValue -ObjectValue $trainabilityResolution -Name "best_attempt" -DefaultValue @{}
            attempts = @((Get-PropValue -ObjectValue $trainabilityResolution -Name "attempts" -DefaultValue @()))
        }
        if ([string](Get-PropValue -ObjectValue $trainabilityResolution -Name "status" -DefaultValue "") -eq "PASS") {
            $script:splitPolicyLaneMode = "promotion_strict"
            $script:splitPolicyPromotionEligible = $true
            $script:splitPolicySelectedBy = "strict_trainability"
            $script:splitPolicySelectedHoldoutDays = [int]$BacktestLookbackDays
            $script:splitPolicyReasonCodes = @()
            $resolvedFeatureWindow = Get-PropValue -ObjectValue $trainabilityResolution -Name "features_build" -DefaultValue @{}
            $trainStartDate = [string](Get-PropValue -ObjectValue $trainabilityResolution -Name "selected_start" -DefaultValue $trainStartDate)
            $report.steps.features_build.exit_code = [int](Get-PropValue -ObjectValue $resolvedFeatureWindow -Name "exit_code" -DefaultValue 0)
            $report.steps.features_build.command = [string](Get-PropValue -ObjectValue $resolvedFeatureWindow -Name "command" -DefaultValue "")
            $report.steps.features_build.output_preview = [string](Get-PropValue -ObjectValue $resolvedFeatureWindow -Name "output_preview" -DefaultValue "")
            $report.steps.features_build.report_path = [string](Get-PropValue -ObjectValue $resolvedFeatureWindow -Name "report_path" -DefaultValue "")
            $report.steps.features_build.rows_final = [int](Get-PropValue -ObjectValue $resolvedFeatureWindow -Name "rows_final" -DefaultValue 0)
            $report.steps.features_build.min_rows_for_train = [int](Get-PropValue -ObjectValue $resolvedFeatureWindow -Name "min_rows_for_train" -DefaultValue 0)
            $report.steps.features_build.effective_start = [string](Get-PropValue -ObjectValue $resolvedFeatureWindow -Name "effective_start" -DefaultValue "")
            $report.steps.features_build.effective_end = [string](Get-PropValue -ObjectValue $resolvedFeatureWindow -Name "effective_end" -DefaultValue "")
            $report.windows_by_step.train = [ordered]@{ start = $trainStartDate; end = $trainEndDate }
            $report.windows_by_step.research = [ordered]@{ start = $trainStartDate; end = $trainEndDate; source = "train_command_window" }
            Sync-SplitPolicyState
        } else {
            $bestTrainabilityAttempt = Get-PropValue -ObjectValue $trainabilityResolution -Name "best_attempt" -DefaultValue @{}
            $bootstrapResolved = $false
            if ((-not [string]::IsNullOrWhiteSpace($script:bootstrapWindowStart)) -and ($script:bootstrapWindowEnd -ge $script:bootstrapWindowStart)) {
                $bootstrapProbe = Invoke-FeaturesBuildAndLoadReport `
                    -PythonPath $resolvedPythonExe `
                    -StartDate $script:bootstrapWindowStart `
                    -EndDate $script:bootstrapWindowEnd
                $bootstrapAttempt = New-TrainabilityAttemptRecord `
                    -Probe $bootstrapProbe `
                    -StartDate $script:bootstrapWindowStart `
                    -EndDate $script:bootstrapWindowEnd
                $script:splitPolicyBootstrapTrainability = [ordered]@{
                    status = if ($bootstrapProbe.Usable) { "PASS" } else { "INSUFFICIENT_TRAINABLE_ROWS" }
                    start = $script:bootstrapWindowStart
                    end = $script:bootstrapWindowEnd
                    attempt = $bootstrapAttempt
                }
                $report.steps.features_build.strict_resolution_status = [string](Get-PropValue -ObjectValue $trainabilityResolution -Name "status" -DefaultValue "")
                $report.steps.features_build.strict_best_attempt = $bestTrainabilityAttempt
                $report.steps.features_build.strict_attempts = @((Get-PropValue -ObjectValue $trainabilityResolution -Name "attempts" -DefaultValue @()))
                $report.steps.features_build.bootstrap_attempt = $bootstrapAttempt
                if ($bootstrapProbe.Usable) {
                    $bootstrapResolved = $true
                    $script:splitPolicyLaneMode = "bootstrap_latest_inclusive"
                    $script:splitPolicyPromotionEligible = $false
                    $script:splitPolicySelectedBy = "bootstrap_latest_inclusive_fallback_after_strict_trainability_failure"
                    $script:splitPolicySelectedHoldoutDays = 0
                    $script:splitPolicyReasonCodes = @(
                        "BOOTSTRAP_ONLY_POLICY",
                        "STRICT_INSUFFICIENT_TRAINABLE_V4_ROWS"
                    )
                    $trainStartDate = $script:bootstrapWindowStart
                    $trainEndDate = $script:bootstrapWindowEnd
                    $report.steps.features_build.start = $trainStartDate
                    $report.steps.features_build.end = $trainEndDate
                    $report.steps.features_build.resolution_status = "BOOTSTRAP_ONLY_POLICY"
                    $report.steps.features_build.exit_code = [int](Get-PropValue -ObjectValue $bootstrapAttempt -Name "exit_code" -DefaultValue 0)
                    $report.steps.features_build.command = [string](Get-PropValue -ObjectValue $bootstrapAttempt -Name "command" -DefaultValue "")
                    $report.steps.features_build.output_preview = [string](Get-PropValue -ObjectValue $bootstrapAttempt -Name "output_preview" -DefaultValue "")
                    $report.steps.features_build.report_path = [string](Get-PropValue -ObjectValue $bootstrapAttempt -Name "report_path" -DefaultValue "")
                    $report.steps.features_build.rows_final = [int](Get-PropValue -ObjectValue $bootstrapAttempt -Name "rows_final" -DefaultValue 0)
                    $report.steps.features_build.min_rows_for_train = [int](Get-PropValue -ObjectValue $bootstrapAttempt -Name "min_rows_for_train" -DefaultValue 0)
                    $report.steps.features_build.effective_start = [string](Get-PropValue -ObjectValue $bootstrapAttempt -Name "effective_start" -DefaultValue "")
                    $report.steps.features_build.effective_end = [string](Get-PropValue -ObjectValue $bootstrapAttempt -Name "effective_end" -DefaultValue "")
                    $report.windows_by_step.train = [ordered]@{ start = $trainStartDate; end = $trainEndDate }
                    $report.windows_by_step.research = [ordered]@{ start = $trainStartDate; end = $trainEndDate; source = "bootstrap_latest_inclusive_fallback" }
                    $report.windows_by_step.bootstrap = [ordered]@{ start = $trainStartDate; end = $trainEndDate; source = "bootstrap_latest_inclusive_fallback" }
                    Sync-SplitPolicyState
                }
            }
            if (-not $bootstrapResolved) {
                $script:splitPolicyLaneMode = "promotion_strict"
                $script:splitPolicyPromotionEligible = $false
                $script:splitPolicySelectedBy = "strict_trainability_failed"
                $script:splitPolicySelectedHoldoutDays = [int]$BacktestLookbackDays
                $script:splitPolicyReasonCodes = @("INSUFFICIENT_TRAINABLE_V4_ROWS")
                Sync-SplitPolicyState
                $report.steps.train = [ordered]@{
                    attempted = $false
                    reason = "INSUFFICIENT_TRAINABLE_V4_ROWS"
                    start = $trainStartDate
                    end = $trainEndDate
                    best_attempt = $bestTrainabilityAttempt
                }
                $report.reasons = @("INSUFFICIENT_TRAINABLE_V4_ROWS")
                $report.gates.overall_pass = $false
                $paths = Save-Report
                Write-ReportPointers -LogTag $LogTag -Paths $paths -OverallPass $false
                exit 2
            }
        }
        } else {
            $report.steps.features_build = [ordered]@{ attempted = $false; reason = "SKIPPED_BY_FLAG" }
        }
    }

    $dataContractRegistryAttempt = Invoke-DataContractRegistryAndLoadReport -PythonPath $resolvedPythonExe
    $report.steps.data_contract_registry = [ordered]@{
        attempted = $true
        exit_code = [int]$dataContractRegistryAttempt.Exec.ExitCode
        command = $dataContractRegistryAttempt.Exec.Command
        output_preview = (Get-OutputPreview -Text ([string]$dataContractRegistryAttempt.Exec.Output))
        registry_path = [string]$dataContractRegistryAttempt.RegistryPath
        contract_count = [int]$dataContractRegistryAttempt.ContractCount
    }
    if (-not $dataContractRegistryAttempt.Usable) {
        $report.reasons = @("DATA_CONTRACT_REGISTRY_MISSING_OR_FAILED")
        $report.gates.overall_pass = $false
        $paths = Save-Report
        Write-ReportPointers -LogTag $LogTag -Paths $paths -OverallPass $false
        exit 2
    }

    $featuresValidateAttempt = $null
    if ([string]::Equals($FeatureSet, "v4", [System.StringComparison]::OrdinalIgnoreCase)) {
        $featuresValidateAttempt = Invoke-FeaturesValidateAndLoadReport `
            -PythonPath $resolvedPythonExe `
            -StartDate $trainStartDate `
            -EndDate $trainEndDate
        $report.steps.features_validate = [ordered]@{
            attempted = $true
            exit_code = [int]$featuresValidateAttempt.Exec.ExitCode
            command = $featuresValidateAttempt.Exec.Command
            output_preview = (Get-OutputPreview -Text ([string]$featuresValidateAttempt.Exec.Output))
            report_path = [string]$featuresValidateAttempt.ReportPath
            checked_files = [int]$featuresValidateAttempt.CheckedFiles
            ok_files = [int]$featuresValidateAttempt.OkFiles
            warn_files = [int]$featuresValidateAttempt.WarnFiles
            fail_files = [int]$featuresValidateAttempt.FailFiles
            schema_ok = [bool]$featuresValidateAttempt.SchemaOk
            leakage_smoke = [string]$featuresValidateAttempt.LeakageSmoke
        }
        if (-not $featuresValidateAttempt.Usable) {
            $report.reasons = @("FEATURES_VALIDATE_MISSING_OR_FAILED")
            $report.gates.overall_pass = $false
            $paths = Save-Report
            Write-ReportPointers -LogTag $LogTag -Paths $paths -OverallPass $false
            exit 2
        }
    } else {
        $report.steps.features_validate = [ordered]@{
            attempted = $false
            reason = "NOT_REQUIRED_FOR_FEATURE_SET"
        }
    }

    $featureParityAttempt = $null
    if ([string]::Equals($FeatureSet, "v4", [System.StringComparison]::OrdinalIgnoreCase)) {
        $featureParityAttempt = Invoke-LiveFeatureParityAndLoadReport -PythonPath $resolvedPythonExe
        $report.steps.features_live_parity = [ordered]@{
            attempted = $true
            exit_code = [int]$featureParityAttempt.Exec.ExitCode
            command = $featureParityAttempt.Exec.Command
            output_preview = (Get-OutputPreview -Text ([string]$featureParityAttempt.Exec.Output))
            report_path = [string]$featureParityAttempt.ReportPath
            sampled_pairs = [int]$featureParityAttempt.SampledPairs
            compared_pairs = [int]$featureParityAttempt.ComparedPairs
            passing_pairs = [int]$featureParityAttempt.PassingPairs
            acceptable = [bool]$featureParityAttempt.Acceptable
            status = [string]$featureParityAttempt.Status
        }
        if (-not $featureParityAttempt.Usable) {
            $report.steps.train = [ordered]@{
                attempted = $false
                reason = "FEATURE_PARITY_MISSING_OR_FAILED"
                start = $trainStartDate
                end = $trainEndDate
            }
            $report.reasons = @("FEATURE_PARITY_MISSING_OR_FAILED")
            $report.gates.overall_pass = $false
            $paths = Save-Report
            Write-ReportPointers -LogTag $LogTag -Paths $paths -OverallPass $false
            exit 2
        }
    } else {
        $report.steps.features_live_parity = [ordered]@{
            attempted = $false
            reason = "NOT_REQUIRED_FOR_FEATURE_SET"
        }
    }

    $featureDatasetCertificationAttempt = $null
    if ([string]::Equals($FeatureSet, "v4", [System.StringComparison]::OrdinalIgnoreCase)) {
        $featureDatasetCertificationAttempt = Invoke-FeatureDatasetCertificationAndLoadReport -PythonPath $resolvedPythonExe
        $report.steps.feature_dataset_certification = [ordered]@{
            attempted = $true
            exit_code = [int]$featureDatasetCertificationAttempt.Exec.ExitCode
            command = $featureDatasetCertificationAttempt.Exec.Command
            output_preview = (Get-OutputPreview -Text ([string]$featureDatasetCertificationAttempt.Exec.Output))
            report_path = [string]$featureDatasetCertificationAttempt.ReportPath
            status = [string]$featureDatasetCertificationAttempt.Status
            pass = [bool]$featureDatasetCertificationAttempt.Pass
            reasons = @($featureDatasetCertificationAttempt.Reasons)
        }
        if (-not $featureDatasetCertificationAttempt.Usable) {
            $report.steps.train = [ordered]@{
                attempted = $false
                reason = "FEATURE_DATASET_CERTIFICATION_MISSING_OR_FAILED"
                start = $trainStartDate
                end = $trainEndDate
            }
            $report.reasons = @("FEATURE_DATASET_CERTIFICATION_MISSING_OR_FAILED")
            $report.gates.overall_pass = $false
            $paths = Save-Report
            Write-ReportPointers -LogTag $LogTag -Paths $paths -OverallPass $false
            exit 2
        }
    } else {
        $report.steps.feature_dataset_certification = [ordered]@{
            attempted = $false
            reason = "NOT_REQUIRED_FOR_FEATURE_SET"
        }
    }

    $requiresPrivateExecutionDataset = (
        [string]::Equals(([string]$Trainer).Trim(), "v5_fusion", [System.StringComparison]::OrdinalIgnoreCase) `
        -or
        [string]::Equals(([string]$Trainer).Trim(), "v5_tradability", [System.StringComparison]::OrdinalIgnoreCase) `
        -or @($DependencyTrainers | Where-Object { [string]::Equals(([string]$_).Trim(), "v5_tradability", [System.StringComparison]::OrdinalIgnoreCase) }).Count -gt 0
    )
    if ($requiresPrivateExecutionDataset) {
        $privateExecutionAttempt = Invoke-PrivateExecutionLabelStoreAndLoadReport -PythonPath $resolvedPythonExe
        $report.steps.private_execution_label_store = [ordered]@{
            attempted = $true
            exit_code = [int]$privateExecutionAttempt.Exec.ExitCode
            command = $privateExecutionAttempt.Exec.Command
            output_preview = (Get-OutputPreview -Text ([string]$privateExecutionAttempt.Exec.Output))
            build_report_path = [string]$privateExecutionAttempt.BuildReportPath
            validate_report_path = [string]$privateExecutionAttempt.ValidateReportPath
            rows_written_total = [int]$privateExecutionAttempt.RowsWrittenTotal
            status = [string]$privateExecutionAttempt.Status
            pass = [bool]$privateExecutionAttempt.Pass
            reasons = @($privateExecutionAttempt.Reasons)
        }
        if (-not $privateExecutionAttempt.Usable) {
            $report.steps.train = [ordered]@{
                attempted = $false
                reason = "PRIVATE_EXECUTION_LABEL_STORE_MISSING_OR_FAILED"
                start = $trainStartDate
                end = $trainEndDate
            }
            $report.reasons = @("PRIVATE_EXECUTION_LABEL_STORE_MISSING_OR_FAILED")
            $report.gates.overall_pass = $false
            $paths = Save-Report
            Write-ReportPointers -LogTag $LogTag -Paths $paths -OverallPass $false
            exit 2
        }
    } else {
        $report.steps.private_execution_label_store = [ordered]@{
            attempted = $false
            reason = "NOT_REQUIRED_FOR_TRAINER"
        }
    }

    $dependencyTrainerResults = @()
    if (@($DependencyTrainers).Count -gt 0) {
        $dependencyTrainerResults = Invoke-DependencyTrainerChain `
            -PythonPath $resolvedPythonExe `
            -TrainStartDate $trainStartDate `
            -TrainEndDate $trainEndDate `
            -ExecutionEvalStartDate $certificationStartDate `
            -ExecutionEvalEndDate $effectiveBatchDate
        $dependencyRunIds = @($dependencyTrainerResults | ForEach-Object { [string](Get-PropValue -ObjectValue $_ -Name "run_id" -DefaultValue "") } | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
        $dependencyFamilies = @($dependencyTrainerResults | ForEach-Object { [string](Get-PropValue -ObjectValue $_ -Name "model_family" -DefaultValue "") } | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
        $dependencySnapshotIds = @($dependencyTrainerResults | ForEach-Object { [string](Get-PropValue -ObjectValue $_ -Name "data_platform_ready_snapshot_id" -DefaultValue "") } | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
        $dependencyTailModes = @($dependencyTrainerResults | ForEach-Object { [string](Get-PropValue -ObjectValue $_ -Name "tail_mode" -DefaultValue "") } | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
        $dependencyUniqueSnapshotIds = @($dependencySnapshotIds | Select-Object -Unique)
        $dependencyReusedCount = @(
            $dependencyTrainerResults |
                Where-Object { [bool](Get-PropValue -ObjectValue $_ -Name "reused" -DefaultValue $false) }
        ).Count
        $dependencyTrainedCount = @($dependencyTrainerResults).Count - $dependencyReusedCount
        $report.steps.dependency_trainers = [ordered]@{
            attempted = $true
            count = @($dependencyTrainerResults).Count
            trained_count = [int]$dependencyTrainedCount
            reused_count = [int]$dependencyReusedCount
            results = @($dependencyTrainerResults)
            run_ids = @($dependencyRunIds)
            model_families = @($dependencyFamilies)
            data_platform_ready_snapshot_ids = @($dependencySnapshotIds)
            tail_modes = @($dependencyTailModes)
            same_snapshot_id = if (@($dependencyUniqueSnapshotIds).Count -eq 1) { [string]$dependencyUniqueSnapshotIds[0] } else { "" }
            snapshot_id_consistent = (@($dependencyUniqueSnapshotIds).Count -le 1)
            expected_snapshot_id = $dataPlatformReadySnapshotId
        }
    } else {
        $report.steps.dependency_trainers = [ordered]@{
            attempted = $false
            reason = "NO_DEPENDENCY_TRAINERS"
        }
    }
    if ($EnableVariantMatrixSelection.IsPresent) {
        $sequenceVariantResult = @($dependencyTrainerResults | Where-Object { [string](Get-PropValue -ObjectValue $_ -Name "trainer" -DefaultValue "") -eq "v5_sequence" } | Select-Object -First 1)
        $lobVariantResult = @($dependencyTrainerResults | Where-Object { [string](Get-PropValue -ObjectValue $_ -Name "trainer" -DefaultValue "") -eq "v5_lob" } | Select-Object -First 1)
        $report.steps.sequence_variant_selection = if ($sequenceVariantResult.Count -gt 0) {
            Test-VariantSelectionStep -TrainerName "v5_sequence" -ResultObject $sequenceVariantResult[0] -FailureCode "SEQUENCE_VARIANT_SELECTION_INCOMPLETE"
        } else {
            [ordered]@{ attempted = $false; reason = "DEPENDENCY_RESULT_MISSING" }
        }
        $report.steps.lob_variant_selection = if ($lobVariantResult.Count -gt 0) {
            Test-VariantSelectionStep -TrainerName "v5_lob" -ResultObject $lobVariantResult[0] -FailureCode "LOB_VARIANT_SELECTION_INCOMPLETE"
        } else {
            [ordered]@{ attempted = $false; reason = "DEPENDENCY_RESULT_MISSING" }
        }
        foreach ($variantStepName in @("sequence_variant_selection", "lob_variant_selection")) {
            $variantStep = Get-PropValue -ObjectValue $report.steps -Name $variantStepName -DefaultValue @{}
            if ([bool](Get-PropValue -ObjectValue $variantStep -Name "attempted" -DefaultValue $false) -and (-not [bool](Get-PropValue -ObjectValue $variantStep -Name "pass" -DefaultValue $false))) {
                $reasons = @((Get-PropValue -ObjectValue $variantStep -Name "reasons" -DefaultValue @()))
                $report.reasons = @($reasons)
                $report.gates.overall_pass = $false
                $failureCode = if (@($reasons).Count -gt 0) { [string]$reasons[0] } else { "VARIANT_SELECTION_INCOMPLETE" }
                Set-ReportFailure -Stage "dependency_train" -Code $failureCode
                $paths = Save-Report
                Write-ReportPointers -LogTag $LogTag -Paths $paths -OverallPass $false
                exit 2
            }
        }
    } else {
        $report.steps.sequence_variant_selection = [ordered]@{ attempted = $false; reason = "SKIPPED_BY_FLAG" }
        $report.steps.lob_variant_selection = [ordered]@{ attempted = $false; reason = "SKIPPED_BY_FLAG" }
    }
    if (([string]$Trainer).Trim().ToLowerInvariant() -eq "v5_fusion" -and @($dependencyTrainerResults).Count -gt 0) {
        $dependencyProvenanceResults = @()
        $dependencyProvenanceReasons = New-Object System.Collections.Generic.List[string]
        foreach ($item in @($dependencyTrainerResults)) {
            $trainerName = [string](Get-PropValue -ObjectValue $item -Name "trainer" -DefaultValue "")
            $runDir = [string](Get-PropValue -ObjectValue $item -Name "run_dir" -DefaultValue "")
            $requiredArtifacts = @(Get-DependencyTrainerProvenanceArtifacts -TrainerName $trainerName)
            if (@($requiredArtifacts).Count -le 0) {
                continue
            }
            $artifactRows = @()
            $missingArtifacts = @()
            foreach ($artifactName in @($requiredArtifacts)) {
                $artifactPath = if ([string]::IsNullOrWhiteSpace($runDir)) { "" } else { Join-Path $runDir $artifactName }
                $exists = (-not [string]::IsNullOrWhiteSpace($artifactPath)) -and (Test-Path $artifactPath)
                $contentCheck = if ($exists) {
                    Test-DependencyTrainerProvenanceArtifactContent -TrainerName $trainerName -ArtifactName $artifactName -ArtifactPath $artifactPath
                } else {
                    [ordered]@{ pass = $false; reasons = @("ARTIFACT_MISSING"); payload = @{} }
                }
                if (-not $exists) {
                    $missingArtifacts += $artifactName
                } elseif (-not [bool](Get-PropValue -ObjectValue $contentCheck -Name "pass" -DefaultValue $false)) {
                    $missingArtifacts += ($artifactName + ":" + ([string]::Join(",", @((Get-PropValue -ObjectValue $contentCheck -Name "reasons" -DefaultValue @())))))
                }
                $artifactRows += [ordered]@{
                    name = $artifactName
                    path = $artifactPath
                    exists = $exists
                    content_pass = [bool](Get-PropValue -ObjectValue $contentCheck -Name "pass" -DefaultValue $false)
                    content_reasons = @((Get-PropValue -ObjectValue $contentCheck -Name "reasons" -DefaultValue @()))
                }
            }
            $oodReportPath = if ([string]::IsNullOrWhiteSpace($runDir)) { "" } else { Join-Path $runDir "ood_generalization_report.json" }
            $oodSummary = if ((-not [string]::IsNullOrWhiteSpace($oodReportPath)) -and (Test-Path $oodReportPath)) {
                $oodPayload = Load-JsonOrEmpty -PathValue $oodReportPath
                [ordered]@{
                    path = $oodReportPath
                    status = [string](Get-PropValue -ObjectValue $oodPayload -Name "status" -DefaultValue "")
                    source_kind = [string](Get-PropValue -ObjectValue $oodPayload -Name "source_kind" -DefaultValue "")
                    invariant_penalty_enabled = (To-Bool (Get-PropValue -ObjectValue $oodPayload -Name "invariant_penalty_enabled" -DefaultValue $false) $false)
                    future_to_train_ratio = (To-Double (Get-PropValue -ObjectValue (Get-PropValue -ObjectValue $oodPayload -Name "train_vs_future_domain_gap_summary" -DefaultValue @{}) -Name "future_to_train_ratio" -DefaultValue 0.0) 0.0)
                }
            } else {
                [ordered]@{ path = ""; status = ""; source_kind = ""; invariant_penalty_enabled = $false; future_to_train_ratio = $null }
            }
            if (@($missingArtifacts).Count -gt 0) {
                $dependencyProvenanceReasons.Add(($trainerName.ToUpperInvariant() + "_PROVENANCE_MISSING")) | Out-Null
            }
            $dependencyProvenanceResults += [ordered]@{
                trainer = $trainerName
                run_id = [string](Get-PropValue -ObjectValue $item -Name "run_id" -DefaultValue "")
                run_dir = $runDir
                required_artifacts = @($artifactRows)
                ood_generalization = $oodSummary
                pass = (@($missingArtifacts).Count -eq 0)
                reasons = @($missingArtifacts)
            }
        }
        $report.steps.dependency_provenance = [ordered]@{
            attempted = $true
            results = @($dependencyProvenanceResults)
            pass = (@($dependencyProvenanceReasons).Count -eq 0)
            reasons = @($dependencyProvenanceReasons | Select-Object -Unique)
        }
        if (-not [bool](Get-PropValue -ObjectValue $report.steps.dependency_provenance -Name "pass" -DefaultValue $false)) {
            $report.reasons = @("DEPENDENCY_PROVENANCE_INCOMPLETE")
            $report.gates.overall_pass = $false
            Set-ReportFailure -Stage "fusion_train" -Code "DEPENDENCY_PROVENANCE_INCOMPLETE"
            $paths = Save-Report
            Write-ReportPointers -LogTag $LogTag -Paths $paths -OverallPass $false
            exit 2
        }
    } else {
        $report.steps.dependency_provenance = [ordered]@{
            attempted = $false
            reason = "NOT_REQUIRED_FOR_TRAINER"
        }
    }
    $dependencyRuntimeExportResults = @()
    $dependencyRuntimeUniverse = @{}
    $commonRuntimeUniverseArtifact = @{}
    $commonRuntimeUniverseArtifactPath = Join-Path $resolvedOutDir "common_runtime_universe.json"
    $dependencyRuntimeCoverageEligible = Test-ShouldAttemptDependencyRuntimeCoverage `
        -TrainerName $Trainer `
        -DependencyResults @($dependencyTrainerResults) `
        -CertificationStartDate $certificationStartDate `
        -CertificationEndDate $effectiveBatchDate `
        -LaneMode ([string](Get-PropValue -ObjectValue $report.split_policy -Name "lane_mode" -DefaultValue ""))
    if ($dependencyRuntimeCoverageEligible) {
        try {
            $dependencyRuntimeUniverse = Resolve-DependencyRuntimeCommonUniverse `
                -PythonPath $resolvedPythonExe `
                -DependencyResults @($dependencyTrainerResults) `
                -CertificationStartDate $certificationStartDate `
                -CertificationEndDate $effectiveBatchDate
            $report.steps.dependency_runtime_universe = $dependencyRuntimeUniverse
            $commonRuntimeUniverseArtifact = New-CommonRuntimeUniverseArtifact `
                -DependencyRuntimeUniverse $dependencyRuntimeUniverse `
                -CertificationStartDate $certificationStartDate `
                -CertificationEndDate $effectiveBatchDate `
                -ExpectedSnapshotId $dataPlatformReadySnapshotId `
                -ArtifactPath $commonRuntimeUniverseArtifactPath
            $report.steps.common_runtime_universe = $commonRuntimeUniverseArtifact
            Write-JsonFile -PathValue $commonRuntimeUniverseArtifactPath -Payload $commonRuntimeUniverseArtifact
            if (-not [bool](Get-PropValue -ObjectValue $dependencyRuntimeUniverse -Name "pass" -DefaultValue $false)) {
                $report.reasons = @([string](Get-PropValue -ObjectValue $dependencyRuntimeUniverse -Name "reason" -DefaultValue "COMMON_RUNTIME_UNIVERSE_EMPTY"))
                $report.gates.overall_pass = $false
                Set-ReportFailure -Stage "runtime_export" -Code ([string](Get-PropValue -ObjectValue $dependencyRuntimeUniverse -Name "reason" -DefaultValue "COMMON_RUNTIME_UNIVERSE_EMPTY")) -ReportPath $commonRuntimeUniverseArtifactPath
                $paths = Save-Report
                Write-ReportPointers -LogTag $LogTag -Paths $paths -OverallPass $false
                exit 2
            }
            $dependencyRuntimeExportResults = Invoke-DependencyRuntimeExportChain `
                -PythonPath $resolvedPythonExe `
                -DependencyResults @($dependencyTrainerResults) `
                -CertificationStartDate $certificationStartDate `
                -CertificationEndDate $effectiveBatchDate `
                -CommonMarkets @((Get-PropValue -ObjectValue $dependencyRuntimeUniverse -Name "common_markets" -DefaultValue @()))
            $dependencyRuntimeExportContract = Test-DependencyRuntimeExportContractAlignment `
                -ExportResults @($dependencyRuntimeExportResults) `
                -CertificationStartDate $certificationStartDate `
                -CertificationEndDate $effectiveBatchDate `
                -ExpectedSnapshotId $dataPlatformReadySnapshotId `
                -CommonMarkets @((Get-PropValue -ObjectValue $commonRuntimeUniverseArtifact -Name "common_markets" -DefaultValue @()))
            $report.steps.dependency_runtime_export_contract = $dependencyRuntimeExportContract
            if (-not [bool](Get-PropValue -ObjectValue $dependencyRuntimeExportContract -Name "pass" -DefaultValue $false)) {
                $runtimeExportReasons = @(
                    @(Get-PropValue -ObjectValue $dependencyRuntimeExportContract -Name "reasons" -DefaultValue @()) |
                        ForEach-Object { [string]$_ } |
                        Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
                )
                $report.reasons = @($runtimeExportReasons)
                $report.gates.overall_pass = $false
                $runtimeExportFailureCode = if (@($runtimeExportReasons).Count -gt 0) { [string]$runtimeExportReasons[0] } else { "DEPENDENCY_RUNTIME_EXPORT_CONTRACT_FAILED" }
                Set-ReportFailure -Stage "runtime_export" -Code $runtimeExportFailureCode -ReportPath $commonRuntimeUniverseArtifactPath
                $paths = Save-Report
                Write-ReportPointers -LogTag $LogTag -Paths $paths -OverallPass $false
                exit 2
            }
        } catch {
            Set-ReportFailure -Stage "runtime_export" -Code "DEPENDENCY_RUNTIME_EXPORT_FAILED"
            throw
        }
        $report.steps.dependency_runtime_exports = [ordered]@{
            attempted = $true
            count = @($dependencyRuntimeExportResults).Count
            results = @($dependencyRuntimeExportResults)
            run_ids = @($dependencyRuntimeExportResults | ForEach-Object { [string](Get-PropValue -ObjectValue $_ -Name "run_id" -DefaultValue "") })
            data_platform_ready_snapshot_ids = @($dependencyRuntimeExportResults | ForEach-Object { [string](Get-PropValue -ObjectValue $_ -Name "data_platform_ready_snapshot_id" -DefaultValue "") })
        }
    } else {
        $runtimeExportSkipReason = if (([string]$Trainer).Trim().ToLowerInvariant() -ne "v5_fusion" -or @($DependencyTrainers).Count -le 0) {
            "NO_RUNTIME_EXPORT_CHAIN_REQUIRED"
        } else {
            "BOOTSTRAP_OR_NO_CERTIFICATION_WINDOW"
        }
        $report.steps.dependency_runtime_exports = [ordered]@{
            attempted = $false
            reason = $runtimeExportSkipReason
        }
        $report.steps.dependency_runtime_universe = [ordered]@{
            attempted = $false
            reason = $runtimeExportSkipReason
        }
        $report.steps.common_runtime_universe = [ordered]@{
            attempted = $false
            reason = $runtimeExportSkipReason
        }
        $report.steps.dependency_runtime_export_contract = [ordered]@{
            attempted = $false
            reason = $runtimeExportSkipReason
        }
    }

    $useFusionVariantMatrix = $EnableVariantMatrixSelection.IsPresent -and (([string]$Trainer).Trim().ToLowerInvariant() -eq "v5_fusion")
    $trainCommand = if ($useFusionVariantMatrix) { "train-variant-matrix" } else { "train" }
    $trainArgs = @(
        "-m", "autobot.cli",
        "model", $trainCommand,
        "--trainer", $Trainer,
        "--model-family", $ModelFamily,
        "--feature-set", $FeatureSet,
        "--label-set", $LabelSet,
        "--task", $Task,
        "--run-scope", $RunScope,
        "--tf", $Tf,
        "--quote", $Quote,
        "--top-n", $TrainTopN,
        "--start", $trainStartDate,
        "--end", $trainEndDate,
        "--booster-sweep-trials", $BoosterSweepTrials,
        "--seed", $Seed,
        "--nthread", $NThread,
        "--live-domain-reweighting",
        "--live-domain-reweighting-db-path", (Join-Path $resolvedProjectRoot "data/state/live_canary/live_state.db"),
        "--execution-acceptance-top-n", $BacktestTopN,
        "--execution-acceptance-top-pct", $BacktestTopPct,
        "--execution-acceptance-min-prob", $BacktestMinProb,
        "--execution-acceptance-min-cands-per-ts", $BacktestMinCandidatesPerTs,
        "--execution-acceptance-hold-bars", $HoldBars
    )
    if ((-not [string]::IsNullOrWhiteSpace($certificationStartDate)) -and (-not [string]::IsNullOrWhiteSpace($effectiveBatchDate))) {
        $trainArgs += @(
            "--execution-eval-start", $certificationStartDate,
            "--execution-eval-end", $effectiveBatchDate
        )
    }
    $fusionDependencyInputs = @{}
    $fusionDependencyRuntimeInputs = @{}
    if (([string]$Trainer).Trim().ToLowerInvariant() -eq "v5_fusion" -and @($DependencyTrainers).Count -gt 0) {
        $fusionDependencyInputs = Resolve-DependencyFusionInputPaths -DependencyResults @($dependencyTrainerResults)
        $fusionDependencyRuntimeInputs = Resolve-DependencyRuntimeFusionInputPaths -DependencyRuntimeExportResults @($dependencyRuntimeExportResults)
        $panelInputPath = [string](Get-PropValue -ObjectValue $fusionDependencyInputs -Name "fusion_panel_input" -DefaultValue "")
        $sequenceInputPath = [string](Get-PropValue -ObjectValue $fusionDependencyInputs -Name "fusion_sequence_input" -DefaultValue "")
        $lobInputPath = [string](Get-PropValue -ObjectValue $fusionDependencyInputs -Name "fusion_lob_input" -DefaultValue "")
        $tradabilityInputPath = [string](Get-PropValue -ObjectValue $fusionDependencyInputs -Name "fusion_tradability_input" -DefaultValue "")
        if (-not [string]::IsNullOrWhiteSpace($panelInputPath)) {
            $trainArgs += @("--fusion-panel-input", $panelInputPath)
        }
        if (-not [string]::IsNullOrWhiteSpace($sequenceInputPath)) {
            $trainArgs += @("--fusion-sequence-input", $sequenceInputPath)
        }
        if (-not [string]::IsNullOrWhiteSpace($lobInputPath)) {
            $trainArgs += @("--fusion-lob-input", $lobInputPath)
        }
        if (-not [string]::IsNullOrWhiteSpace($tradabilityInputPath)) {
            $trainArgs += @("--fusion-tradability-input", $tradabilityInputPath)
        }
        $panelRuntimeInputPath = [string](Get-PropValue -ObjectValue $fusionDependencyRuntimeInputs -Name "fusion_panel_runtime_input" -DefaultValue "")
        $sequenceRuntimeInputPath = [string](Get-PropValue -ObjectValue $fusionDependencyRuntimeInputs -Name "fusion_sequence_runtime_input" -DefaultValue "")
        $lobRuntimeInputPath = [string](Get-PropValue -ObjectValue $fusionDependencyRuntimeInputs -Name "fusion_lob_runtime_input" -DefaultValue "")
        $tradabilityRuntimeInputPath = [string](Get-PropValue -ObjectValue $fusionDependencyRuntimeInputs -Name "fusion_tradability_runtime_input" -DefaultValue "")
        if (-not [string]::IsNullOrWhiteSpace($panelRuntimeInputPath)) {
            $trainArgs += @("--fusion-panel-runtime-input", $panelRuntimeInputPath)
        }
        if (-not [string]::IsNullOrWhiteSpace($sequenceRuntimeInputPath)) {
            $trainArgs += @("--fusion-sequence-runtime-input", $sequenceRuntimeInputPath)
        }
        if (-not [string]::IsNullOrWhiteSpace($lobRuntimeInputPath)) {
            $trainArgs += @("--fusion-lob-runtime-input", $lobRuntimeInputPath)
        }
        if (-not [string]::IsNullOrWhiteSpace($tradabilityRuntimeInputPath)) {
            $trainArgs += @("--fusion-tradability-runtime-input", $tradabilityRuntimeInputPath)
        }
        if ((-not [string]::IsNullOrWhiteSpace($certificationStartDate)) -and (-not [string]::IsNullOrWhiteSpace($effectiveBatchDate))) {
            $trainArgs += @("--fusion-runtime-start", $certificationStartDate, "--fusion-runtime-end", $effectiveBatchDate)
        }
    }
    $trainExec = Invoke-CommandCapture -Exe $resolvedPythonExe -ArgList $trainArgs
    $candidateMatrixPayload = if ($useFusionVariantMatrix) { Resolve-JsonObjectFromText -TextValue ([string]$trainExec.Output) } else { @{} }
    $candidateRunDir = if ($DryRun) {
        ""
    } elseif ($useFusionVariantMatrix) {
        [string](Get-PropValue -ObjectValue $candidateMatrixPayload -Name "run_dir" -DefaultValue "")
    } else {
        Resolve-RunDirFromText -TextValue ([string]$trainExec.Output)
    }
    $candidateRunId = if ($DryRun) {
        ""
    } elseif ($useFusionVariantMatrix) {
        [string](Get-PropValue -ObjectValue $candidateMatrixPayload -Name "run_id" -DefaultValue "")
    } elseif ([string]::IsNullOrWhiteSpace($candidateRunDir)) {
        ""
    } else {
        Split-Path -Leaf $candidateRunDir
    }
    if ([string]::IsNullOrWhiteSpace($candidateRunDir) -and (-not [string]::IsNullOrWhiteSpace($candidateRunId))) {
        $candidateRunDir = Join-Path (Join-Path $resolvedRegistryRoot $ModelFamily) $candidateRunId
    }
    $championRunId = [string](Get-PropValue -ObjectValue $championBefore -Name "run_id" -DefaultValue "")
    $resolvedChampionModelFamily = if ([string]::IsNullOrWhiteSpace($ChampionModelFamily)) { $ModelFamily } else { $ChampionModelFamily.Trim() }
    $championModelRunDir = if ([string]::IsNullOrWhiteSpace($championRunId)) { "" } else { Join-Path (Join-Path $resolvedRegistryRoot $resolvedChampionModelFamily) $championRunId }
    $duplicateCandidateArtifacts = Resolve-DuplicateCandidateArtifacts -CandidateRunDir $candidateRunDir -ChampionRunDir $championModelRunDir
    $promotionDecisionPath = if ([string]::IsNullOrWhiteSpace($candidateRunDir)) { "" } else { Join-Path $candidateRunDir "promotion_decision.json" }
    $promotionDecision = if ([string]::IsNullOrWhiteSpace($promotionDecisionPath)) { @{} } else { Load-JsonOrEmpty -PathValue $promotionDecisionPath }
    $researchEvidencePath = if ([string]::IsNullOrWhiteSpace($candidateRunDir)) { "" } else { Join-Path $candidateRunDir "trainer_research_evidence.json" }
    $researchEvidenceArtifact = if ([string]::IsNullOrWhiteSpace($researchEvidencePath)) { @{} } else { Load-JsonOrEmpty -PathValue $researchEvidencePath }
    $candidateTrainConfigPath = if ([string]::IsNullOrWhiteSpace($candidateRunDir)) { "" } else { Join-Path $candidateRunDir "train_config.yaml" }
    $candidateTrainConfig = if ([string]::IsNullOrWhiteSpace($candidateTrainConfigPath)) { @{} } else { Load-JsonOrEmpty -PathValue $candidateTrainConfigPath }
    $candidateSnapshotId = [string](Get-PropValue -ObjectValue $candidateTrainConfig -Name "data_platform_ready_snapshot_id" -DefaultValue "")
    $candidateRuntimeRecommendationsPath = if ([string]::IsNullOrWhiteSpace($candidateRunDir)) { "" } else { Join-Path $candidateRunDir "runtime_recommendations.json" }
    $candidateRuntimeRecommendations = if ([string]::IsNullOrWhiteSpace($candidateRuntimeRecommendationsPath)) { @{} } else { Load-JsonOrEmpty -PathValue $candidateRuntimeRecommendationsPath }
    $candidateFusionModelContractPath = if ([string]::IsNullOrWhiteSpace($candidateRunDir)) { "" } else { Join-Path $candidateRunDir "fusion_model_contract.json" }
    $candidateFusionModelContract = if ([string]::IsNullOrWhiteSpace($candidateFusionModelContractPath)) { @{} } else { Load-JsonOrEmpty -PathValue $candidateFusionModelContractPath }
    $candidateDomainWeightingReportPath = if ([string]::IsNullOrWhiteSpace($candidateRunDir)) { "" } else { Join-Path $candidateRunDir "domain_weighting_report.json" }
    $candidateDomainWeightingReport = if ([string]::IsNullOrWhiteSpace($candidateDomainWeightingReportPath)) { @{} } else { Load-JsonOrEmpty -PathValue $candidateDomainWeightingReportPath }
    $candidateVariantMetadata = if ([string]::IsNullOrWhiteSpace($candidateRunDir)) { @{} } else { Resolve-RunVariantMetadata -RunDir $candidateRunDir -TrainerName $Trainer }
    $sequenceBackboneName = [string](Get-PropValue -ObjectValue $candidateRuntimeRecommendations -Name "sequence_backbone_name" -DefaultValue "")
    $lobBackboneName = [string](Get-PropValue -ObjectValue $candidateRuntimeRecommendations -Name "lob_backbone_name" -DefaultValue "")
    $tradabilitySourceRunId = [string](Get-PropValue -ObjectValue $candidateRuntimeRecommendations -Name "tradability_source_run_id" -DefaultValue "")
    $fusionStackerFamily = [string](Get-PropValue -ObjectValue $candidateRuntimeRecommendations -Name "fusion_stacker_family" -DefaultValue "")
    $fusionGatingPolicy = [string](Get-PropValue -ObjectValue $candidateRuntimeRecommendations -Name "fusion_gating_policy" -DefaultValue "")
    $candidateSequenceVariantName = [string](Get-PropValue -ObjectValue $candidateVariantMetadata -Name "sequence_variant_name" -DefaultValue (Get-PropValue -ObjectValue $candidateRuntimeRecommendations -Name "sequence_variant_name" -DefaultValue ""))
    if ([string]::IsNullOrWhiteSpace($candidateSequenceVariantName) -and (-not [string]::IsNullOrWhiteSpace($sequenceBackboneName))) {
        $candidateSequenceVariantName = $sequenceBackboneName + "__" + ([string](Get-PropValue -ObjectValue $candidateRuntimeRecommendations -Name "sequence_pretrain_method" -DefaultValue "none"))
    }
    $candidateLobVariantName = [string](Get-PropValue -ObjectValue $candidateVariantMetadata -Name "lob_variant_name" -DefaultValue (Get-PropValue -ObjectValue $candidateRuntimeRecommendations -Name "lob_variant_name" -DefaultValue ""))
    if ([string]::IsNullOrWhiteSpace($candidateLobVariantName) -and (-not [string]::IsNullOrWhiteSpace($lobBackboneName))) {
        $candidateLobVariantName = $lobBackboneName
    }
    $candidateFusionVariantName = [string](Get-PropValue -ObjectValue $candidateVariantMetadata -Name "fusion_variant_name" -DefaultValue (Get-PropValue -ObjectValue $candidateRuntimeRecommendations -Name "fusion_variant_name" -DefaultValue ""))
    if ([string]::IsNullOrWhiteSpace($candidateFusionVariantName) -and (-not [string]::IsNullOrWhiteSpace($fusionStackerFamily))) {
        $candidateFusionVariantName = $fusionStackerFamily
    }
    $candidateVariantReportPath = [string](Get-PropValue -ObjectValue $candidateVariantMetadata -Name "variant_report_path" -DefaultValue "")
    $fusionInputExperts = Get-PropValue -ObjectValue $candidateFusionModelContract -Name "input_experts" -DefaultValue @{}
    $fusionTradabilityInput = Get-PropValue -ObjectValue $fusionInputExperts -Name "tradability" -DefaultValue @{}
    $sequencePretrainMethod = [string](Get-PropValue -ObjectValue $candidateRuntimeRecommendations -Name "sequence_pretrain_method" -DefaultValue "")
    $sequencePretrainReady = To-Bool (Get-PropValue -ObjectValue $candidateRuntimeRecommendations -Name "sequence_pretrain_ready" -DefaultValue $false) $false
    $sequencePretrainStatus = [string](Get-PropValue -ObjectValue $candidateRuntimeRecommendations -Name "sequence_pretrain_status" -DefaultValue "")
    $sequencePretrainObjective = [string](Get-PropValue -ObjectValue $candidateRuntimeRecommendations -Name "sequence_pretrain_objective" -DefaultValue "")
    $sequencePretrainBestEpoch = [int](To-Int64 (Get-PropValue -ObjectValue $candidateRuntimeRecommendations -Name "sequence_pretrain_best_epoch" -DefaultValue 0) 0)
    $sequencePretrainEncoderPresent = To-Bool (Get-PropValue -ObjectValue $candidateRuntimeRecommendations -Name "sequence_pretrain_encoder_present" -DefaultValue $false) $false
    $sequencePretrainContractPath = [string](Get-PropValue -ObjectValue $candidateRuntimeRecommendations -Name "sequence_pretrain_contract_path" -DefaultValue "")
    $sequencePretrainReportPath = [string](Get-PropValue -ObjectValue $candidateRuntimeRecommendations -Name "sequence_pretrain_report_path" -DefaultValue "")
    $fusionRegimeClusterCount = [int](To-Int64 (Get-PropValue -ObjectValue $candidateFusionModelContract -Name "regime_cluster_count" -DefaultValue 0) 0)
    $fusionRegimeFeatureColumns = @((Get-PropValue -ObjectValue $candidateFusionModelContract -Name "regime_feature_columns" -DefaultValue @()))
    $domainWeightingPolicy = [string](Get-PropValue -ObjectValue $candidateDomainWeightingReport -Name "policy" -DefaultValue "")
    $domainWeightingSourceKind = [string](Get-PropValue -ObjectValue (Get-PropValue -ObjectValue $candidateDomainWeightingReport -Name "domain_details" -DefaultValue @{}) -Name "source_kind" -DefaultValue "")
    $domainWeightingEnabled = To-Bool (Get-PropValue -ObjectValue $candidateDomainWeightingReport -Name "domain_weighting_enabled" -DefaultValue $false) $false
    $oodStatus = [string](Get-PropValue -ObjectValue $candidateRuntimeRecommendations -Name "ood_status" -DefaultValue "")
    $oodSourceKind = [string](Get-PropValue -ObjectValue $candidateRuntimeRecommendations -Name "ood_source_kind" -DefaultValue "")
    $oodPenaltyEnabled = To-Bool (Get-PropValue -ObjectValue $candidateRuntimeRecommendations -Name "ood_penalty_enabled" -DefaultValue $false) $false
    $domainWeightingSummary = [ordered]@{
        policy = $domainWeightingPolicy
        source_kind = $domainWeightingSourceKind
        enabled = $domainWeightingEnabled
        effective_sample_weight_summary = (Get-PropValue -ObjectValue $candidateDomainWeightingReport -Name "effective_sample_weight_summary" -DefaultValue @{})
        domain_weight_summary = (Get-PropValue -ObjectValue $candidateDomainWeightingReport -Name "domain_weight_summary" -DefaultValue @{})
    }
    $fusionProvenanceSummary = [ordered]@{
        runtime_recommendations_path = $candidateRuntimeRecommendationsPath
        fusion_model_contract_path = $candidateFusionModelContractPath
        domain_weighting_report_path = $candidateDomainWeightingReportPath
        sequence_backbone_name = $sequenceBackboneName
        lob_backbone_name = $lobBackboneName
        tradability_source_run_id = $tradabilitySourceRunId
        sequence_variant_name = $candidateSequenceVariantName
        lob_variant_name = $candidateLobVariantName
        fusion_variant_name = $candidateFusionVariantName
        fusion_variant_report_path = $candidateVariantReportPath
        sequence_pretrain_method = $sequencePretrainMethod
        sequence_pretrain_ready = $sequencePretrainReady
        sequence_pretrain_status = $sequencePretrainStatus
        sequence_pretrain_objective = $sequencePretrainObjective
        sequence_pretrain_best_epoch = $sequencePretrainBestEpoch
        sequence_pretrain_encoder_present = $sequencePretrainEncoderPresent
        sequence_pretrain_contract_path = $sequencePretrainContractPath
        sequence_pretrain_report_path = $sequencePretrainReportPath
        fusion_stacker_family = $fusionStackerFamily
        fusion_gating_policy = $fusionGatingPolicy
        regime_cluster_count = $fusionRegimeClusterCount
        regime_feature_columns = @($fusionRegimeFeatureColumns)
        tradability_input_trainer = [string](Get-PropValue -ObjectValue $fusionTradabilityInput -Name "trainer" -DefaultValue "")
        tradability_input_model_family = [string](Get-PropValue -ObjectValue $fusionTradabilityInput -Name "model_family" -DefaultValue "")
        domain_weighting = $domainWeightingSummary
        ood_status = $oodStatus
        ood_source_kind = $oodSourceKind
        ood_penalty_enabled = $oodPenaltyEnabled
    }
    $searchBudgetDecisionPath = if ([string]::IsNullOrWhiteSpace($candidateRunDir)) { "" } else { Join-Path $candidateRunDir "search_budget_decision.json" }
    $searchBudgetDecision = if ([string]::IsNullOrWhiteSpace($searchBudgetDecisionPath)) { @{} } else { Load-JsonOrEmpty -PathValue $searchBudgetDecisionPath }
    $economicObjectiveProfilePath = if ([string]::IsNullOrWhiteSpace($candidateRunDir)) { "" } else { Join-Path $candidateRunDir "economic_objective_profile.json" }
    $economicObjectiveProfile = if ([string]::IsNullOrWhiteSpace($economicObjectiveProfilePath)) { @{} } else { Load-JsonOrEmpty -PathValue $economicObjectiveProfilePath }
    $laneGovernancePath = if ([string]::IsNullOrWhiteSpace($candidateRunDir)) { "" } else { Join-Path $candidateRunDir "lane_governance.json" }
    $laneGovernance = if ([string]::IsNullOrWhiteSpace($laneGovernancePath)) { @{} } else { Load-JsonOrEmpty -PathValue $laneGovernancePath }
    if (Test-IsEffectivelyEmptyObject -ObjectValue $laneGovernance) {
        $normalizedTask = [string]$Task
        $normalizedTask = $normalizedTask.Trim().ToLowerInvariant()
        $normalizedRunScope = [string]$RunScope
        $normalizedRunScope = $normalizedRunScope.Trim().ToLowerInvariant()
        $laneGovernance = [ordered]@{
            policy = "v4_lane_governance_v1"
            lane_id = if ($normalizedTask -eq "rank" -and ($normalizedRunScope.Contains("rank_governed") -or $normalizedRunScope.Contains("rank_promotable"))) { "rank_governed_primary" } elseif ($normalizedTask -eq "rank") { "rank_shadow" } elseif ($normalizedTask -eq "cls") { "cls_primary" } else { "$normalizedTask`_research" }
            task = if ([string]::IsNullOrWhiteSpace($normalizedTask)) { "cls" } else { $normalizedTask }
            run_scope = [string]$RunScope
            lane_role = if ($normalizedTask -eq "rank" -and ($normalizedRunScope.Contains("rank_governed") -or $normalizedRunScope.Contains("rank_promotable"))) { "production_candidate" } elseif ($normalizedTask -eq "rank") { "shadow" } elseif ($normalizedTask -eq "cls") { "primary" } else { "research" }
            shadow_only = ($normalizedTask -eq "rank" -and (-not ($normalizedRunScope.Contains("rank_governed") -or $normalizedRunScope.Contains("rank_promotable"))))
            production_lane_id = "cls_primary"
            production_task = "cls"
            promotion_allowed = ($normalizedTask -eq "cls" -or ($normalizedTask -eq "rank" -and ($normalizedRunScope.Contains("rank_governed") -or $normalizedRunScope.Contains("rank_promotable"))))
            live_replacement_allowed = ($normalizedTask -eq "cls")
            governance_reasons = if ($normalizedTask -eq "rank" -and ($normalizedRunScope.Contains("rank_governed") -or $normalizedRunScope.Contains("rank_promotable"))) { @("AUTO_GOVERNED_FROM_RANK_SHADOW_PASS") } elseif ($normalizedTask -eq "rank") { @("RANK_LANE_SHADOW_EVALUATION_ONLY", "EXPLICIT_GOVERNANCE_DECISION_REQUIRED") } elseif ($normalizedTask -eq "cls") { @("PRIMARY_LANE_ELIGIBLE") } else { @("NON_PRIMARY_LANE_REQUIRES_EXPLICIT_GOVERNANCE") }
        }
    }
    $laneId = [string](Get-PropValue -ObjectValue $laneGovernance -Name "lane_id" -DefaultValue "")
    $laneRole = [string](Get-PropValue -ObjectValue $laneGovernance -Name "lane_role" -DefaultValue "")
    $laneShadowOnly = To-Bool (Get-PropValue -ObjectValue $laneGovernance -Name "shadow_only" -DefaultValue $false) $false
    $lanePromotionAllowed = To-Bool (Get-PropValue -ObjectValue $laneGovernance -Name "promotion_allowed" -DefaultValue (-not $laneShadowOnly)) (-not $laneShadowOnly)
    $laneGovernanceReasons = @((Get-PropValue -ObjectValue $laneGovernance -Name "governance_reasons" -DefaultValue @()))
    $promotionPolicyConfig = Resolve-PromotionPolicyConfig -PolicyName $PromotionPolicy -BoundParams $PSBoundParameters -EconomicObjectiveProfile $economicObjectiveProfile
    Sync-PromotionPolicyConfigToReport -ReportValue $report -PromotionPolicyConfig $promotionPolicyConfig
    $report.config.lane_governance_path = $laneGovernancePath
    $report.config.lane_id = $laneId
    $report.config.lane_role = $laneRole
    $report.config.lane_shadow_only = $laneShadowOnly
    $report.config.lane_promotion_allowed = $lanePromotionAllowed
    $report.config.lane_governance_reasons = @($laneGovernanceReasons)
    $decisionSurfacePath = if ([string]::IsNullOrWhiteSpace($candidateRunDir)) { "" } else { Join-Path $candidateRunDir "decision_surface.json" }
    $certificationArtifactPath = if ([string]::IsNullOrWhiteSpace($candidateRunDir)) { "" } else { Join-Path $candidateRunDir "certification_report.json" }
    $trainerResearchPrior = Resolve-ResearchEvidenceFromArtifact -ResearchEvidenceArtifact $researchEvidenceArtifact -Mode $TrainerEvidenceMode
    $certificationArtifact = if ([string]::IsNullOrWhiteSpace($certificationArtifactPath)) {
        @{}
    } else {
        New-CertificationArtifact `
            -CandidateRunId $candidateRunId `
            -CandidateRunDir $candidateRunDir `
            -PromotionDecisionPath $promotionDecisionPath `
            -ResearchEvidencePath $researchEvidencePath `
            -EconomicObjectiveProfilePath $economicObjectiveProfilePath `
            -DecisionSurfacePath $decisionSurfacePath `
            -TrainStartDate $trainStartDate `
            -TrainEndDate $trainEndDate `
            -CertificationStartDate $certificationStartDate `
            -CertificationEndDate $effectiveBatchDate `
            -TrainerEvidenceMode $TrainerEvidenceMode `
            -TrainerResearchPrior $trainerResearchPrior
    }
    if ((-not $DryRun) -and (-not [string]::IsNullOrWhiteSpace($certificationArtifactPath))) {
        $certificationArtifact.provenance.lane_governance_path = $laneGovernancePath
        $certificationArtifact.provenance.lane_governance_present = -not (Test-IsEffectivelyEmptyObject -ObjectValue $laneGovernance)
        $certificationArtifact.provenance.lane_id = $laneId
        $certificationArtifact.provenance.lane_role = $laneRole
        $certificationArtifact.provenance.lane_shadow_only = $laneShadowOnly
        $certificationArtifact.provenance.lane_promotion_allowed = $lanePromotionAllowed
        $certificationArtifact.lane_governance = $laneGovernance
        Write-JsonFile -PathValue $certificationArtifactPath -Payload $certificationArtifact
    }
    $report.windows_by_step.research = (Get-PropValue -ObjectValue (Get-PropValue -ObjectValue $certificationArtifact -Name "windows" -DefaultValue @{}) -Name "research_window" -DefaultValue $report.windows_by_step.research)
    $report.windows_by_step.certification = (Get-PropValue -ObjectValue (Get-PropValue -ObjectValue $certificationArtifact -Name "windows" -DefaultValue @{}) -Name "certification_window" -DefaultValue $report.windows_by_step.certification)
    $report.windows_by_step.backtest = [ordered]@{
        start = $certificationStartDate
        end = $effectiveBatchDate
        alias_of = "certification"
    }
    $trainerEvidence = [ordered]@{
        mode = $TrainerEvidenceMode
        available = $false
        required = ($TrainerEvidenceMode -eq "required")
        source = "certification_artifact"
        pass = $null
        offline_pass = $false
        execution_pass = $true
        reasons = @("CERTIFICATION_EVIDENCE_PENDING")
        checks = [ordered]@{}
        offline = [ordered]@{}
        spa_like = [ordered]@{}
        white_rc = [ordered]@{}
        hansen_spa = [ordered]@{}
        execution = [ordered]@{}
        certification_window_valid = (To-Bool (Get-PropValue -ObjectValue $certificationArtifact -Name "valid_window_contract" -DefaultValue $false) $false)
        certification_window_reasons = @((Get-PropValue -ObjectValue $certificationArtifact -Name "reasons" -DefaultValue @()))
    }
    $report.steps.train = [ordered]@{
        exit_code = [int]$trainExec.ExitCode
        command = $trainExec.Command
        output_preview = (Get-OutputPreview -Text ([string]$trainExec.Output))
        start = $trainStartDate
        end = $trainEndDate
        candidate_run_id = $candidateRunId
        candidate_run_dir = $candidateRunDir
        research_evidence_path = $researchEvidencePath
        trainer_research_prior_path = $researchEvidencePath
        search_budget_decision_path = $searchBudgetDecisionPath
        economic_objective_profile_path = $economicObjectiveProfilePath
        lane_governance_path = $laneGovernancePath
        lane_id = $laneId
        lane_role = $laneRole
        lane_shadow_only = $laneShadowOnly
        lane_promotion_allowed = $lanePromotionAllowed
        lane_governance_reasons = @($laneGovernanceReasons)
        promotion_policy_contract_source = [string]$promotionPolicyConfig.threshold_source
        promotion_policy_contract_profile_id = [string]$promotionPolicyConfig.profile_id
        promotion_policy_cli_override_keys = @($promotionPolicyConfig.cli_override_keys)
        promotion_decision_path = $promotionDecisionPath
        data_platform_ready_snapshot_id = $candidateSnapshotId
        fusion_dependency_inputs = $fusionDependencyInputs
        fusion_dependency_runtime_inputs = $fusionDependencyRuntimeInputs
        dependency_runtime_common_markets = @((Get-PropValue -ObjectValue $dependencyRuntimeUniverse -Name "common_markets" -DefaultValue @()))
        decision_surface_path = $decisionSurfacePath
        certification_artifact_path = $certificationArtifactPath
        fusion_provenance = $fusionProvenanceSummary
        sequence_variant_name = $candidateSequenceVariantName
        lob_variant_name = $candidateLobVariantName
        fusion_variant_name = $candidateFusionVariantName
        fusion_variant_report_path = $candidateVariantReportPath
        promotion_decision_status = [string](Get-PropValue -ObjectValue $promotionDecision -Name "status" -DefaultValue "")
        trainer_evidence = $trainerEvidence
    }
    if (($trainExec.ExitCode -ne 0) -or ((-not $DryRun) -and [string]::IsNullOrWhiteSpace($candidateRunId))) {
        $report.reasons = @("TRAIN_OR_CANDIDATE_POINTER_FAILED")
        $report.gates.overall_pass = $false
        $trainFailureStage = if (([string]$Trainer).Trim().ToLowerInvariant() -eq "v5_fusion") {
            "fusion_train"
        } else {
            "acceptance_gate"
        }
        Set-ReportFailure `
            -Stage $trainFailureStage `
            -Code "TRAIN_OR_CANDIDATE_POINTER_FAILED" `
            -ReportPath $candidateRunDir
        Update-RunArtifactStatus `
            -RunDir $candidateRunDir `
            -RunId $candidateRunId `
            -Status "acceptance_incomplete" `
            -AcceptanceCompleted $false `
            -CandidateAdoptable $false `
            -CandidateAdopted $false `
            -Promoted $false | Out-Null
        $paths = Save-Report
        Write-ReportPointers -LogTag $LogTag -Paths $paths -OverallPass $false
        exit 2
    }
    $candidateBacktestModelRef = if ([string]::IsNullOrWhiteSpace($candidateRunId)) { $CandidateModelRef } else { $candidateRunId }
    $candidatePaperModelRef = $candidateBacktestModelRef
    $championBacktestModelRef = if ([string]::IsNullOrWhiteSpace($championRunId)) { $ChampionModelRef } else { $championRunId }
    $report.split_policy.candidate_run_id = $candidateRunId
    $report.split_policy.candidate_run_dir = $candidateRunDir
    $script:splitPolicyArtifactPath = if ($DryRun) {
        if ([string]::IsNullOrWhiteSpace($candidateRunDir)) { "" } else { Join-Path $candidateRunDir "split_policy_decision.json" }
    } else {
        Write-SplitPolicyDecisionArtifact -CandidateRunDir $candidateRunDir -SplitPolicyDecision $report.split_policy
    }
    $report.split_policy.artifact_path = $script:splitPolicyArtifactPath
    $report.candidate = [ordered]@{
        run_id = $candidateRunId
        fusion_run_id = $candidateRunId
        run_dir = $candidateRunDir
        lane_mode = [string](Get-PropValue -ObjectValue $report.split_policy -Name "lane_mode" -DefaultValue "")
        promotion_eligible = [bool](Get-PropValue -ObjectValue $report.split_policy -Name "promotion_eligible" -DefaultValue $false)
        split_policy_id = [string](Get-PropValue -ObjectValue $report.split_policy -Name "policy_id" -DefaultValue "")
        split_policy_artifact_path = $script:splitPolicyArtifactPath
        research_evidence_path = $researchEvidencePath
        trainer_research_prior_path = $researchEvidencePath
        search_budget_decision_path = $searchBudgetDecisionPath
        economic_objective_profile_path = $economicObjectiveProfilePath
        economic_objective_profile_id = [string](Get-PropValue -ObjectValue $economicObjectiveProfile -Name "profile_id" -DefaultValue "")
        lane_governance_path = $laneGovernancePath
        lane_id = $laneId
        lane_role = $laneRole
        lane_shadow_only = $laneShadowOnly
        lane_promotion_allowed = $lanePromotionAllowed
        lane_governance_reasons = @($laneGovernanceReasons)
        promotion_decision_path = $promotionDecisionPath
        data_platform_ready_snapshot_id = $candidateSnapshotId
        fusion_snapshot_id = $candidateSnapshotId
        decision_surface_path = $decisionSurfacePath
        certification_artifact_path = $certificationArtifactPath
        fusion_provenance = $fusionProvenanceSummary
        sequence_variant_name = $candidateSequenceVariantName
        lob_variant_name = $candidateLobVariantName
        fusion_variant_name = $candidateFusionVariantName
        fusion_variant_report_path = $candidateVariantReportPath
        promotion_decision = $promotionDecision
        candidate_model_ref_requested = $CandidateModelRef
        candidate_run_id_used_for_backtest = $candidateBacktestModelRef
        candidate_run_id_used_for_paper = $candidatePaperModelRef
        champion_model_ref_requested = $ChampionModelRef
        champion_model_family_used_for_backtest = $resolvedChampionModelFamily
        champion_run_id_used_for_backtest = $championBacktestModelRef
        champion_before_run_id = [string](Get-PropValue -ObjectValue $championBefore -Name "run_id" -DefaultValue "")
        duplicate_candidate = (To-Bool (Get-PropValue -ObjectValue $duplicateCandidateArtifacts -Name "duplicate" -DefaultValue $false) $false)
        duplicate_artifacts = $duplicateCandidateArtifacts
        dependency_trainers = @((Get-PropValue -ObjectValue (Get-PropValue -ObjectValue $report.steps -Name "dependency_trainers" -DefaultValue @{}) -Name "results" -DefaultValue @()))
        dependency_trainer_run_ids = @((Get-PropValue -ObjectValue (Get-PropValue -ObjectValue $report.steps -Name "dependency_trainers" -DefaultValue @{}) -Name "run_ids" -DefaultValue @()))
        dependency_trainer_model_families = @((Get-PropValue -ObjectValue (Get-PropValue -ObjectValue $report.steps -Name "dependency_trainers" -DefaultValue @{}) -Name "model_families" -DefaultValue @()))
        dependency_snapshot_id = [string](Get-PropValue -ObjectValue (Get-PropValue -ObjectValue $report.steps -Name "dependency_trainers" -DefaultValue @{}) -Name "same_snapshot_id" -DefaultValue "")
        dependency_snapshot_id_consistent = [bool](Get-PropValue -ObjectValue (Get-PropValue -ObjectValue $report.steps -Name "dependency_trainers" -DefaultValue @{}) -Name "snapshot_id_consistent" -DefaultValue $false)
        snapshot_chain_consistent = ([string]::IsNullOrWhiteSpace([string](Get-PropValue -ObjectValue (Get-PropValue -ObjectValue $report.steps -Name "dependency_trainers" -DefaultValue @{}) -Name "same_snapshot_id" -DefaultValue "")) -or ([string](Get-PropValue -ObjectValue (Get-PropValue -ObjectValue $report.steps -Name "dependency_trainers" -DefaultValue @{}) -Name "same_snapshot_id" -DefaultValue "") -eq $candidateSnapshotId))
        fusion_dependency_inputs = $fusionDependencyInputs
        fusion_dependency_runtime_inputs = $fusionDependencyRuntimeInputs
        dependency_runtime_common_markets = @((Get-PropValue -ObjectValue $dependencyRuntimeUniverse -Name "common_markets" -DefaultValue @()))
        common_runtime_universe_id = [string](Get-PropValue -ObjectValue $commonRuntimeUniverseArtifact -Name "common_runtime_universe_id" -DefaultValue "")
        common_runtime_universe_path = $commonRuntimeUniverseArtifactPath
    }
    if ((-not $DryRun) -and (-not [string]::IsNullOrWhiteSpace($certificationArtifactPath))) {
        $certificationArtifact.sequence_variant_name = $candidateSequenceVariantName
        $certificationArtifact.lob_variant_name = $candidateLobVariantName
        $certificationArtifact.fusion_variant_name = $candidateFusionVariantName
        $certificationArtifact.fusion_variant_report_path = $candidateVariantReportPath
        $certificationArtifact.provenance.sequence_variant_name = $candidateSequenceVariantName
        $certificationArtifact.provenance.lob_variant_name = $candidateLobVariantName
        $certificationArtifact.provenance.fusion_variant_name = $candidateFusionVariantName
        $certificationArtifact.provenance.fusion_variant_report_path = $candidateVariantReportPath
        Write-JsonFile -PathValue $certificationArtifactPath -Payload $certificationArtifact
    }
    Sync-SplitPolicyState
    $report.steps.train.fusion_run_id = $candidateRunId
    $report.steps.train.fusion_snapshot_id = $candidateSnapshotId
    $report.steps.train.fusion_dependency_inputs = $fusionDependencyInputs
    $report.steps.train.fusion_dependency_runtime_inputs = $fusionDependencyRuntimeInputs
    $report.steps.train.dependency_runtime_common_markets = @((Get-PropValue -ObjectValue $dependencyRuntimeUniverse -Name "common_markets" -DefaultValue @()))
    $report.steps.train.common_runtime_universe_id = [string](Get-PropValue -ObjectValue $commonRuntimeUniverseArtifact -Name "common_runtime_universe_id" -DefaultValue "")
    $report.steps.train.common_runtime_universe_path = $commonRuntimeUniverseArtifactPath
    $report.steps.train.dependency_trainer_run_ids = @((Get-PropValue -ObjectValue $report.candidate -Name "dependency_trainer_run_ids" -DefaultValue @()))
    $report.steps.train.dependency_trainer_model_families = @((Get-PropValue -ObjectValue $report.candidate -Name "dependency_trainer_model_families" -DefaultValue @()))
    $report.steps.train.dependency_snapshot_id = [string](Get-PropValue -ObjectValue $report.candidate -Name "dependency_snapshot_id" -DefaultValue "")
    $report.steps.train.snapshot_chain_consistent = [bool](Get-PropValue -ObjectValue $report.candidate -Name "snapshot_chain_consistent" -DefaultValue $true)
    $report.steps.train.fusion_provenance = $fusionProvenanceSummary
    $report.steps.train.sequence_variant_name = $candidateSequenceVariantName
    $report.steps.train.lob_variant_name = $candidateLobVariantName
    $report.steps.train.fusion_variant_name = $candidateFusionVariantName
    $report.steps.train.fusion_variant_report_path = $candidateVariantReportPath
    $report.steps.train.duplicate_candidate = (To-Bool (Get-PropValue -ObjectValue $duplicateCandidateArtifacts -Name "duplicate" -DefaultValue $false) $false)
    $report.steps.train.duplicate_candidate_artifacts = $duplicateCandidateArtifacts

    if ($useFusionVariantMatrix) {
        $report.steps.fusion_variant_selection = Test-VariantSelectionStep -TrainerName "v5_fusion" -ResultObject ([ordered]@{
            run_dir = $candidateRunDir
            variant_metadata = $candidateVariantMetadata
        }) -FailureCode "FUSION_VARIANT_SELECTION_INCOMPLETE"
        if (-not [bool](Get-PropValue -ObjectValue $report.steps.fusion_variant_selection -Name "pass" -DefaultValue $false)) {
            $variantReasons = @((Get-PropValue -ObjectValue $report.steps.fusion_variant_selection -Name "reasons" -DefaultValue @()))
            $report.reasons = @($variantReasons)
            $report.gates.overall_pass = $false
            $variantFailureCode = if (@($variantReasons).Count -gt 0) { [string]$variantReasons[0] } else { "FUSION_VARIANT_SELECTION_INCOMPLETE" }
            Set-ReportFailure -Stage "fusion_train" -Code $variantFailureCode -ReportPath $candidateRunDir
            $paths = Save-Report
            Write-ReportPointers -LogTag $LogTag -Paths $paths -OverallPass $false
            exit 2
        }
    } else {
        $report.steps.fusion_variant_selection = [ordered]@{ attempted = $false; reason = "SKIPPED_BY_FLAG" }
    }

    if ((@($DependencyTrainers).Count -gt 0) -and (-not [bool](Get-PropValue -ObjectValue $report.candidate -Name "dependency_snapshot_id_consistent" -DefaultValue $false))) {
        $report.reasons = @("DEPENDENCY_TRAINER_SNAPSHOT_CHAIN_INCONSISTENT")
        $report.gates.overall_pass = $false
        Set-ReportFailure -Stage "runtime_export" -Code "DEPENDENCY_TRAINER_SNAPSHOT_CHAIN_INCONSISTENT"
        Update-RunArtifactStatus `
            -RunDir $candidateRunDir `
            -RunId $candidateRunId `
            -Status "acceptance_incomplete" `
            -AcceptanceCompleted $false `
            -CandidateAdoptable $false `
            -CandidateAdopted $false `
            -Promoted $false | Out-Null
        $paths = Save-Report
        Write-ReportPointers -LogTag $LogTag -Paths $paths -OverallPass $false
        exit 2
    }
    if ((@($DependencyTrainers).Count -gt 0) -and (-not [bool](Get-PropValue -ObjectValue $report.candidate -Name "snapshot_chain_consistent" -DefaultValue $true))) {
        $report.reasons = @("FUSION_SNAPSHOT_ID_MISMATCH")
        $report.gates.overall_pass = $false
        Set-ReportFailure -Stage "fusion_train" -Code "FUSION_SNAPSHOT_ID_MISMATCH" -ReportPath $candidateRunDir
        Update-RunArtifactStatus `
            -RunDir $candidateRunDir `
            -RunId $candidateRunId `
            -Status "acceptance_incomplete" `
            -AcceptanceCompleted $false `
            -CandidateAdoptable $false `
            -CandidateAdopted $false `
            -Promoted $false | Out-Null
        $paths = Save-Report
        Write-ReportPointers -LogTag $LogTag -Paths $paths -OverallPass $false
        exit 2
    }

    $requiresV5FusionProvenance = [string]::Equals(([string]$Trainer).Trim(), "v5_fusion", [System.StringComparison]::OrdinalIgnoreCase)
    if ($requiresV5FusionProvenance) {
        $missingFusionProvenanceReasons = New-Object System.Collections.Generic.List[string]
        if ([string]::IsNullOrWhiteSpace($candidateSequenceVariantName)) {
            $missingFusionProvenanceReasons.Add("SEQUENCE_VARIANT_NAME_MISSING") | Out-Null
        }
        if ([string]::IsNullOrWhiteSpace($sequencePretrainMethod)) {
            $missingFusionProvenanceReasons.Add("SEQUENCE_PRETRAIN_METHOD_MISSING") | Out-Null
        }
        if ([string]::IsNullOrWhiteSpace($sequencePretrainStatus)) {
            $missingFusionProvenanceReasons.Add("SEQUENCE_PRETRAIN_STATUS_MISSING") | Out-Null
        }
        if ([string]::IsNullOrWhiteSpace($sequencePretrainObjective)) {
            $missingFusionProvenanceReasons.Add("SEQUENCE_PRETRAIN_OBJECTIVE_MISSING") | Out-Null
        }
        if ([string]::IsNullOrWhiteSpace($sequencePretrainContractPath) -or (-not (Test-Path $sequencePretrainContractPath))) {
            $missingFusionProvenanceReasons.Add("SEQUENCE_PRETRAIN_CONTRACT_PATH_MISSING") | Out-Null
        }
        if ([string]::IsNullOrWhiteSpace($sequencePretrainReportPath) -or (-not (Test-Path $sequencePretrainReportPath))) {
            $missingFusionProvenanceReasons.Add("SEQUENCE_PRETRAIN_REPORT_PATH_MISSING") | Out-Null
        }
        if ([string]::IsNullOrWhiteSpace($candidateLobVariantName)) {
            $missingFusionProvenanceReasons.Add("LOB_VARIANT_NAME_MISSING") | Out-Null
        }
        if ([string]::IsNullOrWhiteSpace($sequenceBackboneName)) {
            $missingFusionProvenanceReasons.Add("SEQUENCE_BACKBONE_NAME_MISSING") | Out-Null
        }
        if ([string]::IsNullOrWhiteSpace($lobBackboneName)) {
            $missingFusionProvenanceReasons.Add("LOB_BACKBONE_NAME_MISSING") | Out-Null
        }
        if ([string]::IsNullOrWhiteSpace($tradabilitySourceRunId)) {
            $missingFusionProvenanceReasons.Add("TRADABILITY_SOURCE_RUN_ID_MISSING") | Out-Null
        }
        if ([string]::IsNullOrWhiteSpace($fusionStackerFamily)) {
            $missingFusionProvenanceReasons.Add("FUSION_STACKER_FAMILY_MISSING") | Out-Null
        }
        if ([string]::Equals($fusionStackerFamily, "regime_moe", [System.StringComparison]::OrdinalIgnoreCase)) {
            if ([string]::IsNullOrWhiteSpace($fusionGatingPolicy)) {
                $missingFusionProvenanceReasons.Add("FUSION_GATING_POLICY_MISSING") | Out-Null
            }
            if ($fusionRegimeClusterCount -le 0) {
                $missingFusionProvenanceReasons.Add("FUSION_REGIME_CLUSTER_COUNT_INVALID") | Out-Null
            }
        }
        if (Test-IsEffectivelyEmptyObject -ObjectValue $fusionTradabilityInput) {
            $missingFusionProvenanceReasons.Add("FUSION_TRADABILITY_INPUT_MISSING") | Out-Null
        }
        if ([string]::IsNullOrWhiteSpace([string](Get-PropValue -ObjectValue $fusionTradabilityInput -Name "run_id" -DefaultValue ""))) {
            $missingFusionProvenanceReasons.Add("FUSION_TRADABILITY_INPUT_RUN_ID_MISSING") | Out-Null
        }
        if ([string]::IsNullOrWhiteSpace($domainWeightingPolicy)) {
            $missingFusionProvenanceReasons.Add("DOMAIN_WEIGHTING_REPORT_MISSING") | Out-Null
        }
        if ([string]::IsNullOrWhiteSpace($domainWeightingSourceKind)) {
            $missingFusionProvenanceReasons.Add("DOMAIN_WEIGHTING_SOURCE_KIND_MISSING") | Out-Null
        }
        if ($useFusionVariantMatrix -and ([string]::IsNullOrWhiteSpace($candidateVariantReportPath) -or (-not (Test-Path $candidateVariantReportPath)))) {
            $missingFusionProvenanceReasons.Add("FUSION_VARIANT_REPORT_MISSING") | Out-Null
        }
        if ((@($missingFusionProvenanceReasons)).Count -gt 0) {
            $report.steps.train.fusion_provenance.required = $true
            $report.steps.train.fusion_provenance.pass = $false
            $report.steps.train.fusion_provenance.reasons = @($missingFusionProvenanceReasons)
            $report.reasons = @("V5_FUSION_PROVENANCE_INCOMPLETE")
            $report.gates.overall_pass = $false
            Set-ReportFailure -Stage "fusion_train" -Code "V5_FUSION_PROVENANCE_INCOMPLETE" -ReportPath $candidateRunDir
            Update-RunArtifactStatus `
                -RunDir $candidateRunDir `
                -RunId $candidateRunId `
                -Status "acceptance_incomplete" `
                -AcceptanceCompleted $false `
                -CandidateAdoptable $false `
                -CandidateAdopted $false `
                -Promoted $false | Out-Null
            $paths = Save-Report
            Write-ReportPointers -LogTag $LogTag -Paths $paths -OverallPass $false
            exit 2
        }
        $report.steps.train.fusion_provenance.required = $true
        $report.steps.train.fusion_provenance.pass = $true
        $report.steps.train.fusion_provenance.reasons = @()
    }

    $isDuplicateCandidate = To-Bool (Get-PropValue -ObjectValue $duplicateCandidateArtifacts -Name "duplicate" -DefaultValue $false) $false
    if (([string]$Trainer).Trim().ToLowerInvariant() -eq "v5_fusion" -and (-not $isDuplicateCandidate)) {
        $runtimeCoveragePreflightEligible = Test-ShouldAttemptRuntimeDatasetCoveragePreflight `
            -TrainerName $Trainer `
            -CertificationStartDate $certificationStartDate `
            -CertificationEndDate $effectiveBatchDate `
            -LaneMode ([string](Get-PropValue -ObjectValue $report.split_policy -Name "lane_mode" -DefaultValue ""))
        if ($DryRun) {
            $report.steps.runtime_dataset_coverage_preflight = [ordered]@{
                attempted = $false
                reason = "DRY_RUN_SKIP"
            }
        } elseif ($runtimeCoveragePreflightEligible) {
            $runtimeCoverage = Resolve-CandidateRuntimeDatasetCoverage -PythonPath $resolvedPythonExe -CandidateRunDir $candidateRunDir
            $runtimeCoverageGate = Test-CandidateRuntimeDatasetCertificationCoverage `
                -Coverage $runtimeCoverage `
                -CertificationStartDate $certificationStartDate `
                -CertificationEndDate $effectiveBatchDate
            $report.steps.runtime_dataset_coverage_preflight = [ordered]@{
                attempted = $true
                contract_path = [string](Get-PropValue -ObjectValue $runtimeCoverage -Name "contract_path" -DefaultValue "")
                dataset_root = [string](Get-PropValue -ObjectValue $runtimeCoverage -Name "dataset_root" -DefaultValue "")
                rows = [int](Get-PropValue -ObjectValue $runtimeCoverage -Name "rows" -DefaultValue 0)
                coverage_start_ts_ms = [int64](Get-PropValue -ObjectValue $runtimeCoverage -Name "coverage_start_ts_ms" -DefaultValue 0)
                coverage_end_ts_ms = [int64](Get-PropValue -ObjectValue $runtimeCoverage -Name "coverage_end_ts_ms" -DefaultValue 0)
                requested_start_ts_ms = [int64](Get-PropValue -ObjectValue $runtimeCoverage -Name "requested_start_ts_ms" -DefaultValue 0)
                requested_end_ts_ms = [int64](Get-PropValue -ObjectValue $runtimeCoverage -Name "requested_end_ts_ms" -DefaultValue 0)
                inspect_command = [string](Get-PropValue -ObjectValue $runtimeCoverage -Name "inspect_command" -DefaultValue "")
                inspect_output_preview = [string](Get-PropValue -ObjectValue $runtimeCoverage -Name "inspect_output_preview" -DefaultValue "")
                actual_dataset_rows = [int](Get-PropValue -ObjectValue $runtimeCoverage -Name "actual_dataset_rows" -DefaultValue 0)
                actual_dataset_min_ts_ms = Get-PropValue -ObjectValue $runtimeCoverage -Name "actual_dataset_min_ts_ms" -DefaultValue $null
                actual_dataset_max_ts_ms = Get-PropValue -ObjectValue $runtimeCoverage -Name "actual_dataset_max_ts_ms" -DefaultValue $null
                manifest_path = [string](Get-PropValue -ObjectValue $runtimeCoverage -Name "manifest_path" -DefaultValue "")
                data_file_count = [int](Get-PropValue -ObjectValue $runtimeCoverage -Name "data_file_count" -DefaultValue 0)
                pass = [bool](Get-PropValue -ObjectValue $runtimeCoverageGate -Name "pass" -DefaultValue $false)
                reason = [string](Get-PropValue -ObjectValue $runtimeCoverageGate -Name "reason" -DefaultValue "")
            }
            if (-not [bool](Get-PropValue -ObjectValue $runtimeCoverageGate -Name "pass" -DefaultValue $false)) {
                $report.reasons = @([string](Get-PropValue -ObjectValue $runtimeCoverageGate -Name "reason" -DefaultValue "CANDIDATE_RUNTIME_DATASET_CERTIFICATION_WINDOW_EMPTY"))
                $report.gates.overall_pass = $false
                Set-ReportFailure `
                    -Stage "acceptance_gate" `
                    -Code ([string](Get-PropValue -ObjectValue $runtimeCoverageGate -Name "reason" -DefaultValue "CANDIDATE_RUNTIME_DATASET_CERTIFICATION_WINDOW_EMPTY")) `
                    -ReportPath ([string](Get-PropValue -ObjectValue $runtimeCoverage -Name "contract_path" -DefaultValue ""))
                Update-RunArtifactStatus `
                    -RunDir $candidateRunDir `
                    -RunId $candidateRunId `
                    -Status "acceptance_incomplete" `
                    -AcceptanceCompleted $false `
                    -CandidateAdoptable $false `
                    -CandidateAdopted $false `
                    -Promoted $false | Out-Null
                $paths = Save-Report
                Write-ReportPointers -LogTag $LogTag -Paths $paths -OverallPass $false
                exit 2
            }
        } else {
            $report.steps.runtime_dataset_coverage_preflight = [ordered]@{
                attempted = $false
                reason = "BOOTSTRAP_OR_NO_CERTIFICATION_WINDOW"
            }
        }
    } elseif (([string]$Trainer).Trim().ToLowerInvariant() -eq "v5_fusion") {
        $report.steps.runtime_dataset_coverage_preflight = [ordered]@{
            attempted = $false
            reason = "DUPLICATE_CANDIDATE"
        }
    } else {
        $report.steps.runtime_dataset_coverage_preflight = [ordered]@{
            attempted = $false
            reason = "NOT_REQUIRED_FOR_TRAINER"
        }
    }

    if (([string]$Trainer).Trim().ToLowerInvariant() -eq "v5_fusion" -and (-not $isDuplicateCandidate)) {
        $runtimeViability = Resolve-CandidateRuntimeViabilityArtifact -CandidateRunDir $candidateRunDir
        $runtimeViabilityGate = Test-CandidateRuntimeViability -Viability $runtimeViability
        $report.steps.runtime_viability_preflight = [ordered]@{
            attempted = $true
            report_path = [string](Get-PropValue -ObjectValue $runtimeViability -Name "report_path" -DefaultValue "")
            exists = [bool](Get-PropValue -ObjectValue $runtimeViability -Name "exists" -DefaultValue $false)
            alpha_lcb_floor = To-Double (Get-PropValue -ObjectValue $runtimeViability -Name "alpha_lcb_floor" -DefaultValue 0.0) 0.0
            runtime_rows_total = [int](To-Int64 (Get-PropValue -ObjectValue $runtimeViability -Name "runtime_rows_total" -DefaultValue 0) 0)
            mean_final_expected_return = To-Double (Get-PropValue -ObjectValue $runtimeViability -Name "mean_final_expected_return" -DefaultValue 0.0) 0.0
            mean_final_expected_es = To-Double (Get-PropValue -ObjectValue $runtimeViability -Name "mean_final_expected_es" -DefaultValue 0.0) 0.0
            mean_final_uncertainty = To-Double (Get-PropValue -ObjectValue $runtimeViability -Name "mean_final_uncertainty" -DefaultValue 0.0) 0.0
            mean_final_alpha_lcb = To-Double (Get-PropValue -ObjectValue $runtimeViability -Name "mean_final_alpha_lcb" -DefaultValue 0.0) 0.0
            alpha_lcb_positive_count = [int](To-Int64 (Get-PropValue -ObjectValue $runtimeViability -Name "alpha_lcb_positive_count" -DefaultValue 0) 0)
            rows_above_alpha_floor = [int](To-Int64 (Get-PropValue -ObjectValue $runtimeViability -Name "rows_above_alpha_floor" -DefaultValue 0) 0)
            rows_above_alpha_floor_ratio = To-Double (Get-PropValue -ObjectValue $runtimeViability -Name "rows_above_alpha_floor_ratio" -DefaultValue 0.0) 0.0
            entry_gate_allowed_count = [int](To-Int64 (Get-PropValue -ObjectValue $runtimeViability -Name "entry_gate_allowed_count" -DefaultValue 0) 0)
            entry_gate_allowed_ratio = To-Double (Get-PropValue -ObjectValue $runtimeViability -Name "entry_gate_allowed_ratio" -DefaultValue 0.0) 0.0
            estimated_intent_candidate_count = [int](To-Int64 (Get-PropValue -ObjectValue $runtimeViability -Name "estimated_intent_candidate_count" -DefaultValue 0) 0)
            top_entry_gate_reason_codes = @((Get-PropValue -ObjectValue $runtimeViability -Name "top_entry_gate_reason_codes" -DefaultValue @()))
            sample_rows = @((Get-PropValue -ObjectValue $runtimeViability -Name "sample_rows" -DefaultValue @()))
            pass = [bool](Get-PropValue -ObjectValue $runtimeViabilityGate -Name "pass" -DefaultValue $false)
            reason = [string](Get-PropValue -ObjectValue $runtimeViabilityGate -Name "reason" -DefaultValue "")
        }
        $report.candidate.runtime_viability_report_path = [string](Get-PropValue -ObjectValue $runtimeViability -Name "report_path" -DefaultValue "")
        $report.candidate.runtime_viability_pass = [bool](Get-PropValue -ObjectValue $runtimeViabilityGate -Name "pass" -DefaultValue $false)
        $report.candidate.runtime_viability_summary = [ordered]@{
            alpha_lcb_floor = To-Double (Get-PropValue -ObjectValue $runtimeViability -Name "alpha_lcb_floor" -DefaultValue 0.0) 0.0
            runtime_rows_total = [int](To-Int64 (Get-PropValue -ObjectValue $runtimeViability -Name "runtime_rows_total" -DefaultValue 0) 0)
            mean_final_expected_return = To-Double (Get-PropValue -ObjectValue $runtimeViability -Name "mean_final_expected_return" -DefaultValue 0.0) 0.0
            mean_final_expected_es = To-Double (Get-PropValue -ObjectValue $runtimeViability -Name "mean_final_expected_es" -DefaultValue 0.0) 0.0
            mean_final_uncertainty = To-Double (Get-PropValue -ObjectValue $runtimeViability -Name "mean_final_uncertainty" -DefaultValue 0.0) 0.0
            mean_final_alpha_lcb = To-Double (Get-PropValue -ObjectValue $runtimeViability -Name "mean_final_alpha_lcb" -DefaultValue 0.0) 0.0
            alpha_lcb_positive_count = [int](To-Int64 (Get-PropValue -ObjectValue $runtimeViability -Name "alpha_lcb_positive_count" -DefaultValue 0) 0)
            rows_above_alpha_floor = [int](To-Int64 (Get-PropValue -ObjectValue $runtimeViability -Name "rows_above_alpha_floor" -DefaultValue 0) 0)
            rows_above_alpha_floor_ratio = To-Double (Get-PropValue -ObjectValue $runtimeViability -Name "rows_above_alpha_floor_ratio" -DefaultValue 0.0) 0.0
            expected_return_positive_count = [int](To-Int64 (Get-PropValue -ObjectValue $runtimeViability -Name "expected_return_positive_count" -DefaultValue 0) 0)
            entry_gate_allowed_count = [int](To-Int64 (Get-PropValue -ObjectValue $runtimeViability -Name "entry_gate_allowed_count" -DefaultValue 0) 0)
            entry_gate_allowed_ratio = To-Double (Get-PropValue -ObjectValue $runtimeViability -Name "entry_gate_allowed_ratio" -DefaultValue 0.0) 0.0
            estimated_intent_candidate_count = [int](To-Int64 (Get-PropValue -ObjectValue $runtimeViability -Name "estimated_intent_candidate_count" -DefaultValue 0) 0)
            primary_reason_code = [string](Get-PropValue -ObjectValue $runtimeViability -Name "primary_reason_code" -DefaultValue "")
            top_entry_gate_reason_codes = @((Get-PropValue -ObjectValue $runtimeViability -Name "top_entry_gate_reason_codes" -DefaultValue @()))
            sample_rows = @((Get-PropValue -ObjectValue $runtimeViability -Name "sample_rows" -DefaultValue @()))
        }
        if (-not [bool](Get-PropValue -ObjectValue $runtimeViabilityGate -Name "pass" -DefaultValue $false)) {
            $runtimeViabilityFailureCode = [string](Get-PropValue -ObjectValue $runtimeViabilityGate -Name "reason" -DefaultValue "FUSION_RUNTIME_ALPHA_LCB_ZERO_VIABILITY")
            $report.reasons = @($runtimeViabilityFailureCode)
            $report.gates.overall_pass = $false
            Set-ReportFailure -Stage "runtime_viability" -Code $runtimeViabilityFailureCode -ReportPath ([string](Get-PropValue -ObjectValue $runtimeViability -Name "report_path" -DefaultValue ""))
            Update-RunArtifactStatus `
                -RunDir $candidateRunDir `
                -RunId $candidateRunId `
                -Status "acceptance_incomplete" `
                -AcceptanceCompleted $false `
                -CandidateAdoptable $false `
                -CandidateAdopted $false `
                -Promoted $false | Out-Null
            $paths = Save-Report
            Write-ReportPointers -LogTag $LogTag -Paths $paths -OverallPass $false
            exit 2
        }
    } elseif (([string]$Trainer).Trim().ToLowerInvariant() -eq "v5_fusion") {
        $report.steps.runtime_viability_preflight = [ordered]@{
            attempted = $false
            reason = "DUPLICATE_CANDIDATE"
        }
    } else {
        $report.steps.runtime_viability_preflight = [ordered]@{
            attempted = $false
            reason = "NOT_REQUIRED_FOR_TRAINER"
        }
    }

    if ($isDuplicateCandidate) {
        $report.steps.backtest_candidate = [ordered]@{
            attempted = $false
            reason = "DUPLICATE_CANDIDATE"
            model_ref_requested = $CandidateModelRef
            model_ref_used = $candidateBacktestModelRef
        }
        $report.steps.backtest_champion = [ordered]@{
            attempted = $false
            reason = "DUPLICATE_CANDIDATE"
            model_ref_requested = $ChampionModelRef
            model_ref_used = $championBacktestModelRef
        }
        $report.steps.paper_candidate = [ordered]@{
            attempted = $false
            reason = "DUPLICATE_CANDIDATE"
            model_ref_requested = $CandidateModelRef
            model_ref_used = $candidatePaperModelRef
        }
        $report.steps.overlay_calibration = [ordered]@{
            attempted = $false
            reason = "DUPLICATE_CANDIDATE"
        }
        $report.gates.backtest = [ordered]@{
            evaluated = $false
            skipped = $true
            pass = $false
            decision_basis = "DUPLICATE_CANDIDATE"
            compare_required = [bool]$promotionPolicyConfig.backtest_compare_required
            duplicate_candidate = $true
            duplicate_artifacts = $duplicateCandidateArtifacts
        }
        $report.gates.paper = [ordered]@{
            evaluated = $false
            skipped = $true
            pass = $null
            reason = "DUPLICATE_CANDIDATE"
        }
        $report.gates.overall_pass = $false
        $report.reasons = @("DUPLICATE_CANDIDATE")
        $report.steps.promote = [ordered]@{
            attempted = $false
            promoted = $false
            reason = "DUPLICATE_CANDIDATE"
        }
        $report.steps.restart_units = @()
        $report.steps.report_refresh = [ordered]@{
            attempted = $false
            reason = "DUPLICATE_CANDIDATE"
        }
        $report.candidate.champion_after_run_id = $championRunId
        if ((-not $DryRun) -and (-not [string]::IsNullOrWhiteSpace($certificationArtifactPath))) {
            $certificationArtifact.status = "skipped"
            $certificationArtifact.certification = [ordered]@{
                evaluated = $false
                decision_basis = "DUPLICATE_CANDIDATE"
                duplicate_candidate = $true
            }
            Write-JsonFile -PathValue $certificationArtifactPath -Payload $certificationArtifact
        }
        Update-RunArtifactStatus `
            -RunDir $candidateRunDir `
            -RunId $candidateRunId `
            -Status "duplicate_candidate" `
            -AcceptanceCompleted $true `
            -CandidateAdoptable $false `
            -CandidateAdopted $false `
            -Promoted $false | Out-Null
        $paths = Save-Report
        Write-Host ("[{0}] candidate_run_id={1}" -f $LogTag, $candidateRunId)
        Write-Host ("[{0}] duplicate_candidate={1}" -f $LogTag, $true)
        Write-Host ("[{0}] backtest_pass={1}" -f $LogTag, $false)
        Write-Host ("[{0}] paper_pass={1}" -f $LogTag, $null)
        Write-ReportPointers -LogTag $LogTag -Paths $paths -OverallPass $false
        if ($DryRun) {
            exit 0
        }
        exit 2
    }

    if ([string](Get-PropValue -ObjectValue $report.split_policy -Name "lane_mode" -DefaultValue "") -eq "bootstrap_latest_inclusive") {
        $report.steps.backtest_candidate = [ordered]@{
            attempted = $false
            reason = "BOOTSTRAP_ONLY_POLICY"
            model_ref_requested = $CandidateModelRef
            model_ref_used = $candidateBacktestModelRef
            start = $certificationStartDate
            end = $effectiveBatchDate
        }
        $report.steps.backtest_champion = [ordered]@{
            attempted = $false
            reason = "BOOTSTRAP_ONLY_POLICY"
            model_ref_requested = $ChampionModelRef
            model_ref_used = $championBacktestModelRef
            start = $certificationStartDate
            end = $effectiveBatchDate
        }
        $report.steps.paper_candidate = [ordered]@{
            attempted = $false
            reason = "BOOTSTRAP_ONLY_POLICY"
            model_ref_requested = $CandidateModelRef
            model_ref_used = $candidatePaperModelRef
        }
        $report.steps.overlay_calibration = [ordered]@{
            attempted = $false
            reason = "BOOTSTRAP_ONLY_POLICY"
        }
        $report.gates.backtest = [ordered]@{
            evaluated = $false
            skipped = $true
            pass = $false
            reason = "BOOTSTRAP_ONLY_POLICY"
            decision_basis = "BOOTSTRAP_ONLY_POLICY"
            compare_required = $false
            lane_mode = "bootstrap_latest_inclusive"
            promotion_eligible = $false
        }
        $report.gates.paper = [ordered]@{
            evaluated = $false
            skipped = $true
            pass = $null
            reason = "BOOTSTRAP_ONLY_POLICY"
        }
        $report.gates.overall_pass = $false
        $report.reasons = @("BOOTSTRAP_ONLY_POLICY")
        $report.notes = @("BOOTSTRAP_LATEST_INCLUSIVE")
        $report.steps.promote = [ordered]@{
            attempted = $false
            promoted = $false
            reason = "BOOTSTRAP_ONLY_POLICY"
        }
        $report.steps.restart_units = @()
        $report.steps.report_refresh = if ($SkipReportRefresh) {
            [ordered]@{ attempted = $false; reason = "SKIPPED_BY_FLAG" }
        } else {
            [ordered]@{ attempted = $false; reason = "BOOTSTRAP_ONLY_POLICY" }
        }
        if ((-not $DryRun) -and (-not [string]::IsNullOrWhiteSpace($certificationArtifactPath))) {
            $certificationArtifact.status = "bootstrap_only"
            $certificationArtifact.certification = [ordered]@{
                evaluated = $false
                decision_basis = "BOOTSTRAP_ONLY_POLICY"
                lane_mode = "bootstrap_latest_inclusive"
                promotion_eligible = $false
            }
            Write-JsonFile -PathValue $certificationArtifactPath -Payload $certificationArtifact
        }
        Update-RunArtifactStatus `
            -RunDir $candidateRunDir `
            -RunId $candidateRunId `
            -Status "bootstrap_only" `
            -AcceptanceCompleted $true `
            -CandidateAdoptable $false `
            -CandidateAdopted $false `
            -Promoted $false | Out-Null
        $paths = Save-Report
        Write-Host ("[{0}] candidate_run_id={1}" -f $LogTag, $candidateRunId)
        Write-Host ("[{0}] backtest_pass={1}" -f $LogTag, $false)
        Write-Host ("[{0}] paper_pass={1}" -f $LogTag, $null)
        Write-ReportPointers -LogTag $LogTag -Paths $paths -OverallPass $false
        if ($DryRun) {
            exit 0
        }
        exit 2
    }

    $candidateBacktest = Invoke-OrReuse-AcceptanceBacktest `
        -PythonPath $resolvedPythonExe `
        -Root $resolvedProjectRoot `
        -RegistryRoot $resolvedRegistryRoot `
        -StepName "backtest_candidate" `
        -ModelRef $candidateBacktestModelRef `
        -ModelFamilyName $ModelFamily `
        -ModelRunDir $candidateRunDir `
        -StartDate $certificationStartDate `
        -EndDate $effectiveBatchDate `
        -Preset "acceptance"
    $candidateSummary = Get-PropValue -ObjectValue $candidateBacktest -Name "Summary" -DefaultValue @{}
    $candidateStatValidation = Invoke-OrReuse-AcceptanceStatValidation `
        -BacktestValue $candidateBacktest `
        -PythonPath $resolvedPythonExe `
        -Root $resolvedProjectRoot `
        -TrialCount $BoosterSweepTrials `
        -ModelRunDir $candidateRunDir
    $candidateEvidence = Build-BacktestEvidenceFromSummary -Summary $candidateSummary -StatValidation $candidateStatValidation
    $candidateOrdersSubmitted = [int64](Get-PropValue -ObjectValue $candidateEvidence -Name "orders_submitted" -DefaultValue 0)
    $candidateOrdersFilled = [int64](Get-PropValue -ObjectValue $candidateEvidence -Name "orders_filled" -DefaultValue 0)
    $candidateRealizedPnl = [double](Get-PropValue -ObjectValue $candidateEvidence -Name "realized_pnl_quote" -DefaultValue 0.0)
    $candidateFillRate = [double](Get-PropValue -ObjectValue $candidateEvidence -Name "fill_rate" -DefaultValue -1.0)
    $candidateMaxDrawdownPct = [double](Get-PropValue -ObjectValue $candidateEvidence -Name "max_drawdown_pct" -DefaultValue -1.0)
    $candidateSlippageBpsMean = Get-PropValue -ObjectValue $candidateEvidence -Name "slippage_bps_mean" -DefaultValue $null
    $candidateAbortedByPolicy = [int64](Get-PropValue -ObjectValue $candidateEvidence -Name "candidates_aborted_by_policy" -DefaultValue 0)
    $candidateExecutionPolicyVetoFailure = [bool](Get-PropValue -ObjectValue $candidateEvidence -Name "execution_policy_veto_failure" -DefaultValue $false)
    $candidateCalmarLikeScore = Get-PropValue -ObjectValue $candidateEvidence -Name "calmar_like_score" -DefaultValue $null
    $candidateExecutionStructure = Get-ExecutionStructureMetrics -Summary $candidateSummary
    $candidateExecutionStructureGate = Test-ExecutionStructureGate `
        -Metrics $candidateExecutionStructure `
        -MinPayoffRatio $BacktestMinPayoffRatio `
        -MaxLossConcentration $BacktestMaxLossConcentration `
        -MinClosedTrades $ExecutionStructureMinClosedTrades
    $candidateDeflatedSharpeRatio = [double](Get-PropValue -ObjectValue $candidateEvidence -Name "deflated_sharpe_ratio_est" -DefaultValue 0.0)
    $candidateProbabilisticSharpeRatio = [double](Get-PropValue -ObjectValue $candidateEvidence -Name "probabilistic_sharpe_ratio" -DefaultValue 0.0)
    $candidateStatComparable = [bool](Get-PropValue -ObjectValue $candidateEvidence -Name "stat_comparable" -DefaultValue $false)

    $championBacktest = $null
    $championSummary = @{}
    $championRealizedPnl = 0.0
    $championFillRate = -1.0
    $championMaxDrawdownPct = -1.0
    $championSlippageBpsMean = $null
    $championCalmarLikeScore = $null
    $championCompareEvaluated = $false
    $championStatValidation = @{}
    $championDeflatedSharpeRatio = $null
    $championProbabilisticSharpeRatio = $null
    $compareRequired = [bool]$promotionPolicyConfig.backtest_compare_required -and (-not $SkipChampionCompare)
    if ((-not $SkipChampionCompare) -and (-not [string]::IsNullOrWhiteSpace($championRunId))) {
        $championBacktest = Invoke-OrReuse-AcceptanceBacktest `
            -PythonPath $resolvedPythonExe `
            -Root $resolvedProjectRoot `
            -RegistryRoot $resolvedRegistryRoot `
            -StepName "backtest_champion" `
            -ModelRef $championBacktestModelRef `
            -ModelFamilyName $resolvedChampionModelFamily `
            -ModelRunDir $championModelRunDir `
            -StartDate $certificationStartDate `
            -EndDate $effectiveBatchDate `
            -Preset "acceptance"
        $championSummary = Get-PropValue -ObjectValue $championBacktest -Name "Summary" -DefaultValue @{}
        $championStatValidation = Invoke-OrReuse-AcceptanceStatValidation `
            -BacktestValue $championBacktest `
            -PythonPath $resolvedPythonExe `
            -Root $resolvedProjectRoot `
            -TrialCount $BoosterSweepTrials `
            -ModelRunDir $championModelRunDir
        $championEvidence = Build-BacktestEvidenceFromSummary -Summary $championSummary -StatValidation $championStatValidation
        $championRealizedPnl = [double](Get-PropValue -ObjectValue $championEvidence -Name "realized_pnl_quote" -DefaultValue 0.0)
        $championFillRate = [double](Get-PropValue -ObjectValue $championEvidence -Name "fill_rate" -DefaultValue -1.0)
        $championMaxDrawdownPct = [double](Get-PropValue -ObjectValue $championEvidence -Name "max_drawdown_pct" -DefaultValue -1.0)
        $championSlippageBpsMean = Get-PropValue -ObjectValue $championEvidence -Name "slippage_bps_mean" -DefaultValue $null
        $championCalmarLikeScore = Get-PropValue -ObjectValue $championEvidence -Name "calmar_like_score" -DefaultValue $null
        $championExecutionStructure = Get-ExecutionStructureMetrics -Summary $championSummary
        $championDeflatedSharpeRatio = Get-PropValue -ObjectValue $championEvidence -Name "deflated_sharpe_ratio_est" -DefaultValue $null
        $championProbabilisticSharpeRatio = Get-PropValue -ObjectValue $championEvidence -Name "probabilistic_sharpe_ratio" -DefaultValue $null
        $championCompareEvaluated = $true
    }

    $candidateDeflatedSharpePass = (-not $candidateStatComparable) -or ($candidateDeflatedSharpeRatio -ge [double]$promotionPolicyConfig.backtest_min_deflated_sharpe_ratio)
    $candidateBacktestPass = ($candidateOrdersFilled -ge [int]$promotionPolicyConfig.backtest_min_orders_filled) -and ($candidateRealizedPnl -ge [double]$promotionPolicyConfig.backtest_min_realized_pnl_quote) -and $candidateDeflatedSharpePass -and (To-Bool (Get-PropValue -ObjectValue $candidateExecutionStructureGate -Name "pass" -DefaultValue $true) $true)
    $championDeltaPass = $true
    $strictChampionDeltaPass = $true
    $championDeltaQuote = $candidateRealizedPnl - $championRealizedPnl
    $championDeltaPct = $null
    $championWithinTolerancePass = $true
    $drawdownImprovementPct = $null
    $drawdownImprovementPass = $true
    $fillRateDegradation = $null
    $fillRateGuardPass = $true
    $slippageDeteriorationBps = $null
    $slippageGuardPass = $true
    $utilityMetric = "calmar_like"
    $candidateUtilityScore = $null
    $championUtilityScore = $null
    $utilityDeltaPct = $null
    $utilityTieBreakPass = $false
    $candidateParetoDominates = $false
    $championParetoDominates = $false
    $paretoIncomparable = $false
    $stabilityOverridePass = $false
    $trainerEvidenceApplied = $TrainerEvidenceMode -ne "ignore"
    $budgetContractApplied = -not (Test-IsEffectivelyEmptyObject -ObjectValue $searchBudgetDecision)
    $budgetLaneClassRequested = if ($budgetContractApplied) {
        [string](Get-PropValue -ObjectValue $searchBudgetDecision -Name "lane_class_requested" -DefaultValue "")
    } else {
        ""
    }
    $budgetLaneClassEffective = if ($budgetContractApplied) {
        [string](Get-PropValue -ObjectValue $searchBudgetDecision -Name "lane_class_effective" -DefaultValue "")
    } else {
        ""
    }
    $budgetContractId = if ($budgetContractApplied) {
        [string](Get-PropValue -ObjectValue $searchBudgetDecision -Name "budget_contract_id" -DefaultValue "")
    } else {
        ""
    }
    $promotionEligibleContract = if ($budgetContractApplied) {
        Get-PropValue -ObjectValue $searchBudgetDecision -Name "promotion_eligible_contract" -DefaultValue @{}
    } else {
        @{}
    }
    $budgetPromotionEligibleSatisfied = if ($budgetContractApplied) {
        To-Bool (Get-PropValue -ObjectValue $promotionEligibleContract -Name "satisfied" -DefaultValue $false) $false
    } else {
        $true
    }
    $budgetContractGatePass = if ($budgetContractApplied) {
        $budgetLaneClassEffective -eq "promotion_eligible"
    } else {
        $true
    }
    $budgetContractReasons = @()
    if ($budgetContractApplied -and (-not $budgetContractGatePass)) {
        $budgetContractReasons += "SCOUT_ONLY_BUDGET_EVIDENCE"
    }
    $economicObjectiveProfileApplied = -not (Test-IsEffectivelyEmptyObject -ObjectValue $economicObjectiveProfile)
    $economicObjectiveProfileId = if ($economicObjectiveProfileApplied) {
        [string](Get-PropValue -ObjectValue $economicObjectiveProfile -Name "profile_id" -DefaultValue "")
    } else {
        ""
    }
    $promotionCompareProfile = if ($economicObjectiveProfileApplied) {
        Get-PropValue -ObjectValue $economicObjectiveProfile -Name "promotion_compare" -DefaultValue @{}
    } else {
        @{}
    }
    $promotionParetoHigherMetrics = Convert-ToStringArray (Get-PropValue -ObjectValue $promotionCompareProfile -Name "pareto_higher_is_better" -DefaultValue @())
    if ($promotionParetoHigherMetrics.Count -le 0) {
        $promotionParetoHigherMetrics = @("realized_pnl_quote", "fill_rate")
    }
    $promotionParetoLowerMetrics = Convert-ToStringArray (Get-PropValue -ObjectValue $promotionCompareProfile -Name "pareto_lower_is_better" -DefaultValue @())
    if ($promotionParetoLowerMetrics.Count -le 0) {
        $promotionParetoLowerMetrics = @("max_drawdown_pct", "slippage_bps_mean")
    }
    $utilityMetric = [string](Get-PropValue -ObjectValue $promotionCompareProfile -Name "utility_metric" -DefaultValue "calmar_like")
    if ([string]::IsNullOrWhiteSpace($utilityMetric)) {
        $utilityMetric = "calmar_like"
    }
    $decisionBasis = if ($championCompareEvaluated) { "PENDING_COMPARE" } elseif ($SkipChampionCompare) { "SKIPPED_CHAMPION_COMPARE" } else { "NO_EXISTING_CHAMPION" }
    if ($championCompareEvaluated) {
        $strictChampionDeltaPass = $championDeltaQuote -ge [double]$promotionPolicyConfig.backtest_min_pnl_delta_vs_champion
        $championWithinTolerancePass = $strictChampionDeltaPass
        if ($championRealizedPnl -ne 0.0) {
            $championDeltaPct = $championDeltaQuote / $championRealizedPnl
            $championWithinTolerancePass = $championDeltaPct -ge (-1.0 * [double]$promotionPolicyConfig.backtest_champion_pnl_tolerance_pct)
        }
        if (($candidateMaxDrawdownPct -ge 0.0) -and ($championMaxDrawdownPct -ge 0.0)) {
            if ($championMaxDrawdownPct -gt 0.0) {
                $drawdownImprovementPct = ($championMaxDrawdownPct - $candidateMaxDrawdownPct) / $championMaxDrawdownPct
                $drawdownImprovementPass = $drawdownImprovementPct -ge [double]$promotionPolicyConfig.backtest_champion_min_drawdown_improvement_pct
            } else {
                $drawdownImprovementPct = 0.0
                $drawdownImprovementPass = $candidateMaxDrawdownPct -le $championMaxDrawdownPct
            }
        } else {
            $drawdownImprovementPass = $false
        }
        if (($candidateFillRate -ge 0.0) -and ($championFillRate -ge 0.0)) {
            $fillRateDegradation = $championFillRate - $candidateFillRate
            $fillRateGuardPass = $fillRateDegradation -le [double]$promotionPolicyConfig.backtest_champion_max_fill_rate_degradation
        }
        if (($null -ne $candidateSlippageBpsMean) -and ($null -ne $championSlippageBpsMean)) {
            $slippageDeteriorationBps = $candidateSlippageBpsMean - $championSlippageBpsMean
            $slippageGuardPass = $slippageDeteriorationBps -le [double]$promotionPolicyConfig.backtest_champion_max_slippage_deterioration_bps
        }
        $candidateCompareMetrics = Build-BacktestCompareMetrics -Evidence $candidateEvidence
        $championCompareMetrics = Build-BacktestCompareMetrics -Evidence $championEvidence
        if ($promotionPolicyConfig.use_pareto) {
            $paretoComparableMetricCount = 0
            $candidateWorseOnAny = $false
            $candidateBetterOnAny = $false
            $championWorseOnAny = $false
            $championBetterOnAny = $false

            foreach ($metricName in $promotionParetoHigherMetrics) {
                $candidateMetric = if ($candidateCompareMetrics.ContainsKey($metricName)) { $candidateCompareMetrics[$metricName] } else { $null }
                $championMetric = if ($championCompareMetrics.ContainsKey($metricName)) { $championCompareMetrics[$metricName] } else { $null }
                if (($null -eq $candidateMetric) -or ($null -eq $championMetric)) {
                    continue
                }
                $paretoComparableMetricCount += 1
                if ($candidateMetric -lt $championMetric) { $candidateWorseOnAny = $true }
                if ($candidateMetric -gt $championMetric) { $candidateBetterOnAny = $true }
                if ($championMetric -lt $candidateMetric) { $championWorseOnAny = $true }
                if ($championMetric -gt $candidateMetric) { $championBetterOnAny = $true }
            }
            foreach ($metricName in $promotionParetoLowerMetrics) {
                $candidateMetric = if ($candidateCompareMetrics.ContainsKey($metricName)) { $candidateCompareMetrics[$metricName] } else { $null }
                $championMetric = if ($championCompareMetrics.ContainsKey($metricName)) { $championCompareMetrics[$metricName] } else { $null }
                if (($null -eq $candidateMetric) -or ($null -eq $championMetric)) {
                    continue
                }
                $paretoComparableMetricCount += 1
                if ($candidateMetric -gt $championMetric) { $candidateWorseOnAny = $true }
                if ($candidateMetric -lt $championMetric) { $candidateBetterOnAny = $true }
                if ($championMetric -gt $candidateMetric) { $championWorseOnAny = $true }
                if ($championMetric -lt $candidateMetric) { $championBetterOnAny = $true }
            }

            if ($paretoComparableMetricCount -gt 0) {
                $candidateParetoDominates = (-not $candidateWorseOnAny) -and $candidateBetterOnAny
                $championParetoDominates = (-not $championWorseOnAny) -and $championBetterOnAny
                $paretoIncomparable = (-not $candidateParetoDominates) -and (-not $championParetoDominates)
            }
        }

        $candidateUtilityScore = if ($candidateCompareMetrics.ContainsKey($utilityMetric)) { $candidateCompareMetrics[$utilityMetric] } else { $null }
        $championUtilityScore = if ($championCompareMetrics.ContainsKey($utilityMetric)) { $championCompareMetrics[$utilityMetric] } else { $null }
        $utilityTieBreakPass = $false
        if ($promotionPolicyConfig.use_utility_tie_break -and ($null -ne $candidateUtilityScore) -and ($null -ne $championUtilityScore)) {
            if ((-not [double]::IsInfinity($championUtilityScore)) -and ($championUtilityScore -ne 0.0)) {
                $utilityDeltaPct = ($candidateUtilityScore - $championUtilityScore) / [Math]::Abs($championUtilityScore)
                $utilityTieBreakPass = $utilityDeltaPct -ge [double]$promotionPolicyConfig.backtest_champion_min_utility_edge_pct
            } else {
                $utilityTieBreakPass = $candidateUtilityScore -ge $championUtilityScore
            }
        }
        $stabilityOverridePass = [bool]$promotionPolicyConfig.backtest_allow_stability_override `
            -and $championWithinTolerancePass `
            -and $drawdownImprovementPass `
            -and $fillRateGuardPass `
            -and $slippageGuardPass `
            -and $utilityTieBreakPass `
            -and (-not $championParetoDominates)

        switch ($promotionPolicyConfig.name) {
            "strict" {
                $championDeltaPass = $strictChampionDeltaPass
                $decisionBasis = if ($championDeltaPass) { "STRICT_PNL_PASS" } else { "STRICT_PNL_FAIL" }
            }
            "conservative_pareto" {
                if ($candidateParetoDominates) {
                    $championDeltaPass = $true
                    $decisionBasis = "PARETO_DOMINANCE"
                } elseif ($championParetoDominates) {
                    $championDeltaPass = $false
                    $decisionBasis = "CHAMPION_PARETO_DOMINATES"
                } elseif ($strictChampionDeltaPass) {
                    $championDeltaPass = $true
                    $decisionBasis = "STRICT_PNL_PASS"
                } else {
                    $championDeltaPass = $stabilityOverridePass
                    $decisionBasis = if ($championDeltaPass) { "UTILITY_TIE_BREAK_PASS" } else { "UTILITY_TIE_BREAK_FAIL" }
                }
            }
            default {
                if ($candidateParetoDominates) {
                    $championDeltaPass = $true
                    $decisionBasis = "PARETO_DOMINANCE"
                } elseif ($championParetoDominates) {
                    $championDeltaPass = $false
                    $decisionBasis = "CHAMPION_PARETO_DOMINATES"
                } elseif ($strictChampionDeltaPass) {
                    $championDeltaPass = $true
                    $decisionBasis = "STRICT_PNL_PASS"
                } else {
                    $championDeltaPass = $stabilityOverridePass
                    $decisionBasis = if ($championDeltaPass) { "UTILITY_TIE_BREAK_PASS" } else { "UTILITY_TIE_BREAK_FAIL" }
                }
            }
        }
    }
    $certificationResearchEvidence = New-CertificationResearchEvidence `
        -CertificationArtifact $certificationArtifact `
        -TrainerResearchPrior $trainerResearchPrior `
        -CertificationStartDate $certificationStartDate `
        -CertificationEndDate $effectiveBatchDate `
        -PromotionPolicyName ([string]$promotionPolicyConfig.name) `
        -EconomicObjectiveProfileId $economicObjectiveProfileId `
        -CandidateBacktestPresent ($null -ne $candidateBacktest) `
        -CandidateOrdersFilled $candidateOrdersFilled `
        -CandidateRealizedPnl $candidateRealizedPnl `
        -CandidateMinOrdersPass ($candidateOrdersFilled -ge [int]$promotionPolicyConfig.backtest_min_orders_filled) `
        -CandidateMinOrdersThreshold ([int]$promotionPolicyConfig.backtest_min_orders_filled) `
        -CandidateMinRealizedPnlPass ($candidateRealizedPnl -ge [double]$promotionPolicyConfig.backtest_min_realized_pnl_quote) `
        -CandidateMinRealizedPnlThreshold ([double]$promotionPolicyConfig.backtest_min_realized_pnl_quote) `
        -CandidateDsrEvaluated $candidateStatComparable `
        -CandidateDeflatedSharpeRatio ([double]$candidateDeflatedSharpeRatio) `
        -CandidateMinDeflatedSharpeRatio ([double]$promotionPolicyConfig.backtest_min_deflated_sharpe_ratio) `
        -CandidateDeflatedSharpePass $candidateDeflatedSharpePass `
        -CandidateBacktestPass $candidateBacktestPass `
        -CompareRequired $compareRequired `
        -ChampionPresent (-not [string]::IsNullOrWhiteSpace($championRunId)) `
        -ChampionCompareEvaluated $championCompareEvaluated `
        -ChampionComparePass $championDeltaPass `
        -CandidateParetoDominates $candidateParetoDominates `
        -ChampionParetoDominates $championParetoDominates `
        -ParetoIncomparable $paretoIncomparable `
        -DecisionBasis $decisionBasis
    $certificationArtifact.research_evidence = $certificationResearchEvidence
    $trainerEvidence = Resolve-TrainerEvidenceFromCertificationArtifact -CertificationArtifact $certificationArtifact -Mode $TrainerEvidenceMode
    $report.steps.train.trainer_evidence = $trainerEvidence
    $trainerEvidenceAvailable = To-Bool (Get-PropValue -ObjectValue $trainerEvidence -Name "available" -DefaultValue $false) $false
    $trainerEvidencePass = To-Bool (Get-PropValue -ObjectValue $trainerEvidence -Name "pass" -DefaultValue $false) $false
    $trainerEvidenceOfflinePass = To-Bool (Get-PropValue -ObjectValue $trainerEvidence -Name "offline_pass" -DefaultValue $false) $false
    $trainerEvidenceExecutionPass = To-Bool (Get-PropValue -ObjectValue $trainerEvidence -Name "execution_pass" -DefaultValue $true) $true
    $trainerEvidenceGatePass = if ($TrainerEvidenceMode -eq "required") { $trainerEvidencePass } else { $true }
    $trainerEvidenceReasons = @(Get-PropValue -ObjectValue $trainerEvidence -Name "reasons" -DefaultValue @())
    $backtestPass = if ($compareRequired) {
        $candidateBacktestPass -and $championDeltaPass -and $trainerEvidenceGatePass -and $budgetContractGatePass
    } else {
        $candidateBacktestPass -and $trainerEvidenceGatePass -and $budgetContractGatePass
    }
    if ((-not $compareRequired) -and $decisionBasis -eq "PENDING_COMPARE") {
        $decisionBasis = "SANITY_ONLY_BACKTEST"
    }
    if ($candidateExecutionPolicyVetoFailure) {
        $decisionBasis = "EXECUTION_POLICY_VETO_FAILURE"
    } elseif (($TrainerEvidenceMode -eq "required") -and (-not $trainerEvidenceGatePass)) {
        $decisionBasis = "TRAINER_EVIDENCE_REQUIRED_FAIL"
    } elseif (-not (To-Bool (Get-PropValue -ObjectValue $candidateExecutionStructureGate -Name "pass" -DefaultValue $true) $true)) {
        $decisionBasis = "EXECUTION_STRUCTURE_FAIL"
    } elseif (-not $budgetContractGatePass) {
        $decisionBasis = "SCOUT_ONLY_BUDGET_EVIDENCE"
    }
    $report.steps.backtest_candidate = Build-BacktestStepReport `
        -StepName "backtest_candidate" `
        -BacktestValue $candidateBacktest `
        -StartDate $certificationStartDate `
        -EndDate $effectiveBatchDate `
        -ModelRefRequested $CandidateModelRef `
        -ModelRefUsed $candidateBacktestModelRef `
        -Preset "acceptance" `
        -Evidence $candidateEvidence `
        -ExtraFields ([ordered]@{
            avg_time_to_fill_ms = [double](To-Double (Get-PropValue -ObjectValue $candidateSummary -Name "avg_time_to_fill_ms" -DefaultValue 0.0) 0.0)
            p50_time_to_fill_ms = [double](To-Double (Get-PropValue -ObjectValue $candidateSummary -Name "p50_time_to_fill_ms" -DefaultValue 0.0) 0.0)
            p90_time_to_fill_ms = [double](To-Double (Get-PropValue -ObjectValue $candidateSummary -Name "p90_time_to_fill_ms" -DefaultValue 0.0) 0.0)
            execution_structure = (Get-PropValue -ObjectValue $candidateExecutionStructure -Name "payload" -DefaultValue @{})
        })
    $report.steps.backtest_champion = if ($championCompareEvaluated) {
        Build-BacktestStepReport `
            -StepName "backtest_champion" `
            -BacktestValue $championBacktest `
            -StartDate $certificationStartDate `
            -EndDate $effectiveBatchDate `
            -ModelRefRequested $ChampionModelRef `
            -ModelRefUsed $championBacktestModelRef `
            -Preset "acceptance" `
            -Evidence $championEvidence `
            -ExtraFields ([ordered]@{
                avg_time_to_fill_ms = [double](To-Double (Get-PropValue -ObjectValue $championSummary -Name "avg_time_to_fill_ms" -DefaultValue 0.0) 0.0)
                p50_time_to_fill_ms = [double](To-Double (Get-PropValue -ObjectValue $championSummary -Name "p50_time_to_fill_ms" -DefaultValue 0.0) 0.0)
                p90_time_to_fill_ms = [double](To-Double (Get-PropValue -ObjectValue $championSummary -Name "p90_time_to_fill_ms" -DefaultValue 0.0) 0.0)
                execution_structure = (Get-PropValue -ObjectValue $championExecutionStructure -Name "payload" -DefaultValue @{})
            })
    } elseif ($SkipChampionCompare) {
        [ordered]@{ attempted = $false; reason = "SKIPPED_BY_FLAG" }
    } else {
        [ordered]@{ attempted = $false; reason = "NO_EXISTING_CHAMPION" }
    }
    $report.gates.backtest = [ordered]@{
        promotion_policy_contract_source = [string]$promotionPolicyConfig.threshold_source
        promotion_policy_contract_profile_id = [string]$promotionPolicyConfig.profile_id
        promotion_policy_cli_override_keys = @($promotionPolicyConfig.cli_override_keys)
        candidate_min_orders_pass = ($candidateOrdersFilled -ge [int]$promotionPolicyConfig.backtest_min_orders_filled)
        candidate_min_orders_threshold = [int]$promotionPolicyConfig.backtest_min_orders_filled
        candidate_orders_submitted = [int64]$candidateOrdersSubmitted
        candidate_candidates_aborted_by_policy = [int64]$candidateAbortedByPolicy
        candidate_execution_policy_veto_failure = $candidateExecutionPolicyVetoFailure
        candidate_min_realized_pnl_pass = ($candidateRealizedPnl -ge [double]$promotionPolicyConfig.backtest_min_realized_pnl_quote)
        candidate_min_realized_pnl_threshold = [double]$promotionPolicyConfig.backtest_min_realized_pnl_quote
        candidate_dsr_evaluated = $candidateStatComparable
        candidate_min_deflated_sharpe_ratio = [double]$promotionPolicyConfig.backtest_min_deflated_sharpe_ratio
        candidate_deflated_sharpe_ratio_est = [double]$candidateDeflatedSharpeRatio
        candidate_deflated_sharpe_pass = $candidateDeflatedSharpePass
        candidate_execution_structure_evaluated = (To-Bool (Get-PropValue -ObjectValue $candidateExecutionStructureGate -Name "evaluated" -DefaultValue $false) $false)
        candidate_payoff_ratio = [double](Get-PropValue -ObjectValue $candidateExecutionStructureGate -Name "payoff_ratio" -DefaultValue 0.0)
        candidate_payoff_ratio_pass = (To-Bool (Get-PropValue -ObjectValue $candidateExecutionStructureGate -Name "payoff_ratio_pass" -DefaultValue $true) $true)
        candidate_payoff_ratio_threshold = [double](Get-PropValue -ObjectValue $candidateExecutionStructureGate -Name "payoff_ratio_threshold" -DefaultValue 0.0)
        candidate_market_loss_concentration = [double](Get-PropValue -ObjectValue $candidateExecutionStructureGate -Name "market_loss_concentration" -DefaultValue 0.0)
        candidate_market_loss_concentration_pass = (To-Bool (Get-PropValue -ObjectValue $candidateExecutionStructureGate -Name "market_loss_concentration_pass" -DefaultValue $true) $true)
        candidate_market_loss_concentration_threshold = [double](Get-PropValue -ObjectValue $candidateExecutionStructureGate -Name "market_loss_concentration_threshold" -DefaultValue 0.0)
        candidate_execution_structure_reasons = @((Get-PropValue -ObjectValue $candidateExecutionStructureGate -Name "reasons" -DefaultValue @()))
        compare_required = $compareRequired
        vs_champion_evaluated = $championCompareEvaluated
        vs_champion_pass = $championDeltaPass
        vs_champion_strict_pnl_pass = $strictChampionDeltaPass
        vs_champion_pareto_candidate_dominates = $candidateParetoDominates
        vs_champion_pareto_champion_dominates = $championParetoDominates
        vs_champion_pareto_incomparable = $paretoIncomparable
        vs_champion_pnl_delta_quote = [double]$championDeltaQuote
        vs_champion_pnl_delta_threshold = [double]$promotionPolicyConfig.backtest_min_pnl_delta_vs_champion
        vs_champion_pnl_delta_pct = $championDeltaPct
        vs_champion_pnl_within_tolerance_pass = $championWithinTolerancePass
        vs_champion_drawdown_improvement_pct = $drawdownImprovementPct
        vs_champion_drawdown_improvement_pass = $drawdownImprovementPass
        vs_champion_drawdown_improvement_threshold = [double]$promotionPolicyConfig.backtest_champion_min_drawdown_improvement_pct
        utility_metric = $utilityMetric
        vs_champion_utility_candidate_score = $candidateUtilityScore
        vs_champion_utility_champion_score = $championUtilityScore
        vs_champion_utility_delta_pct = $utilityDeltaPct
        vs_champion_utility_tie_break_pass = $utilityTieBreakPass
        vs_champion_fill_rate_degradation = $fillRateDegradation
        vs_champion_fill_rate_guard_pass = $fillRateGuardPass
        vs_champion_slippage_deterioration_bps = $slippageDeteriorationBps
        vs_champion_slippage_guard_pass = $slippageGuardPass
        vs_champion_stability_override_pass = $stabilityOverridePass
        economic_objective_profile_applied = $economicObjectiveProfileApplied
        economic_objective_profile_id = $economicObjectiveProfileId
        trainer_evidence_applied = $trainerEvidenceApplied
        trainer_evidence_available = $trainerEvidenceAvailable
        trainer_evidence_pass = $trainerEvidencePass
        trainer_evidence_gate_pass = $trainerEvidenceGatePass
        trainer_evidence_offline_pass = $trainerEvidenceOfflinePass
        trainer_evidence_execution_pass = $trainerEvidenceExecutionPass
        trainer_evidence_reasons = @($trainerEvidenceReasons)
        budget_contract_applied = $budgetContractApplied
        budget_contract_id = $budgetContractId
        budget_lane_class_requested = $budgetLaneClassRequested
        budget_lane_class_effective = $budgetLaneClassEffective
        budget_promotion_eligible_satisfied = $budgetPromotionEligibleSatisfied
        budget_contract_gate_pass = $budgetContractGatePass
        budget_contract_reasons = @($budgetContractReasons)
        lane_governance_path = $laneGovernancePath
        lane_id = $laneId
        lane_role = $laneRole
        lane_shadow_only = $laneShadowOnly
        lane_promotion_allowed = $lanePromotionAllowed
        lane_governance_reasons = @($laneGovernanceReasons)
        certification_artifact_path = $certificationArtifactPath
        certification_window_start = $certificationStartDate
        certification_window_end = $effectiveBatchDate
        certification_window_valid = (To-Bool (Get-PropValue -ObjectValue $trainerEvidence -Name "certification_window_valid" -DefaultValue $false) $false)
        certification_window_reasons = @((Get-PropValue -ObjectValue $trainerEvidence -Name "certification_window_reasons" -DefaultValue @()))
        decision_basis = $decisionBasis
        compare_mode = $promotionPolicyConfig.name
        pass = $backtestPass
    }
    if ((-not $DryRun) -and (-not [string]::IsNullOrWhiteSpace($certificationArtifactPath))) {
        $certificationArtifact.status = "evaluated"
        $certificationArtifact.certification = [ordered]@{
            evaluated = $true
            candidate_backtest = $report.steps.backtest_candidate
            champion_backtest = $report.steps.backtest_champion
            gate = $report.gates.backtest
        }
        Write-JsonFile -PathValue $certificationArtifactPath -Payload $certificationArtifact
    }

    $runtimeParityPass = $true
    $runtimeParityCandidateExecutionPolicyVetoFailure = $false
    $runtimeParityCandidateOrdersSubmitted = [int64]0
    $runtimeParityCandidateAbortedByPolicy = [int64]0
    if (-not $BacktestRuntimeParityEnabled) {
        $report.steps.backtest_runtime_parity_candidate = [ordered]@{
            attempted = $false
            reason = "SKIPPED_BY_FLAG"
        }
        $report.steps.backtest_runtime_parity_champion = [ordered]@{
            attempted = $false
            reason = "SKIPPED_BY_FLAG"
        }
        $report.gates.runtime_parity = [ordered]@{
            evaluated = $false
            skipped = $true
            required = $false
            pass = $null
        }
    } else {
        $runtimeParityCandidateBacktest = Invoke-OrReuse-AcceptanceBacktest `
            -PythonPath $resolvedPythonExe `
            -Root $resolvedProjectRoot `
            -RegistryRoot $resolvedRegistryRoot `
            -StepName "backtest_runtime_parity_candidate" `
            -ModelRef $candidateBacktestModelRef `
            -ModelFamilyName $ModelFamily `
            -ModelRunDir $candidateRunDir `
            -StartDate $certificationStartDate `
            -EndDate $effectiveBatchDate `
            -Preset "runtime_parity"
        $runtimeParityCandidateSummary = Get-PropValue -ObjectValue $runtimeParityCandidateBacktest -Name "Summary" -DefaultValue @{}
        $runtimeParityCandidateStatValidation = Invoke-OrReuse-AcceptanceStatValidation `
            -BacktestValue $runtimeParityCandidateBacktest `
            -PythonPath $resolvedPythonExe `
            -Root $resolvedProjectRoot `
            -TrialCount $BoosterSweepTrials `
            -ModelRunDir $candidateRunDir
        $runtimeParityCandidateEvidence = Build-BacktestEvidenceFromSummary -Summary $runtimeParityCandidateSummary -StatValidation $runtimeParityCandidateStatValidation
        $runtimeParityCandidateOrdersSubmitted = [int64](Get-PropValue -ObjectValue $runtimeParityCandidateEvidence -Name "orders_submitted" -DefaultValue 0)
        $runtimeParityCandidateOrdersFilled = [int64](Get-PropValue -ObjectValue $runtimeParityCandidateEvidence -Name "orders_filled" -DefaultValue 0)
        $runtimeParityCandidateRealizedPnl = [double](Get-PropValue -ObjectValue $runtimeParityCandidateEvidence -Name "realized_pnl_quote" -DefaultValue 0.0)
        $runtimeParityCandidateFillRate = [double](Get-PropValue -ObjectValue $runtimeParityCandidateEvidence -Name "fill_rate" -DefaultValue -1.0)
        $runtimeParityCandidateMaxDrawdownPct = [double](Get-PropValue -ObjectValue $runtimeParityCandidateEvidence -Name "max_drawdown_pct" -DefaultValue -1.0)
        $runtimeParityCandidateSlippageBpsMean = Get-PropValue -ObjectValue $runtimeParityCandidateEvidence -Name "slippage_bps_mean" -DefaultValue $null
        $runtimeParityCandidateAbortedByPolicy = [int64](Get-PropValue -ObjectValue $runtimeParityCandidateEvidence -Name "candidates_aborted_by_policy" -DefaultValue 0)
        $runtimeParityCandidateExecutionPolicyVetoFailure = [bool](Get-PropValue -ObjectValue $runtimeParityCandidateEvidence -Name "execution_policy_veto_failure" -DefaultValue $false)
        $runtimeParityCandidateCalmarLikeScore = Get-PropValue -ObjectValue $runtimeParityCandidateEvidence -Name "calmar_like_score" -DefaultValue $null
        $runtimeParityCandidateDeflatedSharpeRatio = [double](Get-PropValue -ObjectValue $runtimeParityCandidateEvidence -Name "deflated_sharpe_ratio_est" -DefaultValue 0.0)
        $runtimeParityCandidateProbabilisticSharpeRatio = [double](Get-PropValue -ObjectValue $runtimeParityCandidateEvidence -Name "probabilistic_sharpe_ratio" -DefaultValue 0.0)
        $runtimeParityCandidateStatComparable = [bool](Get-PropValue -ObjectValue $runtimeParityCandidateEvidence -Name "stat_comparable" -DefaultValue $false)
        $report.steps.backtest_runtime_parity_candidate = Build-BacktestStepReport `
            -StepName "backtest_runtime_parity_candidate" `
            -BacktestValue $runtimeParityCandidateBacktest `
            -StartDate $certificationStartDate `
            -EndDate $effectiveBatchDate `
            -ModelRefRequested $CandidateModelRef `
            -ModelRefUsed $candidateBacktestModelRef `
            -Preset "runtime_parity" `
            -Evidence $runtimeParityCandidateEvidence

        $runtimeParityCompareRequired = [bool]$promotionPolicyConfig.backtest_compare_required -and (-not $SkipChampionCompare)
        $runtimeParityChampionCompareEvaluated = $false
        $runtimeParityChampionRealizedPnl = 0.0
        $runtimeParityChampionFillRate = -1.0
        $runtimeParityChampionMaxDrawdownPct = -1.0
        $runtimeParityChampionSlippageBpsMean = $null
        $runtimeParityChampionCalmarLikeScore = $null
        $runtimeParityChampionDeflatedSharpeRatio = $null
        $runtimeParityChampionProbabilisticSharpeRatio = $null
        $runtimeParityChampionStatValidation = @{}
        if ((-not $SkipChampionCompare) -and (-not [string]::IsNullOrWhiteSpace($championRunId))) {
            $runtimeParityChampionBacktest = Invoke-OrReuse-AcceptanceBacktest `
                -PythonPath $resolvedPythonExe `
                -Root $resolvedProjectRoot `
                -RegistryRoot $resolvedRegistryRoot `
                -StepName "backtest_runtime_parity_champion" `
                -ModelRef $championBacktestModelRef `
                -ModelFamilyName $resolvedChampionModelFamily `
                -ModelRunDir $championModelRunDir `
                -StartDate $certificationStartDate `
                -EndDate $effectiveBatchDate `
                -Preset "runtime_parity"
            $runtimeParityChampionSummary = Get-PropValue -ObjectValue $runtimeParityChampionBacktest -Name "Summary" -DefaultValue @{}
            $runtimeParityChampionStatValidation = Invoke-OrReuse-AcceptanceStatValidation `
                -BacktestValue $runtimeParityChampionBacktest `
                -PythonPath $resolvedPythonExe `
                -Root $resolvedProjectRoot `
                -TrialCount $BoosterSweepTrials `
                -ModelRunDir $championModelRunDir
            $runtimeParityChampionEvidence = Build-BacktestEvidenceFromSummary -Summary $runtimeParityChampionSummary -StatValidation $runtimeParityChampionStatValidation
            $runtimeParityChampionRealizedPnl = [double](Get-PropValue -ObjectValue $runtimeParityChampionEvidence -Name "realized_pnl_quote" -DefaultValue 0.0)
            $runtimeParityChampionFillRate = [double](Get-PropValue -ObjectValue $runtimeParityChampionEvidence -Name "fill_rate" -DefaultValue -1.0)
            $runtimeParityChampionMaxDrawdownPct = [double](Get-PropValue -ObjectValue $runtimeParityChampionEvidence -Name "max_drawdown_pct" -DefaultValue -1.0)
            $runtimeParityChampionSlippageBpsMean = Get-PropValue -ObjectValue $runtimeParityChampionEvidence -Name "slippage_bps_mean" -DefaultValue $null
            $runtimeParityChampionCalmarLikeScore = Get-PropValue -ObjectValue $runtimeParityChampionEvidence -Name "calmar_like_score" -DefaultValue $null
            $runtimeParityChampionDeflatedSharpeRatio = Get-PropValue -ObjectValue $runtimeParityChampionEvidence -Name "deflated_sharpe_ratio_est" -DefaultValue $null
            $runtimeParityChampionProbabilisticSharpeRatio = Get-PropValue -ObjectValue $runtimeParityChampionEvidence -Name "probabilistic_sharpe_ratio" -DefaultValue $null
            $runtimeParityChampionCompareEvaluated = $true
            $report.steps.backtest_runtime_parity_champion = Build-BacktestStepReport `
                -StepName "backtest_runtime_parity_champion" `
                -BacktestValue $runtimeParityChampionBacktest `
                -StartDate $certificationStartDate `
                -EndDate $effectiveBatchDate `
                -ModelRefRequested $ChampionModelRef `
                -ModelRefUsed $championBacktestModelRef `
                -Preset "runtime_parity" `
                -Evidence $runtimeParityChampionEvidence
        } elseif ($SkipChampionCompare) {
            $report.steps.backtest_runtime_parity_champion = [ordered]@{ attempted = $false; reason = "SKIPPED_BY_FLAG" }
        } else {
            $report.steps.backtest_runtime_parity_champion = [ordered]@{ attempted = $false; reason = "NO_EXISTING_CHAMPION" }
        }

        $runtimeParityCandidateDeflatedSharpePass = (-not $runtimeParityCandidateStatComparable) -or ($runtimeParityCandidateDeflatedSharpeRatio -ge [double]$promotionPolicyConfig.backtest_min_deflated_sharpe_ratio)
        $runtimeParityCandidatePass = ($runtimeParityCandidateOrdersFilled -ge [int]$promotionPolicyConfig.backtest_min_orders_filled) -and ($runtimeParityCandidateRealizedPnl -ge [double]$promotionPolicyConfig.backtest_min_realized_pnl_quote) -and $runtimeParityCandidateDeflatedSharpePass
        $runtimeParityDecisionBasis = if ($runtimeParityChampionCompareEvaluated) { "PENDING_COMPARE" } elseif ($SkipChampionCompare) { "SKIPPED_CHAMPION_COMPARE" } else { "NO_EXISTING_CHAMPION" }
        if ($runtimeParityCandidateExecutionPolicyVetoFailure) {
            $runtimeParityDecisionBasis = "EXECUTION_POLICY_VETO_FAILURE"
        }
        $runtimeParityStrictPnlPass = $true
        $runtimeParityDrawdownImprovementPct = $null
        $runtimeParityDrawdownImprovementPass = $true
        $runtimeParityFillRateDegradation = $null
        $runtimeParityFillRateGuardPass = $true
        $runtimeParitySlippageDeteriorationBps = $null
        $runtimeParitySlippageGuardPass = $true
        $runtimeParityUtilityCandidateScore = $runtimeParityCandidateCalmarLikeScore
        $runtimeParityUtilityChampionScore = $runtimeParityChampionCalmarLikeScore
        $runtimeParityUtilityDeltaPct = $null
        $runtimeParityUtilityTieBreakPass = $false
        $runtimeParityComparePass = $true
        $runtimeParityPnlDeltaQuote = $runtimeParityCandidateRealizedPnl - $runtimeParityChampionRealizedPnl
        if ($runtimeParityChampionCompareEvaluated) {
            $runtimeParityStrictPnlPass = $runtimeParityPnlDeltaQuote -ge [double]$promotionPolicyConfig.backtest_min_pnl_delta_vs_champion
            if (($runtimeParityCandidateMaxDrawdownPct -ge 0.0) -and ($runtimeParityChampionMaxDrawdownPct -ge 0.0)) {
                if ($runtimeParityChampionMaxDrawdownPct -gt 0.0) {
                    $runtimeParityDrawdownImprovementPct = ($runtimeParityChampionMaxDrawdownPct - $runtimeParityCandidateMaxDrawdownPct) / $runtimeParityChampionMaxDrawdownPct
                    $runtimeParityDrawdownImprovementPass = $runtimeParityDrawdownImprovementPct -ge [double]$promotionPolicyConfig.backtest_champion_min_drawdown_improvement_pct
                } else {
                    $runtimeParityDrawdownImprovementPct = 0.0
                    $runtimeParityDrawdownImprovementPass = $runtimeParityCandidateMaxDrawdownPct -le $runtimeParityChampionMaxDrawdownPct
                }
            } else {
                $runtimeParityDrawdownImprovementPass = $false
            }
            if (($runtimeParityCandidateFillRate -ge 0.0) -and ($runtimeParityChampionFillRate -ge 0.0)) {
                $runtimeParityFillRateDegradation = $runtimeParityChampionFillRate - $runtimeParityCandidateFillRate
                $runtimeParityFillRateGuardPass = $runtimeParityFillRateDegradation -le [double]$promotionPolicyConfig.backtest_champion_max_fill_rate_degradation
            }
            if (($null -ne $runtimeParityCandidateSlippageBpsMean) -and ($null -ne $runtimeParityChampionSlippageBpsMean)) {
                $runtimeParitySlippageDeteriorationBps = $runtimeParityCandidateSlippageBpsMean - $runtimeParityChampionSlippageBpsMean
                $runtimeParitySlippageGuardPass = $runtimeParitySlippageDeteriorationBps -le [double]$promotionPolicyConfig.backtest_champion_max_slippage_deterioration_bps
            }
            if (($null -ne $runtimeParityUtilityCandidateScore) -and ($null -ne $runtimeParityUtilityChampionScore)) {
                if ((-not [double]::IsInfinity($runtimeParityUtilityChampionScore)) -and ($runtimeParityUtilityChampionScore -ne 0.0)) {
                    $runtimeParityUtilityDeltaPct = ($runtimeParityUtilityCandidateScore - $runtimeParityUtilityChampionScore) / [Math]::Abs($runtimeParityUtilityChampionScore)
                    $runtimeParityUtilityTieBreakPass = $runtimeParityUtilityDeltaPct -ge [double]$promotionPolicyConfig.backtest_champion_min_utility_edge_pct
                } else {
                    $runtimeParityUtilityTieBreakPass = $runtimeParityUtilityCandidateScore -ge $runtimeParityUtilityChampionScore
                }
            }
            if ($runtimeParityStrictPnlPass) {
                $runtimeParityComparePass = $true
                $runtimeParityDecisionBasis = "STRICT_PNL_PASS"
            } else {
                $runtimeParityComparePass = $runtimeParityDrawdownImprovementPass -and $runtimeParityFillRateGuardPass -and $runtimeParitySlippageGuardPass -and $runtimeParityUtilityTieBreakPass
                $runtimeParityDecisionBasis = if ($runtimeParityComparePass) { "UTILITY_AND_EXECUTION_GUARDS_PASS" } else { "UTILITY_AND_EXECUTION_GUARDS_FAIL" }
            }
        }
        if ($runtimeParityCandidateExecutionPolicyVetoFailure) {
            $runtimeParityDecisionBasis = "EXECUTION_POLICY_VETO_FAILURE"
        }
        $runtimeParityPass = $runtimeParityCandidatePass -and $runtimeParityComparePass
        $report.gates.runtime_parity = [ordered]@{
            evaluated = $true
            skipped = $false
            required = $true
            preset = "runtime_parity"
            candidate_min_orders_pass = ($runtimeParityCandidateOrdersFilled -ge [int]$promotionPolicyConfig.backtest_min_orders_filled)
            candidate_min_orders_threshold = [int]$promotionPolicyConfig.backtest_min_orders_filled
            candidate_orders_submitted = [int64]$runtimeParityCandidateOrdersSubmitted
            candidate_candidates_aborted_by_policy = [int64]$runtimeParityCandidateAbortedByPolicy
            candidate_execution_policy_veto_failure = $runtimeParityCandidateExecutionPolicyVetoFailure
            candidate_min_realized_pnl_pass = ($runtimeParityCandidateRealizedPnl -ge [double]$promotionPolicyConfig.backtest_min_realized_pnl_quote)
            candidate_min_realized_pnl_threshold = [double]$promotionPolicyConfig.backtest_min_realized_pnl_quote
            candidate_dsr_evaluated = $runtimeParityCandidateStatComparable
            candidate_min_deflated_sharpe_ratio = [double]$promotionPolicyConfig.backtest_min_deflated_sharpe_ratio
            candidate_deflated_sharpe_ratio_est = [double]$runtimeParityCandidateDeflatedSharpeRatio
            candidate_deflated_sharpe_pass = $runtimeParityCandidateDeflatedSharpePass
            compare_required = $runtimeParityCompareRequired
            vs_champion_evaluated = $runtimeParityChampionCompareEvaluated
            vs_champion_pnl_delta_quote = [double]$runtimeParityPnlDeltaQuote
            vs_champion_strict_pnl_pass = $runtimeParityStrictPnlPass
            vs_champion_drawdown_improvement_pct = $runtimeParityDrawdownImprovementPct
            vs_champion_drawdown_improvement_pass = $runtimeParityDrawdownImprovementPass
            vs_champion_fill_rate_degradation = $runtimeParityFillRateDegradation
            vs_champion_fill_rate_guard_pass = $runtimeParityFillRateGuardPass
            vs_champion_slippage_deterioration_bps = $runtimeParitySlippageDeteriorationBps
            vs_champion_slippage_guard_pass = $runtimeParitySlippageGuardPass
            utility_metric = "calmar_like"
            vs_champion_utility_candidate_score = $runtimeParityUtilityCandidateScore
            vs_champion_utility_champion_score = $runtimeParityUtilityChampionScore
            vs_champion_utility_delta_pct = $runtimeParityUtilityDeltaPct
            vs_champion_utility_tie_break_pass = $runtimeParityUtilityTieBreakPass
            decision_basis = $runtimeParityDecisionBasis
            pass = $runtimeParityPass
        }
        if ((-not $DryRun) -and (-not [string]::IsNullOrWhiteSpace($certificationArtifactPath))) {
            $certificationArtifact.runtime_parity = [ordered]@{
                evaluated = $true
                candidate_backtest = $report.steps.backtest_runtime_parity_candidate
                champion_backtest = $report.steps.backtest_runtime_parity_champion
                gate = $report.gates.runtime_parity
            }
            Write-JsonFile -PathValue $certificationArtifactPath -Payload $certificationArtifact
        }
    }

    $report.backtest_cache_hits = @(
        @($report.steps.backtest_candidate, $report.steps.backtest_champion) |
            Where-Object { To-Bool (Get-PropValue -ObjectValue $_ -Name "reused" -DefaultValue $false) $false }
    ).Count
    $report.backtest_cache_misses = @(
        @($report.steps.backtest_candidate, $report.steps.backtest_champion) |
            Where-Object {
                (-not (To-Bool (Get-PropValue -ObjectValue $_ -Name "reused" -DefaultValue $false) $false)) `
                    -and (-not [string]::IsNullOrWhiteSpace([string](Get-PropValue -ObjectValue $_ -Name "run_dir" -DefaultValue "")))
            }
    ).Count
    $report.runtime_parity_cache_hits = @(
        @($report.steps.backtest_runtime_parity_candidate, $report.steps.backtest_runtime_parity_champion) |
            Where-Object { To-Bool (Get-PropValue -ObjectValue $_ -Name "reused" -DefaultValue $false) $false }
    ).Count
    $report.runtime_parity_cache_misses = @(
        @($report.steps.backtest_runtime_parity_candidate, $report.steps.backtest_runtime_parity_champion) |
            Where-Object {
                (-not (To-Bool (Get-PropValue -ObjectValue $_ -Name "reused" -DefaultValue $false) $false)) `
                    -and (-not [string]::IsNullOrWhiteSpace([string](Get-PropValue -ObjectValue $_ -Name "run_dir" -DefaultValue "")))
            }
    ).Count

    $paperPass = $null
    $paperSmokeLatestPath = Join-Path $resolvedPaperSmokeOutDir "latest.json"
    $paperSmokeRunPath = ""
    $paperSmokeEffectivePath = ""
    if ($SkipPaperSoak) {
        $report.steps.paper_candidate = [ordered]@{
            attempted = $false
            reason = "SKIPPED_BY_FLAG"
        }
        $report.gates.paper = [ordered]@{
            evaluated = $false
            skipped = $true
            pass = $null
        }
    } else {
        $paperSmokeArgs = @(
            "-NoProfile",
            "-File", $resolvedPaperSmokeScript,
            "-ProjectRoot", $resolvedProjectRoot,
            "-PythonExe", $resolvedPythonExe,
            "-DurationSec", $PaperSoakDurationSec,
            "-Quote", $Quote,
            "-TopN", $BacktestTopN,
            "-MaxFallbackRatio", $PaperMaxFallbackRatio,
            "-MinOrdersSubmitted", $PaperMinOrdersSubmitted,
            "-MinTierCount", $PaperMinTierCount,
            "-MinPolicyEvents", $PaperMinPolicyEvents,
            "-PaperMicroProvider", $PaperMicroProvider,
            "-WarmupSec", $PaperWarmupSec,
            "-WarmupMinTradeEventsPerMarket", $PaperWarmupMinTradeEventsPerMarket,
            "-Strategy", $StrategyName,
            "-Tf", $Tf,
            "-ModelRef", $candidatePaperModelRef,
            "-ModelFamily", $ModelFamily,
            "-FeatureSet", $FeatureSet,
            "-PaperFeatureProvider", $PaperFeatureProvider,
            "-OutDir", $resolvedPaperSmokeOutDir
        )
        if (-not $PaperUseLearnedRuntime) {
            $paperSmokeArgs += @(
                "-TopPct", $BacktestTopPct,
                "-MinProb", $BacktestMinProb,
                "-MinCandsPerTs", $BacktestMinCandidatesPerTs,
                "-ExitMode", "hold",
                "-HoldBars", $HoldBars
            )
        }
        $paperExec = Invoke-CommandCapture -Exe $psExe -ArgList $paperSmokeArgs
        $paperSmokeRunPath = if ($DryRun) { "" } else { Resolve-ReportedJsonPathFromText -TextValue ([string]$paperExec.Output) -LogTag "paper-smoke" }
        $paperSmokeEffectivePath = if ($DryRun) {
            ""
        } elseif (-not [string]::IsNullOrWhiteSpace($paperSmokeRunPath) -and (Test-Path $paperSmokeRunPath)) {
            $paperSmokeRunPath
        } else {
            throw "paper smoke run completed but report path was not reported by stdout or does not exist"
        }
        $paperSmoke = if ($DryRun) { @{} } elseif (-not [string]::IsNullOrWhiteSpace($paperSmokeEffectivePath)) { Load-JsonOrEmpty -PathValue $paperSmokeEffectivePath } else { @{} }
        $paperT15GatePass = To-Bool (Get-PropValue -ObjectValue (Get-PropValue -ObjectValue $paperSmoke -Name "gates" -DefaultValue @{}) -Name "t15_gate_pass" -DefaultValue $false) $false
        $paperOrdersFilled = [int64](To-Int64 (Get-PropValue -ObjectValue $paperSmoke -Name "orders_filled" -DefaultValue 0) 0)
        $paperRealizedPnl = [double](To-Double (Get-PropValue -ObjectValue $paperSmoke -Name "realized_pnl_quote" -DefaultValue 0.0) 0.0)
        $paperMicroQualityScoreMean = [double](To-Double (Get-PropValue -ObjectValue $paperSmoke -Name "micro_quality_score_mean" -DefaultValue 0.0) 0.0)
        $paperActiveWindows = [int64](To-Int64 (Get-PropValue -ObjectValue $paperSmoke -Name "rolling_active_windows" -DefaultValue 0) 0)
        $paperNonnegativeWindowRatio = [double](To-Double (Get-PropValue -ObjectValue $paperSmoke -Name "rolling_nonnegative_active_window_ratio" -DefaultValue 0.0) 0.0)
        $paperFillConcentrationRatio = [double](To-Double (Get-PropValue -ObjectValue $paperSmoke -Name "rolling_max_fill_concentration_ratio" -DefaultValue 0.0) 0.0)
        $paperExecutionStructure = Get-ExecutionStructureMetrics -Summary $paperSmoke
        $paperExecutionStructureGate = Test-ExecutionStructureGate `
            -Metrics $paperExecutionStructure `
            -MinPayoffRatio $PaperMinPayoffRatio `
            -MaxLossConcentration $PaperMaxLossConcentration `
            -MinClosedTrades $ExecutionStructureMinClosedTrades
        $paperOrdersFilledPass = $paperOrdersFilled -ge $PaperMinOrdersFilled
        $paperRealizedPnlPass = $paperRealizedPnl -ge $PaperMinRealizedPnlQuote
        $paperMicroQualityPass = $paperMicroQualityScoreMean -ge $PaperMinMicroQualityScoreMean
        $paperActiveWindowsPass = $paperActiveWindows -ge $PaperMinActiveWindows
        $paperNonnegativeWindowPass = $paperNonnegativeWindowRatio -ge $PaperMinNonnegativeWindowRatio
        $paperFillConcentrationPass = $paperFillConcentrationRatio -le $PaperMaxFillConcentrationRatio
        $paperHistoryEvidence = Get-PaperHistoryEvidence `
            -DirectoryPath $resolvedPaperSmokeOutDir `
            -WindowRuns $PaperHistoryWindowRuns `
            -MinCompletedRuns $PaperHistoryMinCompletedRuns
        $paperHistorySufficient = To-Bool (Get-PropValue -ObjectValue $paperHistoryEvidence -Name "sufficient_history" -DefaultValue $false) $false
        $paperHistoryCompletedRuns = [int](To-Int64 (Get-PropValue -ObjectValue $paperHistoryEvidence -Name "completed_runs" -DefaultValue 0) 0)
        $paperHistoryNonnegativeRunRatio = [double](To-Double (Get-PropValue -ObjectValue $paperHistoryEvidence -Name "nonnegative_run_ratio" -DefaultValue 0.0) 0.0)
        $paperHistoryPositiveRunRatio = [double](To-Double (Get-PropValue -ObjectValue $paperHistoryEvidence -Name "positive_run_ratio" -DefaultValue 0.0) 0.0)
        $paperHistoryMedianMicroQualityScore = [double](To-Double (Get-PropValue -ObjectValue $paperHistoryEvidence -Name "median_micro_quality_score_mean" -DefaultValue 0.0) 0.0)
        $paperHistoryMedianFallbackRatio = [double](To-Double (Get-PropValue -ObjectValue $paperHistoryEvidence -Name "median_fallback_ratio" -DefaultValue 0.0) 0.0)
        $paperHistoryNonnegativePass = (-not $paperHistorySufficient) -or ($paperHistoryNonnegativeRunRatio -ge $PaperHistoryMinNonnegativeRunRatio)
        $paperHistoryPositivePass = (-not $paperHistorySufficient) -or ($paperHistoryPositiveRunRatio -ge $PaperHistoryMinPositiveRunRatio)
        $paperHistoryMicroQualityPass = (-not $paperHistorySufficient) -or ($paperHistoryMedianMicroQualityScore -ge $PaperHistoryMinMedianMicroQualityScore)
        $paperEvidenceDecision = Resolve-PaperEvidenceDecision `
            -T15GatePass $paperT15GatePass `
            -OrdersFilled $paperOrdersFilled `
            -RealizedPnlQuote $paperRealizedPnl `
            -MicroQualityScoreMean $paperMicroQualityScoreMean `
            -ActiveWindows $paperActiveWindows `
            -NonnegativeWindowRatio $paperNonnegativeWindowRatio `
            -FillConcentrationRatio $paperFillConcentrationRatio `
            -HistorySufficient $paperHistorySufficient `
            -HistoryNonnegativeRunRatio $paperHistoryNonnegativeRunRatio `
            -HistoryPositiveRunRatio $paperHistoryPositiveRunRatio `
            -HistoryMedianMicroQualityScore $paperHistoryMedianMicroQualityScore `
            -HistoryMedianFallbackRatio $paperHistoryMedianFallbackRatio `
            -MinOrdersFilled $PaperMinOrdersFilled `
            -MinRealizedPnlQuote $PaperMinRealizedPnlQuote `
            -MinMicroQualityScoreMean $PaperMinMicroQualityScoreMean `
            -MinActiveWindows $PaperMinActiveWindows `
            -MinNonnegativeWindowRatio $PaperMinNonnegativeWindowRatio `
            -MaxFillConcentrationRatio $PaperMaxFillConcentrationRatio `
            -HistoryMinNonnegativeRunRatio $PaperHistoryMinNonnegativeRunRatio `
            -HistoryMinPositiveRunRatio $PaperHistoryMinPositiveRunRatio `
            -HistoryMinMedianMicroQualityScore $PaperHistoryMinMedianMicroQualityScore `
            -EvidenceEdgeScore $PaperEvidenceEdgeScore `
            -EvidenceHoldScore $PaperEvidenceHoldScore
        $paperPass = if ($promotionPolicyConfig.paper_final_gate) {
            (To-Bool (Get-PropValue -ObjectValue $paperEvidenceDecision -Name "pass" -DefaultValue $false) $false) -and (To-Bool (Get-PropValue -ObjectValue $paperExecutionStructureGate -Name "pass" -DefaultValue $true) $true)
        } else {
            $paperT15GatePass -and (To-Bool (Get-PropValue -ObjectValue $paperExecutionStructureGate -Name "pass" -DefaultValue $true) $true)
        }
        $report.steps.paper_candidate = [ordered]@{
            exit_code = [int]$paperExec.ExitCode
            command = $paperExec.Command
            output_preview = (Get-OutputPreview -Text ([string]$paperExec.Output))
            model_ref_requested = $CandidateModelRef
            model_ref_used = $candidatePaperModelRef
            smoke_report_path = $paperSmokeEffectivePath
            smoke_report_run_path = $paperSmokeRunPath
            smoke_report_latest_path = $paperSmokeLatestPath
            smoke_report_source = if ($paperSmokeEffectivePath -eq $paperSmokeRunPath) { "run_report" } elseif (-not [string]::IsNullOrWhiteSpace($paperSmokeEffectivePath)) { "unexpected" } else { "missing" }
            run_id = [string](Get-PropValue -ObjectValue $paperSmoke -Name "run_id" -DefaultValue "")
            evaluation_contract_id = [string](Get-PropValue -ObjectValue $paperSmoke -Name "evaluation_contract_id" -DefaultValue "")
            evaluation_contract_role = [string](Get-PropValue -ObjectValue $paperSmoke -Name "evaluation_contract_role" -DefaultValue "")
            orders_submitted = [int64](To-Int64 (Get-PropValue -ObjectValue $paperSmoke -Name "orders_submitted" -DefaultValue 0) 0)
            orders_filled = $paperOrdersFilled
            fill_rate = [double](To-Double (Get-PropValue -ObjectValue $paperSmoke -Name "fill_rate" -DefaultValue 0.0) 0.0)
            avg_time_to_fill_ms = [double](To-Double (Get-PropValue -ObjectValue $paperSmoke -Name "avg_time_to_fill_ms" -DefaultValue 0.0) 0.0)
            p50_time_to_fill_ms = [double](To-Double (Get-PropValue -ObjectValue $paperSmoke -Name "p50_time_to_fill_ms" -DefaultValue 0.0) 0.0)
            p90_time_to_fill_ms = [double](To-Double (Get-PropValue -ObjectValue $paperSmoke -Name "p90_time_to_fill_ms" -DefaultValue 0.0) 0.0)
            realized_pnl_quote = $paperRealizedPnl
            max_drawdown_pct = [double](To-Double (Get-PropValue -ObjectValue $paperSmoke -Name "max_drawdown_pct" -DefaultValue 0.0) 0.0)
            slippage_bps_mean = [double](To-Double (Get-PropValue -ObjectValue $paperSmoke -Name "slippage_bps_mean" -DefaultValue 0.0) 0.0)
            micro_quality_score_mean = $paperMicroQualityScoreMean
            runtime_risk_multiplier_mean = [double](To-Double (Get-PropValue -ObjectValue $paperSmoke -Name "runtime_risk_multiplier_mean" -DefaultValue 1.0) 1.0)
            rolling_active_windows = $paperActiveWindows
            rolling_nonnegative_active_window_ratio = $paperNonnegativeWindowRatio
            rolling_max_fill_concentration_ratio = $paperFillConcentrationRatio
            history_completed_runs = $paperHistoryCompletedRuns
            history_sufficient = $paperHistorySufficient
            history_nonnegative_run_ratio = $paperHistoryNonnegativeRunRatio
            history_positive_run_ratio = $paperHistoryPositiveRunRatio
            history_median_micro_quality_score_mean = $paperHistoryMedianMicroQualityScore
            history_median_fallback_ratio = $paperHistoryMedianFallbackRatio
            history_reports = @((Get-PropValue -ObjectValue $paperHistoryEvidence -Name "reports" -DefaultValue @()))
            replace_cancel_timeout_total = [int64](To-Int64 (Get-PropValue -ObjectValue $paperSmoke -Name "replace_cancel_timeout_total" -DefaultValue 0) 0)
            execution_structure = (Get-PropValue -ObjectValue $paperExecutionStructure -Name "payload" -DefaultValue @{})
            t15_gate_pass = $paperT15GatePass
            evidence_score = [double](To-Double (Get-PropValue -ObjectValue $paperEvidenceDecision -Name "evidence_score" -DefaultValue 0.0) 0.0)
            final_decision_basis = [string](Get-PropValue -ObjectValue $paperEvidenceDecision -Name "final_decision_basis" -DefaultValue "")
            final_decision = [string](Get-PropValue -ObjectValue $paperEvidenceDecision -Name "decision" -DefaultValue "")
            hard_failures = @((Get-PropValue -ObjectValue $paperEvidenceDecision -Name "hard_failures" -DefaultValue @()))
            soft_failures = @((Get-PropValue -ObjectValue $paperEvidenceDecision -Name "soft_failures" -DefaultValue @()))
            evidence_components = (Get-PropValue -ObjectValue $paperEvidenceDecision -Name "evidence_components" -DefaultValue @{})
            learned_runtime = [bool]$PaperUseLearnedRuntime
            feature_provider_preflight = (Get-PropValue -ObjectValue $paperSmoke -Name "feature_provider_preflight" -DefaultValue @{})
        }
        $report.gates.paper = [ordered]@{
            evaluated = $true
            skipped = $false
            pass = $paperPass
            final_gate_mode = if ($promotionPolicyConfig.paper_final_gate) { "paper_final" } else { "operational" }
            min_orders_filled_pass = $paperOrdersFilledPass
            min_realized_pnl_pass = $paperRealizedPnlPass
            min_micro_quality_score_mean_pass = $paperMicroQualityPass
            min_micro_quality_score_mean = [double]$PaperMinMicroQualityScoreMean
            micro_quality_score_mean = $paperMicroQualityScoreMean
            min_active_windows_pass = $paperActiveWindowsPass
            min_nonnegative_window_ratio_pass = $paperNonnegativeWindowPass
            max_fill_concentration_pass = $paperFillConcentrationPass
            execution_structure_evaluated = (To-Bool (Get-PropValue -ObjectValue $paperExecutionStructureGate -Name "evaluated" -DefaultValue $false) $false)
            payoff_ratio = [double](Get-PropValue -ObjectValue $paperExecutionStructureGate -Name "payoff_ratio" -DefaultValue 0.0)
            payoff_ratio_pass = (To-Bool (Get-PropValue -ObjectValue $paperExecutionStructureGate -Name "payoff_ratio_pass" -DefaultValue $true) $true)
            payoff_ratio_threshold = [double](Get-PropValue -ObjectValue $paperExecutionStructureGate -Name "payoff_ratio_threshold" -DefaultValue 0.0)
            market_loss_concentration = [double](Get-PropValue -ObjectValue $paperExecutionStructureGate -Name "market_loss_concentration" -DefaultValue 0.0)
            market_loss_concentration_pass = (To-Bool (Get-PropValue -ObjectValue $paperExecutionStructureGate -Name "market_loss_concentration_pass" -DefaultValue $true) $true)
            market_loss_concentration_threshold = [double](Get-PropValue -ObjectValue $paperExecutionStructureGate -Name "market_loss_concentration_threshold" -DefaultValue 0.0)
            execution_structure_reasons = @((Get-PropValue -ObjectValue $paperExecutionStructureGate -Name "reasons" -DefaultValue @()))
            history_completed_runs = $paperHistoryCompletedRuns
            history_sufficient = $paperHistorySufficient
            history_min_completed_runs = [int]$PaperHistoryMinCompletedRuns
            history_nonnegative_run_ratio = $paperHistoryNonnegativeRunRatio
            history_positive_run_ratio = $paperHistoryPositiveRunRatio
            history_median_micro_quality_score_mean = $paperHistoryMedianMicroQualityScore
            history_median_fallback_ratio = $paperHistoryMedianFallbackRatio
            history_min_nonnegative_run_ratio_pass = $paperHistoryNonnegativePass
            history_min_positive_run_ratio_pass = $paperHistoryPositivePass
            history_min_median_micro_quality_score_pass = $paperHistoryMicroQualityPass
            smoke_connectivity_pass = To-Bool (Get-PropValue -ObjectValue (Get-PropValue -ObjectValue $paperSmoke -Name "gates" -DefaultValue @{}) -Name "smoke_connectivity_pass" -DefaultValue $false) $false
            t15_gate_pass = $paperT15GatePass
            evidence_score = [double](To-Double (Get-PropValue -ObjectValue $paperEvidenceDecision -Name "evidence_score" -DefaultValue 0.0) 0.0)
            evidence_edge_threshold = [double]$PaperEvidenceEdgeScore
            evidence_hold_threshold = [double]$PaperEvidenceHoldScore
            hard_failures = @((Get-PropValue -ObjectValue $paperEvidenceDecision -Name "hard_failures" -DefaultValue @()))
            soft_failures = @((Get-PropValue -ObjectValue $paperEvidenceDecision -Name "soft_failures" -DefaultValue @()))
            evidence_components = (Get-PropValue -ObjectValue $paperEvidenceDecision -Name "evidence_components" -DefaultValue @{})
            final_decision = [string](Get-PropValue -ObjectValue $paperEvidenceDecision -Name "decision" -DefaultValue "")
            final_decision_basis = [string](Get-PropValue -ObjectValue $paperEvidenceDecision -Name "final_decision_basis" -DefaultValue "")
        }
    }

    $resolvedOverlayCalibrationPath = if ([System.IO.Path]::IsPathRooted($OverlayCalibrationArtifactPath)) {
        $OverlayCalibrationArtifactPath
    } else {
        Join-Path $resolvedProjectRoot $OverlayCalibrationArtifactPath
    }
    if ($SkipPaperSoak) {
        $report.steps.overlay_calibration = [ordered]@{ attempted = $false; reason = "PAPER_SOAK_SKIPPED" }
    } else {
        $overlayCalibrationArgs = @(
            "-m", "autobot.common.operational_overlay_calibration",
            "--report-dir", $resolvedPaperSmokeOutDir,
            "--output-path", $resolvedOverlayCalibrationPath,
            "--lane", $FeatureSet,
            "--window-runs", $OverlayCalibrationWindowRuns,
            "--min-reports", $OverlayCalibrationMinReports
        )
        $overlayCalibrationExec = Invoke-CommandCapture -Exe $resolvedPythonExe -ArgList $overlayCalibrationArgs -AllowFailure
        $overlayCalibrationDoc = if ((-not $DryRun) -and (Test-Path $resolvedOverlayCalibrationPath)) {
            Load-JsonOrEmpty -PathValue $resolvedOverlayCalibrationPath
        } else {
            @{}
        }
        $report.steps.overlay_calibration = [ordered]@{
            attempted = $true
            exit_code = [int]$overlayCalibrationExec.ExitCode
            command = $overlayCalibrationExec.Command
            output_preview = (Get-OutputPreview -Text ([string]$overlayCalibrationExec.Output))
            artifact_path = $resolvedOverlayCalibrationPath
            report_count = [int](To-Int64 (Get-PropValue -ObjectValue $overlayCalibrationDoc -Name "report_count" -DefaultValue 0) 0)
            sufficient_reports = To-Bool (Get-PropValue -ObjectValue $overlayCalibrationDoc -Name "sufficient_reports" -DefaultValue $false) $false
            applied_fields = @((Get-PropValue -ObjectValue $overlayCalibrationDoc -Name "applied_fields" -DefaultValue @()))
        }
    }

    $fusionEvidenceReasonCode = ""
    $fusionCandidateDefaultEligible = $false
    $fusionEvidenceWinner = ""
    $fusionOfflineWinner = ""
    $fusionDefaultEligibleWinner = ""
    $pairedPaperArtifact = Resolve-LatestPairedPaperArtifact -ProjectRoot $resolvedProjectRoot
    $canaryConfidenceArtifact = Resolve-LatestCanaryConfidenceArtifact -ProjectRoot $resolvedProjectRoot
    if (([string]$Trainer).Trim().ToLowerInvariant() -eq "v5_fusion") {
        $paperEvidencePass = if ($SkipPaperSoak) { $true } else { $paperPass }
        $canaryPass = if (Test-IsEffectivelyEmptyObject -ObjectValue (Get-PropValue -ObjectValue $canaryConfidenceArtifact -Name "payload" -DefaultValue @{})) {
            $true
        } else {
            -not [string]::Equals(
                [string](Get-PropValue -ObjectValue (Get-PropValue -ObjectValue (Get-PropValue -ObjectValue $canaryConfidenceArtifact -Name "payload" -DefaultValue @{}) -Name "decision" -DefaultValue @{}) -Name "status" -DefaultValue ""),
                "abort",
                [System.StringComparison]::OrdinalIgnoreCase
            )
        }
        $pairedPass = if (Test-IsEffectivelyEmptyObject -ObjectValue (Get-PropValue -ObjectValue $pairedPaperArtifact -Name "report" -DefaultValue @{})) {
            $true
        } else {
            $pairedDeltas = Get-PropValue -ObjectValue (Get-PropValue -ObjectValue $pairedPaperArtifact -Name "report" -DefaultValue @{}) -Name "paired_deltas" -DefaultValue @{}
            ([int](To-Int64 (Get-PropValue -ObjectValue $pairedDeltas -Name "matched_no_trade_delta" -DefaultValue 0) 0) -ge -1) -and
            ([double](To-Double (Get-PropValue -ObjectValue $pairedDeltas -Name "matched_slippage_delta_bps" -DefaultValue 0.0) 0.0) -le 2.5)
        }
        $fusionCandidateDefaultEligible = `
            (([string]$fusionStackerFamily).Trim().ToLowerInvariant() -ne "regime_moe") -or `
            (
                (To-Bool (Get-PropValue -ObjectValue $report.gates.backtest -Name "pass" -DefaultValue $false) $false) -and
                (To-Bool (Get-PropValue -ObjectValue $report.gates.backtest -Name "candidate_execution_structure_evaluated" -DefaultValue $false) $false) -and
                $paperEvidencePass -and
                $pairedPass -and
                $canaryPass
            )
        if ([string]::Equals($fusionStackerFamily, "regime_moe", [System.StringComparison]::OrdinalIgnoreCase)) {
            $fusionEvidenceReasonCode = if ($fusionCandidateDefaultEligible) { "REGIME_MOE_EVIDENCE_STRONG_ENOUGH" } else { "BASELINE_RETAINED_EVIDENCE_NOT_STRONG_ENOUGH" }
            $fusionEvidenceWinner = if ($fusionCandidateDefaultEligible) { "regime_moe" } else { "linear" }
        } else {
            $fusionEvidenceReasonCode = "LINEAR_BASELINE_WINNER"
            $fusionEvidenceWinner = if ([string]::IsNullOrWhiteSpace($fusionStackerFamily)) { "linear" } else { $fusionStackerFamily }
        }
        $fusionEvidencePayload = Update-FusionVariantEvidenceArtifact `
            -VariantReportPath $candidateVariantReportPath `
            -FusionVariantName $fusionStackerFamily `
            -ReasonCode $fusionEvidenceReasonCode `
            -CandidateDefaultEligible $fusionCandidateDefaultEligible `
            -BacktestGate $report.gates.backtest `
            -PaperGate $report.gates.paper `
            -PairedArtifact $pairedPaperArtifact `
            -CanaryArtifact $canaryConfidenceArtifact
        $fusionOfflineWinner = [string](Get-PropValue -ObjectValue $fusionEvidencePayload -Name "offline_winner_variant_name" -DefaultValue $candidateFusionVariantName)
        $fusionDefaultEligibleWinner = [string](Get-PropValue -ObjectValue $fusionEvidencePayload -Name "default_eligible_variant_name" -DefaultValue $fusionEvidenceWinner)
        $report.steps.fusion_variant_selection.selection_evidence = Get-PropValue -ObjectValue $fusionEvidencePayload -Name "selection_evidence" -DefaultValue @{}
        $report.steps.fusion_variant_selection.fusion_candidate_default_eligible = $fusionCandidateDefaultEligible
        $report.steps.fusion_variant_selection.fusion_evidence_winner = $fusionEvidenceWinner
        $report.steps.fusion_variant_selection.fusion_evidence_reason_code = $fusionEvidenceReasonCode
        $report.steps.fusion_variant_selection.fusion_offline_winner = $fusionOfflineWinner
        $report.steps.fusion_variant_selection.fusion_default_eligible_winner = $fusionDefaultEligibleWinner
        $report.candidate.fusion_candidate_default_eligible = $fusionCandidateDefaultEligible
        $report.candidate.fusion_evidence_winner = $fusionEvidenceWinner
        $report.candidate.fusion_evidence_reason_code = $fusionEvidenceReasonCode
        $report.candidate.fusion_offline_winner = $fusionOfflineWinner
        $report.candidate.fusion_default_eligible_winner = $fusionDefaultEligibleWinner
        $report.candidate.paired_paper_latest_path = [string](Get-PropValue -ObjectValue $pairedPaperArtifact -Name "latest_path" -DefaultValue "")
        $report.candidate.canary_confidence_latest_path = [string](Get-PropValue -ObjectValue $canaryConfidenceArtifact -Name "path" -DefaultValue "")
        if ((-not $DryRun) -and (-not [string]::IsNullOrWhiteSpace($candidateRunDir))) {
            $runtimeRecommendationsPath = Join-Path $candidateRunDir "runtime_recommendations.json"
            $runtimeRecommendationsPayload = Load-JsonOrEmpty -PathValue $runtimeRecommendationsPath
            $runtimeRecommendationsPayload | Add-Member -NotePropertyName "fusion_candidate_default_eligible" -NotePropertyValue $fusionCandidateDefaultEligible -Force
            $runtimeRecommendationsPayload | Add-Member -NotePropertyName "fusion_evidence_winner" -NotePropertyValue $fusionEvidenceWinner -Force
            $runtimeRecommendationsPayload | Add-Member -NotePropertyName "fusion_evidence_reason_code" -NotePropertyValue $fusionEvidenceReasonCode -Force
            $runtimeRecommendationsPayload | Add-Member -NotePropertyName "fusion_offline_winner" -NotePropertyValue $fusionOfflineWinner -Force
            $runtimeRecommendationsPayload | Add-Member -NotePropertyName "fusion_default_eligible_winner" -NotePropertyValue $fusionDefaultEligibleWinner -Force
            Write-JsonFile -PathValue $runtimeRecommendationsPath -Payload $runtimeRecommendationsPayload
            $promotionDecisionPayload = Load-JsonOrEmpty -PathValue $promotionDecisionPath
            $promotionDecisionPayload | Add-Member -NotePropertyName "fusion_candidate_default_eligible" -NotePropertyValue $fusionCandidateDefaultEligible -Force
            $promotionDecisionPayload | Add-Member -NotePropertyName "fusion_evidence_winner" -NotePropertyValue $fusionEvidenceWinner -Force
            $promotionDecisionPayload | Add-Member -NotePropertyName "fusion_evidence_reason_code" -NotePropertyValue $fusionEvidenceReasonCode -Force
            $promotionDecisionPayload | Add-Member -NotePropertyName "fusion_offline_winner" -NotePropertyValue $fusionOfflineWinner -Force
            $promotionDecisionPayload | Add-Member -NotePropertyName "fusion_default_eligible_winner" -NotePropertyValue $fusionDefaultEligibleWinner -Force
            Write-JsonFile -PathValue $promotionDecisionPath -Payload $promotionDecisionPayload
            if (-not [string]::IsNullOrWhiteSpace($certificationArtifactPath)) {
                $certificationArtifact | Add-Member -NotePropertyName "fusion_candidate_default_eligible" -NotePropertyValue $fusionCandidateDefaultEligible -Force
                $certificationArtifact | Add-Member -NotePropertyName "fusion_evidence_winner" -NotePropertyValue $fusionEvidenceWinner -Force
                $certificationArtifact | Add-Member -NotePropertyName "fusion_evidence_reason_code" -NotePropertyValue $fusionEvidenceReasonCode -Force
                $certificationArtifact | Add-Member -NotePropertyName "fusion_offline_winner" -NotePropertyValue $fusionOfflineWinner -Force
                $certificationArtifact | Add-Member -NotePropertyName "fusion_default_eligible_winner" -NotePropertyValue $fusionDefaultEligibleWinner -Force
                $certificationArtifact.provenance | Add-Member -NotePropertyName "fusion_candidate_default_eligible" -NotePropertyValue $fusionCandidateDefaultEligible -Force
                $certificationArtifact.provenance | Add-Member -NotePropertyName "fusion_evidence_winner" -NotePropertyValue $fusionEvidenceWinner -Force
                $certificationArtifact.provenance | Add-Member -NotePropertyName "fusion_evidence_reason_code" -NotePropertyValue $fusionEvidenceReasonCode -Force
                $certificationArtifact.provenance | Add-Member -NotePropertyName "fusion_offline_winner" -NotePropertyValue $fusionOfflineWinner -Force
                $certificationArtifact.provenance | Add-Member -NotePropertyName "fusion_default_eligible_winner" -NotePropertyValue $fusionDefaultEligibleWinner -Force
                Write-JsonFile -PathValue $certificationArtifactPath -Payload $certificationArtifact
            }
        }
    }

    $overallPass = if ($SkipPaperSoak) { $backtestPass -and $runtimeParityPass } else { $backtestPass -and $runtimeParityPass -and $paperPass }
    $reasons = @()
    $notes = @()
    if (-not $backtestPass) {
        $reasons += "BACKTEST_ACCEPTANCE_FAILED"
    }
    if ($candidateExecutionPolicyVetoFailure) {
        $reasons += "EXECUTION_POLICY_VETO_FAILURE"
    }
    if (-not (To-Bool (Get-PropValue -ObjectValue $candidateExecutionStructureGate -Name "pass" -DefaultValue $true) $true)) {
        $reasons += @((Get-PropValue -ObjectValue $candidateExecutionStructureGate -Name "reasons" -DefaultValue @()))
    }
    if ($BacktestRuntimeParityEnabled -and (-not $runtimeParityPass)) {
        $reasons += "RUNTIME_PARITY_BACKTEST_FAILED"
    }
    if ($BacktestRuntimeParityEnabled -and $runtimeParityCandidateExecutionPolicyVetoFailure) {
        $reasons += "RUNTIME_PARITY_EXECUTION_POLICY_VETO_FAILURE"
    }
    if (($TrainerEvidenceMode -eq "required") -and (-not $trainerEvidenceGatePass)) {
        $reasons += "TRAINER_EVIDENCE_REQUIRED_FAILED"
    }
    if (-not $budgetContractGatePass) {
        $reasons += "SCOUT_ONLY_BUDGET_EVIDENCE"
    }
    if ((-not [string]::IsNullOrWhiteSpace($oodStatus)) -and (-not [string]::Equals($oodStatus, "informative_ready", [System.StringComparison]::OrdinalIgnoreCase))) {
        $notes += "OOD_GENERALIZATION_REPORT_NOT_READY"
    }
    if ($laneShadowOnly -or (-not $lanePromotionAllowed)) {
        $notes += "SHADOW_LANE_ONLY"
    }
    if ($SkipPaperSoak) {
        $notes += "PAPER_SOAK_SKIPPED"
    } elseif (-not $paperPass) {
        if (-not (To-Bool (Get-PropValue -ObjectValue $paperExecutionStructureGate -Name "pass" -DefaultValue $true) $true)) {
            $reasons += @((Get-PropValue -ObjectValue $paperExecutionStructureGate -Name "reasons" -DefaultValue @()))
        }
        if ($promotionPolicyConfig.paper_final_gate) {
            $paperGate = Get-PropValue -ObjectValue $report.gates.paper -Name "final_decision_basis" -DefaultValue ""
            if (-not [string]::IsNullOrWhiteSpace([string]$paperGate)) {
                $reasons += ("PAPER_FINAL_GATE_" + [string]$paperGate)
            } else {
                $reasons += "PAPER_FINAL_GATE_FAILED"
            }
        } else {
            $reasons += "PAPER_SOAK_FAILED"
        }
    }
    $report.gates.overall_pass = $overallPass
    $report.reasons = @($reasons)
    $report.notes = @($notes)
    $updateLatestCandidateStep = [ordered]@{ attempted = $false; updated = $false }
    if ($overallPass -and (-not $laneShadowOnly) -and $lanePromotionAllowed -and (-not $DryRun) -and (-not [string]::IsNullOrWhiteSpace($candidateRunId))) {
        $pointerUpdate = Update-LatestCandidatePointers -RegistryRoot $resolvedRegistryRoot -Family $ModelFamily -RunId $candidateRunId
        $updateLatestCandidateStep = [ordered]@{
            attempted = $true
            updated = $true
            candidate_run_id = $candidateRunId
            family_path = [string](Get-PropValue -ObjectValue $pointerUpdate -Name "family_path" -DefaultValue "")
            global_path = [string](Get-PropValue -ObjectValue $pointerUpdate -Name "global_path" -DefaultValue "")
            updated_at_utc = [string](Get-PropValue -ObjectValue $pointerUpdate -Name "updated_at_utc" -DefaultValue "")
        }
    } elseif ($DryRun) {
        $updateLatestCandidateStep.reason = "SKIPPED_BY_DRY_RUN"
    } elseif (-not $overallPass) {
        $updateLatestCandidateStep.reason = "OVERALL_GATE_FAILED"
    } elseif ($laneShadowOnly -or (-not $lanePromotionAllowed)) {
        $updateLatestCandidateStep.reason = "SHADOW_LANE_GOVERNANCE_BLOCK"
        $updateLatestCandidateStep.lane_id = $laneId
        $updateLatestCandidateStep.lane_role = $laneRole
        $updateLatestCandidateStep.shadow_only = $laneShadowOnly
        $updateLatestCandidateStep.promotion_allowed = $lanePromotionAllowed
    } else {
        $updateLatestCandidateStep.reason = "CANDIDATE_RUN_ID_MISSING"
    }
    $report.steps.update_latest_candidate = $updateLatestCandidateStep

    $promoteStep = [ordered]@{ attempted = $false; promoted = $false }
    if ($SkipPaperSoak) {
        $promoteStep.reason = "SKIPPED_PAPER_SOAK_REQUIRES_MANUAL_PROMOTE"
    } elseif ($overallPass -and ($laneShadowOnly -or (-not $lanePromotionAllowed))) {
        $promoteStep.reason = "SHADOW_LANE_GOVERNANCE_BLOCK"
        $promoteStep.lane_id = $laneId
        $promoteStep.lane_role = $laneRole
        $promoteStep.shadow_only = $laneShadowOnly
        $promoteStep.promotion_allowed = $lanePromotionAllowed
        $promoteStep.governance_reasons = @($laneGovernanceReasons)
    } elseif ($overallPass -and (-not $SkipPromote)) {
        $promoteArgs = @(
            "-m", "autobot.cli",
            "model", "promote",
            "--model-ref", $candidateRunId,
            "--model-family", $ModelFamily
        )
        $promoteExec = Invoke-CommandCapture -Exe $resolvedPythonExe -ArgList $promoteArgs
        $promoteStep = [ordered]@{
            attempted = $true
            exit_code = [int]$promoteExec.ExitCode
            command = $promoteExec.Command
            output_preview = (Get-OutputPreview -Text ([string]$promoteExec.Output))
            promoted = ($promoteExec.ExitCode -eq 0)
        }
    } elseif ($SkipPromote) {
        $promoteStep.reason = "SKIPPED_BY_FLAG"
    } else {
        $promoteStep.reason = "OVERALL_GATE_FAILED"
    }
    $report.steps.promote = $promoteStep
    $championAfter = if ($DryRun) { @{} } else { Load-JsonOrEmpty -PathValue $championPointerPath }
    $report.candidate.champion_after_run_id = [string](Get-PropValue -ObjectValue $championAfter -Name "run_id" -DefaultValue "")

    if (To-Bool (Get-PropValue -ObjectValue $promoteStep -Name "promoted" -DefaultValue $false) $false) {
        $restartResults = Invoke-RestartUnits -UnitsToRestart $effectiveRestartUnits
        $report.steps.restart_units = @($restartResults)
    } else {
        $report.steps.restart_units = @()
    }

    if (-not $SkipReportRefresh) {
        $refreshSmokeReportPath = if (-not [string]::IsNullOrWhiteSpace($paperSmokeEffectivePath)) {
            $paperSmokeEffectivePath
        } else {
            $paperSmokeLatestPath
        }
        $refreshArgs = @(
            "-NoProfile",
            "-File", $resolvedDailyPipelineScript,
            "-ProjectRoot", $resolvedProjectRoot,
            "-PythonExe", $resolvedPythonExe,
            "-Date", $effectiveBatchDate,
            "-SmokeReportJson", $refreshSmokeReportPath,
            "-SkipCandles",
            "-SkipTicks",
            "-SkipAggregate",
            "-SkipValidate",
            "-SkipSmoke",
            "-SkipTieringRecommend"
        )
        $refreshExec = Invoke-CommandCapture -Exe $psExe -ArgList $refreshArgs -AllowFailure
        $report.steps.report_refresh = [ordered]@{
            attempted = $true
            exit_code = [int]$refreshExec.ExitCode
            command = $refreshExec.Command
            output_preview = (Get-OutputPreview -Text ([string]$refreshExec.Output))
        }
    } else {
        $report.steps.report_refresh = [ordered]@{ attempted = $false; reason = "SKIPPED_BY_FLAG" }
    }

    $candidateAdoptable = $overallPass -and (-not $laneShadowOnly) -and $lanePromotionAllowed
    $candidateAdopted = To-Bool (Get-PropValue -ObjectValue $updateLatestCandidateStep -Name "updated" -DefaultValue $false) $false
    $promotedSucceeded = To-Bool (Get-PropValue -ObjectValue $promoteStep -Name "promoted" -DefaultValue $false) $false
    $artifactStatusValue = if ($promotedSucceeded) {
        "champion"
    } elseif ($candidateAdopted) {
        "candidate_adopted"
    } elseif ($candidateAdoptable) {
        "candidate_adoptable"
    } else {
        "acceptance_completed"
    }
    Update-RunArtifactStatus `
        -RunDir $candidateRunDir `
        -RunId $candidateRunId `
        -Status $artifactStatusValue `
        -AcceptanceCompleted $true `
        -CandidateAdoptable $candidateAdoptable `
        -CandidateAdopted $candidateAdopted `
        -Promoted $promotedSucceeded | Out-Null

    if ($DryRun) {
        $report.gates.overall_pass = $null
        $report.reasons = @("DRY_RUN_ONLY")
    } elseif ((-not $overallPass) -and [string]::IsNullOrWhiteSpace([string]$report.failure_stage)) {
        $acceptanceFailureCode = if (@($report.reasons).Count -gt 0) {
            [string]$report.reasons[0]
        } else {
            "ACCEPTANCE_GATE_FAILED"
        }
        Set-ReportFailure `
            -Stage "acceptance_gate" `
            -Code $acceptanceFailureCode `
            -ReportPath $certificationArtifactPath
    }
    $paths = Save-Report
    Write-Host ("[{0}] candidate_run_id={1}" -f $LogTag, $candidateRunId)
    Write-Host ("[{0}] backtest_pass={1}" -f $LogTag, $backtestPass)
    Write-Host ("[{0}] paper_pass={1}" -f $LogTag, $paperPass)
    Write-ReportPointers -LogTag $LogTag -Paths $paths -OverallPass $overallPass
    if ($DryRun) {
        exit 0
    }
    if ($overallPass) {
        exit 0
    }
    exit 2
} catch {
    $report.gates.overall_pass = $false
    $report.reasons = @("UNHANDLED_EXCEPTION")
    $invocation = $_.InvocationInfo
    $exceptionMessage = [string]$_.Exception.Message
    $positionMessage = if ($null -eq $invocation) { "" } else { [string]$invocation.PositionMessage }
    $stackTrace = [string]$_.ScriptStackTrace
    if (-not [string]::IsNullOrWhiteSpace($positionMessage)) {
        $exceptionMessage += " | position=" + ($positionMessage -replace "\s+", " ").Trim()
    }
    if (-not [string]::IsNullOrWhiteSpace($stackTrace)) {
        $exceptionMessage += " | stack=" + ($stackTrace -replace "\s+", " ").Trim()
    }
    $report.steps.exception = [ordered]@{
        message = $exceptionMessage
        exception_type = if ($null -eq $_.Exception) { "" } else { [string]$_.Exception.GetType().FullName }
        script_name = if ($null -eq $invocation) { "" } else { [string]$invocation.ScriptName }
        line = if ($null -eq $invocation) { 0 } else { [int]$invocation.ScriptLineNumber }
        offset_in_line = if ($null -eq $invocation) { 0 } else { [int]$invocation.OffsetInLine }
        position_message = if ($null -eq $invocation) { "" } else { [string]$invocation.PositionMessage }
        script_stack_trace = [string]$_.ScriptStackTrace
    }
    if ([string]::IsNullOrWhiteSpace([string]$report.failure_stage)) {
        $defaultStage = "acceptance_gate"
        $defaultCode = "UNHANDLED_EXCEPTION"
        if ($exceptionMessage -like "*dependency runtime export*") {
            $defaultStage = "runtime_export"
            $defaultCode = "DEPENDENCY_RUNTIME_EXPORT_FAILED"
        } elseif ($exceptionMessage -like "*FUSION_RUNTIME_*" -or (([string]$Trainer).Trim().ToLowerInvariant() -eq "v5_fusion")) {
            $defaultStage = "fusion_train"
        } elseif ($exceptionMessage -like "*TRAIN_SNAPSHOT_CLOSE*") {
            $defaultStage = "data_close"
            $defaultCode = "TRAIN_SNAPSHOT_CLOSE_FAILED"
        }
        Set-ReportFailure -Stage $defaultStage -Code $defaultCode -ReportPath $candidateRunDir
    }
    Update-RunArtifactStatus `
        -RunDir $candidateRunDir `
        -RunId $candidateRunId `
        -Status "acceptance_incomplete" `
        -AcceptanceCompleted $false `
        -CandidateAdoptable $false `
        -CandidateAdopted $false `
        -Promoted $false | Out-Null
    $paths = Save-Report
    Write-Host ("[{0}][error] {1}" -f $LogTag, $exceptionMessage)
    Write-ReportPointers -LogTag $LogTag -Paths $paths -OverallPass $false
    exit 2
}
