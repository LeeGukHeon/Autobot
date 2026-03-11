param(
    [string]$PythonExe = "",
    [string]$ProjectRoot = "",
    [string]$BatchDate = "",
    [string]$DailyPipelineScript = "",
    [string]$PaperSmokeScript = "",
    [string]$OutDir = "logs/model_acceptance",
    [int]$TrainLookbackDays = 30,
    [int]$BacktestLookbackDays = 8,
    [bool]$TrainLookbackRampEnabled = $true,
    [string]$TrainLookbackRampMicroRoot = "data/parquet/micro_v1",
    [int]$TrainLookbackRampMinMarketsPerDate = 1,
    [string]$TrainStartFloorDate = "",
    [string]$Tf = "5m",
    [string]$Quote = "KRW",
    [int]$TrainTopN = 50,
    [int]$BacktestTopN = 20,
    [string]$ModelFamily = "train_v4_crypto_cs",
    [string]$Trainer = "v4_crypto_cs",
    [string]$FeatureSet = "v4",
    [string]$LabelSet = "v2",
    [string]$Task = "cls",
    [string]$RunScope = "scheduled_daily",
    [string]$CandidateModelRef = "latest_candidate_v4",
    [string]$ChampionModelRef = "champion_v4",
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
    [string[]]$KnownRuntimeUnits = @("autobot-paper-v4.service", "autobot-live-alpha.service"),
    [bool]$AutoRestartKnownUnits = $true,
    [switch]$SkipDailyPipeline,
    [switch]$SkipPaperSoak,
    [switch]$SkipReportRefresh,
    [switch]$SkipPromote,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
$script:IsWindowsPlatform = [System.IO.Path]::DirectorySeparatorChar -eq '\'

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
        [string]$TrainStartFloorDate
    )
    $resolvedMicroRoot = Resolve-PathFromProjectRoot -Root $ProjectRoot -PathValue $MicroRoot
    $batchDateValue = Resolve-DateToken -DateText $BatchDate -LabelForError "batch_date"
    $batchDateObj = [DateTime]::ParseExact($batchDateValue, "yyyy-MM-dd", [System.Globalization.CultureInfo]::InvariantCulture)
    $trainStartFloorDateValue = if ([string]::IsNullOrWhiteSpace($TrainStartFloorDate)) {
        ""
    } else {
        Resolve-DateToken -DateText $TrainStartFloorDate -LabelForError "train_start_floor_date"
    }
    $trainStartFloorObj = if ([string]::IsNullOrWhiteSpace($trainStartFloorDateValue)) {
        $null
    } else {
        [DateTime]::ParseExact($trainStartFloorDateValue, "yyyy-MM-dd", [System.Globalization.CultureInfo]::InvariantCulture)
    }
    $requestedTrainDays = [Math]::Max([int]$RequestedTrainLookbackDays, 1)
    $requestedBacktestDays = [Math]::Max([int]$RequestedBacktestLookbackDays, 1)
    $minMarkets = [Math]::Max([int]$MinMarketsPerDate, 1)
    $coverageCounts = Get-MicroDateCoverageCounts -MicroRoot $resolvedMicroRoot -Tf $Tf
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
    $trainStartFloorApplied = $false
    if ($effectiveTrainDays -gt 0) {
        $trainEndObj = [DateTime]::ParseExact($trainEndDate, "yyyy-MM-dd", [System.Globalization.CultureInfo]::InvariantCulture)
        $trainStartDate = $trainEndObj.AddDays(-1 * [Math]::Max($effectiveTrainDays - 1, 0)).ToString("yyyy-MM-dd")
        if ($null -ne $trainStartFloorObj) {
            $trainStartObj = [DateTime]::ParseExact($trainStartDate, "yyyy-MM-dd", [System.Globalization.CultureInfo]::InvariantCulture)
            if ($trainStartObj -lt $trainStartFloorObj) {
                $trainStartFloorApplied = $true
                if ($trainEndObj -lt $trainStartFloorObj) {
                    $trainStartDate = ""
                    $effectiveTrainDays = 0
                    $reason = "TRAIN_START_FLOOR_AFTER_TRAIN_END"
                } else {
                    $trainStartDate = $trainStartFloorDateValue
                    $effectiveTrainDays = [int](($trainEndObj - $trainStartFloorObj).TotalDays) + 1
                    $reason = "TRAIN_START_FLOOR_ACTIVE"
                }
            }
        }
    }
    return [ordered]@{
        enabled = [bool]$RampEnabled
        micro_root = $resolvedMicroRoot
        micro_coverage_present = $coveragePresent
        requested_train_lookback_days = [int]$requestedTrainDays
        requested_backtest_lookback_days = [int]$requestedBacktestDays
        effective_train_lookback_days = [int]$effectiveTrainDays
        min_markets_per_date = [int]$minMarkets
        available_contiguous_micro_days = [int]$availableContiguousDays
        available_market_dates = @($availableDates)
        first_available_micro_date = if ($availableDates.Count -gt 0) { [string]$availableDates[0] } else { "" }
        last_available_micro_date = if ($availableDates.Count -gt 0) { [string]$availableDates[-1] } else { "" }
        ramp_active = (([bool]$RampEnabled) -and ($coveragePresent) -and ($effectiveTrainDays -lt $requestedTrainDays)) -or $trainStartFloorApplied
        comparable_window_available = ($effectiveTrainDays -gt 0)
        reason = $reason
        train_start_date = $trainStartDate
        train_end_date = $trainEndDate
        certification_start_date = $certificationStartDate
        certification_end_date = $batchDateValue
        train_start_floor_date = $trainStartFloorDateValue
        train_start_floor_applied = $trainStartFloorApplied
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
    if ($ObjectValue.PSObject -and $ObjectValue.PSObject.Properties.Name -contains $Name) {
        return $ObjectValue.$Name
    }
    return $DefaultValue
}

function Convert-ToStringArray {
    param([Parameter(Mandatory = $false)]$Value)
    if ($null -eq $Value) {
        return @()
    }
    if ($Value -is [string]) {
        if ([string]::IsNullOrWhiteSpace($Value)) {
            return @()
        }
        return @($Value.Trim())
    }
    $items = @()
    if ($Value -is [System.Array] -or $Value -is [System.Collections.IEnumerable]) {
        foreach ($item in $Value) {
            $text = [string]$item
            if (-not [string]::IsNullOrWhiteSpace($text)) {
                $items += $text.Trim()
            }
        }
        return @($items)
    }
    $text = [string]$Value
    if ([string]::IsNullOrWhiteSpace($text)) {
        return @()
    }
    return @($text.Trim())
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
    $output = & $Exe @ArgList 2>&1
    $exitCode = [int]$LASTEXITCODE
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
    return (Join-Path (Join-Path $RegistryRoot $Family) ($PointerName + ".json"))
}

function Invoke-BacktestAndLoadSummary {
    param(
        [string]$PythonPath,
        [string]$Root,
        [string]$ModelRef,
        [string]$StartDate,
        [string]$EndDate
    )
    $args = @(
        "-m", "autobot.cli",
        "backtest", "alpha",
        "--preset", "acceptance",
        "--model-ref", $ModelRef,
        "--model-family", $ModelFamily,
        "--feature-set", $FeatureSet,
        "--tf", $Tf,
        "--quote", $Quote,
        "--top-n", $BacktestTopN,
        "--start", $StartDate,
        "--end", $EndDate,
        "--top-pct", $BacktestTopPct,
        "--min-prob", $BacktestMinProb,
        "--min-cands-per-ts", $BacktestMinCandidatesPerTs,
        "--exit-mode", "hold",
        "--hold-bars", $HoldBars
    )
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
    return @($results)
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
    return @($results)
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
    return @($values.ToArray())
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
    foreach ($gateName in @("backtest", "paper")) {
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
    $lines.Add("- backtest_orders_filled: $([string](Get-PropValue -ObjectValue $backtestCandidate -Name 'orders_filled' -DefaultValue ''))") | Out-Null
    $lines.Add("- backtest_realized_pnl_quote: $([string](Get-PropValue -ObjectValue $backtestCandidate -Name 'realized_pnl_quote' -DefaultValue ''))") | Out-Null
    $lines.Add("- backtest_fill_rate: $([string](Get-PropValue -ObjectValue $backtestCandidate -Name 'fill_rate' -DefaultValue ''))") | Out-Null
    $lines.Add("- backtest_max_drawdown_pct: $([string](Get-PropValue -ObjectValue $backtestCandidate -Name 'max_drawdown_pct' -DefaultValue ''))") | Out-Null
    $lines.Add("- backtest_slippage_bps_mean: $([string](Get-PropValue -ObjectValue $backtestCandidate -Name 'slippage_bps_mean' -DefaultValue ''))") | Out-Null
    $lines.Add("- backtest_calmar_like_score: $([string](Get-PropValue -ObjectValue $backtestCandidate -Name 'calmar_like_score' -DefaultValue ''))") | Out-Null
    $lines.Add("- paper_orders_submitted: $([string](Get-PropValue -ObjectValue $paperCandidate -Name 'orders_submitted' -DefaultValue ''))") | Out-Null
    $lines.Add("- paper_orders_filled: $([string](Get-PropValue -ObjectValue $paperCandidate -Name 'orders_filled' -DefaultValue ''))") | Out-Null
    $lines.Add("- paper_realized_pnl_quote: $([string](Get-PropValue -ObjectValue $paperCandidate -Name 'realized_pnl_quote' -DefaultValue ''))") | Out-Null
    $lines.Add("- paper_slippage_bps_mean: $([string](Get-PropValue -ObjectValue $paperCandidate -Name 'slippage_bps_mean' -DefaultValue ''))") | Out-Null
    $lines.Add("- paper_t15_gate_pass: $([string](Get-PropValue -ObjectValue $paperCandidate -Name 't15_gate_pass' -DefaultValue ''))") | Out-Null
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
$resolvedPaperSmokeScript = if ([string]::IsNullOrWhiteSpace($PaperSmokeScript)) { Join-Path $resolvedProjectRoot "scripts/paper_micro_smoke.ps1" } else { $PaperSmokeScript }
$resolvedOutDir = if ([System.IO.Path]::IsPathRooted($OutDir)) { $OutDir } else { Join-Path $resolvedProjectRoot $OutDir }
$resolvedPaperSmokeOutDir = Join-Path $resolvedOutDir "paper_smoke"
$resolvedRegistryRoot = Join-Path $resolvedProjectRoot "models/registry"
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
$windowRamp = Resolve-TrainWindowRamp `
    -ProjectRoot $resolvedProjectRoot `
    -BatchDate $effectiveBatchDate `
    -Tf $Tf `
    -RequestedTrainLookbackDays $TrainLookbackDays `
    -RequestedBacktestLookbackDays $BacktestLookbackDays `
    -RampEnabled $TrainLookbackRampEnabled `
    -MicroRoot $TrainLookbackRampMicroRoot `
    -MinMarketsPerDate $TrainLookbackRampMinMarketsPerDate `
    -TrainStartFloorDate $TrainStartFloorDate
$certificationStartDate = [string](Get-PropValue -ObjectValue $windowRamp -Name "certification_start_date" -DefaultValue "")
$trainEndDate = [string](Get-PropValue -ObjectValue $windowRamp -Name "train_end_date" -DefaultValue "")
$trainStartDate = [string](Get-PropValue -ObjectValue $windowRamp -Name "train_start_date" -DefaultValue "")
$promotionPolicyConfig = Resolve-PromotionPolicyConfig -PolicyName $PromotionPolicy -BoundParams $PSBoundParameters -EconomicObjectiveProfile @{}

$candidatePointerPath = Resolve-RegistryPointerPath -RegistryRoot $resolvedRegistryRoot -Family $ModelFamily -PointerName "latest_candidate"
$championPointerPath = Resolve-RegistryPointerPath -RegistryRoot $resolvedRegistryRoot -Family $ModelFamily -PointerName "champion"
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
        trainer_evidence_mode = $TrainerEvidenceMode
        train_top_n = [int]$TrainTopN
        backtest_top_n = [int]$BacktestTopN
        booster_sweep_trials = [int]$BoosterSweepTrials
        backtest_top_pct = [double]$BacktestTopPct
        backtest_min_prob = [double]$BacktestMinProb
        backtest_min_candidates_per_ts = [int]$BacktestMinCandidatesPerTs
        hold_bars = [int]$HoldBars
        paper_soak_duration_sec = [int]$PaperSoakDurationSec
        paper_micro_provider = $PaperMicroProvider
        paper_feature_provider = $PaperFeatureProvider
        paper_use_learned_runtime = [bool]$PaperUseLearnedRuntime
        paper_min_active_windows = [int]$PaperMinActiveWindows
        paper_min_nonnegative_window_ratio = [double]$PaperMinNonnegativeWindowRatio
        paper_max_fill_concentration_ratio = [double]$PaperMaxFillConcentrationRatio
        backtest_min_orders_filled = [int]$promotionPolicyConfig.backtest_min_orders_filled
        backtest_min_realized_pnl_quote = [double]$promotionPolicyConfig.backtest_min_realized_pnl_quote
        backtest_min_deflated_sharpe_ratio = [double]$promotionPolicyConfig.backtest_min_deflated_sharpe_ratio
        backtest_min_pnl_delta_vs_champion = [double]$promotionPolicyConfig.backtest_min_pnl_delta_vs_champion
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
    }
    windows_by_step = [ordered]@{
        train = [ordered]@{ start = $trainStartDate; end = $trainEndDate }
        research = [ordered]@{ start = $trainStartDate; end = $trainEndDate; source = "train_command_window" }
        certification = [ordered]@{ start = $certificationStartDate; end = $effectiveBatchDate }
        backtest = [ordered]@{ start = $certificationStartDate; end = $effectiveBatchDate; alias_of = "certification" }
    }
    window_ramp = $windowRamp
    steps = [ordered]@{}
    candidate = [ordered]@{}
    gates = [ordered]@{}
    reasons = @()
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
$report.steps.window_ramp = $windowRamp

function Sync-ReportTopLevelSummary {
    $candidate = Get-PropValue -ObjectValue $report -Name "candidate" -DefaultValue @{}
    $gates = Get-PropValue -ObjectValue $report -Name "gates" -DefaultValue @{}
    $backtestGate = Get-PropValue -ObjectValue $gates -Name "backtest" -DefaultValue @{}
    $paperGate = Get-PropValue -ObjectValue $gates -Name "paper" -DefaultValue @{}
    $report.candidate_run_id = [string](Get-PropValue -ObjectValue $candidate -Name "run_id" -DefaultValue "")
    $report.candidate_run_dir = [string](Get-PropValue -ObjectValue $candidate -Name "run_dir" -DefaultValue "")
    $report.champion_before_run_id = [string](Get-PropValue -ObjectValue $candidate -Name "champion_before_run_id" -DefaultValue "")
    $report.champion_after_run_id = [string](Get-PropValue -ObjectValue $candidate -Name "champion_after_run_id" -DefaultValue "")
    $report.overall_pass = Get-PropValue -ObjectValue $gates -Name "overall_pass" -DefaultValue $null
    $report.backtest_pass = Get-PropValue -ObjectValue $backtestGate -Name "pass" -DefaultValue $null
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

    $lineMatch = [Regex]::Match($TextValue, '(?m)^\[[^\]]+\]\s+run_dir=(.+)$')
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
        $dailyArgs = @(
            "-NoProfile",
            "-File", $resolvedDailyPipelineScript,
            "-ProjectRoot", $resolvedProjectRoot,
            "-PythonExe", $resolvedPythonExe,
            "-Date", $effectiveBatchDate,
            "-SkipSmoke"
        )
        $dailyExec = Invoke-CommandCapture -Exe $psExe -ArgList $dailyArgs
        $report.steps.daily_pipeline = [ordered]@{
            attempted = $true
            exit_code = [int]$dailyExec.ExitCode
            command = $dailyExec.Command
            output_preview = (Get-OutputPreview -Text ([string]$dailyExec.Output))
        }
        if ($dailyExec.ExitCode -ne 0) {
            $report.reasons = @("DAILY_PIPELINE_FAILED")
            $report.gates.overall_pass = $false
            $paths = Save-Report
            Write-ReportPointers -LogTag $LogTag -Paths $paths -OverallPass $false
            exit 2
        }
    } else {
        $report.steps.daily_pipeline = [ordered]@{ attempted = $false; reason = "SKIPPED_BY_FLAG" }
    }

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
        $featuresBuildArgs = @(
            "-m", "autobot.cli",
            "features", "build",
            "--feature-set", $FeatureSet,
            "--label-set", $LabelSet,
            "--tf", $Tf,
            "--quote", $Quote,
            "--top-n", $TrainTopN,
            "--start", $trainStartDate,
            "--end", $trainEndDate
        )
        $featuresBuildExec = Invoke-CommandCapture -Exe $resolvedPythonExe -ArgList $featuresBuildArgs
        $report.steps.features_build = [ordered]@{
            attempted = $true
            exit_code = [int]$featuresBuildExec.ExitCode
            command = $featuresBuildExec.Command
            output_preview = (Get-OutputPreview -Text ([string]$featuresBuildExec.Output))
            feature_set = $FeatureSet
            label_set = $LabelSet
            start = $trainStartDate
            end = $trainEndDate
        }
        if ($featuresBuildExec.ExitCode -ne 0) {
            $report.reasons = @("FEATURES_BUILD_FAILED")
            $report.gates.overall_pass = $false
            $paths = Save-Report
            Write-ReportPointers -LogTag $LogTag -Paths $paths -OverallPass $false
            exit 2
        }
    } else {
        $report.steps.features_build = [ordered]@{ attempted = $false; reason = "SKIPPED_BY_FLAG" }
    }

    $trainArgs = @(
        "-m", "autobot.cli",
        "model", "train",
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
        "--execution-acceptance-top-n", $BacktestTopN,
        "--execution-acceptance-top-pct", $BacktestTopPct,
        "--execution-acceptance-min-prob", $BacktestMinProb,
        "--execution-acceptance-min-cands-per-ts", $BacktestMinCandidatesPerTs,
        "--execution-acceptance-hold-bars", $HoldBars
    )
    $trainExec = Invoke-CommandCapture -Exe $resolvedPythonExe -ArgList $trainArgs
    $candidateRunDir = if ($DryRun) { "" } else { Resolve-RunDirFromText -TextValue ([string]$trainExec.Output) }
    $candidateRunId = if ([string]::IsNullOrWhiteSpace($candidateRunDir)) { "" } else { Split-Path -Leaf $candidateRunDir }
    if ([string]::IsNullOrWhiteSpace($candidateRunId)) {
        $candidatePointer = if ($DryRun) { @{} } else { Load-JsonOrEmpty -PathValue $candidatePointerPath }
        $candidateRunId = [string](Get-PropValue -ObjectValue $candidatePointer -Name "run_id" -DefaultValue "")
        $candidateRunDir = if ([string]::IsNullOrWhiteSpace($candidateRunId)) { "" } else { Join-Path (Join-Path $resolvedRegistryRoot $ModelFamily) $candidateRunId }
    }
    $promotionDecisionPath = if ([string]::IsNullOrWhiteSpace($candidateRunDir)) { "" } else { Join-Path $candidateRunDir "promotion_decision.json" }
    $promotionDecision = if ([string]::IsNullOrWhiteSpace($promotionDecisionPath)) { @{} } else { Load-JsonOrEmpty -PathValue $promotionDecisionPath }
    $researchEvidencePath = if ([string]::IsNullOrWhiteSpace($candidateRunDir)) { "" } else { Join-Path $candidateRunDir "trainer_research_evidence.json" }
    $researchEvidenceArtifact = if ([string]::IsNullOrWhiteSpace($researchEvidencePath)) { @{} } else { Load-JsonOrEmpty -PathValue $researchEvidencePath }
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
        decision_surface_path = $decisionSurfacePath
        certification_artifact_path = $certificationArtifactPath
        promotion_decision_status = [string](Get-PropValue -ObjectValue $promotionDecision -Name "status" -DefaultValue "")
        trainer_evidence = $trainerEvidence
    }
    if (($trainExec.ExitCode -ne 0) -or ((-not $DryRun) -and [string]::IsNullOrWhiteSpace($candidateRunId))) {
        $report.reasons = @("TRAIN_OR_CANDIDATE_POINTER_FAILED")
        $report.gates.overall_pass = $false
        $paths = Save-Report
        Write-ReportPointers -LogTag $LogTag -Paths $paths -OverallPass $false
        exit 2
    }
    $candidateBacktestModelRef = if ([string]::IsNullOrWhiteSpace($candidateRunId)) { $CandidateModelRef } else { $candidateRunId }
    $candidatePaperModelRef = $candidateBacktestModelRef
    $championRunId = [string](Get-PropValue -ObjectValue $championBefore -Name "run_id" -DefaultValue "")
    $championBacktestModelRef = if ([string]::IsNullOrWhiteSpace($championRunId)) { $ChampionModelRef } else { $championRunId }
    $championModelRunDir = if ([string]::IsNullOrWhiteSpace($championRunId)) { "" } else { Join-Path (Join-Path $resolvedRegistryRoot $ModelFamily) $championRunId }
    $duplicateCandidateArtifacts = Resolve-DuplicateCandidateArtifacts -CandidateRunDir $candidateRunDir -ChampionRunDir $championModelRunDir
    $report.candidate = [ordered]@{
        run_id = $candidateRunId
        run_dir = $candidateRunDir
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
        decision_surface_path = $decisionSurfacePath
        certification_artifact_path = $certificationArtifactPath
        promotion_decision = $promotionDecision
        candidate_model_ref_requested = $CandidateModelRef
        candidate_run_id_used_for_backtest = $candidateBacktestModelRef
        candidate_run_id_used_for_paper = $candidatePaperModelRef
        champion_model_ref_requested = $ChampionModelRef
        champion_run_id_used_for_backtest = $championBacktestModelRef
        champion_before_run_id = [string](Get-PropValue -ObjectValue $championBefore -Name "run_id" -DefaultValue "")
        duplicate_candidate = (To-Bool (Get-PropValue -ObjectValue $duplicateCandidateArtifacts -Name "duplicate" -DefaultValue $false) $false)
        duplicate_artifacts = $duplicateCandidateArtifacts
    }
    $report.steps.train.duplicate_candidate = (To-Bool (Get-PropValue -ObjectValue $duplicateCandidateArtifacts -Name "duplicate" -DefaultValue $false) $false)
    $report.steps.train.duplicate_candidate_artifacts = $duplicateCandidateArtifacts

    if (To-Bool (Get-PropValue -ObjectValue $duplicateCandidateArtifacts -Name "duplicate" -DefaultValue $false) $false) {
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

    $candidateBacktest = Invoke-BacktestAndLoadSummary -PythonPath $resolvedPythonExe -Root $resolvedProjectRoot -ModelRef $candidateBacktestModelRef -StartDate $certificationStartDate -EndDate $effectiveBatchDate
    $candidateSummary = $candidateBacktest.Summary
    $candidateOrdersFilled = To-Int64 (Get-PropValue -ObjectValue $candidateSummary -Name "orders_filled" -DefaultValue 0) 0
    $candidateRealizedPnl = To-Double (Get-PropValue -ObjectValue $candidateSummary -Name "realized_pnl_quote" -DefaultValue 0.0) 0.0
    $candidateFillRate = To-Double (Get-PropValue -ObjectValue $candidateSummary -Name "fill_rate" -DefaultValue -1.0) -1.0
    $candidateMaxDrawdownPct = To-Double (Get-PropValue -ObjectValue $candidateSummary -Name "max_drawdown_pct" -DefaultValue -1.0) -1.0
    $candidateSlippageBpsMean = Get-NullableDouble (Get-PropValue -ObjectValue $candidateSummary -Name "slippage_bps_mean" -DefaultValue $null)
    $candidateCalmarLikeScore = Get-CalmarLikeScore -RealizedPnlQuote $candidateRealizedPnl -MaxDrawdownPct $candidateMaxDrawdownPct
    $candidateStatValidation = Invoke-BacktestStatValidation `
        -PythonPath $resolvedPythonExe `
        -Root $resolvedProjectRoot `
        -RunDir $candidateBacktest.RunDir `
        -TrialCount $BoosterSweepTrials `
        -ModelRunDir $candidateRunDir
    $candidateDeflatedSharpeRatio = To-Double (Get-PropValue -ObjectValue $candidateStatValidation -Name "deflated_sharpe_ratio_est" -DefaultValue 0.0) 0.0
    $candidateProbabilisticSharpeRatio = To-Double (Get-PropValue -ObjectValue $candidateStatValidation -Name "probabilistic_sharpe_ratio" -DefaultValue 0.0) 0.0
    $candidateStatComparable = To-Bool (Get-PropValue -ObjectValue $candidateStatValidation -Name "comparable" -DefaultValue $false) $false

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
    if (-not [string]::IsNullOrWhiteSpace($championRunId)) {
        $championBacktest = Invoke-BacktestAndLoadSummary -PythonPath $resolvedPythonExe -Root $resolvedProjectRoot -ModelRef $championBacktestModelRef -StartDate $certificationStartDate -EndDate $effectiveBatchDate
        $championSummary = $championBacktest.Summary
        $championRealizedPnl = To-Double (Get-PropValue -ObjectValue $championSummary -Name "realized_pnl_quote" -DefaultValue 0.0) 0.0
        $championFillRate = To-Double (Get-PropValue -ObjectValue $championSummary -Name "fill_rate" -DefaultValue -1.0) -1.0
        $championMaxDrawdownPct = To-Double (Get-PropValue -ObjectValue $championSummary -Name "max_drawdown_pct" -DefaultValue -1.0) -1.0
        $championSlippageBpsMean = Get-NullableDouble (Get-PropValue -ObjectValue $championSummary -Name "slippage_bps_mean" -DefaultValue $null)
        $championCalmarLikeScore = Get-CalmarLikeScore -RealizedPnlQuote $championRealizedPnl -MaxDrawdownPct $championMaxDrawdownPct
        $championStatValidation = Invoke-BacktestStatValidation `
            -PythonPath $resolvedPythonExe `
            -Root $resolvedProjectRoot `
            -RunDir $championBacktest.RunDir `
            -TrialCount $BoosterSweepTrials `
            -ModelRunDir $championModelRunDir
        $championDeflatedSharpeRatio = Get-NullableDouble (Get-PropValue -ObjectValue $championStatValidation -Name "deflated_sharpe_ratio_est" -DefaultValue $null)
        $championProbabilisticSharpeRatio = Get-NullableDouble (Get-PropValue -ObjectValue $championStatValidation -Name "probabilistic_sharpe_ratio" -DefaultValue $null)
        $championCompareEvaluated = $true
    }

    $candidateDeflatedSharpePass = (-not $candidateStatComparable) -or ($candidateDeflatedSharpeRatio -ge [double]$promotionPolicyConfig.backtest_min_deflated_sharpe_ratio)
    $candidateBacktestPass = ($candidateOrdersFilled -ge [int]$promotionPolicyConfig.backtest_min_orders_filled) -and ($candidateRealizedPnl -ge [double]$promotionPolicyConfig.backtest_min_realized_pnl_quote) -and $candidateDeflatedSharpePass
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
    $decisionBasis = if ($championCompareEvaluated) { "PENDING_COMPARE" } else { "NO_EXISTING_CHAMPION" }
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
        $candidateCompareMetrics = @{
            realized_pnl_quote = [double]$candidateRealizedPnl
            fill_rate = if ($candidateFillRate -ge 0.0) { [double]$candidateFillRate } else { $null }
            max_drawdown_pct = if ($candidateMaxDrawdownPct -ge 0.0) { [double]$candidateMaxDrawdownPct } else { $null }
            slippage_bps_mean = $candidateSlippageBpsMean
            calmar_like = $candidateCalmarLikeScore
        }
        $championCompareMetrics = @{
            realized_pnl_quote = [double]$championRealizedPnl
            fill_rate = if ($championFillRate -ge 0.0) { [double]$championFillRate } else { $null }
            max_drawdown_pct = if ($championMaxDrawdownPct -ge 0.0) { [double]$championMaxDrawdownPct } else { $null }
            slippage_bps_mean = $championSlippageBpsMean
            calmar_like = $championCalmarLikeScore
        }
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
        -CompareRequired ([bool]$promotionPolicyConfig.backtest_compare_required) `
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
    $backtestPass = if ($promotionPolicyConfig.backtest_compare_required) {
        $candidateBacktestPass -and $championDeltaPass -and $trainerEvidenceGatePass -and $budgetContractGatePass
    } else {
        $candidateBacktestPass -and $trainerEvidenceGatePass -and $budgetContractGatePass
    }
    if ((-not $promotionPolicyConfig.backtest_compare_required) -and $decisionBasis -eq "PENDING_COMPARE") {
        $decisionBasis = "SANITY_ONLY_BACKTEST"
    }
    if (($TrainerEvidenceMode -eq "required") -and (-not $trainerEvidenceGatePass)) {
        $decisionBasis = "TRAINER_EVIDENCE_REQUIRED_FAIL"
    } elseif (-not $budgetContractGatePass) {
        $decisionBasis = "SCOUT_ONLY_BUDGET_EVIDENCE"
    }
    $report.steps.backtest_candidate = [ordered]@{
        exit_code = [int]$candidateBacktest.Exec.ExitCode
        command = $candidateBacktest.Exec.Command
        output_preview = (Get-OutputPreview -Text ([string]$candidateBacktest.Exec.Output))
        start = $certificationStartDate
        end = $effectiveBatchDate
        model_ref_requested = $CandidateModelRef
        model_ref_used = $candidateBacktestModelRef
        run_dir = $candidateBacktest.RunDir
        summary_path = $candidateBacktest.SummaryPath
        orders_filled = [int64]$candidateOrdersFilled
        realized_pnl_quote = [double]$candidateRealizedPnl
        fill_rate = [double]$candidateFillRate
        max_drawdown_pct = [double]$candidateMaxDrawdownPct
        slippage_bps_mean = $candidateSlippageBpsMean
        calmar_like_score = $candidateCalmarLikeScore
        deflated_sharpe_ratio_est = [double]$candidateDeflatedSharpeRatio
        probabilistic_sharpe_ratio = [double]$candidateProbabilisticSharpeRatio
        stat_validation = $candidateStatValidation
    }
    $report.steps.backtest_champion = if ($championCompareEvaluated) {
        [ordered]@{
            exit_code = [int]$championBacktest.Exec.ExitCode
            command = $championBacktest.Exec.Command
            output_preview = (Get-OutputPreview -Text ([string]$championBacktest.Exec.Output))
            start = $certificationStartDate
            end = $effectiveBatchDate
            model_ref_requested = $ChampionModelRef
            model_ref_used = $championBacktestModelRef
            run_dir = $championBacktest.RunDir
            summary_path = $championBacktest.SummaryPath
            realized_pnl_quote = [double]$championRealizedPnl
            fill_rate = [double]$championFillRate
            max_drawdown_pct = [double]$championMaxDrawdownPct
            slippage_bps_mean = $championSlippageBpsMean
            calmar_like_score = $championCalmarLikeScore
            deflated_sharpe_ratio_est = $championDeflatedSharpeRatio
            probabilistic_sharpe_ratio = $championProbabilisticSharpeRatio
            stat_validation = $championStatValidation
        }
    } else {
        [ordered]@{ attempted = $false; reason = "NO_EXISTING_CHAMPION" }
    }
    $report.gates.backtest = [ordered]@{
        promotion_policy_contract_source = [string]$promotionPolicyConfig.threshold_source
        promotion_policy_contract_profile_id = [string]$promotionPolicyConfig.profile_id
        promotion_policy_cli_override_keys = @($promotionPolicyConfig.cli_override_keys)
        candidate_min_orders_pass = ($candidateOrdersFilled -ge [int]$promotionPolicyConfig.backtest_min_orders_filled)
        candidate_min_orders_threshold = [int]$promotionPolicyConfig.backtest_min_orders_filled
        candidate_min_realized_pnl_pass = ($candidateRealizedPnl -ge [double]$promotionPolicyConfig.backtest_min_realized_pnl_quote)
        candidate_min_realized_pnl_threshold = [double]$promotionPolicyConfig.backtest_min_realized_pnl_quote
        candidate_dsr_evaluated = $candidateStatComparable
        candidate_min_deflated_sharpe_ratio = [double]$promotionPolicyConfig.backtest_min_deflated_sharpe_ratio
        candidate_deflated_sharpe_ratio_est = [double]$candidateDeflatedSharpeRatio
        candidate_deflated_sharpe_pass = $candidateDeflatedSharpePass
        compare_required = [bool]$promotionPolicyConfig.backtest_compare_required
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
            To-Bool (Get-PropValue -ObjectValue $paperEvidenceDecision -Name "pass" -DefaultValue $false) $false
        } else {
            $paperT15GatePass
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
            orders_submitted = [int64](To-Int64 (Get-PropValue -ObjectValue $paperSmoke -Name "orders_submitted" -DefaultValue 0) 0)
            orders_filled = $paperOrdersFilled
            fill_rate = [double](To-Double (Get-PropValue -ObjectValue $paperSmoke -Name "fill_rate" -DefaultValue 0.0) 0.0)
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
            t15_gate_pass = $paperT15GatePass
            evidence_score = [double](To-Double (Get-PropValue -ObjectValue $paperEvidenceDecision -Name "evidence_score" -DefaultValue 0.0) 0.0)
            final_decision_basis = [string](Get-PropValue -ObjectValue $paperEvidenceDecision -Name "final_decision_basis" -DefaultValue "")
            final_decision = [string](Get-PropValue -ObjectValue $paperEvidenceDecision -Name "decision" -DefaultValue "")
            hard_failures = @((Get-PropValue -ObjectValue $paperEvidenceDecision -Name "hard_failures" -DefaultValue @()))
            soft_failures = @((Get-PropValue -ObjectValue $paperEvidenceDecision -Name "soft_failures" -DefaultValue @()))
            evidence_components = (Get-PropValue -ObjectValue $paperEvidenceDecision -Name "evidence_components" -DefaultValue @{})
            learned_runtime = [bool]$PaperUseLearnedRuntime
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

    $overallPass = if ($SkipPaperSoak) { $backtestPass } else { $backtestPass -and $paperPass }
    $reasons = @()
    $notes = @()
    if (-not $backtestPass) {
        $reasons += "BACKTEST_ACCEPTANCE_FAILED"
    }
    if (($TrainerEvidenceMode -eq "required") -and (-not $trainerEvidenceGatePass)) {
        $reasons += "TRAINER_EVIDENCE_REQUIRED_FAILED"
    }
    if (-not $budgetContractGatePass) {
        $reasons += "SCOUT_ONLY_BUDGET_EVIDENCE"
    }
    if ($laneShadowOnly -or (-not $lanePromotionAllowed)) {
        $notes += "SHADOW_LANE_ONLY"
    }
    if ($SkipPaperSoak) {
        $notes += "PAPER_SOAK_SKIPPED"
    } elseif (-not $paperPass) {
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

    if ($DryRun) {
        $report.gates.overall_pass = $null
        $report.reasons = @("DRY_RUN_ONLY")
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
    $report.steps.exception = [ordered]@{
        message = $_.Exception.Message
    }
    $paths = Save-Report
    Write-Host ("[{0}][error] {1}" -f $LogTag, $_.Exception.Message)
    Write-ReportPointers -LogTag $LogTag -Paths $paths -OverallPass $false
    exit 2
}
