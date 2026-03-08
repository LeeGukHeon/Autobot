param(
    [string]$PythonExe = "",
    [string]$ProjectRoot = "",
    [string]$BatchDate = "",
    [string]$DailyPipelineScript = "",
    [string]$PaperSmokeScript = "",
    [string]$OutDir = "logs/model_acceptance",
    [int]$TrainLookbackDays = 30,
    [int]$BacktestLookbackDays = 8,
    [string]$Tf = "5m",
    [string]$Quote = "KRW",
    [int]$TrainTopN = 50,
    [int]$BacktestTopN = 20,
    [string]$ModelFamily = "train_v3_mtf_micro",
    [string]$Trainer = "v3_mtf_micro",
    [string]$FeatureSet = "v3",
    [string]$LabelSet = "v1",
    [string]$Task = "cls",
    [string]$CandidateModelRef = "latest_candidate_v3",
    [string]$ChampionModelRef = "champion_v3",
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
    [string]$PaperFeatureProvider = "live_v3",
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
    [string[]]$RestartUnits = @(),
    [string[]]$KnownRuntimeUnits = @("autobot-paper-alpha.service", "autobot-live-alpha.service"),
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
        [System.Collections.IDictionary]$BoundParams
    )
    $resolved = [ordered]@{
        name = $PolicyName
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
    if ($BoundParams.ContainsKey("BacktestAllowStabilityOverride")) {
        $resolved.backtest_allow_stability_override = [bool]$BacktestAllowStabilityOverride
    }
    if ($BoundParams.ContainsKey("BacktestChampionPnlTolerancePct")) {
        $resolved.backtest_champion_pnl_tolerance_pct = [double]$BacktestChampionPnlTolerancePct
    }
    if ($BoundParams.ContainsKey("BacktestChampionMaxFillRateDegradation")) {
        $resolved.backtest_champion_max_fill_rate_degradation = [double]$BacktestChampionMaxFillRateDegradation
    }
    if ($BoundParams.ContainsKey("BacktestChampionMaxSlippageDeteriorationBps")) {
        $resolved.backtest_champion_max_slippage_deterioration_bps = [double]$BacktestChampionMaxSlippageDeteriorationBps
    }
    if ($BoundParams.ContainsKey("BacktestChampionMinUtilityEdgePct")) {
        $resolved.backtest_champion_min_utility_edge_pct = [double]$BacktestChampionMinUtilityEdgePct
    }
    return $resolved
}

function Resolve-TrainerEvidence {
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
    if ($null -eq $PromotionDecision -or (@($PromotionDecision.PSObject.Properties).Count -eq 0 -and -not ($PromotionDecision -is [System.Collections.IDictionary]))) {
        $resolved.pass = $false
        $resolved.reasons = @("MISSING_PROMOTION_DECISION")
        return $resolved
    }

    $checks = Get-PropValue -ObjectValue $PromotionDecision -Name "checks" -DefaultValue @{}
    $research = Get-PropValue -ObjectValue $PromotionDecision -Name "research_acceptance" -DefaultValue @{}
    $offlineCompare = Get-PropValue -ObjectValue $research -Name "compare_to_champion" -DefaultValue @{}
    $walkSummary = Get-PropValue -ObjectValue $research -Name "walk_forward_summary" -DefaultValue @{}
    $executionDoc = Get-PropValue -ObjectValue $PromotionDecision -Name "execution_acceptance" -DefaultValue @{}
    $executionCompare = Get-PropValue -ObjectValue $executionDoc -Name "compare_to_champion" -DefaultValue @{}

    $existingChampionPresent = To-Bool (Get-PropValue -ObjectValue $checks -Name "existing_champion_present" -DefaultValue $false) $false
    $walkForwardPresent = To-Bool (Get-PropValue -ObjectValue $checks -Name "walk_forward_present" -DefaultValue $false) $false
    $walkForwardWindowsRun = [int](To-Int64 (Get-PropValue -ObjectValue $checks -Name "walk_forward_windows_run" -DefaultValue 0) 0)
    $offlineComparable = To-Bool (Get-PropValue -ObjectValue $checks -Name "balanced_pareto_comparable" -DefaultValue $false) $false
    $offlineCandidateEdge = To-Bool (Get-PropValue -ObjectValue $checks -Name "balanced_pareto_candidate_edge" -DefaultValue $false) $false
    $executionEnabled = To-Bool (Get-PropValue -ObjectValue $checks -Name "execution_acceptance_enabled" -DefaultValue $false) $false
    $executionPresent = To-Bool (Get-PropValue -ObjectValue $checks -Name "execution_acceptance_present" -DefaultValue $false) $false
    $executionComparable = To-Bool (Get-PropValue -ObjectValue $checks -Name "execution_balanced_pareto_comparable" -DefaultValue $false) $false
    $executionCandidateEdge = To-Bool (Get-PropValue -ObjectValue $checks -Name "execution_balanced_pareto_candidate_edge" -DefaultValue $false) $false

    $offlineDecision = [string](Get-PropValue -ObjectValue $offlineCompare -Name "decision" -DefaultValue "")
    $offlinePolicy = [string](Get-PropValue -ObjectValue $research -Name "policy" -DefaultValue "")
    $executionStatus = [string](Get-PropValue -ObjectValue $executionDoc -Name "status" -DefaultValue "")
    $executionDecision = [string](Get-PropValue -ObjectValue $executionCompare -Name "decision" -DefaultValue "")
    $executionPolicy = [string](Get-PropValue -ObjectValue $executionCompare -Name "policy" -DefaultValue "")

    $resolved.available = $walkForwardPresent -or $executionPresent -or (-not [string]::IsNullOrWhiteSpace($offlineDecision)) -or (-not [string]::IsNullOrWhiteSpace($executionDecision))
    $resolved.checks.existing_champion_present = $existingChampionPresent
    $resolved.checks.walk_forward_present = $walkForwardPresent
    $resolved.checks.walk_forward_windows_run = $walkForwardWindowsRun
    $resolved.checks.offline_comparable = $offlineComparable
    $resolved.checks.offline_candidate_edge = $offlineCandidateEdge
    $resolved.checks.execution_acceptance_enabled = $executionEnabled
    $resolved.checks.execution_acceptance_present = $executionPresent
    $resolved.checks.execution_comparable = $executionComparable
    $resolved.checks.execution_candidate_edge = $executionCandidateEdge
    $resolved.offline.policy = $offlinePolicy
    $resolved.offline.decision = $offlineDecision
    $resolved.offline.comparable = $offlineComparable
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
    }

    $offlinePass = $walkForwardPresent -and ((-not $existingChampionPresent) -or ($offlineComparable -and $offlineCandidateEdge))
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
        [int]$TrialCount
    )
    if ([string]::IsNullOrWhiteSpace($RunDir)) {
        return @{}
    }
    $args = @(
        "-m", "autobot.models.stat_validation",
        "--run-dir", $RunDir,
        "--trial-count", ([string]([Math]::Max([int]$TrialCount, 1)))
    )
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

function Get-PowerShellExe {
    if ($script:IsWindowsPlatform) {
        return "powershell.exe"
    }
    return "pwsh"
}

function Get-DirectorySnapshot {
    param([string]$PathValue)
    $snapshot = @{}
    if (Test-Path $PathValue) {
        Get-ChildItem -Path $PathValue -Directory -ErrorAction SilentlyContinue | ForEach-Object {
            $snapshot[$_.Name] = $true
        }
    }
    return $snapshot
}

function Resolve-NewRunDirectory {
    param(
        [string]$RunsDir,
        [hashtable]$BeforeSnapshot
    )
    if (-not (Test-Path $RunsDir)) {
        return $null
    }
    $directories = @(Get-ChildItem -Path $RunsDir -Directory | Sort-Object LastWriteTime -Descending)
    foreach ($item in $directories) {
        if (-not $BeforeSnapshot.ContainsKey($item.Name)) {
            return $item.FullName
        }
    }
    if ($directories.Count -gt 0) {
        return $directories[0].FullName
    }
    return $null
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
    $runsDir = Join-Path $Root "data/backtest/runs"
    $before = Get-DirectorySnapshot -PathValue $runsDir
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
    $runDir = if ($DryRun) { "" } else { Resolve-NewRunDirectory -RunsDir $runsDir -BeforeSnapshot $before }
    $summaryPath = if ([string]::IsNullOrWhiteSpace($runDir)) { "" } else { Join-Path $runDir "summary.json" }
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
$batchDateObj = [DateTime]::ParseExact($effectiveBatchDate, "yyyy-MM-dd", [System.Globalization.CultureInfo]::InvariantCulture)
$trainStartDate = $batchDateObj.AddDays(-1 * [Math]::Max($TrainLookbackDays - 1, 0)).ToString("yyyy-MM-dd")
$backtestStartDate = $batchDateObj.AddDays(-1 * [Math]::Max($BacktestLookbackDays - 1, 0)).ToString("yyyy-MM-dd")
$promotionPolicyConfig = Resolve-PromotionPolicyConfig -PolicyName $PromotionPolicy -BoundParams $PSBoundParameters

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
    windows = [bool]$script:IsWindowsPlatform
    config = [ordered]@{
        train_lookback_days = [int]$TrainLookbackDays
        backtest_lookback_days = [int]$BacktestLookbackDays
        tf = $Tf
        quote = $Quote
        trainer = $Trainer
        feature_set = $FeatureSet
        label_set = $LabelSet
        task = $Task
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
        backtest_min_orders_filled = [int]$BacktestMinOrdersFilled
        backtest_min_realized_pnl_quote = [double]$BacktestMinRealizedPnlQuote
        backtest_min_pnl_delta_vs_champion = [double]$BacktestMinPnlDeltaVsChampion
        promotion_policy = $promotionPolicyConfig.name
        backtest_allow_stability_override = [bool]$promotionPolicyConfig.backtest_allow_stability_override
        backtest_champion_pnl_tolerance_pct = [double]$promotionPolicyConfig.backtest_champion_pnl_tolerance_pct
        backtest_champion_min_drawdown_improvement_pct = [double]$BacktestChampionMinDrawdownImprovementPct
        backtest_champion_max_fill_rate_degradation = [double]$promotionPolicyConfig.backtest_champion_max_fill_rate_degradation
        backtest_champion_max_slippage_deterioration_bps = [double]$promotionPolicyConfig.backtest_champion_max_slippage_deterioration_bps
        backtest_champion_min_utility_edge_pct = [double]$promotionPolicyConfig.backtest_champion_min_utility_edge_pct
        backtest_use_pareto = [bool]$promotionPolicyConfig.use_pareto
        backtest_use_utility_tie_break = [bool]$promotionPolicyConfig.use_utility_tie_break
        skip_paper_soak = [bool]$SkipPaperSoak
        restart_units = @($RestartUnits)
        known_runtime_units = @($KnownRuntimeUnits)
        auto_restart_known_units = [bool]$AutoRestartKnownUnits
    }
    windows_by_step = [ordered]@{
        train = [ordered]@{ start = $trainStartDate; end = $effectiveBatchDate }
        backtest = [ordered]@{ start = $backtestStartDate; end = $effectiveBatchDate }
    }
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

function Save-Report {
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

function Resolve-FreshLatestJsonPath {
    param(
        [string]$LatestPath,
        [DateTime]$StartedAtUtc
    )
    if ([string]::IsNullOrWhiteSpace($LatestPath) -or (-not (Test-Path $LatestPath))) {
        return ""
    }
    $item = Get-Item -Path $LatestPath -ErrorAction SilentlyContinue
    if ($null -eq $item) {
        return ""
    }
    if ($item.LastWriteTimeUtc -lt $StartedAtUtc.AddSeconds(-2)) {
        return ""
    }
    return $item.FullName
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

    $trainArgs = @(
        "-m", "autobot.cli",
        "model", "train",
        "--trainer", $Trainer,
        "--model-family", $ModelFamily,
        "--feature-set", $FeatureSet,
        "--label-set", $LabelSet,
        "--task", $Task,
        "--tf", $Tf,
        "--quote", $Quote,
        "--top-n", $TrainTopN,
        "--start", $trainStartDate,
        "--end", $effectiveBatchDate,
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
    $candidatePointer = if ($DryRun) { @{} } else { Load-JsonOrEmpty -PathValue $candidatePointerPath }
    $candidateRunId = [string](Get-PropValue -ObjectValue $candidatePointer -Name "run_id" -DefaultValue "")
    $candidateRunDir = if ([string]::IsNullOrWhiteSpace($candidateRunId)) { "" } else { Join-Path (Join-Path $resolvedRegistryRoot $ModelFamily) $candidateRunId }
    $promotionDecisionPath = if ([string]::IsNullOrWhiteSpace($candidateRunDir)) { "" } else { Join-Path $candidateRunDir "promotion_decision.json" }
    $promotionDecision = if ([string]::IsNullOrWhiteSpace($promotionDecisionPath)) { @{} } else { Load-JsonOrEmpty -PathValue $promotionDecisionPath }
    $trainerEvidence = Resolve-TrainerEvidence -PromotionDecision $promotionDecision -Mode $TrainerEvidenceMode
    $report.steps.train = [ordered]@{
        exit_code = [int]$trainExec.ExitCode
        command = $trainExec.Command
        output_preview = (Get-OutputPreview -Text ([string]$trainExec.Output))
        candidate_run_id = $candidateRunId
        candidate_run_dir = $candidateRunDir
        promotion_decision_path = $promotionDecisionPath
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
    $report.candidate = [ordered]@{
        run_id = $candidateRunId
        run_dir = $candidateRunDir
        promotion_decision_path = $promotionDecisionPath
        promotion_decision = $promotionDecision
        champion_before_run_id = [string](Get-PropValue -ObjectValue $championBefore -Name "run_id" -DefaultValue "")
    }

    $candidateBacktest = Invoke-BacktestAndLoadSummary -PythonPath $resolvedPythonExe -Root $resolvedProjectRoot -ModelRef $CandidateModelRef -StartDate $backtestStartDate -EndDate $effectiveBatchDate
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
        -TrialCount $BoosterSweepTrials
    $candidateDeflatedSharpeRatio = To-Double (Get-PropValue -ObjectValue $candidateStatValidation -Name "deflated_sharpe_ratio_est" -DefaultValue 0.0) 0.0
    $candidateProbabilisticSharpeRatio = To-Double (Get-PropValue -ObjectValue $candidateStatValidation -Name "probabilistic_sharpe_ratio" -DefaultValue 0.0) 0.0
    $candidateStatComparable = To-Bool (Get-PropValue -ObjectValue $candidateStatValidation -Name "comparable" -DefaultValue $false) $false

    $championRunId = [string](Get-PropValue -ObjectValue $championBefore -Name "run_id" -DefaultValue "")
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
        $championBacktest = Invoke-BacktestAndLoadSummary -PythonPath $resolvedPythonExe -Root $resolvedProjectRoot -ModelRef $ChampionModelRef -StartDate $backtestStartDate -EndDate $effectiveBatchDate
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
            -TrialCount $BoosterSweepTrials
        $championDeflatedSharpeRatio = Get-NullableDouble (Get-PropValue -ObjectValue $championStatValidation -Name "deflated_sharpe_ratio_est" -DefaultValue $null)
        $championProbabilisticSharpeRatio = Get-NullableDouble (Get-PropValue -ObjectValue $championStatValidation -Name "probabilistic_sharpe_ratio" -DefaultValue $null)
        $championCompareEvaluated = $true
    }

    $candidateDeflatedSharpePass = (-not $candidateStatComparable) -or ($candidateDeflatedSharpeRatio -ge $BacktestMinDeflatedSharpeRatio)
    $candidateBacktestPass = ($candidateOrdersFilled -ge $BacktestMinOrdersFilled) -and ($candidateRealizedPnl -ge $BacktestMinRealizedPnlQuote) -and $candidateDeflatedSharpePass
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
    $utilityDeltaPct = $null
    $utilityTieBreakPass = $false
    $candidateParetoDominates = $false
    $championParetoDominates = $false
    $paretoIncomparable = $false
    $stabilityOverridePass = $false
    $trainerEvidenceApplied = $TrainerEvidenceMode -ne "ignore"
    $trainerEvidenceAvailable = To-Bool (Get-PropValue -ObjectValue $trainerEvidence -Name "available" -DefaultValue $false) $false
    $trainerEvidencePass = To-Bool (Get-PropValue -ObjectValue $trainerEvidence -Name "pass" -DefaultValue $false) $false
    $trainerEvidenceOfflinePass = To-Bool (Get-PropValue -ObjectValue $trainerEvidence -Name "offline_pass" -DefaultValue $false) $false
    $trainerEvidenceExecutionPass = To-Bool (Get-PropValue -ObjectValue $trainerEvidence -Name "execution_pass" -DefaultValue $true) $true
    $trainerEvidenceGatePass = if ($TrainerEvidenceMode -eq "required") { $trainerEvidencePass } else { $true }
    $trainerEvidenceReasons = @(Get-PropValue -ObjectValue $trainerEvidence -Name "reasons" -DefaultValue @())
    $decisionBasis = if ($championCompareEvaluated) { "PENDING_COMPARE" } else { "NO_EXISTING_CHAMPION" }
    if ($championCompareEvaluated) {
        $strictChampionDeltaPass = $championDeltaQuote -ge $BacktestMinPnlDeltaVsChampion
        $championWithinTolerancePass = $strictChampionDeltaPass
        if ($championRealizedPnl -ne 0.0) {
            $championDeltaPct = $championDeltaQuote / $championRealizedPnl
            $championWithinTolerancePass = $championDeltaPct -ge (-1.0 * [double]$promotionPolicyConfig.backtest_champion_pnl_tolerance_pct)
        }
        if (($candidateMaxDrawdownPct -ge 0.0) -and ($championMaxDrawdownPct -ge 0.0)) {
            if ($championMaxDrawdownPct -gt 0.0) {
                $drawdownImprovementPct = ($championMaxDrawdownPct - $candidateMaxDrawdownPct) / $championMaxDrawdownPct
                $drawdownImprovementPass = $drawdownImprovementPct -ge 0.0
                if ($PSBoundParameters.ContainsKey("BacktestChampionMinDrawdownImprovementPct")) {
                    $drawdownImprovementPass = $drawdownImprovementPct -ge $BacktestChampionMinDrawdownImprovementPct
                }
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
        if ($promotionPolicyConfig.use_pareto) {
            $paretoComparableMetricCount = 0
            $candidateWorseOnAny = $false
            $candidateBetterOnAny = $false
            $championWorseOnAny = $false
            $championBetterOnAny = $false

            $paretoComparableMetricCount += 1
            if ($candidateRealizedPnl -lt $championRealizedPnl) { $candidateWorseOnAny = $true }
            if ($candidateRealizedPnl -gt $championRealizedPnl) { $candidateBetterOnAny = $true }
            if ($championRealizedPnl -lt $candidateRealizedPnl) { $championWorseOnAny = $true }
            if ($championRealizedPnl -gt $candidateRealizedPnl) { $championBetterOnAny = $true }

            if (($candidateMaxDrawdownPct -ge 0.0) -and ($championMaxDrawdownPct -ge 0.0)) {
                $paretoComparableMetricCount += 1
                if ($candidateMaxDrawdownPct -gt $championMaxDrawdownPct) { $candidateWorseOnAny = $true }
                if ($candidateMaxDrawdownPct -lt $championMaxDrawdownPct) { $candidateBetterOnAny = $true }
                if ($championMaxDrawdownPct -gt $candidateMaxDrawdownPct) { $championWorseOnAny = $true }
                if ($championMaxDrawdownPct -lt $candidateMaxDrawdownPct) { $championBetterOnAny = $true }
            }
            if (($candidateFillRate -ge 0.0) -and ($championFillRate -ge 0.0)) {
                $paretoComparableMetricCount += 1
                if ($candidateFillRate -lt $championFillRate) { $candidateWorseOnAny = $true }
                if ($candidateFillRate -gt $championFillRate) { $candidateBetterOnAny = $true }
                if ($championFillRate -lt $candidateFillRate) { $championWorseOnAny = $true }
                if ($championFillRate -gt $candidateFillRate) { $championBetterOnAny = $true }
            }
            if (($null -ne $candidateSlippageBpsMean) -and ($null -ne $championSlippageBpsMean)) {
                $paretoComparableMetricCount += 1
                if ($candidateSlippageBpsMean -gt $championSlippageBpsMean) { $candidateWorseOnAny = $true }
                if ($candidateSlippageBpsMean -lt $championSlippageBpsMean) { $candidateBetterOnAny = $true }
                if ($championSlippageBpsMean -gt $candidateSlippageBpsMean) { $championWorseOnAny = $true }
                if ($championSlippageBpsMean -lt $candidateSlippageBpsMean) { $championBetterOnAny = $true }
            }

            if ($paretoComparableMetricCount -gt 0) {
                $candidateParetoDominates = (-not $candidateWorseOnAny) -and $candidateBetterOnAny
                $championParetoDominates = (-not $championWorseOnAny) -and $championBetterOnAny
                $paretoIncomparable = (-not $candidateParetoDominates) -and (-not $championParetoDominates)
            }
        }

        $utilityTieBreakPass = $false
        if ($promotionPolicyConfig.use_utility_tie_break -and ($null -ne $candidateCalmarLikeScore) -and ($null -ne $championCalmarLikeScore)) {
            if ((-not [double]::IsInfinity($championCalmarLikeScore)) -and ($championCalmarLikeScore -ne 0.0)) {
                $utilityDeltaPct = ($candidateCalmarLikeScore - $championCalmarLikeScore) / [Math]::Abs($championCalmarLikeScore)
                $utilityTieBreakPass = $utilityDeltaPct -ge [double]$promotionPolicyConfig.backtest_champion_min_utility_edge_pct
            } else {
                $utilityTieBreakPass = $candidateCalmarLikeScore -ge $championCalmarLikeScore
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
    $backtestPass = if ($promotionPolicyConfig.backtest_compare_required) {
        $candidateBacktestPass -and $championDeltaPass -and $trainerEvidenceGatePass
    } else {
        $candidateBacktestPass -and $trainerEvidenceGatePass
    }
    if ((-not $promotionPolicyConfig.backtest_compare_required) -and $decisionBasis -eq "PENDING_COMPARE") {
        $decisionBasis = "SANITY_ONLY_BACKTEST"
    }
    if (($TrainerEvidenceMode -eq "required") -and (-not $trainerEvidenceGatePass)) {
        $decisionBasis = "TRAINER_EVIDENCE_REQUIRED_FAIL"
    }
    $report.steps.backtest_candidate = [ordered]@{
        exit_code = [int]$candidateBacktest.Exec.ExitCode
        command = $candidateBacktest.Exec.Command
        output_preview = (Get-OutputPreview -Text ([string]$candidateBacktest.Exec.Output))
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
        candidate_min_orders_pass = ($candidateOrdersFilled -ge $BacktestMinOrdersFilled)
        candidate_min_realized_pnl_pass = ($candidateRealizedPnl -ge $BacktestMinRealizedPnlQuote)
        candidate_dsr_evaluated = $candidateStatComparable
        candidate_min_deflated_sharpe_ratio = [double]$BacktestMinDeflatedSharpeRatio
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
        vs_champion_pnl_delta_pct = $championDeltaPct
        vs_champion_pnl_within_tolerance_pass = $championWithinTolerancePass
        vs_champion_drawdown_improvement_pct = $drawdownImprovementPct
        vs_champion_drawdown_improvement_pass = $drawdownImprovementPass
        utility_metric = $utilityMetric
        vs_champion_utility_candidate_score = $candidateCalmarLikeScore
        vs_champion_utility_champion_score = $championCalmarLikeScore
        vs_champion_utility_delta_pct = $utilityDeltaPct
        vs_champion_utility_tie_break_pass = $utilityTieBreakPass
        vs_champion_fill_rate_degradation = $fillRateDegradation
        vs_champion_fill_rate_guard_pass = $fillRateGuardPass
        vs_champion_slippage_deterioration_bps = $slippageDeteriorationBps
        vs_champion_slippage_guard_pass = $slippageGuardPass
        vs_champion_stability_override_pass = $stabilityOverridePass
        trainer_evidence_applied = $trainerEvidenceApplied
        trainer_evidence_available = $trainerEvidenceAvailable
        trainer_evidence_pass = $trainerEvidencePass
        trainer_evidence_gate_pass = $trainerEvidenceGatePass
        trainer_evidence_offline_pass = $trainerEvidenceOfflinePass
        trainer_evidence_execution_pass = $trainerEvidenceExecutionPass
        trainer_evidence_reasons = @($trainerEvidenceReasons)
        decision_basis = $decisionBasis
        compare_mode = $promotionPolicyConfig.name
        pass = $backtestPass
    }

    $paperPass = $null
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
        $paperExecStartedAtUtc = [DateTime]::UtcNow
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
            "-ModelRef", $CandidateModelRef,
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
        $paperSmokeLatestPath = Join-Path $resolvedPaperSmokeOutDir "latest.json"
        $paperSmokeRunPath = if ($DryRun) { "" } else { Resolve-ReportedJsonPathFromText -TextValue ([string]$paperExec.Output) -LogTag "paper-smoke" }
        $paperSmokeEffectivePath = if (-not [string]::IsNullOrWhiteSpace($paperSmokeRunPath) -and (Test-Path $paperSmokeRunPath)) {
            $paperSmokeRunPath
        } else {
            Resolve-FreshLatestJsonPath -LatestPath $paperSmokeLatestPath -StartedAtUtc $paperExecStartedAtUtc
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
        $paperPass = if ($promotionPolicyConfig.paper_final_gate) {
            $paperT15GatePass `
                -and $paperOrdersFilledPass `
                -and $paperRealizedPnlPass `
                -and $paperMicroQualityPass `
                -and $paperActiveWindowsPass `
                -and $paperNonnegativeWindowPass `
                -and $paperFillConcentrationPass `
                -and $paperHistoryNonnegativePass `
                -and $paperHistoryPositivePass `
                -and $paperHistoryMicroQualityPass
        } else {
            $paperT15GatePass
        }
        $report.steps.paper_candidate = [ordered]@{
            exit_code = [int]$paperExec.ExitCode
            command = $paperExec.Command
            output_preview = (Get-OutputPreview -Text ([string]$paperExec.Output))
            smoke_report_path = $paperSmokeEffectivePath
            smoke_report_run_path = $paperSmokeRunPath
            smoke_report_latest_path = $paperSmokeLatestPath
            smoke_report_source = if ($paperSmokeEffectivePath -eq $paperSmokeRunPath) { "run_report" } elseif (-not [string]::IsNullOrWhiteSpace($paperSmokeEffectivePath)) { "latest_fresh_fallback" } else { "missing" }
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
        }
    }

    $overallPass = if ($SkipPaperSoak) { $backtestPass } else { $backtestPass -and $paperPass }
    $reasons = @()
    if (-not $backtestPass) {
        $reasons += "BACKTEST_ACCEPTANCE_FAILED"
    }
    if (($TrainerEvidenceMode -eq "required") -and (-not $trainerEvidenceGatePass)) {
        $reasons += "TRAINER_EVIDENCE_REQUIRED_FAILED"
    }
    if ($SkipPaperSoak) {
        $reasons += "PAPER_SOAK_SKIPPED"
    } elseif (-not $paperPass) {
        if ($promotionPolicyConfig.paper_final_gate) {
            $reasons += "PAPER_FINAL_GATE_FAILED"
        } else {
            $reasons += "PAPER_SOAK_FAILED"
        }
    }
    $report.gates.overall_pass = $overallPass
    $report.reasons = @($reasons)

    $promoteStep = [ordered]@{ attempted = $false; promoted = $false }
    if ($SkipPaperSoak) {
        $promoteStep.reason = "SKIPPED_PAPER_SOAK_REQUIRES_MANUAL_PROMOTE"
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
        $refreshArgs = @(
            "-NoProfile",
            "-File", $resolvedDailyPipelineScript,
            "-ProjectRoot", $resolvedProjectRoot,
            "-PythonExe", $resolvedPythonExe,
            "-Date", $effectiveBatchDate,
            "-SmokeReportJson", $(if ([string]::IsNullOrWhiteSpace($paperSmokeEffectivePath)) { $paperSmokeLatestPath } else { $paperSmokeEffectivePath }),
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
