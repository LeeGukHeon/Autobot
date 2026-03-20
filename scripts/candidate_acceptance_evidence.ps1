$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

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
