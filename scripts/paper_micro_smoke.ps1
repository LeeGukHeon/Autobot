param(
    [string]$PythonExe = "C:\Python314\python.exe",
    [string]$ProjectRoot = "D:\MyApps\Autobot",
    [int]$DurationSec = 600,
    [string]$Quote = "KRW",
    [int]$TopN = 20,
    [double]$MaxFallbackRatio = 0.10,
    [int]$MinOrdersSubmitted = 1,
    [int]$MinTierCount = 2,
    [int]$MinPolicyEvents = 1,
    [string]$PaperMicroProvider = "offline_parquet",
    [int]$WarmupSec = 60,
    [int]$WarmupMinTradeEventsPerMarket = 1,
    [string]$Strategy = "",
    [string]$Tf = "",
    [string]$ModelRef = "",
    [string]$ModelFamily = "",
    [string]$FeatureSet = "",
    [double]$TopPct = -1.0,
    [double]$MinProb = -1.0,
    [int]$MinCandsPerTs = -1,
    [int]$MaxPositionsTotal = -1,
    [string]$ExitMode = "",
    [int]$HoldBars = -1,
    [double]$TpPct = -1.0,
    [double]$SlPct = -1.0,
    [double]$TrailingPct = -1.0,
    [string]$PaperFeatureProvider = "",
    [string]$OutDir = "logs/paper_micro_smoke"
)

$ErrorActionPreference = "Stop"
Set-Location $ProjectRoot
$script:IsWindowsPlatform = [System.IO.Path]::DirectorySeparatorChar -eq '\'

$vendorSitePackages = Join-Path $ProjectRoot "python\site-packages"
if ($script:IsWindowsPlatform -and (Test-Path $vendorSitePackages)) {
    if ([string]::IsNullOrWhiteSpace($env:PYTHONPATH)) {
        $env:PYTHONPATH = $vendorSitePackages
    } elseif ($env:PYTHONPATH -notlike "*$vendorSitePackages*") {
        $env:PYTHONPATH = "$vendorSitePackages;$($env:PYTHONPATH)"
    }
}

function Invoke-CommandCapture {
    param(
        [string]$Exe,
        [string[]]$ArgList
    )
    $output = & $Exe @ArgList 2>&1
    return [PSCustomObject]@{
        ExitCode = [int]$LASTEXITCODE
        Output = ($output -join "`n")
    }
}

function Load-JsonOrEmpty {
    param([string]$PathValue)
    if (-not (Test-Path $PathValue)) {
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
    param(
        [Parameter(Mandatory = $false)]$Value,
        [double]$DefaultValue = 0.0
    )
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
    param(
        [Parameter(Mandatory = $false)]$Value,
        [long]$DefaultValue = 0
    )
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
    param(
        [Parameter(Mandatory = $false)]$Value,
        [bool]$DefaultValue = $false
    )
    if ($null -eq $Value) {
        return $DefaultValue
    }
    try {
        return [bool]$Value
    } catch {
        return $DefaultValue
    }
}

function Get-MapSum {
    param([Parameter(Mandatory = $false)]$MapValue)
    $sum = 0L
    if ($null -eq $MapValue) {
        return $sum
    }
    if ($MapValue -is [System.Collections.IDictionary]) {
        foreach ($value in $MapValue.Values) {
            $sum += To-Int64 -Value $value -DefaultValue 0
        }
        return $sum
    }
    if ($MapValue.PSObject) {
        foreach ($prop in $MapValue.PSObject.Properties) {
            $sum += To-Int64 -Value $prop.Value -DefaultValue 0
        }
    }
    return $sum
}

function Get-PositiveMapKeys {
    param([Parameter(Mandatory = $false)]$MapValue)
    $keys = @()
    if ($null -eq $MapValue) {
        return $keys
    }
    if ($MapValue -is [System.Collections.IDictionary]) {
        foreach ($key in $MapValue.Keys) {
            $value = To-Int64 -Value $MapValue[$key] -DefaultValue 0
            if ($value -gt 0) {
                $keys += [string]$key
            }
        }
        return $keys
    }
    if ($MapValue.PSObject) {
        foreach ($prop in $MapValue.PSObject.Properties) {
            $value = To-Int64 -Value $prop.Value -DefaultValue 0
            if ($value -gt 0) {
                $keys += [string]$prop.Name
            }
        }
    }
    return $keys
}

function Load-RunStartedPayload {
    param([string]$RunDir)
    $eventsPath = Join-Path $RunDir "events.jsonl"
    if (-not (Test-Path $eventsPath)) {
        return @{}
    }
    $runStartedLine = Get-Content -Path $eventsPath -Encoding UTF8 |
        Where-Object { $_ -match '"event_type":"RUN_STARTED"' } |
        Select-Object -First 1
    if ([string]::IsNullOrWhiteSpace($runStartedLine)) {
        return @{}
    }
    try {
        $eventObj = $runStartedLine | ConvertFrom-Json
        return Get-PropValue -ObjectValue $eventObj -Name "payload" -DefaultValue @{}
    } catch {
        return @{}
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

$outputDir = if ([System.IO.Path]::IsPathRooted($OutDir)) { $OutDir } else { Join-Path $ProjectRoot $OutDir }
New-Item -ItemType Directory -Path $outputDir -Force | Out-Null

$args = @(
    "-m", "autobot.cli",
    "paper", "run",
    "--duration-sec", $DurationSec,
    "--quote", $Quote,
    "--top-n", $TopN,
    "--micro-gate", "off",
    "--micro-order-policy", "on",
    "--micro-order-policy-mode", "trade_only",
    "--micro-order-policy-on-missing", "static_fallback",
    "--paper-micro-provider", $PaperMicroProvider,
    "--paper-micro-warmup-sec", $WarmupSec,
    "--paper-micro-warmup-min-trade-events-per-market", $WarmupMinTradeEventsPerMarket
)
if (-not [string]::IsNullOrWhiteSpace($Strategy)) {
    $args += @("--strategy", $Strategy)
}
if (-not [string]::IsNullOrWhiteSpace($Tf)) {
    $args += @("--tf", $Tf)
}
if (-not [string]::IsNullOrWhiteSpace($ModelRef)) {
    $args += @("--model-ref", $ModelRef)
}
if (-not [string]::IsNullOrWhiteSpace($ModelFamily)) {
    $args += @("--model-family", $ModelFamily)
}
if (-not [string]::IsNullOrWhiteSpace($FeatureSet)) {
    $args += @("--feature-set", $FeatureSet)
}
if ($TopPct -ge 0.0) {
    $args += @("--top-pct", $TopPct)
}
if ($MinProb -ge 0.0) {
    $args += @("--min-prob", $MinProb)
}
if ($MinCandsPerTs -ge 0) {
    $args += @("--min-cands-per-ts", $MinCandsPerTs)
}
if ($MaxPositionsTotal -ge 0) {
    $args += @("--max-positions-total", $MaxPositionsTotal)
}
if (-not [string]::IsNullOrWhiteSpace($ExitMode)) {
    $args += @("--exit-mode", $ExitMode)
}
if ($HoldBars -ge 0) {
    $args += @("--hold-bars", $HoldBars)
}
if ($TpPct -ge 0.0) {
    $args += @("--tp-pct", $TpPct)
}
if ($SlPct -ge 0.0) {
    $args += @("--sl-pct", $SlPct)
}
if ($TrailingPct -ge 0.0) {
    $args += @("--trailing-pct", $TrailingPct)
}
if (-not [string]::IsNullOrWhiteSpace($PaperFeatureProvider)) {
    $args += @("--paper-feature-provider", $PaperFeatureProvider)
}
$exec = Invoke-CommandCapture -Exe $PythonExe -ArgList $args
if ($exec.ExitCode -ne 0) {
    throw "paper smoke run failed (exit=$($exec.ExitCode)): $($exec.Output)"
}

$runDir = Resolve-RunDirFromText -TextValue ([string]$exec.Output)
if ([string]::IsNullOrWhiteSpace($runDir)) {
    throw "paper smoke run completed but run_dir was not reported by CLI stdout"
}
if (-not (Test-Path $runDir)) {
    throw "paper smoke run_dir does not exist: $runDir"
}

$summaryPath = Join-Path $runDir "summary.json"
if (-not (Test-Path $summaryPath)) {
    throw "paper smoke summary.json does not exist: $summaryPath"
}
$summary = Load-JsonOrEmpty -PathValue $summaryPath
$policy = Load-JsonOrEmpty -PathValue (Join-Path $runDir "micro_order_policy_report.json")
$runStartedPayload = Load-RunStartedPayload -RunDir $runDir

$microProvider = [string](Get-PropValue -ObjectValue $runStartedPayload -Name "micro_provider" -DefaultValue "")
$microProviderInfo = Get-PropValue -ObjectValue $runStartedPayload -Name "micro_provider_info" -DefaultValue @{}
if ([string]::IsNullOrWhiteSpace($microProvider)) {
    $microProvider = [string](Get-PropValue -ObjectValue $summary -Name "micro_provider" -DefaultValue "")
}
if (($microProviderInfo -is [System.Collections.IDictionary] -and $microProviderInfo.Count -eq 0) -or
    ($microProviderInfo.PSObject -and $microProviderInfo.PSObject.Properties.Count -eq 0)) {
    $microProviderInfo = Get-PropValue -ObjectValue $summary -Name "micro_provider_info" -DefaultValue @{}
}
if ([string]::IsNullOrWhiteSpace($microProvider)) {
    $microProvider = [string](Get-PropValue -ObjectValue $microProviderInfo -Name "provider" -DefaultValue "NA")
}
if ([string]::IsNullOrWhiteSpace($microProvider)) {
    $microProvider = "NA"
}

$wsHealthPath = Join-Path $ProjectRoot "data/raw_ws/upbit/_meta/ws_public_health.json"
$wsHealth = Load-JsonOrEmpty -PathValue $wsHealthPath
$wsHealthAvailable = -not ($wsHealth -is [System.Collections.IDictionary] -and $wsHealth.Count -eq 0)
$wsConnected = To-Bool (Get-PropValue -ObjectValue $wsHealth -Name "connected" -DefaultValue $false) $false
$wsLastRxObj = Get-PropValue -ObjectValue $wsHealth -Name "last_rx_ts_ms" -DefaultValue @{}
$wsOrderbookRxMs = To-Int64 (Get-PropValue -ObjectValue $wsLastRxObj -Name "orderbook" -DefaultValue 0) 0
$wsTradeRxMs = To-Int64 (Get-PropValue -ObjectValue $wsLastRxObj -Name "trade" -DefaultValue 0) 0
$wsLastRxMaxMs = [Math]::Max($wsOrderbookRxMs, $wsTradeRxMs)
$wsSubscribedMarketsCount = To-Int64 (Get-PropValue -ObjectValue $wsHealth -Name "subscribed_markets_count" -DefaultValue 0) 0
$microSnapshotAgeMs = if ($wsLastRxMaxMs -gt 0) { [int64]([DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds() - $wsLastRxMaxMs) } else { -1L }
$providerSubscribedMarketsCount = To-Int64 (Get-PropValue -ObjectValue $microProviderInfo -Name "subscribed_markets_count" -DefaultValue 0) 0
$providerSnapshotAgeMs = To-Int64 (Get-PropValue -ObjectValue $microProviderInfo -Name "micro_snapshot_age_ms" -DefaultValue -1) -1
if ($providerSnapshotAgeMs -ge 0) {
    $microSnapshotAgeMs = [int64]$providerSnapshotAgeMs
}
$warmupElapsedSec = To-Double (Get-PropValue -ObjectValue $summary -Name "warmup_elapsed_sec" -DefaultValue $null) -1.0
if ($warmupElapsedSec -lt 0) {
    $warmupElapsedSec = To-Double (Get-PropValue -ObjectValue $microProviderInfo -Name "warmup_elapsed_sec" -DefaultValue 0.0) 0.0
}
$warmupSatisfied = To-Bool (Get-PropValue -ObjectValue $summary -Name "warmup_satisfied" -DefaultValue $null) $false
if (-not $warmupSatisfied) {
    $warmupSatisfied = To-Bool (Get-PropValue -ObjectValue $microProviderInfo -Name "warmup_satisfied" -DefaultValue $false) $false
}
$warmupTradeEventsTotal = To-Int64 (Get-PropValue -ObjectValue $summary -Name "warmup_trade_events_total" -DefaultValue $null) -1
if ($warmupTradeEventsTotal -lt 0) {
    $warmupTradeEventsTotal = To-Int64 (Get-PropValue -ObjectValue $microProviderInfo -Name "warmup_trade_events_total" -DefaultValue 0) 0
}
$microCacheMarketsWithSamples = To-Int64 (Get-PropValue -ObjectValue $summary -Name "micro_cache_markets_with_samples" -DefaultValue $null) -1
if ($microCacheMarketsWithSamples -lt 0) {
    $microCacheMarketsWithSamples = To-Int64 (Get-PropValue -ObjectValue $microProviderInfo -Name "micro_cache_markets_with_samples" -DefaultValue 0) 0
}

$ordersSubmitted = To-Int64 (Get-PropValue -ObjectValue $summary -Name "orders_submitted" -DefaultValue 0) 0
$ordersFilled = To-Int64 (Get-PropValue -ObjectValue $summary -Name "orders_filled" -DefaultValue 0) 0
$fillRate = To-Double (Get-PropValue -ObjectValue $summary -Name "fill_rate" -DefaultValue 0.0) 0.0
$realizedPnlQuote = To-Double (Get-PropValue -ObjectValue $summary -Name "realized_pnl_quote" -DefaultValue 0.0) 0.0
$maxDrawdownPct = To-Double (Get-PropValue -ObjectValue $summary -Name "max_drawdown_pct" -DefaultValue 0.0) 0.0
$slippageBpsMean = To-Double (Get-PropValue -ObjectValue $summary -Name "slippage_bps_mean" -DefaultValue 0.0) 0.0
$microQualityScoreMean = To-Double (Get-PropValue -ObjectValue $summary -Name "micro_quality_score_mean" -DefaultValue 0.0) 0.0
$runtimeRiskMultiplierMean = To-Double (Get-PropValue -ObjectValue $summary -Name "runtime_risk_multiplier_mean" -DefaultValue 1.0) 1.0
$operationalRegimeScoreMean = To-Double (Get-PropValue -ObjectValue $summary -Name "operational_regime_score_mean" -DefaultValue 0.0) 0.0
$operationalBreadthRatioMean = To-Double (Get-PropValue -ObjectValue $summary -Name "operational_breadth_ratio_mean" -DefaultValue 0.0) 0.0
$operationalMaxPositionsMean = To-Double (Get-PropValue -ObjectValue $summary -Name "operational_max_positions_mean" -DefaultValue 0.0) 0.0
$rollingEvidence = Get-PropValue -ObjectValue $summary -Name "rolling_evidence" -DefaultValue @{}
$rollingWindowMinutes = To-Int64 (Get-PropValue -ObjectValue $summary -Name "rolling_window_minutes" -DefaultValue 0) 0
if ($rollingWindowMinutes -le 0) {
    $rollingWindowMinutes = To-Int64 (Get-PropValue -ObjectValue $rollingEvidence -Name "window_minutes" -DefaultValue 0) 0
}
$rollingActiveWindows = To-Int64 (Get-PropValue -ObjectValue $summary -Name "rolling_active_windows" -DefaultValue 0) 0
if ($rollingActiveWindows -le 0) {
    $rollingActiveWindows = To-Int64 (Get-PropValue -ObjectValue $rollingEvidence -Name "active_windows" -DefaultValue 0) 0
}
$rollingWindowsTotal = To-Int64 (Get-PropValue -ObjectValue $summary -Name "rolling_windows_total" -DefaultValue 0) 0
if ($rollingWindowsTotal -le 0) {
    $rollingWindowsTotal = To-Int64 (Get-PropValue -ObjectValue $rollingEvidence -Name "windows_total" -DefaultValue 0) 0
}
$rollingNonnegativeWindowRatio = To-Double (Get-PropValue -ObjectValue $summary -Name "rolling_nonnegative_active_window_ratio" -DefaultValue -1.0) -1.0
if ($rollingNonnegativeWindowRatio -lt 0.0) {
    $rollingNonnegativeWindowRatio = To-Double (Get-PropValue -ObjectValue $rollingEvidence -Name "nonnegative_active_window_ratio" -DefaultValue 0.0) 0.0
}
$rollingFillConcentrationRatio = To-Double (Get-PropValue -ObjectValue $summary -Name "rolling_max_fill_concentration_ratio" -DefaultValue -1.0) -1.0
if ($rollingFillConcentrationRatio -lt 0.0) {
    $rollingFillConcentrationRatio = To-Double (Get-PropValue -ObjectValue $rollingEvidence -Name "max_fill_concentration_ratio" -DefaultValue 0.0) 0.0
}
$rollingMaxWindowDrawdownPct = To-Double (Get-PropValue -ObjectValue $summary -Name "rolling_max_window_drawdown_pct" -DefaultValue -1.0) -1.0
if ($rollingMaxWindowDrawdownPct -lt 0.0) {
    $rollingMaxWindowDrawdownPct = To-Double (Get-PropValue -ObjectValue $rollingEvidence -Name "max_window_drawdown_pct" -DefaultValue 0.0) 0.0
}
$rollingWorstWindowRealizedPnlQuote = To-Double (Get-PropValue -ObjectValue $summary -Name "rolling_worst_window_realized_pnl_quote" -DefaultValue 0.0) 0.0
if ($rollingWorstWindowRealizedPnlQuote -eq 0.0) {
    $rollingWorstWindowRealizedPnlQuote = To-Double (Get-PropValue -ObjectValue $rollingEvidence -Name "worst_window_realized_pnl_quote" -DefaultValue 0.0) 0.0
}
$runtimeMinProbUsed = To-Double (Get-PropValue -ObjectValue $summary -Name "model_alpha_min_prob_used" -DefaultValue $MinProb) $MinProb
$runtimeMinProbSource = [string](Get-PropValue -ObjectValue $summary -Name "model_alpha_min_prob_source" -DefaultValue ($(if ($MinProb -ge 0.0) { "manual" } else { "runtime_default" })))
$runtimeTopPctUsed = To-Double (Get-PropValue -ObjectValue $summary -Name "model_alpha_top_pct_used" -DefaultValue $TopPct) $TopPct
$runtimeTopPctSource = [string](Get-PropValue -ObjectValue $summary -Name "model_alpha_top_pct_source" -DefaultValue ($(if ($TopPct -ge 0.0) { "manual" } else { "runtime_default" })))
$runtimeMinCandsUsed = To-Int64 (Get-PropValue -ObjectValue $summary -Name "model_alpha_min_candidates_used" -DefaultValue $MinCandsPerTs) $MinCandsPerTs
$runtimeMinCandsSource = [string](Get-PropValue -ObjectValue $summary -Name "model_alpha_min_candidates_source" -DefaultValue ($(if ($MinCandsPerTs -ge 0) { "manual" } else { "runtime_default" })))
$fallbackReasons = Get-PropValue -ObjectValue $policy -Name "fallback_reasons" -DefaultValue @{}
$microMissingFallbackCount = To-Int64 (Get-PropValue -ObjectValue $fallbackReasons -Name "MICRO_MISSING_FALLBACK" -DefaultValue 0) 0
$fallbackTotalCount = Get-MapSum -MapValue $fallbackReasons
$ordersSubmittedPass = $ordersSubmitted -ge $MinOrdersSubmitted
$fallbackRatioEvaluated = $ordersSubmitted -gt 0
$microMissingFallbackRatio = if ($fallbackRatioEvaluated) { [double]$microMissingFallbackCount / [double]$ordersSubmitted } else { 0.0 }

$tiers = Get-PropValue -ObjectValue $policy -Name "tiers" -DefaultValue @{}
[object[]]$tierKeys = @(Get-PositiveMapKeys -MapValue $tiers)
$tierUniqueCount = $tierKeys.Count

$replacesTotal = To-Int64 (Get-PropValue -ObjectValue $policy -Name "replaces_total" -DefaultValue 0) 0
$cancelsTotal = To-Int64 (Get-PropValue -ObjectValue $policy -Name "cancels_total" -DefaultValue 0) 0
$abortedTimeoutTotal = To-Int64 (Get-PropValue -ObjectValue $policy -Name "aborted_timeout_total" -DefaultValue 0) 0
$replaceCancelTimeoutTotal = $replacesTotal + $cancelsTotal + $abortedTimeoutTotal

$gateFallbackPass = if ($fallbackRatioEvaluated) { $microMissingFallbackRatio -lt $MaxFallbackRatio } else { $true }
$gateTierPass = $tierUniqueCount -ge $MinTierCount
$gatePolicyEventsPass = $replaceCancelTimeoutTotal -ge $MinPolicyEvents
$smokeConnectivityPass = $ordersSubmittedPass -and $gateFallbackPass
$t151GatePass = $ordersSubmittedPass -and $gateFallbackPass -and $gateTierPass -and $gatePolicyEventsPass
$gateFailures = @()
if (-not $ordersSubmittedPass) {
    $gateFailures += "MIN_ORDERS_SUBMITTED"
}
if (-not $gateFallbackPass) {
    $gateFailures += "FALLBACK_RATIO_EXCEEDED"
}
if (-not $gateTierPass) {
    $gateFailures += "MIN_TIER_COUNT"
}
if (-not $gatePolicyEventsPass) {
    $gateFailures += "MIN_POLICY_EVENTS"
}

$runId = [System.IO.Path]::GetFileName($runDir)
$generatedAt = (Get-Date).ToString("o")
$payload = [ordered]@{
    generated_at = $generatedAt
    run_id = $runId
    run_dir = $runDir
    duration_sec = [int]$DurationSec
    quote = $Quote
    top_n = [int]$TopN
    strategy = $Strategy
    tf = $Tf
    model_ref = $ModelRef
    model_family = $ModelFamily
    feature_set = $FeatureSet
    top_pct = [double]$runtimeTopPctUsed
    top_pct_source = $runtimeTopPctSource
    min_prob = [double]$runtimeMinProbUsed
    min_prob_source = $runtimeMinProbSource
    min_cands_per_ts = [int]$runtimeMinCandsUsed
    min_cands_per_ts_source = $runtimeMinCandsSource
    requested_top_pct = [double]$TopPct
    requested_min_prob = [double]$MinProb
    requested_min_cands_per_ts = [int]$MinCandsPerTs
    max_positions_total = [int]$MaxPositionsTotal
    exit_mode = $ExitMode
    hold_bars = [int]$HoldBars
    tp_pct = [double]$TpPct
    sl_pct = [double]$SlPct
    trailing_pct = [double]$TrailingPct
    paper_feature_provider = $PaperFeatureProvider
    micro_provider = $microProvider
    micro_provider_info = $microProviderInfo
    warmup_elapsed_sec = [double]$warmupElapsedSec
    warmup_satisfied = [bool]$warmupSatisfied
    warmup_trade_events_total = [int64]$warmupTradeEventsTotal
    micro_cache_markets_with_samples = [int64]$microCacheMarketsWithSamples
    orders_submitted = [int64]$ordersSubmitted
    orders_filled = [int64]$ordersFilled
    fill_rate = [double]$fillRate
    realized_pnl_quote = [double]$realizedPnlQuote
    max_drawdown_pct = [double]$maxDrawdownPct
    slippage_bps_mean = [double]$slippageBpsMean
    micro_quality_score_mean = [double]$microQualityScoreMean
    runtime_risk_multiplier_mean = [double]$runtimeRiskMultiplierMean
    operational_regime_score_mean = [double]$operationalRegimeScoreMean
    operational_breadth_ratio_mean = [double]$operationalBreadthRatioMean
    operational_max_positions_mean = [double]$operationalMaxPositionsMean
    rolling_window_minutes = [int64]$rollingWindowMinutes
    rolling_windows_total = [int64]$rollingWindowsTotal
    rolling_active_windows = [int64]$rollingActiveWindows
    rolling_nonnegative_active_window_ratio = [double]$rollingNonnegativeWindowRatio
    rolling_max_fill_concentration_ratio = [double]$rollingFillConcentrationRatio
    rolling_max_window_drawdown_pct = [double]$rollingMaxWindowDrawdownPct
    rolling_worst_window_realized_pnl_quote = [double]$rollingWorstWindowRealizedPnlQuote
    micro_missing_fallback_count = [int64]$microMissingFallbackCount
    fallback_total_count = [int64]$fallbackTotalCount
    micro_missing_fallback_ratio = [double]$microMissingFallbackRatio
    tiers = $tiers
    tier_unique_count = [int]$tierUniqueCount
    tier_keys = @($tierKeys)
    replaces_total = [int64]$replacesTotal
    cancels_total = [int64]$cancelsTotal
    aborted_timeout_total = [int64]$abortedTimeoutTotal
    replace_cancel_timeout_total = [int64]$replaceCancelTimeoutTotal
    thresholds = [ordered]@{
        max_fallback_ratio = [double]$MaxFallbackRatio
        min_orders_submitted = [int]$MinOrdersSubmitted
        min_tier_count = [int]$MinTierCount
        min_policy_events = [int]$MinPolicyEvents
    }
    gates = [ordered]@{
        orders_submitted_pass = $ordersSubmittedPass
        fallback_ratio_evaluated = $fallbackRatioEvaluated
        smoke_connectivity_pass = $smokeConnectivityPass
        fallback_ratio_pass = $gateFallbackPass
        tier_diversity_pass = $gateTierPass
        policy_events_pass = $gatePolicyEventsPass
        t15_gate_pass = $t151GatePass
    }
    gate_failures = @($gateFailures)
    live_ws = [ordered]@{
        health_snapshot_available = [bool]$wsHealthAvailable
        ws_connected = $wsConnected
        subscribed_markets_count = [int64]$wsSubscribedMarketsCount
        provider_subscribed_markets_count = [int64]$providerSubscribedMarketsCount
        last_rx_ts_ms = [ordered]@{
            orderbook = [int64]$wsOrderbookRxMs
            trade = [int64]$wsTradeRxMs
            max = [int64]$wsLastRxMaxMs
        }
        micro_snapshot_age_ms = [int64]$microSnapshotAgeMs
        health_snapshot_path = $wsHealthPath
    }
}

$stamp = Get-Date -Format "yyyyMMdd-HHmmss"
$runReportPath = Join-Path $outputDir ("paper_micro_smoke_" + $stamp + ".json")
$latestReportPath = Join-Path $outputDir "latest.json"

$payload | ConvertTo-Json -Depth 8 | Set-Content -Path $runReportPath -Encoding UTF8
$payload | ConvertTo-Json -Depth 8 | Set-Content -Path $latestReportPath -Encoding UTF8

Write-Host ("[paper-smoke] run_id={0}" -f $runId)
Write-Host ("[paper-smoke] micro_provider={0}" -f $microProvider)
Write-Host ("[paper-smoke] orders_submitted={0}" -f $ordersSubmitted)
Write-Host ("[paper-smoke] micro_missing_fallback_count={0}" -f $microMissingFallbackCount)
Write-Host ("[paper-smoke] micro_missing_fallback_ratio={0:N6}" -f $microMissingFallbackRatio)
Write-Host ("[paper-smoke] fallback_ratio_evaluated={0}" -f $fallbackRatioEvaluated)
Write-Host ("[paper-smoke] tier_unique_count={0} ({1})" -f $tierUniqueCount, ($tierKeys -join ","))
Write-Host ("[paper-smoke] replace_cancel_timeout_total={0}" -f $replaceCancelTimeoutTotal)
Write-Host ("[paper-smoke] warmup_elapsed_sec={0:N3}" -f $warmupElapsedSec)
Write-Host ("[paper-smoke] warmup_satisfied={0}" -f $warmupSatisfied)
Write-Host ("[paper-smoke] warmup_trade_events_total={0}" -f $warmupTradeEventsTotal)
Write-Host ("[paper-smoke] micro_cache_markets_with_samples={0}" -f $microCacheMarketsWithSamples)
Write-Host ("[paper-smoke] orders_filled={0}" -f $ordersFilled)
Write-Host ("[paper-smoke] fill_rate={0:N6}" -f $fillRate)
Write-Host ("[paper-smoke] realized_pnl_quote={0:N6}" -f $realizedPnlQuote)
Write-Host ("[paper-smoke] max_drawdown_pct={0:N6}" -f $maxDrawdownPct)
Write-Host ("[paper-smoke] slippage_bps_mean={0:N6}" -f $slippageBpsMean)
Write-Host ("[paper-smoke] micro_quality_score_mean={0:N6}" -f $microQualityScoreMean)
Write-Host ("[paper-smoke] runtime_risk_multiplier_mean={0:N6}" -f $runtimeRiskMultiplierMean)
Write-Host ("[paper-smoke] operational_regime_score_mean={0:N6}" -f $operationalRegimeScoreMean)
Write-Host ("[paper-smoke] operational_breadth_ratio_mean={0:N6}" -f $operationalBreadthRatioMean)
Write-Host ("[paper-smoke] operational_max_positions_mean={0:N6}" -f $operationalMaxPositionsMean)
Write-Host ("[paper-smoke] rolling_active_windows={0}/{1} window_minutes={2}" -f $rollingActiveWindows, $rollingWindowsTotal, $rollingWindowMinutes)
Write-Host ("[paper-smoke] rolling_nonnegative_active_window_ratio={0:N6}" -f $rollingNonnegativeWindowRatio)
Write-Host ("[paper-smoke] rolling_max_fill_concentration_ratio={0:N6}" -f $rollingFillConcentrationRatio)
Write-Host ("[paper-smoke] rolling_max_window_drawdown_pct={0:N6}" -f $rollingMaxWindowDrawdownPct)
if ($microProvider -eq "LIVE_WS") {
    Write-Host ("[paper-smoke] live_ws_connected={0}" -f $wsConnected)
    Write-Host ("[paper-smoke] live_ws_subscribed_markets={0}" -f $wsSubscribedMarketsCount)
    Write-Host ("[paper-smoke] live_ws_micro_snapshot_age_ms={0}" -f $microSnapshotAgeMs)
}
Write-Host ("[paper-smoke] smoke_connectivity_pass={0}" -f $smokeConnectivityPass)
Write-Host ("[paper-smoke] t15_gate_pass={0}" -f $t151GatePass)
Write-Host ("[paper-smoke] gate_failures={0}" -f ($gateFailures -join ","))
Write-Host ("[paper-smoke] report={0}" -f $runReportPath)
Write-Host ("[paper-smoke] latest={0}" -f $latestReportPath)
