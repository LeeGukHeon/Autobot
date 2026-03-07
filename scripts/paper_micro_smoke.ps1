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

$outputDir = if ([System.IO.Path]::IsPathRooted($OutDir)) { $OutDir } else { Join-Path $ProjectRoot $OutDir }
New-Item -ItemType Directory -Path $outputDir -Force | Out-Null

$runsDir = Join-Path $ProjectRoot "data\paper\runs"
$before = @{}
if (Test-Path $runsDir) {
    Get-ChildItem -Path $runsDir -Directory | ForEach-Object {
        $before[$_.Name] = $true
    }
}

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

$newRunDir = $null
if (Test-Path $runsDir) {
    $after = Get-ChildItem -Path $runsDir -Directory | Sort-Object LastWriteTime -Descending
    foreach ($item in $after) {
        if (-not $before.ContainsKey($item.Name)) {
            $newRunDir = $item.FullName
            break
        }
    }
    if ($null -eq $newRunDir -and $after.Count -gt 0) {
        $newRunDir = $after[0].FullName
    }
}
if ([string]::IsNullOrWhiteSpace($newRunDir)) {
    throw "paper smoke run completed but run_dir was not found"
}

$summary = Load-JsonOrEmpty -PathValue (Join-Path $newRunDir "summary.json")
$policy = Load-JsonOrEmpty -PathValue (Join-Path $newRunDir "micro_order_policy_report.json")
$runStartedPayload = Load-RunStartedPayload -RunDir $newRunDir

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
$fallbackReasons = Get-PropValue -ObjectValue $policy -Name "fallback_reasons" -DefaultValue @{}
$microMissingFallbackCount = To-Int64 (Get-PropValue -ObjectValue $fallbackReasons -Name "MICRO_MISSING_FALLBACK" -DefaultValue 0) 0
$fallbackTotalCount = Get-MapSum -MapValue $fallbackReasons
$microMissingFallbackRatio = if ($ordersSubmitted -gt 0) { [double]$microMissingFallbackCount / [double]$ordersSubmitted } else { 1.0 }

$tiers = Get-PropValue -ObjectValue $policy -Name "tiers" -DefaultValue @{}
[object[]]$tierKeys = @(Get-PositiveMapKeys -MapValue $tiers)
$tierUniqueCount = $tierKeys.Count

$replacesTotal = To-Int64 (Get-PropValue -ObjectValue $policy -Name "replaces_total" -DefaultValue 0) 0
$cancelsTotal = To-Int64 (Get-PropValue -ObjectValue $policy -Name "cancels_total" -DefaultValue 0) 0
$abortedTimeoutTotal = To-Int64 (Get-PropValue -ObjectValue $policy -Name "aborted_timeout_total" -DefaultValue 0) 0
$replaceCancelTimeoutTotal = $replacesTotal + $cancelsTotal + $abortedTimeoutTotal

$smokeConnectivityPass = ($ordersSubmitted -ge $MinOrdersSubmitted) -and ($microMissingFallbackRatio -lt $MaxFallbackRatio)
$gateFallbackPass = ($ordersSubmitted -ge $MinOrdersSubmitted) -and ($microMissingFallbackRatio -lt $MaxFallbackRatio)
$gateTierPass = $tierUniqueCount -ge $MinTierCount
$gatePolicyEventsPass = $replaceCancelTimeoutTotal -ge $MinPolicyEvents
$t151GatePass = $gateFallbackPass -and $gateTierPass -and $gatePolicyEventsPass

$runId = [System.IO.Path]::GetFileName($newRunDir)
$generatedAt = (Get-Date).ToString("o")
$payload = [ordered]@{
    generated_at = $generatedAt
    run_id = $runId
    run_dir = $newRunDir
    duration_sec = [int]$DurationSec
    quote = $Quote
    top_n = [int]$TopN
    strategy = $Strategy
    tf = $Tf
    model_ref = $ModelRef
    model_family = $ModelFamily
    feature_set = $FeatureSet
    top_pct = [double]$TopPct
    min_prob = [double]$MinProb
    min_cands_per_ts = [int]$MinCandsPerTs
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
        smoke_connectivity_pass = $smokeConnectivityPass
        fallback_ratio_pass = $gateFallbackPass
        tier_diversity_pass = $gateTierPass
        policy_events_pass = $gatePolicyEventsPass
        t15_gate_pass = $t151GatePass
    }
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
if ($microProvider -eq "LIVE_WS") {
    Write-Host ("[paper-smoke] live_ws_connected={0}" -f $wsConnected)
    Write-Host ("[paper-smoke] live_ws_subscribed_markets={0}" -f $wsSubscribedMarketsCount)
    Write-Host ("[paper-smoke] live_ws_micro_snapshot_age_ms={0}" -f $microSnapshotAgeMs)
}
Write-Host ("[paper-smoke] smoke_connectivity_pass={0}" -f $smokeConnectivityPass)
Write-Host ("[paper-smoke] t15_gate_pass={0}" -f $t151GatePass)
Write-Host ("[paper-smoke] report={0}" -f $runReportPath)
Write-Host ("[paper-smoke] latest={0}" -f $latestReportPath)
