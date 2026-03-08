param(
    [string]$PythonExe = "/home/ubuntu/MyApps/Autobot/.venv/bin/python",
    [string]$ProjectRoot = "/home/ubuntu/MyApps/Autobot",
    [string]$Quote = "KRW",
    [int]$TopN = 50,
    [string]$ParquetRoot = "data/parquet",
    [int]$DaysAgo = 1,
    [int]$MaxPagesPerTarget = 50,
    [int]$Workers = 1,
    [string]$CandlesBaseDataset = "candles_api_v1",
    [string]$CandlesOutDataset = "candles_api_v1",
    [string]$CandlesPlanPath = "data/collect/_meta/candle_topup_plan_daily.json",
    [int]$CandlesLookbackMonths = 3,
    [string]$CandlesTf = "1m,5m,15m,60m,240m",
    [int]$CandlesMaxBackfillDays1m = 3,
    [string]$CandlesMarketMode = "top_n_by_recent_value_est",
    [string]$CandlesMarkets = "",
    [int]$CandlesWorkers = 1,
    [string]$CandlesRateLimitStrict = "true",
    [string]$RawTicksRoot = "data/raw_ticks/upbit/trades",
    [string]$RawWsRoot = "data/raw_ws/upbit/public",
    [string]$RawWsMetaDir = "data/raw_ws/upbit/_meta",
    [string]$OutRoot = "data/parquet/micro_v1",
    [string]$SmokeReportJson = "logs/paper_micro_smoke/latest.json",
    [int]$SmokeDurationSec = 600,
    [int]$SmokeTopN = 20,
    [string]$SmokePaperMicroProvider = "live_ws",
    [int]$SmokeWarmupSec = 60,
    [int]$SmokeWarmupMinTradeEventsPerMarket = 1,
    [string]$TieringReportJson = "logs/micro_tiering/latest.json",
    [int]$TieringRecentHours = 24,
    [int]$TieringMinSamples = 30,
    [double]$GateBookAvailableMin = 0.20,
    [double]$GateTradeSourceWsMin = 0.20,
    [double]$GateFallbackRatioMax = 0.10,
    [int]$GateTierMinCount = 2,
    [int]$GatePolicyEventMin = 1,
    [int]$HealthLagWarnSec = 180,
    [string]$WsValidateQuarantineCorrupt = "true",
    [int]$WsValidateMinAgeSec = 300,
    [switch]$SkipCandles,
    [switch]$SkipTicks,
    [switch]$SkipAggregate,
    [switch]$SkipValidate,
    [switch]$SkipSmoke,
    [switch]$SkipTieringRecommend,
    [string]$Date = ""
)

$ErrorActionPreference = "Stop"
Set-Location $ProjectRoot

$vendorSitePackages = Join-Path $ProjectRoot "python\site-packages"
if ($IsWindows -and (Test-Path $vendorSitePackages)) {
    if ([string]::IsNullOrWhiteSpace($env:PYTHONPATH)) {
        $env:PYTHONPATH = $vendorSitePackages
    } elseif ($env:PYTHONPATH -notlike "*$vendorSitePackages*") {
        $env:PYTHONPATH = "${vendorSitePackages}:$($env:PYTHONPATH)"
    }
}

function Invoke-CheckedCommand {
    param(
        [string]$Exe,
        [string[]]$ArgList
    )
    & $Exe @ArgList
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed (exit=$LASTEXITCODE): $Exe $($ArgList -join ' ')"
    }
}

function Invoke-CheckedCommandWithOutput {
    param(
        [string]$Exe,
        [string[]]$ArgList
    )
    $output = & $Exe @ArgList 2>&1
    if ($LASTEXITCODE -ne 0) {
        $outputText = $output -join "`n"
        throw "Command failed (exit=$LASTEXITCODE): $Exe $($ArgList -join ' ')`n$outputText"
    }
    return ($output -join "`n")
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

function Get-PowerShellExe {
    if ($IsWindows) {
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

function Get-PassFail {
    param([bool]$Condition)
    if ($Condition) {
        return "PASS"
    }
    return "FAIL"
}

function Resolve-DateToken {
    param(
        [string]$DateText,
        [string]$LabelForError
    )
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

function Get-WsPartsForDate {
    param(
        [string]$RawRoot,
        [string]$DateValue
    )
    $orderbookDatePath = Join-Path $RawRoot ("orderbook/date=" + $DateValue)
    $tradeDatePath = Join-Path $RawRoot ("trade/date=" + $DateValue)
    $orderbookParts = @()
    $tradeParts = @()
    if (Test-Path $orderbookDatePath) {
        $orderbookParts = @(Get-ChildItem -Path $orderbookDatePath -Recurse -File -Filter "*.jsonl.zst")
    }
    if (Test-Path $tradeDatePath) {
        $tradeParts = @(Get-ChildItem -Path $tradeDatePath -Recurse -File -Filter "*.jsonl.zst")
    }
    $orderbookPartCount = [long]$orderbookParts.Count
    $tradePartCount = [long]$tradeParts.Count
    $orderbookBytes = To-Int64 (($orderbookParts | Measure-Object -Property Length -Sum).Sum) 0
    $tradeBytes = To-Int64 (($tradeParts | Measure-Object -Property Length -Sum).Sum) 0
    return [PSCustomObject]@{
        date = $DateValue
        orderbook_date_path = $orderbookDatePath
        trade_date_path = $tradeDatePath
        orderbook_parts = $orderbookParts
        trade_parts = $tradeParts
        orderbook_part_count = $orderbookPartCount
        trade_part_count = $tradePartCount
        orderbook_bytes = $orderbookBytes
        trade_bytes = $tradeBytes
        has_parts = ($orderbookPartCount + $tradePartCount) -gt 0
    }
}

function Get-LatestWsPartitionDate {
    param([string]$RawRoot)
    $dateSet = [System.Collections.Generic.HashSet[string]]::new()
    foreach ($channel in @("orderbook", "trade")) {
        $channelPath = Join-Path $RawRoot $channel
        if (-not (Test-Path $channelPath)) {
            continue
        }
        $channelDates = @(Get-ChildItem -Path $channelPath -Directory -ErrorAction SilentlyContinue | Where-Object { $_.Name -match "^date=\d{4}-\d{2}-\d{2}$" })
        foreach ($dateDir in $channelDates) {
            [void]$dateSet.Add($dateDir.Name.Substring(5))
        }
    }
    if ($dateSet.Count -eq 0) {
        return $null
    }
    return (@($dateSet | Sort-Object)[-1])
}

$batchDateSource = "DEFAULT_KST_YESTERDAY"
$batchDate = (Get-Date).Date.AddDays(-1).ToString("yyyy-MM-dd")

$envBatchDate = [string]$env:AUTOBOT_DAILY_BATCH_DATE
if (-not [string]::IsNullOrWhiteSpace($envBatchDate)) {
    $batchDate = $envBatchDate
    $batchDateSource = "ENV_AUTOBOT_DAILY_BATCH_DATE"
}
if (-not [string]::IsNullOrWhiteSpace($Date)) {
    $batchDate = $Date
    $batchDateSource = "CLI_DATE_PARAM"
}
$batchDate = Resolve-DateToken -DateText $batchDate -LabelForError "batch_date"

$utcTodayDate = (Get-Date).ToUniversalTime().ToString("yyyy-MM-dd")
$batchWsParts = Get-WsPartsForDate -RawRoot $RawWsRoot -DateValue $batchDate
$utcTodayWsParts = $null
$latestWsPartitionDate = $null
$wsDateReason = "NO_WS_DATA_ANYWHERE"
$wsDate = $batchDate

if ($batchWsParts.has_parts) {
    $wsDate = $batchDate
    $wsDateReason = "MATCHED_BATCH_DATE_HAS_WS_PARTS"
} else {
    $utcTodayWsParts = Get-WsPartsForDate -RawRoot $RawWsRoot -DateValue $utcTodayDate
    if ($utcTodayWsParts.has_parts) {
        $wsDate = $utcTodayDate
        $wsDateReason = "FALLBACK_TO_UTC_TODAY_HAS_WS_PARTS"
    } else {
        $latestWsPartitionDate = Get-LatestWsPartitionDate -RawRoot $RawWsRoot
        if (-not [string]::IsNullOrWhiteSpace($latestWsPartitionDate)) {
            $wsDate = $latestWsPartitionDate
            $wsDateReason = "FALLBACK_TO_LATEST_AVAILABLE_PARTITION"
        }
    }
}

if ($null -eq $utcTodayWsParts) {
    $utcTodayWsParts = Get-WsPartsForDate -RawRoot $RawWsRoot -DateValue $utcTodayDate
}

$wsParts = if ($wsDate -eq $batchDate) {
    $batchWsParts
} elseif ($wsDate -eq $utcTodayDate) {
    $utcTodayWsParts
} else {
    Get-WsPartsForDate -RawRoot $RawWsRoot -DateValue $wsDate
}

$candlesPlanPathResolved = if ([System.IO.Path]::IsPathRooted($CandlesPlanPath)) { $CandlesPlanPath } else { Join-Path $ProjectRoot $CandlesPlanPath }
$candlesCollectReportPath = Join-Path $ProjectRoot "data/collect/_meta/candle_collect_report.json"
$candlesValidateReportPath = Join-Path $ProjectRoot "data/collect/_meta/candle_validate_report.json"

$candlesPlan = @{}
$candlesCollectReport = @{}
$candlesValidateReport = @{}
$candlesTopupPass = $true
$candlesTopupStatus = "SKIPPED"
$candlesSelectedMarkets = 0
$candlesTargets = 0
$candlesSkippedRanges = 0
$candlesProcessedTargets = 0
$candlesOkTargets = 0
$candlesWarnTargets = 0
$candlesFailTargets = 0
$candlesCallsMade = 0
$candlesValidateCheckedFiles = 0
$candlesValidateFailFiles = 0
$candlesValidateWarnFiles = 0
$candlesCoverageDeltaPct = $null

if (-not $SkipCandles) {
    if (Test-Path $candlesPlanPathResolved) {
        Remove-Item -Path $candlesPlanPathResolved -Force
    }

    $candlesPlanArgs = @(
        "-m", "autobot.cli",
        "collect", "plan-candles",
        "--base-dataset", $CandlesBaseDataset,
        "--parquet-root", $ParquetRoot,
        "--out", $candlesPlanPathResolved,
        "--lookback-months", $CandlesLookbackMonths,
        "--tf", $CandlesTf,
        "--quote", $Quote,
        "--market-mode", $CandlesMarketMode,
        "--top-n", $TopN,
        "--max-backfill-days-1m", $CandlesMaxBackfillDays1m,
        "--end", $batchDate
    )
    if (-not [string]::IsNullOrWhiteSpace($CandlesMarkets)) {
        $candlesPlanArgs += @("--markets", $CandlesMarkets)
    }
    Invoke-CheckedCommand -Exe $PythonExe -ArgList $candlesPlanArgs
    $candlesPlan = Load-JsonOrEmpty -PathValue $candlesPlanPathResolved

    $candlesCollectArgs = @(
        "-m", "autobot.cli",
        "collect", "candles",
        "--plan", $candlesPlanPathResolved,
        "--out-dataset", $CandlesOutDataset,
        "--parquet-root", $ParquetRoot,
        "--workers", $CandlesWorkers,
        "--dry-run", "false",
        "--rate-limit-strict", $CandlesRateLimitStrict
    )
    Invoke-CheckedCommand -Exe $PythonExe -ArgList $candlesCollectArgs
    $candlesCollectReport = Load-JsonOrEmpty -PathValue $candlesCollectReportPath
    $candlesValidateReport = Load-JsonOrEmpty -PathValue $candlesValidateReportPath

    $candlesPlanSummary = Get-PropValue -ObjectValue $candlesPlan -Name "summary" -DefaultValue @{}
    $candlesSelectedMarkets = To-Int64 (Get-PropValue -ObjectValue $candlesPlanSummary -Name "selected_markets" -DefaultValue 0) 0
    $candlesTargets = To-Int64 (Get-PropValue -ObjectValue $candlesPlanSummary -Name "targets" -DefaultValue 0) 0
    $candlesSkippedRanges = To-Int64 (Get-PropValue -ObjectValue $candlesPlanSummary -Name "skipped_ranges" -DefaultValue 0) 0

    $candlesProcessedTargets = To-Int64 (Get-PropValue -ObjectValue $candlesCollectReport -Name "processed_targets" -DefaultValue 0) 0
    $candlesOkTargets = To-Int64 (Get-PropValue -ObjectValue $candlesCollectReport -Name "ok_targets" -DefaultValue 0) 0
    $candlesWarnTargets = To-Int64 (Get-PropValue -ObjectValue $candlesCollectReport -Name "warn_targets" -DefaultValue 0) 0
    $candlesFailTargets = To-Int64 (Get-PropValue -ObjectValue $candlesCollectReport -Name "fail_targets" -DefaultValue 0) 0
    $candlesCallsMade = To-Int64 (Get-PropValue -ObjectValue $candlesCollectReport -Name "calls_made" -DefaultValue 0) 0

    $candlesValidateCheckedFiles = To-Int64 (Get-PropValue -ObjectValue $candlesValidateReport -Name "checked_files" -DefaultValue 0) 0
    $candlesValidateFailFiles = To-Int64 (Get-PropValue -ObjectValue $candlesValidateReport -Name "fail_files" -DefaultValue 0) 0
    $candlesValidateWarnFiles = To-Int64 (Get-PropValue -ObjectValue $candlesValidateReport -Name "warn_files" -DefaultValue 0) 0
    $candlesCoverageDelta = Get-PropValue -ObjectValue $candlesValidateReport -Name "coverage_delta" -DefaultValue @{}
    $candlesCoverageDeltaPct = Get-PropValue -ObjectValue $candlesCoverageDelta -Name "average_delta_pct" -DefaultValue $null

    $candlesTopupPass = ($candlesFailTargets -eq 0) -and ($candlesValidateFailFiles -eq 0)
    $candlesTopupStatus = Get-PassFail -Condition $candlesTopupPass
}

$ticksPlanPath = "data/raw_ticks/upbit/_meta/ticks_plan_daily_auto.json"
if (Test-Path $ticksPlanPath) {
    Remove-Item -Path $ticksPlanPath -Force
}

$ticksArgs = @(
    "-m", "autobot.cli",
    "collect", "ticks",
    "--plan", $ticksPlanPath,
    "--mode", "daily",
    "--quote", $Quote,
    "--top-n", $TopN,
    "--days-ago", $DaysAgo,
    "--raw-root", $RawTicksRoot,
    "--rate-limit-strict", "true",
    "--workers", $Workers,
    "--max-pages-per-target", $MaxPagesPerTarget,
    "--dry-run", "false"
)
if (-not $SkipTicks) {
    Invoke-CheckedCommand -Exe $PythonExe -ArgList $ticksArgs
}

$aggregateArgs = @(
    "-m", "autobot.cli",
    "micro", "aggregate",
    "--start", $batchDate,
    "--end", $batchDate,
    "--quote", $Quote,
    "--top-n", $TopN,
    "--raw-ticks-root", $RawTicksRoot,
    "--raw-ws-root", $RawWsRoot,
    "--out-root", $OutRoot
)
if (-not $SkipAggregate) {
    Invoke-CheckedCommand -Exe $PythonExe -ArgList $aggregateArgs
}

$validateArgs = @(
    "-m", "autobot.cli",
    "micro", "validate",
    "--out-root", $OutRoot
)
if (-not $SkipValidate) {
    Invoke-CheckedCommand -Exe $PythonExe -ArgList $validateArgs
}

$statsArgs = @(
    "-m", "autobot.cli",
    "micro", "stats",
    "--out-root", $OutRoot
)
$statsRaw = Invoke-CheckedCommandWithOutput -Exe $PythonExe -ArgList $statsArgs
$stats = $statsRaw | ConvertFrom-Json

$aggregateReportPath = Join-Path $OutRoot "_meta/aggregate_report.json"
$validateReportPath = Join-Path $OutRoot "_meta/validate_report.json"
$manifestPath = Join-Path $OutRoot "_meta/manifest.parquet"
$aggregateReport = Load-JsonOrEmpty -PathValue $aggregateReportPath
$validateReport = Load-JsonOrEmpty -PathValue $validateReportPath
$ticksReport = Load-JsonOrEmpty -PathValue "data/raw_ticks/upbit/_meta/ticks_collect_report.json"

$reportDir = Join-Path $ProjectRoot "docs\reports"
New-Item -ItemType Directory -Path $reportDir -Force | Out-Null
$reportPath = Join-Path $reportDir ("DAILY_MICRO_REPORT_" + $batchDate + ".md")

$bookAvailableRatio = To-Double (Get-PropValue -ObjectValue $stats -Name "book_available_ratio" -DefaultValue 0.0) 0.0
$tradeSourceWsRatio = To-Double (Get-PropValue -ObjectValue $stats -Name "trade_source_ws_ratio" -DefaultValue $null) -1.0
if ($tradeSourceWsRatio -lt 0.0) {
    $tradeSourceRatioObj = Get-PropValue -ObjectValue $stats -Name "trade_source_ratio" -DefaultValue @{}
    $tradeSourceWsRatio = To-Double (Get-PropValue -ObjectValue $tradeSourceRatioObj -Name "ws" -DefaultValue 0.0) 0.0
}
$microAvailableRatio = To-Double (Get-PropValue -ObjectValue $stats -Name "micro_available_ratio" -DefaultValue 0.0) 0.0
$joinMatchRatio = "NA"
$joinObj = Get-PropValue -ObjectValue $validateReport -Name "join" -DefaultValue $null
$joinMatchValue = Get-PropValue -ObjectValue $joinObj -Name "join_match_ratio" -DefaultValue $null
if ($null -ne $joinMatchValue) {
    $joinMatchRatio = [string]$joinMatchValue
}

# 1) orderbook verification: folder / validate / stats / health
$orderbookDatePath = [string](Get-PropValue -ObjectValue $wsParts -Name "orderbook_date_path" -DefaultValue (Join-Path $RawWsRoot ("orderbook/date=" + $wsDate)))
$tradeDatePath = [string](Get-PropValue -ObjectValue $wsParts -Name "trade_date_path" -DefaultValue (Join-Path $RawWsRoot ("trade/date=" + $wsDate)))
$orderbookParts = @(Get-PropValue -ObjectValue $wsParts -Name "orderbook_parts" -DefaultValue @())
$tradeParts = @(Get-PropValue -ObjectValue $wsParts -Name "trade_parts" -DefaultValue @())
$orderbookPartCount = To-Int64 (Get-PropValue -ObjectValue $wsParts -Name "orderbook_part_count" -DefaultValue 0) 0
$tradePartCount = To-Int64 (Get-PropValue -ObjectValue $wsParts -Name "trade_part_count" -DefaultValue 0) 0
$orderbookBytes = To-Int64 (Get-PropValue -ObjectValue $wsParts -Name "orderbook_bytes" -DefaultValue 0) 0
$tradeBytes = To-Int64 (Get-PropValue -ObjectValue $wsParts -Name "trade_bytes" -DefaultValue 0) 0
$batchDateOrderbookPartCount = To-Int64 (Get-PropValue -ObjectValue $batchWsParts -Name "orderbook_part_count" -DefaultValue 0) 0
$batchDateTradePartCount = To-Int64 (Get-PropValue -ObjectValue $batchWsParts -Name "trade_part_count" -DefaultValue 0) 0
$batchDateHasWsParts = To-Bool (Get-PropValue -ObjectValue $batchWsParts -Name "has_parts" -DefaultValue $false) $false
$utcTodayHasWsParts = To-Bool (Get-PropValue -ObjectValue $utcTodayWsParts -Name "has_parts" -DefaultValue $false) $false

$wsValidateArgs = @(
    "-m", "autobot.cli",
    "collect", "ws-public", "validate",
    "--date", $wsDate,
    "--raw-root", $RawWsRoot,
    "--meta-dir", $RawWsMetaDir,
    "--quarantine-corrupt", $WsValidateQuarantineCorrupt,
    "--min-age-sec", $WsValidateMinAgeSec
)
$wsValidateExec = Invoke-CommandCapture -Exe $PythonExe -ArgList $wsValidateArgs
$wsValidateReportPath = Join-Path $RawWsMetaDir "ws_validate_report.json"
$wsValidateReport = Load-JsonOrEmpty -PathValue $wsValidateReportPath
$wsValidateCheckedFiles = To-Int64 (Get-PropValue -ObjectValue $wsValidateReport -Name "checked_files" -DefaultValue 0) 0
$wsValidateFailFiles = To-Int64 (Get-PropValue -ObjectValue $wsValidateReport -Name "fail_files" -DefaultValue 0) 0
$wsValidateParseOkRatio = To-Double (Get-PropValue -ObjectValue $wsValidateReport -Name "parse_ok_ratio" -DefaultValue 0.0) 0.0

$wsStatsArgs = @(
    "-m", "autobot.cli",
    "collect", "ws-public", "stats",
    "--date", $wsDate,
    "--raw-root", $RawWsRoot,
    "--meta-dir", $RawWsMetaDir
)
$wsStatsExec = Invoke-CommandCapture -Exe $PythonExe -ArgList $wsStatsArgs
$wsStats = @{}
if ($wsStatsExec.ExitCode -eq 0 -and -not [string]::IsNullOrWhiteSpace($wsStatsExec.Output)) {
    try {
        $wsStats = $wsStatsExec.Output | ConvertFrom-Json
    } catch {
        $wsStats = @{}
    }
}
$wsManifest = Get-PropValue -ObjectValue $wsStats -Name "manifest" -DefaultValue @{}
$wsManifestByChannel = Get-PropValue -ObjectValue $wsManifest -Name "by_channel" -DefaultValue @{}
$wsManifestRootFilter = Get-PropValue -ObjectValue $wsManifest -Name "raw_root_filter" -DefaultValue @{}
$wsManifestRowsBefore = To-Int64 (Get-PropValue -ObjectValue $wsManifestRootFilter -Name "rows_before" -DefaultValue 0) 0
$wsManifestRowsAfter = To-Int64 (Get-PropValue -ObjectValue $wsManifestRootFilter -Name "rows_after" -DefaultValue 0) 0
$wsManifestIgnoredRows = To-Int64 (Get-PropValue -ObjectValue $wsManifestRootFilter -Name "ignored_outside_raw_root" -DefaultValue 0) 0
$wsStatsOrderbookRows = To-Int64 (Get-PropValue -ObjectValue $wsManifestByChannel -Name "orderbook" -DefaultValue 0) 0
$wsStatsTradeRows = To-Int64 (Get-PropValue -ObjectValue $wsManifestByChannel -Name "trade" -DefaultValue 0) 0

$wsHealth = Get-PropValue -ObjectValue $wsStats -Name "health_snapshot" -DefaultValue $null
if ($null -eq $wsHealth -or ($wsHealth -is [System.Collections.IDictionary] -and $wsHealth.Count -eq 0)) {
    $wsHealth = Load-JsonOrEmpty -PathValue (Join-Path $RawWsMetaDir "ws_public_health.json")
}
$wsHealthConnected = To-Bool (Get-PropValue -ObjectValue $wsHealth -Name "connected" -DefaultValue $false) $false
$wsHealthUpdatedAtMs = To-Int64 (Get-PropValue -ObjectValue $wsHealth -Name "updated_at_ms" -DefaultValue 0) 0
$wsLastRx = Get-PropValue -ObjectValue $wsHealth -Name "last_rx_ts_ms" -DefaultValue @{}
$wsOrderbookRxMs = To-Int64 (Get-PropValue -ObjectValue $wsLastRx -Name "orderbook" -DefaultValue 0) 0
$wsTradeRxMs = To-Int64 (Get-PropValue -ObjectValue $wsLastRx -Name "trade" -DefaultValue 0) 0
$wsSubscribedMarkets = To-Int64 (Get-PropValue -ObjectValue $wsHealth -Name "subscribed_markets_count" -DefaultValue 0) 0
$nowMs = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
$wsHealthLagSec = if ($wsHealthUpdatedAtMs -gt 0) { [math]::Round(($nowMs - $wsHealthUpdatedAtMs) / 1000.0, 2) } else { -1.0 }
$wsOrderbookRxLagSec = if ($wsOrderbookRxMs -gt 0) { [math]::Round(($nowMs - $wsOrderbookRxMs) / 1000.0, 2) } else { -1.0 }
$wsTradeRxLagSec = if ($wsTradeRxMs -gt 0) { [math]::Round(($nowMs - $wsTradeRxMs) / 1000.0, 2) } else { -1.0 }

$orderbookFolderPass = $orderbookPartCount -gt 0
$orderbookValidatePass = ($wsValidateExec.ExitCode -eq 0) -and ($wsValidateCheckedFiles -gt 0) -and ($wsValidateFailFiles -eq 0) -and ($wsValidateParseOkRatio -ge 0.99)
$orderbookStatsPass = ($wsStatsExec.ExitCode -eq 0) -and ($wsStatsOrderbookRows -gt 0)
$orderbookHealthPass = $wsHealthConnected -and ($wsOrderbookRxLagSec -ge 0) -and ($wsOrderbookRxLagSec -le [double]$HealthLagWarnSec)
$orderbookVerificationPass = $orderbookFolderPass -and $orderbookValidatePass -and $orderbookStatsPass -and $orderbookHealthPass

# 2) T15.1 revalidation gate: auto PASS/FAIL
$smokeReportPath = if ([System.IO.Path]::IsPathRooted($SmokeReportJson)) { $SmokeReportJson } else { Join-Path $ProjectRoot $SmokeReportJson }
$smokeScriptPath = Join-Path $ProjectRoot "scripts/paper_micro_smoke.ps1"
$smokeRunAttempted = $false
$smokeRunExitCode = -1
$smokeRunOutput = ""
if (-not $SkipSmoke -and (Test-Path $smokeScriptPath)) {
    $smokeRunAttempted = $true
    $pwshExe = Get-PowerShellExe
    $smokeRunArgs = @(
        "-NoProfile",
        "-ExecutionPolicy", "Bypass",
        "-File", $smokeScriptPath,
        "-PythonExe", $PythonExe,
        "-ProjectRoot", $ProjectRoot,
        "-DurationSec", $SmokeDurationSec,
        "-Quote", $Quote,
        "-TopN", $SmokeTopN,
        "-PaperMicroProvider", $SmokePaperMicroProvider,
        "-WarmupSec", $SmokeWarmupSec,
        "-WarmupMinTradeEventsPerMarket", $SmokeWarmupMinTradeEventsPerMarket,
        "-MaxFallbackRatio", $GateFallbackRatioMax,
        "-MinTierCount", $GateTierMinCount,
        "-MinPolicyEvents", $GatePolicyEventMin
    )
    $smokeRunExec = Invoke-CommandCapture -Exe $pwshExe -ArgList $smokeRunArgs
    $smokeRunExitCode = $smokeRunExec.ExitCode
    $smokeRunOutput = [string](Get-PropValue -ObjectValue $smokeRunExec -Name "Output" -DefaultValue "")
}
$smokeRunOutputPreview = if ([string]::IsNullOrWhiteSpace($smokeRunOutput)) { "" } else { ($smokeRunOutput.Trim() -replace "\r?\n", " | ") }
if ($smokeRunOutputPreview.Length -gt 400) {
    $smokeRunOutputPreview = $smokeRunOutputPreview.Substring(0, 400)
}
$smokeReport = Load-JsonOrEmpty -PathValue $smokeReportPath
$smokeAvailable = (Test-Path $smokeReportPath) -and (-not ($smokeReport -is [System.Collections.IDictionary] -and $smokeReport.Count -eq 0))
if ($smokeRunAttempted -and $smokeRunExitCode -ne 0) {
    $smokeAvailable = $false
}
$smokeGeneratedAt = [string](Get-PropValue -ObjectValue $smokeReport -Name "generated_at" -DefaultValue "NA")
$smokeRunId = [string](Get-PropValue -ObjectValue $smokeReport -Name "run_id" -DefaultValue "NA")
$smokeOrdersSubmitted = To-Int64 (Get-PropValue -ObjectValue $smokeReport -Name "orders_submitted" -DefaultValue 0) 0
$smokeFallbackCount = To-Int64 (Get-PropValue -ObjectValue $smokeReport -Name "micro_missing_fallback_count" -DefaultValue 0) 0
$smokeFallbackRatio = To-Double (Get-PropValue -ObjectValue $smokeReport -Name "micro_missing_fallback_ratio" -DefaultValue 1.0) 1.0
$smokeTierUniqueCount = To-Int64 (Get-PropValue -ObjectValue $smokeReport -Name "tier_unique_count" -DefaultValue 0) 0
$smokePolicyEvents = To-Int64 (Get-PropValue -ObjectValue $smokeReport -Name "replace_cancel_timeout_total" -DefaultValue 0) 0
$smokeMicroProvider = [string](Get-PropValue -ObjectValue $smokeReport -Name "micro_provider" -DefaultValue "NA")
$smokeProviderInfo = Get-PropValue -ObjectValue $smokeReport -Name "micro_provider_info" -DefaultValue @{}
$smokeProviderSubscribedMarkets = To-Int64 (Get-PropValue -ObjectValue $smokeProviderInfo -Name "subscribed_markets_count" -DefaultValue 0) 0
$smokeLiveWs = Get-PropValue -ObjectValue $smokeReport -Name "live_ws" -DefaultValue @{}
$smokeLiveWsConnected = To-Bool (Get-PropValue -ObjectValue $smokeLiveWs -Name "ws_connected" -DefaultValue $false) $false
$smokeLiveWsSubscribedMarkets = To-Int64 (Get-PropValue -ObjectValue $smokeLiveWs -Name "subscribed_markets_count" -DefaultValue 0) 0
$smokeLiveWsSnapshotAgeMs = To-Int64 (Get-PropValue -ObjectValue $smokeLiveWs -Name "micro_snapshot_age_ms" -DefaultValue -1) -1
$smokeLiveWsHealthAvailable = To-Bool (Get-PropValue -ObjectValue $smokeLiveWs -Name "health_snapshot_available" -DefaultValue $false) $false
$smokeLiveWsHealthPath = [string](Get-PropValue -ObjectValue $smokeLiveWs -Name "health_snapshot_path" -DefaultValue "NA")

# 3) Tiering recommendation (liq_score quantiles from recent paper runs)
$tieringReportPath = if ([System.IO.Path]::IsPathRooted($TieringReportJson)) { $TieringReportJson } else { Join-Path $ProjectRoot $TieringReportJson }
$tieringScriptPath = Join-Path $ProjectRoot "scripts/recommend_micro_tiering.ps1"
$tieringRunAttempted = $false
$tieringRunExitCode = -1
if (-not $SkipTieringRecommend -and (Test-Path $tieringScriptPath)) {
    $tieringRunAttempted = $true
    $pwshExe = Get-PowerShellExe
    $tieringRunArgs = @(
        "-NoProfile",
        "-ExecutionPolicy", "Bypass",
        "-File", $tieringScriptPath,
        "-ProjectRoot", $ProjectRoot,
        "-RecentHours", $TieringRecentHours,
        "-MinSamples", $TieringMinSamples
    )
    $tieringRunExec = Invoke-CommandCapture -Exe $pwshExe -ArgList $tieringRunArgs
    $tieringRunExitCode = $tieringRunExec.ExitCode
}
$tieringReport = Load-JsonOrEmpty -PathValue $tieringReportPath
$tieringStatus = [string](Get-PropValue -ObjectValue (Get-PropValue -ObjectValue $tieringReport -Name "recommendation" -DefaultValue @{}) -Name "status" -DefaultValue "NA")
$tieringT1 = Get-PropValue -ObjectValue (Get-PropValue -ObjectValue $tieringReport -Name "recommendation" -DefaultValue @{}) -Name "t1" -DefaultValue $null
$tieringT2 = Get-PropValue -ObjectValue (Get-PropValue -ObjectValue $tieringReport -Name "recommendation" -DefaultValue @{}) -Name "t2" -DefaultValue $null
$tieringSampleCount = To-Int64 (Get-PropValue -ObjectValue $tieringReport -Name "sample_count" -DefaultValue 0) 0
$tieringFallbackCount = To-Int64 (Get-PropValue -ObjectValue $tieringReport -Name "micro_missing_fallback_count" -DefaultValue 0) 0

$microParts = To-Int64 (Get-PropValue -ObjectValue $stats -Name "parts" -DefaultValue 0) 0
$gateBookAvailablePass = $bookAvailableRatio -ge $GateBookAvailableMin
$gateTradeSourceWsPass = $tradeSourceWsRatio -ge $GateTradeSourceWsMin
$gateFallbackRatioPass = $smokeAvailable -and ($smokeOrdersSubmitted -gt 0) -and ($smokeFallbackRatio -lt $GateFallbackRatioMax)
$gateTierDiversityPass = $smokeAvailable -and ($smokeTierUniqueCount -ge $GateTierMinCount)
$gatePolicyEventsPass = $smokeAvailable -and ($smokePolicyEvents -ge $GatePolicyEventMin)
$t15PolicyGatePass = $gateFallbackRatioPass -and $gateTierDiversityPass -and $gatePolicyEventsPass
$t15PolicyGateStatus = Get-PassFail -Condition $t15PolicyGatePass
$t15PolicyGateReason = if (-not $smokeAvailable) { "SMOKE_REPORT_UNAVAILABLE" } elseif ($t15PolicyGatePass) { "OK" } else { "THRESHOLD_NOT_MET" }

$t15DataGatePassByThreshold = $gateBookAvailablePass -and $gateTradeSourceWsPass
$t15DataGatePass = $t15DataGatePassByThreshold
$t15DataGateStatus = if ($t15DataGatePass) { "PASS" } else { "FAIL" }
$t15DataGateReason = if ($t15DataGatePass) { "OK" } else { "THRESHOLD_NOT_MET" }
if (-not $batchDateHasWsParts) {
    $t15DataGatePass = $false
    $t15DataGateStatus = "DEFER"
    $t15DataGateReason = "NO_WS_PARTS_FOR_BATCH_DATE"
}
$t15DataGateDisplayStatus = if ($t15DataGateStatus -eq "DEFER") { "DEFER" } else { Get-PassFail -Condition $t15DataGatePass }

$t15OverallGatePass = $t15PolicyGatePass -and $t15DataGatePass
$t15OverallGateStatus = Get-PassFail -Condition $t15OverallGatePass
$t151GatePass = $t15OverallGatePass

$outMetaDir = Join-Path $OutRoot "_meta"
New-Item -ItemType Directory -Path $outMetaDir -Force | Out-Null
$dateAlignmentPath = Join-Path $outMetaDir "daily_date_alignment.json"
$gateReportPath = Join-Path $outMetaDir "daily_t15_gate_report.json"

$dateAlignmentPayload = [ordered]@{
    generated_at = (Get-Date).ToString("o")
    batch_date = $batchDate
    batch_date_source = $batchDateSource
    ws_date = $wsDate
    ws_date_reason = $wsDateReason
    utc_today_date = $utcTodayDate
    latest_available_partition = $latestWsPartitionDate
    batch_date_has_ws_parts = $batchDateHasWsParts
    utc_today_has_ws_parts = $utcTodayHasWsParts
    batch_date_orderbook_parts = $batchDateOrderbookPartCount
    batch_date_trade_parts = $batchDateTradePartCount
    ws_date_orderbook_parts = $orderbookPartCount
    ws_date_trade_parts = $tradePartCount
}
$dateAlignmentPayload | ConvertTo-Json -Depth 8 | Set-Content -Path $dateAlignmentPath -Encoding UTF8

$gatePayload = [ordered]@{
    generated_at = (Get-Date).ToString("o")
    target_date = $batchDate
    batch_date = $batchDate
    batch_date_source = $batchDateSource
    ws_date = $wsDate
    ws_date_reason = $wsDateReason
    thresholds = [ordered]@{
        book_available_ratio_min = $GateBookAvailableMin
        trade_source_ws_ratio_min = $GateTradeSourceWsMin
        micro_missing_fallback_ratio_max = $GateFallbackRatioMax
        tier_unique_count_min = $GateTierMinCount
        policy_events_min = $GatePolicyEventMin
    }
    values = [ordered]@{
        book_available_ratio = $bookAvailableRatio
        trade_source_ws_ratio = $tradeSourceWsRatio
        micro_parts = $microParts
        join_match_ratio = $joinMatchValue
        micro_missing_fallback_ratio = $smokeFallbackRatio
        micro_missing_fallback_count = $smokeFallbackCount
        orders_submitted = $smokeOrdersSubmitted
        tier_unique_count = $smokeTierUniqueCount
        replace_cancel_timeout_total = $smokePolicyEvents
    }
    policy_gate = [ordered]@{
        pass = $t15PolicyGatePass
        status = $t15PolicyGateStatus
        reason = $t15PolicyGateReason
        checks = [ordered]@{
            fallback_ratio_pass = $gateFallbackRatioPass
            tier_diversity_pass = $gateTierDiversityPass
            policy_events_pass = $gatePolicyEventsPass
        }
    }
    data_gate = [ordered]@{
        pass = $t15DataGatePass
        status = $t15DataGateStatus
        reason = $t15DataGateReason
        checks = [ordered]@{
            book_available_pass = $gateBookAvailablePass
            trade_source_ws_pass = $gateTradeSourceWsPass
            pass_by_threshold = $t15DataGatePassByThreshold
        }
        context = [ordered]@{
            batch_date_has_ws_parts = $batchDateHasWsParts
            batch_date_orderbook_parts = $batchDateOrderbookPartCount
            batch_date_trade_parts = $batchDateTradePartCount
        }
    }
    overall_gate = [ordered]@{
        pass = $t15OverallGatePass
        status = $t15OverallGateStatus
        formula = "policy_gate.pass AND data_gate.pass"
    }
    gate = [ordered]@{
        book_available_pass = $gateBookAvailablePass
        trade_source_ws_pass = $gateTradeSourceWsPass
        fallback_ratio_pass = $gateFallbackRatioPass
        tier_diversity_pass = $gateTierDiversityPass
        policy_events_pass = $gatePolicyEventsPass
        overall_pass = $t15OverallGatePass
    }
    t15_policy_gate = $t15PolicyGatePass
    t15_data_gate = $t15DataGatePass
    t15_data_gate_status = $t15DataGateStatus
    t15_overall_gate = $t15OverallGatePass
    t15_1_revalidation_gate = $t15OverallGatePass
    orderbook_verification_gate = $orderbookVerificationPass
    data_gate_deferred = ($t15DataGateStatus -eq "DEFER")
    data_gate_defer_reason = if ($t15DataGateStatus -eq "DEFER") { $t15DataGateReason } else { "" }
    date_alignment_report = $dateAlignmentPath
    ws_partition = [ordered]@{
        batch_date = $batchDate
        ws_date = $wsDate
        ws_date_reason = $wsDateReason
        utc_today = $utcTodayDate
        latest_available_partition = $latestWsPartitionDate
    }
    orderbook_verification = [ordered]@{
        ws_date = $wsDate
        ws_date_reason = $wsDateReason
        folder_pass = $orderbookFolderPass
        validate_pass = $orderbookValidatePass
        stats_pass = $orderbookStatsPass
        health_pass = $orderbookHealthPass
        overall_pass = $orderbookVerificationPass
        stats_raw_root_filter = [ordered]@{
            rows_before = $wsManifestRowsBefore
            rows_after = $wsManifestRowsAfter
            ignored_outside_raw_root = $wsManifestIgnoredRows
        }
    }
    candles_topup = [ordered]@{
        skipped = [bool]$SkipCandles
        pass = $candlesTopupPass
        status = $candlesTopupStatus
        selected_markets = $candlesSelectedMarkets
        targets = $candlesTargets
        skipped_ranges = $candlesSkippedRanges
        processed_targets = $candlesProcessedTargets
        ok_targets = $candlesOkTargets
        warn_targets = $candlesWarnTargets
        fail_targets = $candlesFailTargets
        calls_made = $candlesCallsMade
        validate_checked_files = $candlesValidateCheckedFiles
        validate_warn_files = $candlesValidateWarnFiles
        validate_fail_files = $candlesValidateFailFiles
        coverage_delta_pct = $candlesCoverageDeltaPct
    }
    smoke = [ordered]@{
        available = $smokeAvailable
        report_path = $smokeReportPath
        generated_at = $smokeGeneratedAt
        run_id = $smokeRunId
        micro_provider = $smokeMicroProvider
        provider_subscribed_markets_count = $smokeProviderSubscribedMarkets
        live_ws_connected = $smokeLiveWsConnected
        live_ws_subscribed_markets_count = $smokeLiveWsSubscribedMarkets
        live_ws_micro_snapshot_age_ms = $smokeLiveWsSnapshotAgeMs
        live_ws_health_snapshot_available = $smokeLiveWsHealthAvailable
        live_ws_health_snapshot_path = $smokeLiveWsHealthPath
        run_attempted = $smokeRunAttempted
        run_exit_code = $smokeRunExitCode
        skipped = [bool]$SkipSmoke
    }
    tiering_recommendation = [ordered]@{
        report_path = $tieringReportPath
        status = $tieringStatus
        t1 = $tieringT1
        t2 = $tieringT2
        sample_count = $tieringSampleCount
        micro_missing_fallback_count = $tieringFallbackCount
        run_attempted = $tieringRunAttempted
        run_exit_code = $tieringRunExitCode
        skipped = [bool]$SkipTieringRecommend
    }
}
$gatePayload | ConvertTo-Json -Depth 8 | Set-Content -Path $gateReportPath -Encoding UTF8

$reportLines = @(
    "# DAILY_MICRO_REPORT_$batchDate",
    "",
    "## Summary",
    "- batch_date: $batchDate",
    "- batch_date_source: $batchDateSource",
    "- ws_date: $wsDate",
    "- ws_date_reason: $wsDateReason",
    "- target_date(legacy): $batchDate",
    "- quote/top_n: $Quote / $TopN",
    ("- candles_topup: {0}" -f $candlesTopupStatus),
    ("- book_available_ratio: {0:N6}" -f $bookAvailableRatio),
    ("- trade_source_ws_ratio: {0:N6}" -f $tradeSourceWsRatio),
    ("- micro_available_ratio: {0:N6}" -f $microAvailableRatio),
    "- join_match_ratio: $joinMatchRatio",
    ("- tiering_recommendation_status: {0}" -f $tieringStatus),
    ("- orderbook_verification_gate(ws_date): {0}" -f (Get-PassFail -Condition $orderbookVerificationPass)),
    ("- t15_policy_gate: {0}" -f (Get-PassFail -Condition $t15PolicyGatePass)),
    ("- t15_data_gate: {0} (reason={1})" -f $t15DataGateDisplayStatus, $t15DataGateReason),
    ("- t15_overall_gate: {0}" -f (Get-PassFail -Condition $t15OverallGatePass)),
    ("- t15_1_revalidation_gate(legacy): {0}" -f (Get-PassFail -Condition $t151GatePass)),
    "",
    "## Candles Daily Top-up",
    ("- skipped: {0}" -f [bool]$SkipCandles),
    ("- status: {0}" -f $candlesTopupStatus),
    ("- plan(selected_markets={0}, targets={1}, skipped_ranges={2})" -f $candlesSelectedMarkets, $candlesTargets, $candlesSkippedRanges),
    ("- collect(processed={0}, ok={1}, warn={2}, fail={3}, calls={4})" -f $candlesProcessedTargets, $candlesOkTargets, $candlesWarnTargets, $candlesFailTargets, $candlesCallsMade),
    ("- validate(checked={0}, warn={1}, fail={2})" -f $candlesValidateCheckedFiles, $candlesValidateWarnFiles, $candlesValidateFailFiles),
    ("- coverage_delta.average_delta_pct: {0}" -f ($(if ($null -eq $candlesCoverageDeltaPct) { "NA" } else { [string]$candlesCoverageDeltaPct }))),
    "",
    "## Orderbook Verification (ws_date basis: Folder / Validate / Stats / Health)",
    ("- ws_date: {0}" -f $wsDate),
    ("- ws_date_reason: {0}" -f $wsDateReason),
    ("- folder(orderbook parts > 0): {0} (parts={1}, bytes={2})" -f (Get-PassFail -Condition $orderbookFolderPass), $orderbookPartCount, $orderbookBytes),
    ("- validate(fail_files=0, parse_ok>=0.99): {0} (exit={1}, checked={2}, fail_files={3}, parse_ok_ratio={4:N6})" -f (Get-PassFail -Condition $orderbookValidatePass), $wsValidateExec.ExitCode, $wsValidateCheckedFiles, $wsValidateFailFiles, $wsValidateParseOkRatio),
    ("- stats(orderbook_rows > 0): {0} (exit={1}, orderbook_rows={2}, trade_rows={3})" -f (Get-PassFail -Condition $orderbookStatsPass), $wsStatsExec.ExitCode, $wsStatsOrderbookRows, $wsStatsTradeRows),
    ("- stats_raw_root_filter(rows_before={0}, rows_after={1}, ignored_outside_raw_root={2})" -f $wsManifestRowsBefore, $wsManifestRowsAfter, $wsManifestIgnoredRows),
    ("- health(connected=true, orderbook_rx_lag_sec<={0}): {1} (connected={2}, orderbook_rx_lag_sec={3:N2}, trade_rx_lag_sec={4:N2}, health_lag_sec={5:N2}, subscribed_markets={6})" -f $HealthLagWarnSec, (Get-PassFail -Condition $orderbookHealthPass), $wsHealthConnected, $wsOrderbookRxLagSec, $wsTradeRxLagSec, $wsHealthLagSec, $wsSubscribedMarkets),
    ("- overall: {0}" -f (Get-PassFail -Condition $orderbookVerificationPass)),
    "",
    "## T15.1 Revalidation Gates (Policy / Data / Overall)",
    ("- t15_policy_gate: {0} (reason={1})" -f (Get-PassFail -Condition $t15PolicyGatePass), $t15PolicyGateReason),
    ("  - MICRO_MISSING_FALLBACK ratio < {0:P0}: {1} (actual={2:N6}, fallback_count={3}, orders_submitted={4})" -f $GateFallbackRatioMax, (Get-PassFail -Condition $gateFallbackRatioPass), $smokeFallbackRatio, $smokeFallbackCount, $smokeOrdersSubmitted),
    ("  - tier_unique_count >= {0}: {1} (actual={2})" -f $GateTierMinCount, (Get-PassFail -Condition $gateTierDiversityPass), $smokeTierUniqueCount),
    ("  - replace+cancel+timeout >= {0}: {1} (actual={2})" -f $GatePolicyEventMin, (Get-PassFail -Condition $gatePolicyEventsPass), $smokePolicyEvents),
    ("- t15_data_gate: {0} (reason={1})" -f $t15DataGateDisplayStatus, $t15DataGateReason),
    ("  - book_available_ratio >= {0:N3}: {1} (actual={2:N6})" -f $GateBookAvailableMin, (Get-PassFail -Condition $gateBookAvailablePass), $bookAvailableRatio),
    ("  - trade_source_ws_ratio >= {0:N3}: {1} (actual={2:N6})" -f $GateTradeSourceWsMin, (Get-PassFail -Condition $gateTradeSourceWsPass), $tradeSourceWsRatio),
    ("  - batch_date_ws_parts(orderbook={0}, trade={1}, has_ws_parts={2})" -f $batchDateOrderbookPartCount, $batchDateTradePartCount, $batchDateHasWsParts),
    "- note: data_gate can be DEFER during early operations when WS backfill is unavailable",
    ("- t15_overall_gate(policy AND data): {0}" -f (Get-PassFail -Condition $t15OverallGatePass)),
    ("- t15_1_revalidation_gate(legacy=overall): {0}" -f (Get-PassFail -Condition $t151GatePass)),
    "",
    "## Latest Paper Smoke (10m)",
    ("- smoke_run_attempted: {0}" -f $smokeRunAttempted),
    ("- smoke_run_exit_code: {0}" -f $smokeRunExitCode),
    ("- smoke_skipped: {0}" -f [bool]$SkipSmoke),
    ("- smoke_run_output_preview: {0}" -f ($(if ([string]::IsNullOrWhiteSpace($smokeRunOutputPreview)) { "NA" } else { $smokeRunOutputPreview }))),
    ("- smoke_report_path: {0}" -f $smokeReportPath),
    ("- smoke_available: {0}" -f $smokeAvailable),
    ("- smoke_generated_at: {0}" -f $smokeGeneratedAt),
    ("- smoke_run_id: {0}" -f $smokeRunId),
    ("- smoke_micro_provider: {0}" -f $smokeMicroProvider),
    ("- smoke_provider_subscribed_markets: {0}" -f $smokeProviderSubscribedMarkets),
    ("- smoke_live_ws_connected: {0}" -f $smokeLiveWsConnected),
    ("- smoke_live_ws_subscribed_markets: {0}" -f $smokeLiveWsSubscribedMarkets),
    ("- smoke_live_ws_micro_snapshot_age_ms: {0}" -f $smokeLiveWsSnapshotAgeMs),
    ("- smoke_live_ws_health_snapshot_available: {0}" -f $smokeLiveWsHealthAvailable),
    ("- smoke_live_ws_health_snapshot_path: {0}" -f $smokeLiveWsHealthPath),
    "",
    "## Tiering Recommendation (Recent Paper LQ Score)",
    ("- tiering_run_attempted: {0}" -f $tieringRunAttempted),
    ("- tiering_run_exit_code: {0}" -f $tieringRunExitCode),
    ("- tiering_skipped: {0}" -f [bool]$SkipTieringRecommend),
    ("- tiering_report_path: {0}" -f $tieringReportPath),
    ("- status: {0}" -f $tieringStatus),
    ("- sample_count: {0}" -f $tieringSampleCount),
    ("- fallback_count: {0}" -f $tieringFallbackCount),
    ("- recommended_t1: {0}" -f ($(if ($null -eq $tieringT1) { "NA" } else { [string]$tieringT1 }))),
    ("- recommended_t2: {0}" -f ($(if ($null -eq $tieringT2) { "NA" } else { [string]$tieringT2 }))),
    "",
    "## Commands",
    "- python -m autobot.cli collect plan-candles --base-dataset $CandlesBaseDataset --parquet-root $ParquetRoot --out $candlesPlanPathResolved --lookback-months $CandlesLookbackMonths --tf $CandlesTf --quote $Quote --market-mode $CandlesMarketMode --top-n $TopN --max-backfill-days-1m $CandlesMaxBackfillDays1m --end $batchDate",
    "- python -m autobot.cli collect candles --plan $candlesPlanPathResolved --out-dataset $CandlesOutDataset --parquet-root $ParquetRoot --workers $CandlesWorkers --dry-run false --rate-limit-strict $CandlesRateLimitStrict",
    "- python -m autobot.cli collect ticks --mode daily --quote $Quote --top-n $TopN --days-ago $DaysAgo --raw-root $RawTicksRoot --rate-limit-strict true --workers $Workers --max-pages-per-target $MaxPagesPerTarget --dry-run false",
    "- python -m autobot.cli micro aggregate --start $batchDate --end $batchDate --quote $Quote --top-n $TopN --raw-ticks-root $RawTicksRoot --raw-ws-root $RawWsRoot --out-root $OutRoot",
    "- python -m autobot.cli micro validate --out-root $OutRoot",
    "- python -m autobot.cli micro stats --out-root $OutRoot",
    "- python -m autobot.cli collect ws-public validate --date $wsDate --raw-root $RawWsRoot --meta-dir $RawWsMetaDir --quarantine-corrupt $WsValidateQuarantineCorrupt --min-age-sec $WsValidateMinAgeSec",
    "- python -m autobot.cli collect ws-public stats --date $wsDate --raw-root $RawWsRoot --meta-dir $RawWsMetaDir",
    "- pwsh -NoProfile -ExecutionPolicy Bypass -File scripts/paper_micro_smoke.ps1 -DurationSec $SmokeDurationSec -PaperMicroProvider $SmokePaperMicroProvider -WarmupSec $SmokeWarmupSec",
    "- pwsh -NoProfile -ExecutionPolicy Bypass -File scripts/recommend_micro_tiering.ps1 -RecentHours $TieringRecentHours -MinSamples $TieringMinSamples",
    "",
    "## Artifacts",
    "- candles_plan: $candlesPlanPathResolved",
    "- candles_collect_report: $candlesCollectReportPath",
    "- candles_validate_report: $candlesValidateReportPath",
    "- ticks_collect_report: data/raw_ticks/upbit/_meta/ticks_collect_report.json",
    "- micro_aggregate_report: $aggregateReportPath",
    "- micro_validate_report: $validateReportPath",
    "- micro_manifest: $manifestPath",
    "- daily_date_alignment: $dateAlignmentPath",
    "- ws_validate_report: $wsValidateReportPath",
    "- t15_gate_report: $gateReportPath",
    "- smoke_report: $smokeReportPath",
    "- tiering_report: $tieringReportPath",
    "",
    "## Excerpts",
    ("- candles collect processed_targets: {0}" -f $candlesProcessedTargets),
    ("- candles collect fail_targets: {0}" -f $candlesFailTargets),
    ("- ticks run_id: {0}" -f (Get-PropValue -ObjectValue $ticksReport -Name "run_id" -DefaultValue "NA")),
    ("- micro aggregate run_id: {0}" -f (Get-PropValue -ObjectValue $aggregateReport -Name "run_id" -DefaultValue "NA")),
    ("- micro rows_written_total: {0}" -f (Get-PropValue -ObjectValue $aggregateReport -Name "rows_written_total" -DefaultValue "NA")),
    ("- micro parts: {0}" -f (Get-PropValue -ObjectValue $stats -Name "parts" -DefaultValue "NA"))
)
$report = $reportLines -join "`n"

Set-Content -Path $reportPath -Value $report -Encoding UTF8
Write-Host ("[daily-micro] batch_date={0}" -f $batchDate)
Write-Host ("[daily-micro] ws_date={0} ({1})" -f $wsDate, $wsDateReason)
Write-Host ("[daily-micro] orderbook_verification={0}" -f (Get-PassFail -Condition $orderbookVerificationPass))
Write-Host ("[daily-micro] t15_policy_gate={0}" -f (Get-PassFail -Condition $t15PolicyGatePass))
Write-Host ("[daily-micro] t15_data_gate={0} ({1})" -f $t15DataGateDisplayStatus, $t15DataGateReason)
Write-Host ("[daily-micro] t15_overall_gate={0}" -f (Get-PassFail -Condition $t15OverallGatePass))
Write-Host ("[daily-micro] t15_1_revalidation_gate={0}" -f (Get-PassFail -Condition $t151GatePass))
Write-Host "[daily-micro] report=$reportPath"
