param(
    [Parameter(Mandatory = $true)]
    [string]$RunDir
)

$ErrorActionPreference = "Stop"

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

function Merge-ReasonMap {
    param(
        [Parameter(Mandatory = $true)][hashtable]$Target,
        [Parameter(Mandatory = $false)]$Source
    )
    if ($null -eq $Source) {
        return
    }
    if ($Source -is [System.Collections.IDictionary]) {
        foreach ($key in $Source.Keys) {
            $k = [string]$key
            $v = To-Int64 -Value $Source[$key] -DefaultValue 0
            if ($Target.ContainsKey($k)) {
                $Target[$k] = [long]$Target[$k] + $v
            } else {
                $Target[$k] = $v
            }
        }
        return
    }
    if ($Source.PSObject) {
        foreach ($prop in $Source.PSObject.Properties) {
            $k = [string]$prop.Name
            $v = To-Int64 -Value $prop.Value -DefaultValue 0
            if ($Target.ContainsKey($k)) {
                $Target[$k] = [long]$Target[$k] + $v
            } else {
                $Target[$k] = $v
            }
        }
    }
}

function Format-ReasonMap {
    param([Parameter(Mandatory = $false)]$MapValue)
    if ($null -eq $MapValue) {
        return "none"
    }
    if ($MapValue.Count -le 0) {
        return "none"
    }
    $pairs = @()
    foreach ($entry in ($MapValue.GetEnumerator() | Sort-Object -Property @{ Expression = "Value"; Descending = $true }, @{ Expression = "Name"; Descending = $false })) {
        $pairs += ("{0}:{1}" -f $entry.Key, $entry.Value)
    }
    return ($pairs -join ", ")
}

$runPath = (Resolve-Path -LiteralPath $RunDir).Path
$summaryPath = Join-Path $runPath "summary.json"
$eventsPath = Join-Path $runPath "events.jsonl"

$summary = $null
if (Test-Path -LiteralPath $summaryPath) {
    $raw = Get-Content -Path $summaryPath -Raw -Encoding UTF8
    if (-not [string]::IsNullOrWhiteSpace($raw)) {
        $summary = $raw | ConvertFrom-Json
    }
}

$selectionTicks = 0L
$liveBuiltTicks = 0L
$providerStatusTicks = 0L
$scoredRowsTotal = 0L
$selectedRowsTotal = 0L
$intentsTotal = 0L
$droppedMinProbRowsTotal = 0L
$droppedTopPctRowsTotal = 0L
$blockedMinCandidatesTotal = 0L
$liveBuiltRowsTotal = 0L
$missingFeatureRatioMax = 0.0
$selectionReasons = @{}
$liveSkipReasonsBuilt = @{}
$liveSkipReasonsSelection = @{}

if (Test-Path -LiteralPath $eventsPath) {
    foreach ($line in Get-Content -Path $eventsPath -Encoding UTF8) {
        if ([string]::IsNullOrWhiteSpace($line)) {
            continue
        }
        $eventObj = $null
        try {
            $eventObj = $line | ConvertFrom-Json
        } catch {
            continue
        }
        if ($null -eq $eventObj) {
            continue
        }
        $eventType = [string](Get-PropValue -ObjectValue $eventObj -Name "event_type" -DefaultValue "")
        $payload = Get-PropValue -ObjectValue $eventObj -Name "payload" -DefaultValue $null
        switch ($eventType) {
            "MODEL_ALPHA_SELECTION" {
                $selectionTicks += 1
                $scoredRowsTotal += To-Int64 -Value (Get-PropValue -ObjectValue $payload -Name "scored_rows" -DefaultValue 0)
                $selectedRowsTotal += To-Int64 -Value (Get-PropValue -ObjectValue $payload -Name "selected_rows" -DefaultValue 0)
                $intentsTotal += To-Int64 -Value (Get-PropValue -ObjectValue $payload -Name "intents" -DefaultValue 0)
                $droppedMinProbRowsTotal += To-Int64 -Value (Get-PropValue -ObjectValue $payload -Name "dropped_min_prob_rows" -DefaultValue 0)
                $droppedTopPctRowsTotal += To-Int64 -Value (Get-PropValue -ObjectValue $payload -Name "dropped_top_pct_rows" -DefaultValue 0)
                $blockedMinCandidatesTotal += To-Int64 -Value (Get-PropValue -ObjectValue $payload -Name "blocked_min_candidates_ts" -DefaultValue 0)
                Merge-ReasonMap -Target $selectionReasons -Source (Get-PropValue -ObjectValue $payload -Name "reasons" -DefaultValue $null)
                Merge-ReasonMap -Target $liveSkipReasonsSelection -Source (Get-PropValue -ObjectValue $payload -Name "live_feature_skip_reasons" -DefaultValue $null)
            }
            "LIVE_FEATURES_BUILT" {
                $liveBuiltTicks += 1
                $liveBuiltRowsTotal += To-Int64 -Value (Get-PropValue -ObjectValue $payload -Name "built_rows" -DefaultValue 0)
                $missingRatio = To-Double -Value (Get-PropValue -ObjectValue $payload -Name "missing_feature_ratio" -DefaultValue 0.0)
                if ($missingRatio -gt $missingFeatureRatioMax) {
                    $missingFeatureRatioMax = $missingRatio
                }
                Merge-ReasonMap -Target $liveSkipReasonsBuilt -Source (Get-PropValue -ObjectValue $payload -Name "skip_reasons" -DefaultValue $null)
            }
            "FEATURE_PROVIDER_STATUS" {
                $providerStatusTicks += 1
            }
        }
    }
}

Write-Host "================ PAPER RUN DIGEST ================"
Write-Host ("run_dir={0}" -f $runPath)
if ($null -ne $summary) {
    Write-Host (
        "orders_submitted={0} orders_filled={1} fill_rate={2} realized_pnl={3} unrealized_pnl={4}" -f
        (Get-PropValue -ObjectValue $summary -Name "orders_submitted" -DefaultValue 0),
        (Get-PropValue -ObjectValue $summary -Name "orders_filled" -DefaultValue 0),
        (Get-PropValue -ObjectValue $summary -Name "fill_rate" -DefaultValue 0),
        (Get-PropValue -ObjectValue $summary -Name "realized_pnl_quote" -DefaultValue 0),
        (Get-PropValue -ObjectValue $summary -Name "unrealized_pnl_quote" -DefaultValue 0)
    )
    Write-Host (
        "feature_provider={0} micro_provider={1} events={2} duration_sec={3}" -f
        (Get-PropValue -ObjectValue $summary -Name "feature_provider" -DefaultValue ""),
        (Get-PropValue -ObjectValue $summary -Name "micro_provider" -DefaultValue ""),
        (Get-PropValue -ObjectValue $summary -Name "events" -DefaultValue 0),
        (Get-PropValue -ObjectValue $summary -Name "duration_sec" -DefaultValue 0)
    )
}
Write-Host (
    "selection_ticks={0} scored_rows_total={1} selected_rows_total={2} intents_total={3}" -f
    $selectionTicks, $scoredRowsTotal, $selectedRowsTotal, $intentsTotal
)
Write-Host (
    "dropped_min_prob_rows_total={0} dropped_top_pct_rows_total={1} blocked_min_candidates_ts_total={2}" -f
    $droppedMinProbRowsTotal, $droppedTopPctRowsTotal, $blockedMinCandidatesTotal
)
Write-Host (
    "feature_status_ticks={0} live_features_built_ticks={1} live_built_rows_total={2} missing_feature_ratio_max={3}" -f
    $providerStatusTicks, $liveBuiltTicks, $liveBuiltRowsTotal, ([Math]::Round($missingFeatureRatioMax, 6))
)
Write-Host ("selection_reasons={0}" -f (Format-ReasonMap -MapValue $selectionReasons))
Write-Host ("live_skip_reasons_built={0}" -f (Format-ReasonMap -MapValue $liveSkipReasonsBuilt))
Write-Host ("live_skip_reasons_selection={0}" -f (Format-ReasonMap -MapValue $liveSkipReasonsSelection))
Write-Host "=================================================="
