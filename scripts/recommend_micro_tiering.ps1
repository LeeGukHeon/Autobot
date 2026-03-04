param(
    [string]$ProjectRoot = "D:\MyApps\Autobot",
    [string]$RunsRoot = "data/paper/runs",
    [int]$RecentHours = 24,
    [double]$QuantileLow = 0.33,
    [double]$QuantileHigh = 0.66,
    [int]$MinSamples = 30,
    [string]$OutDir = "logs/micro_tiering"
)

$ErrorActionPreference = "Stop"
Set-Location $ProjectRoot

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

function Get-Quantile {
    param(
        [double[]]$Values,
        [double]$Q
    )
    if ($null -eq $Values -or $Values.Count -le 0) {
        return $null
    }
    $sorted = @($Values | Sort-Object)
    $n = $sorted.Count
    if ($n -eq 1) {
        return [double]$sorted[0]
    }
    $q = [Math]::Min([Math]::Max($Q, 0.0), 1.0)
    $pos = ($n - 1) * $q
    $lower = [int][Math]::Floor($pos)
    $upper = [int][Math]::Ceiling($pos)
    if ($lower -eq $upper) {
        return [double]$sorted[$lower]
    }
    $weight = $pos - $lower
    return ((1.0 - $weight) * [double]$sorted[$lower]) + ($weight * [double]$sorted[$upper])
}

$runsRootPath = if ([System.IO.Path]::IsPathRooted($RunsRoot)) { $RunsRoot } else { Join-Path $ProjectRoot $RunsRoot }
$outDirPath = if ([System.IO.Path]::IsPathRooted($OutDir)) { $OutDir } else { Join-Path $ProjectRoot $OutDir }
New-Item -ItemType Directory -Path $outDirPath -Force | Out-Null

$cutoff = (Get-Date).AddHours(-1 * [Math]::Max($RecentHours, 1))
$scores = New-Object System.Collections.Generic.List[double]
$fallbackCount = 0
$policyOkCount = 0
$runCount = 0
$eventCount = 0

if (Test-Path $runsRootPath) {
    $runDirs = Get-ChildItem -Path $runsRootPath -Directory | Where-Object { $_.LastWriteTime -ge $cutoff } | Sort-Object LastWriteTime
    foreach ($runDir in $runDirs) {
        $eventsPath = Join-Path $runDir.FullName "events.jsonl"
        if (-not (Test-Path $eventsPath)) {
            continue
        }
        $runCount += 1
        foreach ($line in Get-Content -Path $eventsPath -Encoding UTF8) {
            if ([string]::IsNullOrWhiteSpace($line)) {
                continue
            }
            $event = $null
            try {
                $event = $line | ConvertFrom-Json
            } catch {
                continue
            }
            $eventType = [string](Get-PropValue -ObjectValue $event -Name "event_type" -DefaultValue "")
            if ($eventType -ne "INTENT_CREATED") {
                continue
            }
            $eventCount += 1
            $payload = Get-PropValue -ObjectValue $event -Name "payload" -DefaultValue $null
            $meta = Get-PropValue -ObjectValue $payload -Name "meta" -DefaultValue $null
            $policy = Get-PropValue -ObjectValue $meta -Name "micro_order_policy" -DefaultValue $null
            $reasonCode = [string](Get-PropValue -ObjectValue $policy -Name "reason_code" -DefaultValue "")
            if ($reasonCode -eq "MICRO_MISSING_FALLBACK") {
                $fallbackCount += 1
                continue
            }
            if ($reasonCode -ne "POLICY_OK") {
                continue
            }
            $diagnostics = Get-PropValue -ObjectValue $meta -Name "micro_diagnostics" -DefaultValue $null
            $liqScoreRaw = Get-PropValue -ObjectValue $diagnostics -Name "liq_score" -DefaultValue $null
            if ($null -eq $liqScoreRaw) {
                continue
            }
            try {
                $score = [double]$liqScoreRaw
                if ([double]::IsNaN($score) -or [double]::IsInfinity($score)) {
                    continue
                }
                $scores.Add($score)
                $policyOkCount += 1
            } catch {
                continue
            }
        }
    }
}

$scoreArray = @($scores.ToArray())
$sampleCount = $scoreArray.Count
$pLow = Get-Quantile -Values $scoreArray -Q $QuantileLow
$pHigh = Get-Quantile -Values $scoreArray -Q $QuantileHigh
if ($null -ne $pLow -and $null -ne $pHigh -and $pHigh -lt $pLow) {
    $pHigh = $pLow
}

$status = if ($sampleCount -ge $MinSamples) { "READY" } else { "INSUFFICIENT_SAMPLES" }
$payload = [ordered]@{
    generated_at = (Get-Date).ToString("o")
    recent_hours = [int]$RecentHours
    runs_root = $runsRootPath
    run_dirs_scanned = [int]$runCount
    intent_events_scanned = [int]$eventCount
    policy_ok_with_liq_score = [int]$policyOkCount
    micro_missing_fallback_count = [int]$fallbackCount
    sample_count = [int]$sampleCount
    thresholds = [ordered]@{
        quantile_low = [double]$QuantileLow
        quantile_high = [double]$QuantileHigh
        min_samples = [int]$MinSamples
    }
    recommendation = [ordered]@{
        status = $status
        t1 = $pLow
        t2 = $pHigh
    }
}

$stamp = Get-Date -Format "yyyyMMdd-HHmmss"
$outPath = Join-Path $outDirPath ("micro_tiering_recommendation_" + $stamp + ".json")
$latestPath = Join-Path $outDirPath "latest.json"
$payload | ConvertTo-Json -Depth 8 | Set-Content -Path $outPath -Encoding UTF8
$payload | ConvertTo-Json -Depth 8 | Set-Content -Path $latestPath -Encoding UTF8

Write-Host ("[tiering] status={0}" -f $status)
Write-Host ("[tiering] sample_count={0}, fallback_count={1}, policy_ok_with_liq_score={2}" -f $sampleCount, $fallbackCount, $policyOkCount)
Write-Host ("[tiering] recommended_t1={0}, recommended_t2={1}" -f $pLow, $pHigh)
Write-Host ("[tiering] report={0}" -f $outPath)
Write-Host ("[tiering] latest={0}" -f $latestPath)
