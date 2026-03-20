$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

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
        [string]$TrainDataQualityFloorDate,
        [string]$TrainStartFloorDate
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
