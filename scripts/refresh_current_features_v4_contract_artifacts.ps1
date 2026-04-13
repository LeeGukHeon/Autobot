param(
    [string]$ProjectRoot = "",
    [string]$PythonExe = "",
    [string]$FeatureSet = "v4",
    [string]$LabelSet = "",
    [string]$Tf = "",
    [string]$Quote = "",
    [string]$StartDate = "",
    [string]$EndDate = "",
    [int]$TopN = 0,
    [int]$ParityTopN = 20,
    [string[]]$Markets = @(),
    [string]$BaseCandlesDataset = "candles_api_v1",
    [string]$RawTicksRoot = "data/raw_ticks/upbit/trades",
    [string]$RawWsRoot = "data/raw_ws/upbit/public",
    [string]$MicroOutRoot = "data/parquet/micro_v1",
    [string]$SummaryPath = "data/features/features_v4/_meta/contract_refresh_report.json",
    [switch]$UseTopNUniverse,
    [switch]$RequireExplicitWindow,
    [switch]$SkipMicroRefresh,
    [switch]$SkipMicroValidate,
    [switch]$SkipFeaturesBuild,
    [switch]$SkipFeaturesValidate,
    [switch]$SkipParity,
    [switch]$SkipRegistryRefresh,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

. (Join-Path $PSScriptRoot "systemd_service_utils.ps1")

function Resolve-ProjectPath {
    param(
        [string]$Root,
        [string]$PathValue
    )
    if ([string]::IsNullOrWhiteSpace($PathValue)) {
        return $Root
    }
    if ([System.IO.Path]::IsPathRooted($PathValue)) {
        return [System.IO.Path]::GetFullPath($PathValue)
    }
    return [System.IO.Path]::GetFullPath((Join-Path $Root $PathValue))
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

function Format-CommandLine {
    param(
        [string]$Exe,
        [string[]]$ArgList
    )
    return ($Exe + " " + (($ArgList | ForEach-Object { Quote-ShellArg ([string]$_) }) -join " "))
}

function Invoke-ProjectPythonStep {
    param(
        [string]$PythonPath,
        [string]$StepName,
        [string[]]$ArgList
    )
    $commandText = Format-CommandLine -Exe $PythonPath -ArgList $ArgList
    Write-Host ("[feature-contract-refresh] step={0}" -f $StepName)
    Write-Host ("[feature-contract-refresh] command={0}" -f $commandText)
    if ($DryRun) {
        return [ordered]@{
            step = $StepName
            command = $commandText
            exit_code = 0
            dry_run = $true
            output_preview = ""
        }
    }
    $output = & $PythonPath @ArgList 2>&1
    $exitCode = [int]$LASTEXITCODE
    $outputText = [string]($output -join [Environment]::NewLine)
    if (-not [string]::IsNullOrWhiteSpace($outputText)) {
        Write-Host $outputText
    }
    if ($exitCode -ne 0) {
        throw ("step failed: " + $StepName + " exit_code=" + $exitCode)
    }
    return [ordered]@{
        step = $StepName
        command = $commandText
        exit_code = $exitCode
        dry_run = $false
        output_preview = if ([string]::IsNullOrWhiteSpace($outputText)) { "" } elseif ($outputText.Length -le 2000) { $outputText } else { $outputText.Substring(0, 2000) }
    }
}

function Resolve-LabelSetFromLabelSpec {
    param(
        [string]$OverrideValue,
        $LabelSpecDoc
    )
    if (-not [string]::IsNullOrWhiteSpace($OverrideValue)) {
        return ([string]$OverrideValue).Trim().ToLowerInvariant()
    }
    $labelColumns = @(Get-PropValue -ObjectValue $LabelSpecDoc -Name "label_columns" -DefaultValue @())
    $normalized = @($labelColumns | ForEach-Object { ([string]$_).Trim() } | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    foreach ($name in $normalized) {
        if ($name -match "_h(3|6|12|24)$") {
            return "v3"
        }
    }
    return "v2"
}

$resolvedProjectRoot = if ([string]::IsNullOrWhiteSpace($ProjectRoot)) { Resolve-DefaultProjectRoot } else { $ProjectRoot }
$resolvedProjectRoot = [System.IO.Path]::GetFullPath($resolvedProjectRoot)
$resolvedPythonExe = if ([string]::IsNullOrWhiteSpace($PythonExe)) { Resolve-DefaultPythonExe -Root $resolvedProjectRoot } else { $PythonExe }
$resolvedSummaryPath = Resolve-ProjectPath -Root $resolvedProjectRoot -PathValue $SummaryPath
$resolvedRawTicksRoot = Resolve-ProjectPath -Root $resolvedProjectRoot -PathValue $RawTicksRoot
$resolvedRawWsRoot = Resolve-ProjectPath -Root $resolvedProjectRoot -PathValue $RawWsRoot
$resolvedMicroOutRoot = Resolve-ProjectPath -Root $resolvedProjectRoot -PathValue $MicroOutRoot

$featureMetaRoot = Join-Path $resolvedProjectRoot "data/features/features_v4/_meta"
$buildReportPath = Join-Path $featureMetaRoot "build_report.json"
$featureSpecPath = Join-Path $featureMetaRoot "feature_spec.json"
$labelSpecPath = Join-Path $featureMetaRoot "label_spec.json"

$buildReport = Load-JsonOrEmpty -PathValue $buildReportPath
$featureSpec = Load-JsonOrEmpty -PathValue $featureSpecPath
$labelSpec = Load-JsonOrEmpty -PathValue $labelSpecPath

$resolvedMarkets = @(Expand-DelimitedStringArray -Value $Markets)
if ($RequireExplicitWindow -and ([string]::IsNullOrWhiteSpace(([string]$StartDate).Trim()) -or [string]::IsNullOrWhiteSpace(([string]$EndDate).Trim()))) {
    throw "explicit features_v4 window is required in this mode; pass -StartDate/-EndDate"
}
if (($resolvedMarkets.Count -eq 0) -and (-not $UseTopNUniverse)) {
    $resolvedMarkets = @(
        @(Get-PropValue -ObjectValue $buildReport -Name "selected_markets" -DefaultValue @()) |
            ForEach-Object { ([string]$_).Trim().ToUpperInvariant() } |
            Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    )
}

$resolvedQuote = ([string]$Quote).Trim().ToUpperInvariant()
if ([string]::IsNullOrWhiteSpace($resolvedQuote)) {
    $resolvedQuote = [string](Get-PropValue -ObjectValue $buildReport -Name "quote" -DefaultValue "")
}
if ([string]::IsNullOrWhiteSpace($resolvedQuote)) {
    $resolvedQuote = [string](Get-PropValue -ObjectValue $featureSpec -Name "quote" -DefaultValue "KRW")
}
if ([string]::IsNullOrWhiteSpace($resolvedQuote)) {
    $resolvedQuote = "KRW"
}

$resolvedTf = ([string]$Tf).Trim().ToLowerInvariant()
if ([string]::IsNullOrWhiteSpace($resolvedTf)) {
    $resolvedTf = [string](Get-PropValue -ObjectValue $buildReport -Name "tf" -DefaultValue "")
}
if ([string]::IsNullOrWhiteSpace($resolvedTf)) {
    $resolvedTf = [string](Get-PropValue -ObjectValue $featureSpec -Name "tf" -DefaultValue "1m")
}
if ([string]::IsNullOrWhiteSpace($resolvedTf)) {
    $resolvedTf = "1m"
}

$resolvedStartDate = ([string]$StartDate).Trim()
if ([string]::IsNullOrWhiteSpace($resolvedStartDate)) {
    $resolvedStartDate = [string](Get-PropValue -ObjectValue $buildReport -Name "effective_start" -DefaultValue "")
}
if ([string]::IsNullOrWhiteSpace($resolvedStartDate)) {
    $resolvedStartDate = [string](Get-PropValue -ObjectValue $buildReport -Name "requested_start" -DefaultValue "")
}

$resolvedEndDate = ([string]$EndDate).Trim()
if ([string]::IsNullOrWhiteSpace($resolvedEndDate)) {
    $resolvedEndDate = [string](Get-PropValue -ObjectValue $buildReport -Name "effective_end" -DefaultValue "")
}
if ([string]::IsNullOrWhiteSpace($resolvedEndDate)) {
    $resolvedEndDate = [string](Get-PropValue -ObjectValue $buildReport -Name "requested_end" -DefaultValue "")
}

$resolvedTopN = [int]$TopN
if ($resolvedTopN -le 0) {
    if ($resolvedMarkets.Count -gt 0) {
        $resolvedTopN = [int]$resolvedMarkets.Count
    } else {
        $resolvedTopN = [int](@(Get-PropValue -ObjectValue $buildReport -Name "selected_markets" -DefaultValue @()).Count)
    }
}

$resolvedLabelSet = Resolve-LabelSetFromLabelSpec -OverrideValue $LabelSet -LabelSpecDoc $labelSpec
$serializedMarkets = Join-DelimitedStringArray -Values $resolvedMarkets

if ([string]::IsNullOrWhiteSpace($resolvedStartDate) -or [string]::IsNullOrWhiteSpace($resolvedEndDate)) {
    throw "unable to resolve features_v4 refresh window; pass -StartDate/-EndDate or keep build_report.json available"
}
if ($resolvedTopN -le 0) {
    throw "unable to resolve features_v4 top_n; pass -TopN or keep selected_markets in build_report.json"
}

$refreshArgumentMode = if ((-not [string]::IsNullOrWhiteSpace(([string]$StartDate).Trim())) -or (-not [string]::IsNullOrWhiteSpace(([string]$EndDate).Trim()))) {
    "explicit_date_range"
} else {
    "cached_build_report_window"
}

$steps = @()
if (-not $SkipMicroRefresh) {
    $microArgs = @(
        "-m", "autobot.cli",
        "micro", "aggregate",
        "--tf", "1m,5m",
        "--start", $resolvedStartDate,
        "--end", $resolvedEndDate,
        "--quote", $resolvedQuote,
        "--raw-ticks-root", $resolvedRawTicksRoot,
        "--raw-ws-root", $resolvedRawWsRoot,
        "--out-root", $resolvedMicroOutRoot,
        "--base-candles", $BaseCandlesDataset,
        "--mode", "overwrite",
        "--alignment-mode", "auto"
    )
    if (-not [string]::IsNullOrWhiteSpace($serializedMarkets)) {
        $microArgs += @("--markets", $serializedMarkets)
    } else {
        $microArgs += @("--top-n", ([string][Math]::Max($resolvedTopN, 1)))
    }
    $steps += ,([ordered]@{ name = "micro_aggregate_contract_window"; args = $microArgs })
}
if (-not $SkipMicroValidate) {
    $steps += ,([ordered]@{
        name = "micro_validate_contract_window"
        args = @(
            "-m", "autobot.cli",
            "micro", "validate",
            "--tf", "1m,5m",
            "--out-root", $resolvedMicroOutRoot,
            "--base-candles", $BaseCandlesDataset
        )
    })
}
if (-not $SkipFeaturesBuild) {
    $steps += ,([ordered]@{
        name = "features_v4_build_contract_window"
        args = @(
            "-m", "autobot.cli",
            "features", "build",
            "--feature-set", $FeatureSet,
            "--label-set", $resolvedLabelSet,
            "--tf", $resolvedTf,
            "--quote", $resolvedQuote,
            "--top-n", ([string][Math]::Max($resolvedTopN, 1)),
            "--start", $resolvedStartDate,
            "--end", $resolvedEndDate,
            "--base-candles", $BaseCandlesDataset,
            "--micro-dataset", "micro_v1",
            "--workers", "1",
            "--fail-on-warn", "false"
        )
    })
}
if (-not $SkipFeaturesValidate) {
    $steps += ,([ordered]@{
        name = "features_v4_validate_contract_window"
        args = @(
            "-m", "autobot.cli",
            "features", "validate",
            "--feature-set", $FeatureSet,
            "--tf", $resolvedTf,
            "--quote", $resolvedQuote,
            "--top-n", ([string][Math]::Max($resolvedTopN, 1)),
            "--start", $resolvedStartDate,
            "--end", $resolvedEndDate
        )
    })
}
if (-not $SkipParity) {
    $steps += ,([ordered]@{
        name = "features_v4_live_parity_contract_window"
        args = @(
            "-m", "autobot.ops.live_feature_parity_report",
            "--project-root", $resolvedProjectRoot,
            "--feature-set", $FeatureSet,
            "--tf", $resolvedTf,
            "--quote", $resolvedQuote,
            "--top-n", ([string][Math]::Max([int]$ParityTopN, 1)),
            "--samples-per-market", "1"
        )
    })
}
if (-not $SkipRegistryRefresh) {
    $steps += ,([ordered]@{
        name = "refresh_data_contract_registry"
        args = @(
            "-m", "autobot.ops.data_contract_registry",
            "--project-root", $resolvedProjectRoot
        )
    })
    $steps += ,([ordered]@{
        name = "refresh_dataset_retention_registry"
        args = @(
            "-m", "autobot.ops.dataset_retention_registry",
            "--project-root", $resolvedProjectRoot
        )
    })
    $steps += ,([ordered]@{
        name = "refresh_raw_to_feature_lineage_report"
        args = @(
            "-m", "autobot.ops.raw_to_feature_lineage_report",
            "--project-root", $resolvedProjectRoot,
            "--feature-set", $FeatureSet
        )
    })
    $steps += ,([ordered]@{
        name = "refresh_feature_dataset_certification"
        args = @(
            "-m", "autobot.ops.feature_dataset_certification",
            "--project-root", $resolvedProjectRoot,
            "--feature-set", $FeatureSet
        )
    })
}

$stepResults = @()
Push-Location $resolvedProjectRoot
try {
    foreach ($step in @($steps)) {
        $stepResults += ,(Invoke-ProjectPythonStep -PythonPath $resolvedPythonExe -StepName ([string]$step.name) -ArgList @($step.args))
    }
} finally {
    Pop-Location
}

$summary = [ordered]@{
    policy = "refresh_current_features_v4_contract_artifacts_v1"
    generated_at_utc = (Get-Date).ToUniversalTime().ToString("o")
    project_root = $resolvedProjectRoot
    python_exe = $resolvedPythonExe
    feature_set = $FeatureSet
    label_set = $resolvedLabelSet
    tf = $resolvedTf
    quote = $resolvedQuote
    start = $resolvedStartDate
    end = $resolvedEndDate
    refresh_argument_mode = $refreshArgumentMode
    top_n = [int]$resolvedTopN
    parity_top_n = [int]([Math]::Max([int]$ParityTopN, 1))
    markets = @($resolvedMarkets)
    universe_mode = if ($UseTopNUniverse) { "top_n_dynamic" } else { "explicit_or_cached_markets" }
    base_candles_dataset = $BaseCandlesDataset
    raw_ticks_root = $resolvedRawTicksRoot
    raw_ws_root = $resolvedRawWsRoot
    micro_out_root = $resolvedMicroOutRoot
    source_build_report = $buildReportPath
    source_feature_spec = $featureSpecPath
    source_label_spec = $labelSpecPath
    steps = @($stepResults)
    artifacts = [ordered]@{
        micro_aggregate_report = (Join-Path $resolvedMicroOutRoot "_meta/aggregate_report.json")
        micro_validate_report = (Join-Path $resolvedMicroOutRoot "_meta/validate_report.json")
        features_build_report = (Join-Path $featureMetaRoot "build_report.json")
        features_validate_report = (Join-Path $featureMetaRoot "validate_report.json")
        features_live_parity_report = (Join-Path $featureMetaRoot "live_feature_parity_report.json")
        feature_dataset_certification = (Join-Path $featureMetaRoot "feature_dataset_certification.json")
        raw_to_feature_lineage_report = (Join-Path $featureMetaRoot "raw_to_feature_lineage_report.json")
        data_contract_registry = (Join-Path $resolvedProjectRoot "data/_meta/data_contract_registry.json")
        dataset_retention_registry = (Join-Path $resolvedProjectRoot "data/_meta/dataset_retention_registry.json")
    }
}

$summaryDir = Split-Path -Parent $resolvedSummaryPath
if (-not [string]::IsNullOrWhiteSpace($summaryDir)) {
    New-Item -ItemType Directory -Force -Path $summaryDir | Out-Null
}
$summary | ConvertTo-Json -Depth 8 | Set-Content -Path $resolvedSummaryPath -Encoding UTF8
Write-Host $resolvedSummaryPath
