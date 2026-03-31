param(
    [string]$ProjectRoot = "",
    [string]$PythonExe = "",
    [string]$RuntimeInstallScript = "",
    [string]$BatchDate = "",
    [string]$CandidateRunId = "",
    [string]$ModelFamily = "train_v5_fusion",
    [string]$StateRootRelPath = "logs/model_v4_challenger",
    [string]$ChampionCompareModelFamily = "",
    [string]$ChampionUnitName = "autobot-paper-v4.service",
    [string]$ChallengerUnitName = "autobot-paper-v4-challenger.service",
    [string]$PairedPaperUnitName = "autobot-paper-v4-paired.service",
    [string]$PairedPaperModelFamily = "",
    [string[]]$PromotionTargetUnits = @(),
    [string[]]$CandidateTargetUnits = @(),
    [string]$LaneMode = "",
    [bool]$PromotionEligible = $true,
    [switch]$BootstrapOnly,
    [string]$SplitPolicyId = "",
    [string]$SplitPolicyArtifactPath = "",
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

. (Join-Path $PSScriptRoot "systemd_service_utils.ps1")
. (Join-Path $PSScriptRoot "v4_candidate_state_helpers.ps1")

function Resolve-DefaultRuntimeInstallScript {
    param([string]$Root)
    return (Join-Path $Root "scripts/install_server_runtime_services.ps1")
}

function Resolve-BatchDateValue {
    param([string]$DateText)
    if (-not [string]::IsNullOrWhiteSpace($DateText)) {
        return [DateTime]::ParseExact(
            $DateText,
            "yyyy-MM-dd",
            [System.Globalization.CultureInfo]::InvariantCulture,
            [System.Globalization.DateTimeStyles]::None
        ).ToString("yyyy-MM-dd")
    }
    return (Get-Date).Date.AddDays(-1).ToString("yyyy-MM-dd")
}

function Invoke-CommandCapture {
    param(
        [string]$Exe,
        [string[]]$ArgList,
        [switch]$AllowFailure
    )
    $output = & $Exe @ArgList 2>&1
    $exitCode = $LASTEXITCODE
    if ((-not $AllowFailure) -and $exitCode -ne 0) {
        throw ("command failed: " + $Exe + " " + ($ArgList -join " "))
    }
    return [PSCustomObject]@{
        ExitCode = [int]$exitCode
        Output = [string]($output -join [Environment]::NewLine)
        Command = ($Exe + " " + (($ArgList | ForEach-Object { Quote-ShellArg ([string]$_) }) -join " "))
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
        $Payload
    )
    if ([string]::IsNullOrWhiteSpace($PathValue)) {
        return
    }
    $parent = Split-Path -Path $PathValue -Parent
    if (-not [string]::IsNullOrWhiteSpace($parent)) {
        New-Item -ItemType Directory -Force -Path $parent | Out-Null
    }
    $json = $Payload | ConvertTo-Json -Depth 20
    Set-Content -Path $PathValue -Value $json -Encoding UTF8
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
    $propertyNames = @(
        $ObjectValue.PSObject.Properties |
            Where-Object { $null -ne $_ -and $null -ne $_.Name } |
            ForEach-Object { [string]$_.Name }
    )
    if ($propertyNames -contains $Name) {
        return $ObjectValue.$Name
    }
    return $DefaultValue
}

function To-Bool {
    param(
        [Parameter(Mandatory = $false)]$Value,
        [bool]$DefaultValue = $false
    )
    if ($null -eq $Value) {
        return $DefaultValue
    }
    if ($Value -is [bool]) {
        return [bool]$Value
    }
    $text = ([string]$Value).Trim()
    if ([string]::IsNullOrWhiteSpace($text)) {
        return $DefaultValue
    }
    switch -Regex ($text.ToLowerInvariant()) {
        "^(1|true|yes|on)$" { return $true }
        "^(0|false|no|off)$" { return $false }
        default { return $DefaultValue }
    }
}

function Resolve-RegistryPointerPath {
    param(
        [string]$RegistryRoot,
        [string]$Family,
        [string]$PointerName
    )
    return (Resolve-V4RegistryPointerPath -RegistryRoot $RegistryRoot -Family $Family -PointerName $PointerName)
}

function Update-LatestCandidatePointers {
    param(
        [string]$RegistryRoot,
        [string]$Family,
        [string]$RunId
    )
    return (Update-V4LatestCandidatePointers -RegistryRoot $RegistryRoot -Family $Family -RunId $RunId -DryRun:$DryRun)
}

function Resolve-ChampionRunId {
    param(
        [string]$Root,
        [string]$ModelFamilyName
    )
    $pointerPath = Join-Path (Join-Path (Join-Path $Root "models/registry") $ModelFamilyName) "champion.json"
    $pointer = Load-JsonOrEmpty -PathValue $pointerPath
    return [string](Get-PropValue -ObjectValue $pointer -Name "run_id" -DefaultValue "")
}

function Test-SystemdUnitActive {
    param([string]$UnitName)
    if ($DryRun) {
        return $false
    }
    $systemctl = Get-Command systemctl -ErrorAction SilentlyContinue
    if ($null -eq $systemctl) {
        return $false
    }
    & $systemctl.Source is-active --quiet $UnitName
    return ($LASTEXITCODE -eq 0)
}

function Restart-Unit {
    param([string]$UnitName)
    if ([string]::IsNullOrWhiteSpace($UnitName) -or $DryRun) {
        return
    }
    & sudo systemctl restart $UnitName
    if ($LASTEXITCODE -ne 0) {
        throw "failed to restart unit: $UnitName"
    }
}

function Update-RunArtifactStatus {
    param(
        [string]$RunDir,
        [string]$RunId,
        [string]$Status
    )
    return (
        Update-V4RunArtifactStatus `
            -RunDir $RunDir `
            -RunId $RunId `
            -Status $Status `
            -AcceptanceCompleted $true `
            -CandidateAdoptable $true `
            -CandidateAdopted $true `
            -DryRun:$DryRun
    )
}

$resolvedProjectRoot = if ([string]::IsNullOrWhiteSpace($ProjectRoot)) { Resolve-DefaultProjectRoot } else { $ProjectRoot }
$resolvedProjectRoot = [System.IO.Path]::GetFullPath($resolvedProjectRoot)
$resolvedPythonExe = if ([string]::IsNullOrWhiteSpace($PythonExe)) { Resolve-DefaultPythonExe -Root $resolvedProjectRoot } else { $PythonExe }
$resolvedRuntimeInstallScript = if ([string]::IsNullOrWhiteSpace($RuntimeInstallScript)) { Resolve-DefaultRuntimeInstallScript -Root $resolvedProjectRoot } else { $RuntimeInstallScript }
$resolvedModelFamily = [string]$ModelFamily
$resolvedModelFamily = $resolvedModelFamily.Trim()
if ([string]::IsNullOrWhiteSpace($resolvedModelFamily) -or (([string]::Equals($resolvedModelFamily, "train_v5_fusion", [System.StringComparison]::OrdinalIgnoreCase)) -and (-not (Test-Path (Join-Path $resolvedProjectRoot ("models/registry/" + $resolvedModelFamily)))))) {
    $resolvedModelFamily = Resolve-PreferredModelFamily -Root $resolvedProjectRoot -PreferredFamily "train_v5_fusion"
}
$resolvedChampionCompareModelFamily = [string]$ChampionCompareModelFamily
$resolvedChampionCompareModelFamily = $resolvedChampionCompareModelFamily.Trim()
if ([string]::IsNullOrWhiteSpace($resolvedChampionCompareModelFamily)) {
    $resolvedChampionCompareModelFamily = $resolvedModelFamily
}
$resolvedPairedPaperModelFamily = [string]$PairedPaperModelFamily
$resolvedPairedPaperModelFamily = $resolvedPairedPaperModelFamily.Trim()
if ([string]::IsNullOrWhiteSpace($resolvedPairedPaperModelFamily)) {
    $resolvedPairedPaperModelFamily = $resolvedModelFamily
}
$resolvedBatchDate = Resolve-BatchDateValue -DateText $BatchDate
$resolvedCandidateRunId = [string]$CandidateRunId
$resolvedCandidateRunId = $resolvedCandidateRunId.Trim()
$resolvedCandidateTargetUnits = @(Expand-DelimitedStringArray -Value $CandidateTargetUnits)
$resolvedPromotionTargetUnits = @(Expand-DelimitedStringArray -Value $PromotionTargetUnits)
$registryRoot = Join-Path $resolvedProjectRoot "models/registry"
$stateRoot = Join-Path $resolvedProjectRoot $StateRootRelPath
$statePath = Join-Path $stateRoot "current_state.json"
$reportPath = Join-Path $stateRoot ("candidate_adoption_" + (Get-Date -Format "yyyyMMdd-HHmmss") + "_" + $resolvedCandidateRunId + ".json")
$latestReportPath = Join-Path $stateRoot "latest_candidate_adoption.json"
$candidateRunDir = if ([string]::IsNullOrWhiteSpace($resolvedCandidateRunId)) { "" } else { Join-Path (Join-Path $registryRoot $resolvedModelFamily) $resolvedCandidateRunId }
$report = [ordered]@{
    batch_date = $resolvedBatchDate
    candidate_run_id = $resolvedCandidateRunId
    candidate_run_dir = $candidateRunDir
    model_family = $resolvedModelFamily
    champion_compare_model_family = $resolvedChampionCompareModelFamily
    paired_paper_unit = $PairedPaperUnitName
    paired_paper_model_family = $resolvedPairedPaperModelFamily
    candidate_target_units = @($resolvedCandidateTargetUnits)
    promotion_target_units = @($resolvedPromotionTargetUnits)
    lane_mode = if ([string]::IsNullOrWhiteSpace($LaneMode)) { "promotion_strict" } else { $LaneMode }
    promotion_eligible = [bool]$PromotionEligible
    bootstrap_only = [bool]$BootstrapOnly
    split_policy_id = [string]$SplitPolicyId
    split_policy_artifact_path = [string]$SplitPolicyArtifactPath
    started_at_utc = (Get-Date).ToUniversalTime().ToString("o")
    steps = [ordered]@{}
}

try {
    if ([string]::IsNullOrWhiteSpace($resolvedCandidateRunId)) {
        throw "candidate_run_id is required"
    }
    if (-not (Test-Path $candidateRunDir)) {
        throw ("candidate run dir not found: " + $candidateRunDir)
    }

    $artifactStatusPath = Join-Path $candidateRunDir "artifact_status.json"
    $artifactStatus = Load-JsonOrEmpty -PathValue $artifactStatusPath
    foreach ($requiredField in @(
        "core_saved",
        "support_artifacts_written",
        "execution_acceptance_complete",
        "runtime_recommendations_complete",
        "governance_artifacts_complete",
        "acceptance_completed"
    )) {
        if (-not (To-Bool (Get-PropValue -ObjectValue $artifactStatus -Name $requiredField -DefaultValue $false) $false)) {
            throw ("candidate adoption blocked by artifact_status field: " + $requiredField)
        }
    }

    $pointerUpdate = Update-LatestCandidatePointers -RegistryRoot $registryRoot -Family $resolvedModelFamily -RunId $resolvedCandidateRunId
    $report.steps.update_latest_candidate = [ordered]@{
        attempted = $true
        candidate_run_id = $resolvedCandidateRunId
        model_family = $resolvedModelFamily
        family_path = [string](Get-PropValue -ObjectValue $pointerUpdate -Name "family_path" -DefaultValue "")
        global_path = [string](Get-PropValue -ObjectValue $pointerUpdate -Name "global_path" -DefaultValue "")
        updated_at_utc = [string](Get-PropValue -ObjectValue $pointerUpdate -Name "updated_at_utc" -DefaultValue "")
    }

    $psExe = Resolve-PwshExe
    $installArgs = @(
        "-NoProfile",
        "-ExecutionPolicy", "Bypass",
        "-File", $resolvedRuntimeInstallScript,
        "-ProjectRoot", $resolvedProjectRoot,
        "-PythonExe", $resolvedPythonExe,
        "-PaperUnitName", $PairedPaperUnitName,
        "-PaperPreset", "paired_v5",
        "-PaperRuntimeRole", "paired",
        "-PaperLaneName", "v4",
        "-PaperModelFamilyOverride", $resolvedPairedPaperModelFamily,
        "-PaperChampionModelFamilyOverride", $resolvedChampionCompareModelFamily,
        "-PaperChallengerModelFamilyOverride", $resolvedPairedPaperModelFamily,
        "-PairedStateRootRelPath", $StateRootRelPath,
        "-NoBootstrapChampion"
    )
    if ($DryRun) {
        $installArgs += "-DryRun"
    }
    $installExec = Invoke-CommandCapture -Exe $psExe -ArgList $installArgs
    $report.steps.start_paired_paper = [ordered]@{
        attempted = $true
        command = $installExec.Command
        output_preview = $installExec.Output
        unit_name = $PairedPaperUnitName
        candidate_run_id = $resolvedCandidateRunId
        model_family = $resolvedPairedPaperModelFamily
        champion_model_family = $resolvedChampionCompareModelFamily
        challenger_model_family = $resolvedPairedPaperModelFamily
        lane_mode = $report.lane_mode
        promotion_eligible = [bool]$report.promotion_eligible
        bootstrap_only = [bool]$report.bootstrap_only
    }

    $championRunIdAtStart = Resolve-ChampionRunId -Root $resolvedProjectRoot -ModelFamilyName $resolvedChampionCompareModelFamily
    $nextState = [ordered]@{
        batch_date = $resolvedBatchDate
        candidate_run_id = $resolvedCandidateRunId
        champion_ref_at_start = "champion"
        champion_run_id_at_start = $championRunIdAtStart
        started_ts_ms = [int64](Get-Date -UFormat %s) * 1000
        started_at_utc = (Get-Date).ToUniversalTime().ToString("o")
        model_family = $resolvedModelFamily
        candidate_model_family = $resolvedModelFamily
        champion_model_family_at_start = $resolvedChampionCompareModelFamily
        champion_compare_model_family = $resolvedChampionCompareModelFamily
        paired_paper_unit = $PairedPaperUnitName
        promotion_target_units = @($resolvedPromotionTargetUnits)
        lane_mode = $report.lane_mode
        promotion_eligible = [bool]$report.promotion_eligible
        bootstrap_only = [bool]$report.bootstrap_only
        split_policy_id = [string]$report.split_policy_id
        split_policy_artifact_path = [string]$report.split_policy_artifact_path
    }
    if (-not $DryRun) {
        Write-JsonFile -PathValue $statePath -Payload $nextState
    }
    $report.current_state = $nextState
    $report.current_state_path = $statePath

    $restartedCandidateUnits = @()
    $startedFromInactiveUnits = @()
    if ($resolvedCandidateTargetUnits.Count -gt 0) {
        foreach ($unit in $resolvedCandidateTargetUnits) {
            $trimmedUnit = [string]$unit
            if ([string]::IsNullOrWhiteSpace($trimmedUnit)) {
                continue
            }
            $trimmedUnit = $trimmedUnit.Trim()
            $targetWasActive = Test-SystemdUnitActive -UnitName $trimmedUnit
            Restart-Unit -UnitName $trimmedUnit
            $restartedCandidateUnits += $trimmedUnit
            if (-not $targetWasActive) {
                $startedFromInactiveUnits += $trimmedUnit
            }
        }
        $report.steps.restart_candidate_targets = [ordered]@{
            attempted = $true
            candidate_run_id = $resolvedCandidateRunId
            restarted_units = @($restartedCandidateUnits)
            started_from_inactive_units = @($startedFromInactiveUnits)
            skipped_units = @()
        }
    } else {
        $report.steps.restart_candidate_targets = [ordered]@{
            attempted = $false
            candidate_run_id = $resolvedCandidateRunId
            reason = "NO_CANDIDATE_TARGET_UNITS"
        }
    }
    $report.steps.start_challenger = [ordered]@{
        attempted = $false
        reason = "REPLACED_BY_PAIRED_PAPER"
        candidate_run_id = $resolvedCandidateRunId
    }

    $artifactStatusUpdate = Update-RunArtifactStatus -RunDir $candidateRunDir -RunId $resolvedCandidateRunId -Status "candidate_adopted"
    $report.steps.update_artifact_status = [ordered]@{
        attempted = (-not $DryRun)
        path = [string](Get-PropValue -ObjectValue $artifactStatusUpdate -Name "path" -DefaultValue "")
    }
    $report.completed_at_utc = (Get-Date).ToUniversalTime().ToString("o")

    if (-not $DryRun) {
        Write-JsonFile -PathValue $reportPath -Payload $report
        Write-JsonFile -PathValue $latestReportPath -Payload $report
    }

    Write-Host ("[candidate-adopt] candidate_run_id={0}" -f $resolvedCandidateRunId)
    Write-Host ("[candidate-adopt] report={0}" -f $reportPath)
    Write-Host ("[candidate-adopt] latest={0}" -f $latestReportPath)
    exit 0
} catch {
    $report.exception = [ordered]@{
        message = $_.Exception.Message
    }
    $report.completed_at_utc = (Get-Date).ToUniversalTime().ToString("o")
    if (-not $DryRun) {
        Write-JsonFile -PathValue $reportPath -Payload $report
        Write-JsonFile -PathValue $latestReportPath -Payload $report
    }
    Write-Host ("[candidate-adopt][error] candidate_run_id={0}" -f $resolvedCandidateRunId)
    Write-Host ("[candidate-adopt][error] reason={0}" -f $_.Exception.Message)
    Write-Host ("[candidate-adopt] report={0}" -f $reportPath)
    Write-Host ("[candidate-adopt] latest={0}" -f $latestReportPath)
    exit 2
}
