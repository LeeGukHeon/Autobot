param(
    [string]$ProjectRoot = "",
    [string]$PythonExe = "",
    [string]$ModelFamily = "train_v4_crypto_cs",
    [string]$ChampionPointerFamily = "",
    [string[]]$RequiredUnitFiles = @(),
    [string[]]$BlockOnFailedUnits = @(),
    [string[]]$ExpectedUnitStates = @(),
    [string[]]$RequiredStateDbPaths = @(),
    [string[]]$RequiredPointers = @(),
    [switch]$CheckCandidateStateConsistency,
    [switch]$FailOnDirtyWorktree
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

. (Join-Path $PSScriptRoot "systemd_service_utils.ps1")

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
        [Parameter(Mandatory = $false)]$Payload,
        [int]$Depth = 12
    )
    if ([string]::IsNullOrWhiteSpace($PathValue)) {
        return
    }
    $parent = Split-Path -Parent $PathValue
    if (-not [string]::IsNullOrWhiteSpace($parent)) {
        New-Item -ItemType Directory -Path $parent -Force | Out-Null
    }
    ($Payload | ConvertTo-Json -Depth $Depth) | Set-Content -Path $PathValue -Encoding UTF8
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

function Get-StringArray {
    param([Parameter(Mandatory = $false)]$Value)
    return @(Expand-DelimitedStringArray -Value $Value)
}

function Test-ObjectHasValues {
    param([Parameter(Mandatory = $false)]$ObjectValue)
    if ($null -eq $ObjectValue) {
        return $false
    }
    if ($ObjectValue -is [System.Collections.IDictionary]) {
        return ($ObjectValue.Count -gt 0)
    }
    if ($ObjectValue.PSObject) {
        return (@($ObjectValue.PSObject.Properties).Count -gt 0)
    }
    return $true
}

function Invoke-ProcessCapture {
    param(
        [string]$Exe,
        [string[]]$ArgList,
        [string]$WorkingDirectory = ""
    )
    $commandText = $Exe + " " + (($ArgList | ForEach-Object { Quote-ShellArg ([string]$_) }) -join " ")
    $startInfo = New-Object System.Diagnostics.ProcessStartInfo
    $startInfo.FileName = $Exe
    $escapedArgs = @()
    foreach ($arg in @($ArgList)) {
        $escapedArgs += ('"' + ([string]$arg).Replace('"', '\"') + '"')
    }
    $startInfo.Arguments = ($escapedArgs -join " ")
    if (-not [string]::IsNullOrWhiteSpace($WorkingDirectory)) {
        $startInfo.WorkingDirectory = $WorkingDirectory
    }
    $startInfo.UseShellExecute = $false
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true
    $process = New-Object System.Diagnostics.Process
    $process.StartInfo = $startInfo
    [void]$process.Start()
    $stdout = $process.StandardOutput.ReadToEnd()
    $stderr = $process.StandardError.ReadToEnd()
    $process.WaitForExit()
    $exitCode = [int]$process.ExitCode
    $combined = @()
    if (-not [string]::IsNullOrWhiteSpace($stdout)) {
        $combined += $stdout.TrimEnd()
    }
    if (-not [string]::IsNullOrWhiteSpace($stderr)) {
        $combined += $stderr.TrimEnd()
    }
    return [PSCustomObject]@{
        ExitCode = $exitCode
        Output = ($combined -join "`n")
        Command = $commandText
        WorkingDirectory = $WorkingDirectory
    }
}

function Resolve-PreflightPythonExe {
    param(
        [string]$Root,
        [string]$ConfiguredPythonExe
    )
    if (-not [string]::IsNullOrWhiteSpace($ConfiguredPythonExe)) {
        return $ConfiguredPythonExe
    }
    $defaultPythonExe = Resolve-DefaultPythonExe -Root $Root
    if (-not [string]::IsNullOrWhiteSpace($defaultPythonExe) -and (Test-Path $defaultPythonExe)) {
        return $defaultPythonExe
    }
    $repoDefaultRoot = Resolve-DefaultProjectRoot
    if (-not [string]::IsNullOrWhiteSpace($repoDefaultRoot)) {
        $repoPythonExe = Resolve-DefaultPythonExe -Root $repoDefaultRoot
        if (-not [string]::IsNullOrWhiteSpace($repoPythonExe) -and (Test-Path $repoPythonExe)) {
            return $repoPythonExe
        }
    }
    foreach ($commandName in @("python", "python3")) {
        $pythonCmd = Get-Command $commandName -ErrorAction SilentlyContinue
        if ($null -ne $pythonCmd -and -not [string]::IsNullOrWhiteSpace($pythonCmd.Source)) {
            return [string]$pythonCmd.Source
        }
    }
    return ""
}

function Invoke-ArtifactReportCapture {
    param(
        [string]$PythonPath,
        [string]$Root,
        [string]$ModuleName,
        [string[]]$ExtraArgs,
        [string]$ExpectedReportPath
    )
    if ([string]::IsNullOrWhiteSpace($PythonPath)) {
        return [PSCustomObject]@{
            ExitCode = 127
            Output = "python executable unavailable"
            Command = ""
            ReportPath = $ExpectedReportPath
            Report = @{}
        }
    }
    $argList = @(
        "-m", $ModuleName,
        "--project-root", $Root
    )
    if ($null -ne $ExtraArgs) {
        $argList += @($ExtraArgs)
    }
    $moduleWorkingDirectory = Resolve-DefaultProjectRoot
    $exec = Invoke-ProcessCapture -Exe $PythonPath -ArgList $argList -WorkingDirectory $moduleWorkingDirectory
    $reportDoc = Load-JsonOrEmpty -PathValue $ExpectedReportPath
    return [PSCustomObject]@{
        ExitCode = [int]$exec.ExitCode
        Output = [string]$exec.Output
        Command = [string]$exec.Command
        ReportPath = $ExpectedReportPath
        Report = $reportDoc
    }
}

function Parse-SystemdUnitRows {
    param([string]$TextValue)
    $rows = @()
    foreach ($rawLine in @([string]$TextValue -split "\r?\n")) {
        $line = [string]$rawLine
        if ([string]::IsNullOrWhiteSpace($line)) {
            continue
        }
        $parts = $line.Trim() -split "\s+", 5
        if ($parts.Count -lt 4) {
            $rows += [ordered]@{ raw = $line.Trim() }
            continue
        }
        $rows += [ordered]@{
            unit = $parts[0]
            load = $parts[1]
            active = $parts[2]
            sub = $parts[3]
            description = if ($parts.Count -ge 5) { $parts[4] } else { "" }
        }
    }
    return @($rows)
}

function Parse-SystemdUnitFileRows {
    param([string]$TextValue)
    $rows = @()
    foreach ($rawLine in @([string]$TextValue -split "\r?\n")) {
        $line = [string]$rawLine
        if ([string]::IsNullOrWhiteSpace($line)) {
            continue
        }
        $parts = $line.Trim() -split "\s+", 3
        if ($parts.Count -lt 2) {
            $rows += [ordered]@{ raw = $line.Trim() }
            continue
        }
        $rows += [ordered]@{
            unit_file = $parts[0]
            state = $parts[1]
            preset = if ($parts.Count -ge 3) { $parts[2] } else { "" }
        }
    }
    return @($rows)
}

function Parse-ExpectedUnitStateSpec {
    param([string]$Spec)
    $text = [string]$Spec
    if ([string]::IsNullOrWhiteSpace($text)) {
        return [ordered]@{
            valid = $false
            raw = $Spec
            unit = ""
            expected_state = ""
        }
    }
    $parts = $text.Split("=", 2)
    if ($parts.Count -ne 2) {
        return [ordered]@{
            valid = $false
            raw = $Spec
            unit = ""
            expected_state = ""
        }
    }
    $unitName = [string]$parts[0]
    $expectedState = [string]$parts[1]
    if ([string]::IsNullOrWhiteSpace($unitName) -or [string]::IsNullOrWhiteSpace($expectedState)) {
        return [ordered]@{
            valid = $false
            raw = $Spec
            unit = ""
            expected_state = ""
        }
    }
    return [ordered]@{
        valid = $true
        raw = $Spec
        unit = $unitName.Trim()
        expected_state = $expectedState.Trim().ToLowerInvariant()
    }
}

function Add-Check {
    param(
        [System.Collections.Generic.List[object]]$Checks,
        [string]$Code,
        [string]$Status,
        [string]$Message,
        [Parameter(Mandatory = $false)]$Evidence = $null
    )
    if ($null -eq $Checks) {
        return
    }
    $Checks.Add([ordered]@{
        code = $Code
        status = $Status
        message = $Message
        evidence = if ($null -eq $Evidence) { @{} } else { $Evidence }
    }) | Out-Null
}

function Load-PointerSnapshot {
    param(
        [string]$RegistryRoot,
        [string]$ModelFamily,
        [string]$PointerName
    )
    if ([string]::IsNullOrWhiteSpace($PointerName)) {
        return @{}
    }
    $isGlobal = $PointerName.StartsWith("global_")
    $effectivePointerName = if ($isGlobal) { $PointerName.Substring(7) } else { $PointerName }
    $pathValue = if ($isGlobal) {
        Join-Path $RegistryRoot ($effectivePointerName + ".json")
    } else {
        Join-Path (Join-Path $RegistryRoot $ModelFamily) ($effectivePointerName + ".json")
    }
    $payload = Load-JsonOrEmpty -PathValue $pathValue
    $runId = [string](Get-PropValue -ObjectValue $payload -Name "run_id" -DefaultValue "")
    $resolvedFamily = if ($isGlobal) {
        [string](Get-PropValue -ObjectValue $payload -Name "model_family" -DefaultValue "")
    } else {
        $ModelFamily
    }
    $runDir = ""
    if ((-not [string]::IsNullOrWhiteSpace($runId)) -and (-not [string]::IsNullOrWhiteSpace($resolvedFamily))) {
        $runDir = Join-Path (Join-Path $RegistryRoot $resolvedFamily) $runId
    }
    return [ordered]@{
        pointer_name = $PointerName
        path = $pathValue
        exists = (Test-Path $pathValue)
        run_id = if ([string]::IsNullOrWhiteSpace($runId)) { $null } else { $runId }
        resolved_family = if ([string]::IsNullOrWhiteSpace($resolvedFamily)) { $null } else { $resolvedFamily }
        run_dir = if ([string]::IsNullOrWhiteSpace($runDir)) { $null } else { $runDir }
        run_dir_exists = if ([string]::IsNullOrWhiteSpace($runDir)) { $false } else { (Test-Path $runDir) }
        payload = $payload
    }
}

function Summarize-Checks {
    param([object[]]$Checks)
    $violationCodes = @(
        $Checks |
            Where-Object { [string](Get-PropValue -ObjectValue $_ -Name "status" -DefaultValue "") -eq "violation" } |
            ForEach-Object { [string](Get-PropValue -ObjectValue $_ -Name "code" -DefaultValue "") }
    )
    $warningCodes = @(
        $Checks |
            Where-Object { [string](Get-PropValue -ObjectValue $_ -Name "status" -DefaultValue "") -eq "warning" } |
            ForEach-Object { [string](Get-PropValue -ObjectValue $_ -Name "code" -DefaultValue "") }
    )
    $passCount = @(
        $Checks |
            Where-Object { [string](Get-PropValue -ObjectValue $_ -Name "status" -DefaultValue "") -eq "pass" }
    ).Count
    return [ordered]@{
        status = if ($violationCodes.Count -gt 0) { "violation" } elseif ($warningCodes.Count -gt 0) { "warning" } else { "healthy" }
        pass_count = [int]$passCount
        warning_count = [int]$warningCodes.Count
        violation_count = [int]$violationCodes.Count
        warning_codes = @($warningCodes)
        violation_codes = @($violationCodes)
        reason_codes = @($violationCodes + $warningCodes)
    }
}

$resolvedProjectRoot = if ([string]::IsNullOrWhiteSpace($ProjectRoot)) { Resolve-DefaultProjectRoot } else { $ProjectRoot }
$resolvedProjectRoot = [System.IO.Path]::GetFullPath($resolvedProjectRoot)
$resolvedModelFamily = if ([string]::IsNullOrWhiteSpace($ModelFamily)) { "train_v4_crypto_cs" } else { $ModelFamily.Trim() }
$resolvedChampionPointerFamily = if ([string]::IsNullOrWhiteSpace($ChampionPointerFamily)) { $resolvedModelFamily } else { $ChampionPointerFamily.Trim() }
$resolvedRequiredUnitFiles = @(Get-StringArray -Value $RequiredUnitFiles)
$resolvedBlockOnFailedUnits = @(Get-StringArray -Value $BlockOnFailedUnits)
$resolvedExpectedUnitStates = @(Get-StringArray -Value $ExpectedUnitStates)
$resolvedRequiredStateDbPaths = @(Get-StringArray -Value $RequiredStateDbPaths)
$resolvedRequiredPointers = @(Get-StringArray -Value $RequiredPointers)
$resolvedPythonExe = Resolve-PreflightPythonExe -Root $resolvedProjectRoot -ConfiguredPythonExe $PythonExe
$registryRoot = Join-Path $resolvedProjectRoot "models/registry"
$reportPath = Join-Path $resolvedProjectRoot "logs/ops/server_preflight/latest.json"
$checks = New-Object System.Collections.Generic.List[object]
$exitCode = 0

$gitSnapshot = [ordered]@{
    available = $false
    head = ""
    status_short = @()
    dirty = $false
}
$gitCmd = Get-Command git -ErrorAction SilentlyContinue
if ($null -ne $gitCmd) {
    $headExec = Invoke-ProcessCapture -Exe $gitCmd.Source -ArgList @("-C", $resolvedProjectRoot, "rev-parse", "HEAD") -WorkingDirectory $resolvedProjectRoot
    if ($headExec.ExitCode -eq 0) {
        $statusExec = Invoke-ProcessCapture -Exe $gitCmd.Source -ArgList @("-C", $resolvedProjectRoot, "status", "--short") -WorkingDirectory $resolvedProjectRoot
        $statusLines = @()
        foreach ($line in @([string]$statusExec.Output -split "\r?\n")) {
            if (-not [string]::IsNullOrWhiteSpace($line)) {
                $statusLines += $line.TrimEnd()
            }
        }
        $gitSnapshot.available = $true
        $gitSnapshot.head = ([string]$headExec.Output).Trim()
        $gitSnapshot.status_short = @($statusLines)
        $gitSnapshot.dirty = ($statusLines.Count -gt 0)
    }
}
if ($FailOnDirtyWorktree -and [bool]$gitSnapshot.available -and [bool]$gitSnapshot.dirty) {
    Add-Check -Checks $checks -Code "DIRTY_WORKTREE" -Status "violation" -Message "git worktree is dirty before batch execution." -Evidence ([ordered]@{
        status_short = @($gitSnapshot.status_short)
        head = [string]$gitSnapshot.head
    })
} else {
    Add-Check -Checks $checks -Code "WORKTREE_ACCEPTABLE" -Status "pass" -Message "git worktree is acceptable for this preflight." -Evidence ([ordered]@{
        git_available = [bool]$gitSnapshot.available
        dirty = [bool]$gitSnapshot.dirty
    })
}

$runtimeTopologyCapture = Invoke-ArtifactReportCapture `
    -PythonPath $resolvedPythonExe `
    -Root $resolvedProjectRoot `
    -ModuleName "autobot.ops.runtime_topology_report" `
    -ExtraArgs @() `
    -ExpectedReportPath (Join-Path $resolvedProjectRoot "logs/runtime_topology/latest.json")
$runtimeTopologySummary = Get-PropValue -ObjectValue $runtimeTopologyCapture.Report -Name "summary" -DefaultValue @{}
if (($runtimeTopologyCapture.ExitCode -ne 0) -or (-not (Test-Path $runtimeTopologyCapture.ReportPath)) -or (-not (Test-ObjectHasValues -ObjectValue $runtimeTopologyCapture.Report))) {
    Add-Check -Checks $checks -Code "RUNTIME_TOPOLOGY_REPORT_FAILED" -Status "violation" -Message "runtime topology report refresh failed during preflight." -Evidence ([ordered]@{
        exit_code = [int]$runtimeTopologyCapture.ExitCode
        report_path = [string]$runtimeTopologyCapture.ReportPath
        command = [string]$runtimeTopologyCapture.Command
    })
} else {
    Add-Check -Checks $checks -Code "RUNTIME_TOPOLOGY_REPORT_REFRESHED" -Status "pass" -Message "runtime topology report refreshed during preflight." -Evidence ([ordered]@{
        report_path = [string]$runtimeTopologyCapture.ReportPath
        command = [string]$runtimeTopologyCapture.Command
        champion_run_id = [string](Get-PropValue -ObjectValue $runtimeTopologySummary -Name "champion_run_id" -DefaultValue "")
        latest_candidate_run_id = [string](Get-PropValue -ObjectValue $runtimeTopologySummary -Name "latest_candidate_run_id" -DefaultValue "")
    })
}
$runtimeTopologyHealthStatus = [string](Get-PropValue -ObjectValue $runtimeTopologySummary -Name "topology_health_status" -DefaultValue "")
$runtimeTopologyHealthReasons = @((Get-PropValue -ObjectValue $runtimeTopologySummary -Name "topology_health_reason_codes" -DefaultValue @()))
$blockingRuntimeTopologyReasons = @()
foreach ($reasonCode in @($runtimeTopologyHealthReasons)) {
    $code = [string]$reasonCode
    if ([string]::IsNullOrWhiteSpace($code)) {
        continue
    }
    if (($code -eq "SYSTEMD_UNAVAILABLE") -or ($code -eq "WS_PUBLIC_STALE")) {
        continue
    }
    if (($code -eq "LIVE_DB_MISSING") -and (-not (@($resolvedRequiredStateDbPaths) | Where-Object { ([string]$_ -match "live_state\.db$") -and (-not ([string]$_ -match "live_candidate")) }))) {
        continue
    }
    if (($code -eq "CANDIDATE_DB_MISSING") -and (-not (@($resolvedRequiredStateDbPaths) | Where-Object { [string]$_ -match "live_candidate.+live_state\.db$" }))) {
        continue
    }
    if (($code -eq "SYSTEMD_UNAVAILABLE") -and (($resolvedRequiredUnitFiles.Count -eq 0) -and ($resolvedBlockOnFailedUnits.Count -eq 0) -and ($resolvedExpectedUnitStates.Count -eq 0))) {
        continue
    }
    $blockingRuntimeTopologyReasons += $code
}
if (($runtimeTopologyHealthStatus -eq "violation") -and ($blockingRuntimeTopologyReasons.Count -gt 0)) {
    Add-Check -Checks $checks -Code "RUNTIME_TOPOLOGY_HEALTH_VIOLATION" -Status "violation" -Message "runtime topology report summary is in violation state." -Evidence ([ordered]@{
        status = $runtimeTopologyHealthStatus
        reason_codes = @($blockingRuntimeTopologyReasons)
    })
} elseif (($runtimeTopologyHealthStatus -eq "degraded") -and ($blockingRuntimeTopologyReasons.Count -gt 0)) {
    Add-Check -Checks $checks -Code "RUNTIME_TOPOLOGY_HEALTH_DEGRADED" -Status "warning" -Message "runtime topology report summary is degraded." -Evidence ([ordered]@{
        status = $runtimeTopologyHealthStatus
        reason_codes = @($blockingRuntimeTopologyReasons)
    })
}

$pointerConsistencyCapture = Invoke-ArtifactReportCapture `
    -PythonPath $resolvedPythonExe `
    -Root $resolvedProjectRoot `
    -ModuleName "autobot.ops.pointer_consistency_report" `
    -ExtraArgs @("--model-family", $resolvedModelFamily, "--champion-pointer-family", $resolvedChampionPointerFamily) `
    -ExpectedReportPath (Join-Path $resolvedProjectRoot "logs/ops/pointer_consistency/latest.json")
$pointerConsistencySummary = Get-PropValue -ObjectValue $pointerConsistencyCapture.Report -Name "summary" -DefaultValue @{}
if (($pointerConsistencyCapture.ExitCode -ne 0) -or (-not (Test-Path $pointerConsistencyCapture.ReportPath)) -or (-not (Test-ObjectHasValues -ObjectValue $pointerConsistencyCapture.Report))) {
    Add-Check -Checks $checks -Code "POINTER_CONSISTENCY_REPORT_FAILED" -Status "violation" -Message "pointer consistency report refresh failed during preflight." -Evidence ([ordered]@{
        exit_code = [int]$pointerConsistencyCapture.ExitCode
        report_path = [string]$pointerConsistencyCapture.ReportPath
        command = [string]$pointerConsistencyCapture.Command
    })
} else {
    Add-Check -Checks $checks -Code "POINTER_CONSISTENCY_REPORT_REFRESHED" -Status "pass" -Message "pointer consistency report refreshed during preflight." -Evidence ([ordered]@{
        report_path = [string]$pointerConsistencyCapture.ReportPath
        command = [string]$pointerConsistencyCapture.Command
        status = [string](Get-PropValue -ObjectValue $pointerConsistencySummary -Name "status" -DefaultValue "")
        violation_count = [int](Get-PropValue -ObjectValue $pointerConsistencySummary -Name "violation_count" -DefaultValue 0)
        warning_count = [int](Get-PropValue -ObjectValue $pointerConsistencySummary -Name "warning_count" -DefaultValue 0)
    })
}
$pointerConsistencyStatus = [string](Get-PropValue -ObjectValue $pointerConsistencySummary -Name "status" -DefaultValue "")
$pointerConsistencyReasons = @((Get-PropValue -ObjectValue $pointerConsistencySummary -Name "reason_codes" -DefaultValue @()))
$blockingPointerConsistencyReasons = @()
foreach ($reasonCode in @($pointerConsistencyReasons)) {
    $code = [string]$reasonCode
    if ([string]::IsNullOrWhiteSpace($code)) {
        continue
    }
    $unitContractChecksRequested = ($resolvedRequiredUnitFiles.Count -gt 0) -or ($resolvedBlockOnFailedUnits.Count -gt 0) -or ($resolvedExpectedUnitStates.Count -gt 0)
    if ($code.StartsWith("CURRENT_STATE_") -and (-not $CheckCandidateStateConsistency)) {
        continue
    }
    if ((@("CANDIDATE_UNITS_ACTIVE_WITHOUT_LATEST_CANDIDATE", "LATEST_CANDIDATE_WITH_NO_ACTIVE_CANDIDATE_LANE") -contains $code) -and (-not $unitContractChecksRequested)) {
        continue
    }
    if (($code.StartsWith("LATEST_CANDIDATE_") -or $code.StartsWith("CANDIDATE_") -or $code.StartsWith("CHAMPION_EQUALS_LATEST_CANDIDATE")) -and (-not $CheckCandidateStateConsistency) -and (-not (@($resolvedRequiredPointers) -contains "latest_candidate"))) {
        continue
    }
    if ($code.StartsWith("LATEST_") -and (-not $code.StartsWith("LATEST_CANDIDATE_")) -and (-not (@($resolvedRequiredPointers) -contains "latest"))) {
        continue
    }
    if ($code.StartsWith("CHAMPION_") -and ($resolvedChampionPointerFamily -ne $resolvedModelFamily)) {
        continue
    }
    if ($code.StartsWith("CHAMPION_") -and (-not (@($resolvedRequiredPointers) -contains "champion")) -and (-not $CheckCandidateStateConsistency)) {
        continue
    }
    $blockingPointerConsistencyReasons += $code
}
if (($pointerConsistencyStatus -eq "violation") -and ($blockingPointerConsistencyReasons.Count -gt 0)) {
    Add-Check -Checks $checks -Code "POINTER_CONSISTENCY_VIOLATION" -Status "violation" -Message "pointer consistency report summary is in violation state." -Evidence ([ordered]@{
        status = $pointerConsistencyStatus
        reason_codes = @($blockingPointerConsistencyReasons)
    })
} elseif (($pointerConsistencyStatus -eq "warning") -and ($blockingPointerConsistencyReasons.Count -gt 0)) {
    Add-Check -Checks $checks -Code "POINTER_CONSISTENCY_WARNING" -Status "warning" -Message "pointer consistency report summary is warning-only." -Evidence ([ordered]@{
        status = $pointerConsistencyStatus
        reason_codes = @($blockingPointerConsistencyReasons)
    })
}

$systemdSnapshot = [ordered]@{
    available = $false
    services = @()
    unit_files = @()
}
if (($resolvedRequiredUnitFiles.Count -gt 0) -or ($resolvedBlockOnFailedUnits.Count -gt 0) -or ($resolvedExpectedUnitStates.Count -gt 0)) {
    $systemctlCmd = Get-Command systemctl -ErrorAction SilentlyContinue
    if ($null -eq $systemctlCmd) {
        Add-Check -Checks $checks -Code "SYSTEMCTL_UNAVAILABLE" -Status "violation" -Message "systemctl is unavailable while unit preflight checks are required."
    } else {
        $servicesExec = Invoke-ProcessCapture -Exe $systemctlCmd.Source -ArgList @("list-units", "autobot*", "--type=service", "--all", "--no-pager", "--plain", "--no-legend")
        $unitFilesExec = Invoke-ProcessCapture -Exe $systemctlCmd.Source -ArgList @("list-unit-files", "autobot*", "--no-pager", "--plain", "--no-legend")
        $systemdSnapshot.available = ($servicesExec.ExitCode -eq 0) -or ($unitFilesExec.ExitCode -eq 0)
        $systemdSnapshot.services = @(Parse-SystemdUnitRows -TextValue ([string]$servicesExec.Output))
        $systemdSnapshot.unit_files = @(Parse-SystemdUnitFileRows -TextValue ([string]$unitFilesExec.Output))
        if (-not $systemdSnapshot.available) {
            Add-Check -Checks $checks -Code "SYSTEMD_SNAPSHOT_FAILED" -Status "violation" -Message "systemd unit snapshot could not be collected."
        }
    }
}
if ($resolvedExpectedUnitStates.Count -gt 0) {
    foreach ($spec in $resolvedExpectedUnitStates) {
        $parsedSpec = Parse-ExpectedUnitStateSpec -Spec $spec
        if (-not [bool](Get-PropValue -ObjectValue $parsedSpec -Name "valid" -DefaultValue $false)) {
            Add-Check -Checks $checks -Code "EXPECTED_UNIT_STATE_SPEC_INVALID" -Status "violation" -Message ("invalid expected unit state spec: " + [string]$spec) -Evidence ([ordered]@{
                raw = [string]$spec
            })
            continue
        }
        $unitName = [string](Get-PropValue -ObjectValue $parsedSpec -Name "unit" -DefaultValue "")
        $expectedState = [string](Get-PropValue -ObjectValue $parsedSpec -Name "expected_state" -DefaultValue "")
        $match = @($systemdSnapshot.unit_files | Where-Object { [string](Get-PropValue -ObjectValue $_ -Name "unit_file" -DefaultValue "") -eq $unitName })
        if ($match.Count -eq 0) {
            Add-Check -Checks $checks -Code "EXPECTED_UNIT_STATE_UNIT_MISSING" -Status "violation" -Message ("expected unit state cannot be verified because unit file is missing: " + $unitName) -Evidence ([ordered]@{
                unit = $unitName
                expected_state = $expectedState
            })
            continue
        }
        $actualState = [string](Get-PropValue -ObjectValue $match[0] -Name "state" -DefaultValue "")
        if ($actualState.Trim().ToLowerInvariant() -ne $expectedState) {
            Add-Check -Checks $checks -Code "UNIT_FILE_STATE_MISMATCH" -Status "violation" -Message ("unit file state mismatch for {0}: expected {1}, got {2}" -f $unitName, $expectedState, $actualState) -Evidence ([ordered]@{
                unit = $unitName
                expected_state = $expectedState
                actual_state = $actualState
                preset = [string](Get-PropValue -ObjectValue $match[0] -Name "preset" -DefaultValue "")
            })
        } else {
            Add-Check -Checks $checks -Code "UNIT_FILE_STATE_EXPECTATION_OK" -Status "pass" -Message ("unit file state matches expectation: " + $unitName) -Evidence ([ordered]@{
                unit = $unitName
                expected_state = $expectedState
                actual_state = $actualState
            })
        }
    }
}

foreach ($rawPath in $resolvedRequiredStateDbPaths) {
    $text = [string]$rawPath
    if ([string]::IsNullOrWhiteSpace($text)) {
        continue
    }
    $resolvedPath = if ([System.IO.Path]::IsPathRooted($text)) { $text } else { Join-Path $resolvedProjectRoot $text }
    if (Test-Path -Path $resolvedPath -PathType Leaf) {
        Add-Check -Checks $checks -Code "STATE_DB_PATH_PRESENT" -Status "pass" -Message ("required state db path present: " + $resolvedPath) -Evidence ([ordered]@{
            path = $resolvedPath
        })
    } else {
        Add-Check -Checks $checks -Code "STATE_DB_PATH_MISSING" -Status "violation" -Message ("required state db path missing: " + $resolvedPath) -Evidence ([ordered]@{
            path = $resolvedPath
        })
    }
}
if ($resolvedRequiredUnitFiles.Count -gt 0) {
    foreach ($unitName in $resolvedRequiredUnitFiles) {
        $match = @($systemdSnapshot.unit_files | Where-Object { [string](Get-PropValue -ObjectValue $_ -Name "unit_file" -DefaultValue "") -eq $unitName })
        if ($match.Count -eq 0) {
            Add-Check -Checks $checks -Code "REQUIRED_UNIT_FILE_MISSING" -Status "violation" -Message ("required unit file missing: " + $unitName) -Evidence ([ordered]@{ unit = $unitName })
        } else {
            Add-Check -Checks $checks -Code "REQUIRED_UNIT_FILE_PRESENT" -Status "pass" -Message ("required unit file present: " + $unitName) -Evidence ([ordered]@{
                unit = $unitName
                state = [string](Get-PropValue -ObjectValue $match[0] -Name "state" -DefaultValue "")
            })
        }
    }
}
if ($resolvedBlockOnFailedUnits.Count -gt 0) {
    foreach ($unitName in $resolvedBlockOnFailedUnits) {
        $match = @($systemdSnapshot.services | Where-Object { [string](Get-PropValue -ObjectValue $_ -Name "unit" -DefaultValue "") -eq $unitName })
        if ($match.Count -gt 0) {
            $activeValue = [string](Get-PropValue -ObjectValue $match[0] -Name "active" -DefaultValue "")
            $subValue = [string](Get-PropValue -ObjectValue $match[0] -Name "sub" -DefaultValue "")
            if (($activeValue -eq "failed") -or ($subValue -eq "failed")) {
                Add-Check -Checks $checks -Code "FAILED_UNIT_PRESENT" -Status "violation" -Message ("failed unit blocks batch execution: " + $unitName) -Evidence ([ordered]@{
                    unit = $unitName
                    active = $activeValue
                    sub = $subValue
                })
                continue
            }
        }
        Add-Check -Checks $checks -Code "FAILED_UNIT_NOT_PRESENT" -Status "pass" -Message ("no blocking failed state for unit: " + $unitName) -Evidence ([ordered]@{ unit = $unitName })
    }
}

$pointerSnapshots = [ordered]@{
    champion = (Load-PointerSnapshot -RegistryRoot $registryRoot -ModelFamily $resolvedModelFamily -PointerName "champion")
    latest = (Load-PointerSnapshot -RegistryRoot $registryRoot -ModelFamily $resolvedModelFamily -PointerName "latest")
    latest_candidate = (Load-PointerSnapshot -RegistryRoot $registryRoot -ModelFamily $resolvedModelFamily -PointerName "latest_candidate")
    global_latest = (Load-PointerSnapshot -RegistryRoot $registryRoot -ModelFamily $resolvedModelFamily -PointerName "global_latest")
    global_latest_candidate = (Load-PointerSnapshot -RegistryRoot $registryRoot -ModelFamily $resolvedModelFamily -PointerName "global_latest_candidate")
}
foreach ($pointerName in $resolvedRequiredPointers) {
    $pointerFamily = if ([string]$pointerName -eq "champion") { $resolvedChampionPointerFamily } else { $resolvedModelFamily }
    $snapshot = Load-PointerSnapshot -RegistryRoot $registryRoot -ModelFamily $pointerFamily -PointerName $pointerName
    $runId = [string](Get-PropValue -ObjectValue $snapshot -Name "run_id" -DefaultValue "")
    $runDirExists = [bool](Get-PropValue -ObjectValue $snapshot -Name "run_dir_exists" -DefaultValue $false)
    if ([string]::IsNullOrWhiteSpace($runId)) {
        Add-Check -Checks $checks -Code "REQUIRED_POINTER_MISSING" -Status "violation" -Message ("required pointer missing run_id: " + $pointerName) -Evidence ([ordered]@{
            pointer = $pointerName
            model_family = $pointerFamily
        })
    } elseif (-not $runDirExists) {
        Add-Check -Checks $checks -Code "REQUIRED_POINTER_RUN_DIR_MISSING" -Status "violation" -Message ("required pointer run_dir missing: " + $pointerName) -Evidence ([ordered]@{
            pointer = $pointerName
            model_family = $pointerFamily
            run_id = $runId
            run_dir = [string](Get-PropValue -ObjectValue $snapshot -Name "run_dir" -DefaultValue "")
        })
    } else {
        Add-Check -Checks $checks -Code "REQUIRED_POINTER_RESOLVED" -Status "pass" -Message ("required pointer resolved: " + $pointerName) -Evidence ([ordered]@{
            pointer = $pointerName
            model_family = $pointerFamily
            run_id = $runId
        })
    }
}

$currentStatePath = Join-Path $resolvedProjectRoot "logs/model_v4_challenger/current_state.json"
$currentState = Load-JsonOrEmpty -PathValue $currentStatePath
if ($CheckCandidateStateConsistency) {
    $latestCandidateRunId = [string](Get-PropValue -ObjectValue $pointerSnapshots.latest_candidate -Name "run_id" -DefaultValue "")
    $championRunId = [string](Get-PropValue -ObjectValue $pointerSnapshots.champion -Name "run_id" -DefaultValue "")
    $currentStateExists = Test-Path $currentStatePath
    $currentCandidateRunId = [string](Get-PropValue -ObjectValue $currentState -Name "candidate_run_id" -DefaultValue "")
    if ((-not [string]::IsNullOrWhiteSpace($latestCandidateRunId)) -and (-not $currentStateExists)) {
        Add-Check -Checks $checks -Code "LATEST_CANDIDATE_WITHOUT_CURRENT_STATE" -Status "violation" -Message "latest_candidate pointer exists but current_state.json is missing." -Evidence ([ordered]@{ latest_candidate_run_id = $latestCandidateRunId })
    } elseif ([string]::IsNullOrWhiteSpace($latestCandidateRunId) -and $currentStateExists) {
        Add-Check -Checks $checks -Code "CURRENT_STATE_WITHOUT_LATEST_CANDIDATE" -Status "violation" -Message "current_state.json exists but latest_candidate pointer is missing." -Evidence ([ordered]@{ candidate_run_id = $currentCandidateRunId })
    } elseif ((-not [string]::IsNullOrWhiteSpace($latestCandidateRunId)) -and $currentStateExists -and ($currentCandidateRunId -ne $latestCandidateRunId)) {
        Add-Check -Checks $checks -Code "LATEST_CANDIDATE_CURRENT_STATE_MISMATCH" -Status "violation" -Message "latest_candidate pointer does not match current_state candidate_run_id." -Evidence ([ordered]@{
            latest_candidate_run_id = $latestCandidateRunId
            current_state_candidate_run_id = $currentCandidateRunId
        })
    } else {
        Add-Check -Checks $checks -Code "CANDIDATE_STATE_ALIGNMENT_OK" -Status "pass" -Message "candidate state alignment is acceptable."
    }
    if ($currentStateExists -and [string]::IsNullOrWhiteSpace($currentCandidateRunId)) {
        Add-Check -Checks $checks -Code "CURRENT_STATE_CANDIDATE_RUN_ID_MISSING" -Status "violation" -Message "current_state.json is present but candidate_run_id is blank."
    }
    if ((-not [string]::IsNullOrWhiteSpace($championRunId)) -and (-not [string]::IsNullOrWhiteSpace($latestCandidateRunId)) -and ($championRunId -eq $latestCandidateRunId) -and (-not $currentStateExists)) {
        Add-Check -Checks $checks -Code "CHAMPION_EQUALS_LATEST_CANDIDATE_NO_TRANSITION_STATE" -Status "violation" -Message "champion and latest_candidate point to the same run without current_state.json." -Evidence ([ordered]@{ run_id = $championRunId })
    }
}

$summary = Summarize-Checks -Checks @($checks.ToArray())
$report = [ordered]@{
    policy = "server_preflight_v1"
    project_root = $resolvedProjectRoot
    model_family = $resolvedModelFamily
    champion_pointer_family = $resolvedChampionPointerFamily
    generated_at_utc = (Get-Date).ToUniversalTime().ToString("o")
    required_unit_files = @($resolvedRequiredUnitFiles)
    block_on_failed_units = @($resolvedBlockOnFailedUnits)
    expected_unit_states = @($resolvedExpectedUnitStates)
    required_state_db_paths = @($resolvedRequiredStateDbPaths)
    required_pointers = @($resolvedRequiredPointers)
    fail_on_dirty_worktree = [bool]$FailOnDirtyWorktree
    check_candidate_state_consistency = [bool]$CheckCandidateStateConsistency
    python_exe = if ([string]::IsNullOrWhiteSpace($resolvedPythonExe)) { $null } else { $resolvedPythonExe }
    runtime_topology_report = [ordered]@{
        attempted = $true
        exit_code = [int]$runtimeTopologyCapture.ExitCode
        command = [string]$runtimeTopologyCapture.Command
        report_path = [string]$runtimeTopologyCapture.ReportPath
        summary = $runtimeTopologySummary
    }
    pointer_consistency_report = [ordered]@{
        attempted = $true
        exit_code = [int]$pointerConsistencyCapture.ExitCode
        command = [string]$pointerConsistencyCapture.Command
        report_path = [string]$pointerConsistencyCapture.ReportPath
        summary = $pointerConsistencySummary
    }
    git = $gitSnapshot
    systemd = $systemdSnapshot
    pointers = $pointerSnapshots
    challenger_state = [ordered]@{
        current_state_path = $currentStatePath
        current_state_exists = (Test-Path $currentStatePath)
        current_state = $currentState
    }
    checks = @($checks.ToArray())
    summary = $summary
}
Write-JsonFile -PathValue $reportPath -Payload $report -Depth 20
Write-Host ("[ops][server-preflight] report={0}" -f $reportPath)
Write-Host ("[ops][server-preflight] status={0} violations={1} warnings={2}" -f $summary.status, $summary.violation_count, $summary.warning_count)

if ([int]$summary.violation_count -gt 0) {
    exit 2
}
exit 0
