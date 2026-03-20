param(
    [string]$ProjectRoot = "",
    [string]$PythonExe = "",
    [string]$AcceptanceScript = "",
    [string]$BatchDate = "",
    [string[]]$BlockOnActiveUnits = @(),
    [string[]]$AcceptanceArgs = @(),
    [switch]$SkipDailyPipeline,
    [switch]$SkipReportRefresh,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

. (Join-Path $PSScriptRoot "systemd_service_utils.ps1")

function Resolve-DefaultAcceptanceScript {
    param([string]$Root)
    return (Join-Path $Root "scripts/v4_governed_candidate_acceptance.ps1")
}

function Invoke-CommandCapture {
    param(
        [string]$Exe,
        [string[]]$ArgList,
        [switch]$AllowFailure
    )
    $commandText = $Exe + " " + (($ArgList | ForEach-Object { Quote-ShellArg ([string]$_) }) -join " ")
    if ($DryRun) {
        Write-Host ("[daily-accept][dry-run] {0}" -f $commandText)
        return [PSCustomObject]@{
            ExitCode = 0
            Output = "[dry-run] $commandText"
            Command = $commandText
        }
    }
    $output = & $Exe @ArgList 2>&1
    $exitCode = [int]$LASTEXITCODE
    if ((-not $AllowFailure) -and $exitCode -ne 0) {
        throw ("command failed: " + $commandText)
    }
    return [PSCustomObject]@{
        ExitCode = $exitCode
        Output = ($output -join "`n")
        Command = $commandText
    }
}

$resolvedProjectRoot = if ([string]::IsNullOrWhiteSpace($ProjectRoot)) { Resolve-DefaultProjectRoot } else { $ProjectRoot }
$resolvedProjectRoot = [System.IO.Path]::GetFullPath($resolvedProjectRoot)
$resolvedPythonExe = if ([string]::IsNullOrWhiteSpace($PythonExe)) { Resolve-DefaultPythonExe -Root $resolvedProjectRoot } else { $PythonExe }
$resolvedAcceptanceScript = if ([string]::IsNullOrWhiteSpace($AcceptanceScript)) { Resolve-DefaultAcceptanceScript -Root $resolvedProjectRoot } else { $AcceptanceScript }
$resolvedBatchDate = Resolve-BatchDateValue -DateText $BatchDate
$resolvedBlockOnActiveUnits = @(Get-StringArray -Value $BlockOnActiveUnits)
$resolvedAcceptanceArgs = @(Get-StringArray -Value $AcceptanceArgs)

$activeBlockUnits = @()
foreach ($unit in $resolvedBlockOnActiveUnits) {
    $trimmed = [string]$unit
    if ([string]::IsNullOrWhiteSpace($trimmed)) {
        continue
    }
    if (Test-SystemdUnitActive -UnitName $trimmed.Trim() -IsDryRun:$DryRun) {
        $activeBlockUnits += $trimmed.Trim()
    }
}

if ($activeBlockUnits.Count -gt 0) {
    Write-Host ("[daily-accept][skip] blocking_units_active={0}" -f ($activeBlockUnits -join ","))
    exit 0
}

$psExe = if ([System.IO.Path]::DirectorySeparatorChar -eq '\') { "powershell.exe" } else { Resolve-PwshExe }
$argList = @(
    "-NoProfile",
    "-ExecutionPolicy", "Bypass",
    "-File", $resolvedAcceptanceScript,
    "-ProjectRoot", $resolvedProjectRoot,
    "-PythonExe", $resolvedPythonExe,
    "-BatchDate", $resolvedBatchDate
)
if ($SkipDailyPipeline) {
    $argList += "-SkipDailyPipeline"
}
if ($SkipReportRefresh) {
    $argList += "-SkipReportRefresh"
}
if ($DryRun) {
    $argList += "-DryRun"
}
$argList += $resolvedAcceptanceArgs

$commandPreview = $psExe + " " + (($argList | ForEach-Object { Quote-ShellArg ([string]$_) }) -join " ")
Write-Host ("[daily-accept] batch_date={0}" -f $resolvedBatchDate)
Write-Host ("[daily-accept] acceptance_script={0}" -f $resolvedAcceptanceScript)
Write-Host ("[daily-accept] command={0}" -f $commandPreview)

$exec = Invoke-CommandCapture -Exe $psExe -ArgList $argList -AllowFailure
$reportPath = Resolve-ReportedJsonPath -OutputText $exec.Output
$acceptanceReport = Load-JsonOrEmpty -PathValue $reportPath
if (Test-AcceptanceFatalFailure -ExitCode $exec.ExitCode -AcceptanceReport $acceptanceReport) {
    exit $exec.ExitCode
}
if (($exec.ExitCode -ne 0) -and (Test-ScoutOnlyBudgetEvidence -AcceptanceReport $acceptanceReport)) {
    Write-Host ("[daily-accept] scout_nonfatal_reason=SCOUT_ONLY_BUDGET_EVIDENCE")
    exit 0
}
if (($exec.ExitCode -ne 0) -and (Test-BootstrapOnlyAcceptanceReport -AcceptanceReport $acceptanceReport)) {
    Write-Host ("[daily-accept] bootstrap_nonfatal_reason=BOOTSTRAP_ONLY_POLICY")
    exit 0
}
exit $exec.ExitCode
