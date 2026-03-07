param(
    [string]$ProjectRoot = "",
    [string]$PythonExe = "",
    [string]$AcceptanceScript = "",
    [string]$BatchDate = "",
    [string[]]$BlockOnActiveUnits = @("autobot-daily-micro.service"),
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
    return (Join-Path $Root "scripts/v4_candidate_acceptance.ps1")
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

$resolvedProjectRoot = if ([string]::IsNullOrWhiteSpace($ProjectRoot)) { Resolve-DefaultProjectRoot } else { $ProjectRoot }
$resolvedProjectRoot = [System.IO.Path]::GetFullPath($resolvedProjectRoot)
$resolvedPythonExe = if ([string]::IsNullOrWhiteSpace($PythonExe)) { Resolve-DefaultPythonExe -Root $resolvedProjectRoot } else { $PythonExe }
$resolvedAcceptanceScript = if ([string]::IsNullOrWhiteSpace($AcceptanceScript)) { Resolve-DefaultAcceptanceScript -Root $resolvedProjectRoot } else { $AcceptanceScript }
$resolvedBatchDate = Resolve-BatchDateValue -DateText $BatchDate

$activeBlockUnits = @()
foreach ($unit in @($BlockOnActiveUnits)) {
    $trimmed = [string]$unit
    if ([string]::IsNullOrWhiteSpace($trimmed)) {
        continue
    }
    if (Test-SystemdUnitActive -UnitName $trimmed.Trim()) {
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
$argList += @($AcceptanceArgs)

$commandPreview = $psExe + " " + (($argList | ForEach-Object { Quote-ShellArg ([string]$_) }) -join " ")
Write-Host ("[daily-accept] batch_date={0}" -f $resolvedBatchDate)
Write-Host ("[daily-accept] acceptance_script={0}" -f $resolvedAcceptanceScript)
Write-Host ("[daily-accept] command={0}" -f $commandPreview)

& $psExe @argList
exit $LASTEXITCODE
