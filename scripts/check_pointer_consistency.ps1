param(
    [string]$ProjectRoot = "",
    [string]$PythonExe = "",
    [string]$ModelFamily = "train_v5_fusion",
    [string]$ChampionPointerFamily = "",
    [switch]$FailOnWarning
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

. (Join-Path $PSScriptRoot "systemd_service_utils.ps1")

$resolvedProjectRoot = if ([string]::IsNullOrWhiteSpace($ProjectRoot)) { Resolve-DefaultProjectRoot } else { $ProjectRoot }
$resolvedProjectRoot = [System.IO.Path]::GetFullPath($resolvedProjectRoot)
$resolvedPythonExe = if ([string]::IsNullOrWhiteSpace($PythonExe)) { Resolve-DefaultPythonExe -Root $resolvedProjectRoot } else { $PythonExe }
$resolvedModelFamily = [string]$ModelFamily
if ([string]::IsNullOrWhiteSpace($resolvedModelFamily) -or (([string]::Equals($resolvedModelFamily, "train_v5_fusion", [System.StringComparison]::OrdinalIgnoreCase)) -and (-not (Test-Path (Join-Path $resolvedProjectRoot ("models/registry/" + $resolvedModelFamily)))))) {
    $resolvedModelFamily = Resolve-PreferredModelFamily -Root $resolvedProjectRoot -PreferredFamily "train_v5_fusion"
}
$resolvedChampionPointerFamily = [string]$ChampionPointerFamily
if ([string]::IsNullOrWhiteSpace($resolvedChampionPointerFamily)) {
    $resolvedChampionPointerFamily = $resolvedModelFamily
}

$output = & $resolvedPythonExe -m autobot.ops.pointer_consistency_report --project-root $resolvedProjectRoot --model-family $resolvedModelFamily --champion-pointer-family $resolvedChampionPointerFamily 2>&1
$exitCode = $LASTEXITCODE
if ($exitCode -ne 0) {
    throw ("pointer consistency report command failed (exit_code={0})" -f $exitCode)
}

$outputText = [string]($output -join [Environment]::NewLine)
$reportPath = ""
$regex = [System.Text.RegularExpressions.Regex]::new("(?m)^\[ops\]\[pointer-consistency\]\s+path=(.+)$")
$matches = $regex.Matches($outputText)
if ($null -ne $matches -and $matches.Count -gt 0) {
    $reportPath = [string]$matches[$matches.Count - 1].Groups[1].Value.Trim()
}
if ([string]::IsNullOrWhiteSpace($reportPath)) {
    $reportPath = Join-Path $resolvedProjectRoot "logs/ops/pointer_consistency/latest.json"
}
if (-not (Test-Path $reportPath)) {
    throw ("pointer consistency report missing: " + $reportPath)
}

$report = Get-Content -Path $reportPath -Raw -Encoding UTF8 | ConvertFrom-Json
$summary = if ($null -eq $report.summary) { @{} } else { $report.summary }
$status = [string]($summary.status)
$violations = [int]($summary.violation_count)
$warnings = [int]($summary.warning_count)

Write-Host ("[ops][pointer-consistency-check] report={0}" -f $reportPath)
Write-Host ("[ops][pointer-consistency-check] status={0} violations={1} warnings={2}" -f $status, $violations, $warnings)

if ($violations -gt 0) {
    exit 2
}
if ($FailOnWarning -and $warnings -gt 0) {
    exit 3
}
exit 0
