param(
    [string]$ProjectRoot = "",
    [string]$PythonExe = "",
    [string]$OutputDir = "logs/live_execution_policy",
    [string[]]$StateDbPaths = @(
        "data/state/live_state.db",
        "data/state/live_candidate/live_state.db"
    ),
    [int]$LookbackDays = 14,
    [int]$Limit = 5000,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

. (Join-Path $PSScriptRoot "systemd_service_utils.ps1")

$resolvedProjectRoot = if ([string]::IsNullOrWhiteSpace($ProjectRoot)) { Resolve-DefaultProjectRoot } else { $ProjectRoot }
$resolvedProjectRoot = [System.IO.Path]::GetFullPath($resolvedProjectRoot)
$resolvedPythonExe = if ([string]::IsNullOrWhiteSpace($PythonExe)) { Resolve-DefaultPythonExe -Root $resolvedProjectRoot } else { $PythonExe }
$resolvedOutputDir = if ([System.IO.Path]::IsPathRooted($OutputDir)) { $OutputDir } else { Join-Path $resolvedProjectRoot $OutputDir }
$resolvedStateDbPaths = @(Expand-DelimitedStringArray -Value $StateDbPaths)

if ($DryRun) {
    Write-Host ("[live-exec-refresh][dry-run] project_root={0}" -f $resolvedProjectRoot)
    Write-Host ("[live-exec-refresh][dry-run] output_dir={0}" -f $resolvedOutputDir)
    Write-Host ("[live-exec-refresh][dry-run] state_db_paths={0}" -f ([string]::Join(",", $resolvedStateDbPaths)))
    Write-Host ("[live-exec-refresh][dry-run] lookback_days={0}" -f $LookbackDays)
    Write-Host ("[live-exec-refresh][dry-run] limit={0}" -f $Limit)
    exit 0
}

New-Item -ItemType Directory -Force -Path $resolvedOutputDir | Out-Null
$refreshed = @()
foreach ($relativeDbPath in @($resolvedStateDbPaths)) {
    if ([string]::IsNullOrWhiteSpace($relativeDbPath)) {
        continue
    }
    $resolvedDbPath = if ([System.IO.Path]::IsPathRooted($relativeDbPath)) {
        $relativeDbPath
    } else {
        Join-Path $resolvedProjectRoot $relativeDbPath
    }
    if (-not (Test-Path $resolvedDbPath)) {
        continue
    }
    $dbName = [System.IO.Path]::GetFileNameWithoutExtension($resolvedDbPath)
    $outputPath = Join-Path $resolvedOutputDir ($dbName + ".json")
    & $resolvedPythonExe -m autobot.live.execution_policy_refresh `
        --db-path $resolvedDbPath `
        --output-path $outputPath `
        --lookback-days $LookbackDays `
        --limit $Limit
    if ($LASTEXITCODE -ne 0) {
        throw "execution policy refresh failed: $resolvedDbPath"
    }
    $refreshed += [ordered]@{
        db_path = $resolvedDbPath
        output_path = $outputPath
    }
}

$summaryPath = Join-Path $resolvedOutputDir "latest_refresh.json"
$summary = [ordered]@{
    refreshed = @($refreshed)
    lookback_days = [int]$LookbackDays
    limit = [int]$Limit
    refreshed_at = (Get-Date).ToUniversalTime().ToString("o")
}
$summary | ConvertTo-Json -Depth 6 | Set-Content -Path $summaryPath -Encoding UTF8
Write-Host $summaryPath
