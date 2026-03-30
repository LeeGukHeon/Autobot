param(
    [string]$ProjectRoot = "",
    [string]$PythonExe = "",
    [switch]$NoStart,
    [switch]$NoEnable,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$candlesScript = Join-Path $PSScriptRoot "install_server_candles_api_refresh_service.ps1"
$ticksScript = Join-Path $PSScriptRoot "install_server_raw_ticks_daily_service.ps1"

$commonParams = @{}
if (-not [string]::IsNullOrWhiteSpace($ProjectRoot)) {
    $commonParams["ProjectRoot"] = $ProjectRoot
}
if (-not [string]::IsNullOrWhiteSpace($PythonExe)) {
    $commonParams["PythonExe"] = $PythonExe
}
if ($NoStart) {
    $commonParams["NoStart"] = $true
}
if ($NoEnable) {
    $commonParams["NoEnable"] = $true
}
if ($DryRun) {
    $commonParams["DryRun"] = $true
}

& $candlesScript @commonParams
if ($LASTEXITCODE -ne 0) {
    throw "install_server_candles_api_refresh_service.ps1 failed"
}
& $ticksScript @commonParams
if ($LASTEXITCODE -ne 0) {
    throw "install_server_raw_ticks_daily_service.ps1 failed"
}
