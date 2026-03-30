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

$commonArgs = @()
if (-not [string]::IsNullOrWhiteSpace($ProjectRoot)) {
    $commonArgs += @("-ProjectRoot", $ProjectRoot)
}
if (-not [string]::IsNullOrWhiteSpace($PythonExe)) {
    $commonArgs += @("-PythonExe", $PythonExe)
}
if ($NoStart) {
    $commonArgs += "-NoStart"
}
if ($NoEnable) {
    $commonArgs += "-NoEnable"
}
if ($DryRun) {
    $commonArgs += "-DryRun"
}

& $candlesScript @commonArgs
if ($LASTEXITCODE -ne 0) {
    throw "install_server_candles_api_refresh_service.ps1 failed"
}
& $ticksScript @commonArgs
if ($LASTEXITCODE -ne 0) {
    throw "install_server_raw_ticks_daily_service.ps1 failed"
}
