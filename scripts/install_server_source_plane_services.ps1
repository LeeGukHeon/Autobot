param(
    [string]$ProjectRoot = "",
    [string]$PythonExe = "",
    [switch]$NoStart,
    [switch]$NoEnable,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$wsPublicScript = Join-Path $PSScriptRoot "install_server_ws_public_service.ps1"
$privateWsScript = Join-Path $PSScriptRoot "install_server_private_ws_archive_service.ps1"
$foundationScript = Join-Path $PSScriptRoot "install_server_foundation_ingestion_services.ps1"
$dataPlatformScript = Join-Path $PSScriptRoot "install_server_data_platform_refresh_service.ps1"

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

& $wsPublicScript @commonParams
if ($LASTEXITCODE -ne 0) {
    throw "install_server_ws_public_service.ps1 failed"
}
& $privateWsScript @commonParams
if ($LASTEXITCODE -ne 0) {
    throw "install_server_private_ws_archive_service.ps1 failed"
}
& $foundationScript @commonParams
if ($LASTEXITCODE -ne 0) {
    throw "install_server_foundation_ingestion_services.ps1 failed"
}
& $dataPlatformScript @commonParams
if ($LASTEXITCODE -ne 0) {
    throw "install_server_data_platform_refresh_service.ps1 failed"
}
