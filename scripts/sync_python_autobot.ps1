param(
    [switch]$Mirror
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$sourceDir = Join-Path $repoRoot "autobot"
$targetDir = Join-Path $repoRoot "python\\autobot"

if (-not (Test-Path $sourceDir)) {
    throw "Source package not found: $sourceDir"
}

New-Item -ItemType Directory -Path $targetDir -Force | Out-Null

$modeArgs = if ($Mirror) { @("/MIR") } else { @("/E") }

$args = @(
    $sourceDir,
    $targetDir
) + $modeArgs + @(
    "/R:1",
    "/W:1",
    "/NFL",
    "/NDL",
    "/NJH",
    "/NJS",
    "/NP",
    "/XD", "__pycache__",
    "/XD", "tests",
    "/XD", "logs",
    "/XD", "data",
    "/XD", "models",
    "/XF", "*.pyc"
)

Write-Host "[sync] source=$sourceDir"
Write-Host "[sync] target=$targetDir"
if ($Mirror) {
    Write-Host "[sync] mode=MIRROR"
} else {
    Write-Host "[sync] mode=COPY"
}

robocopy @args | Out-Host
$code = $LASTEXITCODE

if ($code -gt 7) {
    throw "robocopy failed with exit code $code"
}

Write-Host "[sync] done (robocopy exit code: $code)"
