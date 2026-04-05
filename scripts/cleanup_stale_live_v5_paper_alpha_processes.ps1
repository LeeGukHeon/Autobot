param(
    [string]$PythonExe = "python",
    [string]$ProjectRoot = "D:\MyApps\Autobot",
    [switch]$Apply,
    [string]$OutPath = ""
)

$ErrorActionPreference = "Stop"
Set-Location $ProjectRoot

$args = @("-m", "autobot.ops.paper_alpha_process_guard")
if ($Apply.IsPresent) {
    $args += "--apply"
}
if (-not [string]::IsNullOrWhiteSpace($OutPath)) {
    $args += @("--out", $OutPath)
}

& $PythonExe @args
exit $LASTEXITCODE
