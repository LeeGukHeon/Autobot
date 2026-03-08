$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

function Resolve-DefaultProjectRoot {
    return (Split-Path -Path $PSScriptRoot -Parent)
}

function Resolve-DefaultPythonExe {
    param([string]$Root)
    if ([System.IO.Path]::DirectorySeparatorChar -eq '\') {
        return (Join-Path $Root ".venv\\Scripts\\python.exe")
    }
    return (Join-Path $Root ".venv/bin/python")
}

function Resolve-PwshExe {
    if ([System.IO.Path]::DirectorySeparatorChar -eq '\') {
        return "powershell.exe"
    }
    $cmd = Get-Command pwsh -ErrorAction SilentlyContinue
    if ($null -ne $cmd -and -not [string]::IsNullOrWhiteSpace($cmd.Source)) {
        $resolved = [string]$cmd.Source
        if (-not $resolved.StartsWith("/snap/")) {
            return $resolved
        }
    }
    foreach ($candidatePath in @(
        "/usr/bin/pwsh",
        "/usr/local/bin/pwsh",
        "/opt/microsoft/powershell/7/pwsh"
    )) {
        if (Test-Path $candidatePath) {
            return $candidatePath
        }
    }
    if ($null -ne $cmd -and -not [string]::IsNullOrWhiteSpace($cmd.Source)) {
        return [string]$cmd.Source
    }
    if (Test-Path "/snap/bin/pwsh") {
        return "/snap/bin/pwsh"
    }
    throw "pwsh executable not found"
}

function Enable-UserLinger {
    param([string]$UserName)
    if ([System.IO.Path]::DirectorySeparatorChar -eq '\') {
        return
    }
    if ([string]::IsNullOrWhiteSpace($UserName)) {
        return
    }
    $loginctl = Get-Command loginctl -ErrorAction SilentlyContinue
    if ($null -eq $loginctl -or [string]::IsNullOrWhiteSpace($loginctl.Source)) {
        return
    }
    & sudo $loginctl.Source enable-linger $UserName
    if ($LASTEXITCODE -ne 0) {
        throw "loginctl enable-linger failed: $UserName"
    }
}

function Quote-ShellArg {
    param([string]$Value)
    if ([string]::IsNullOrWhiteSpace($Value)) {
        return "''"
    }
    return "'" + $Value.Replace("'", "'""'""'") + "'"
}

function Install-UnitFile {
    param(
        [string]$UnitName,
        [string]$Content
    )
    $tmpPath = [System.IO.Path]::GetTempFileName()
    try {
        Set-Content -Path $tmpPath -Encoding UTF8 -Value $Content
        & sudo install -m 0644 $tmpPath ("/etc/systemd/system/" + $UnitName)
        if ($LASTEXITCODE -ne 0) {
            throw "failed to install unit file: $UnitName"
        }
    } finally {
        Remove-Item -Path $tmpPath -Force -ErrorAction SilentlyContinue
    }
}

function Install-DropInFile {
    param(
        [string]$UnitName,
        [string]$DropInName,
        [string]$Content
    )
    $tmpPath = [System.IO.Path]::GetTempFileName()
    try {
        Set-Content -Path $tmpPath -Encoding UTF8 -Value $Content
        & sudo install -D -m 0644 $tmpPath ("/etc/systemd/system/" + $UnitName + ".d/" + $DropInName)
        if ($LASTEXITCODE -ne 0) {
            throw "failed to install drop-in file: $UnitName/$DropInName"
        }
    } finally {
        Remove-Item -Path $tmpPath -Force -ErrorAction SilentlyContinue
    }
}
