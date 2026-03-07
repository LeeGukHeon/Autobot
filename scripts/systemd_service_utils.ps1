$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

function Resolve-DefaultProjectRoot {
    return (Split-Path -Path $PSScriptRoot -Parent)
}

function Resolve-DefaultPythonExe {
    param([string]$Root)
    return (Join-Path $Root ".venv/bin/python")
}

function Resolve-PwshExe {
    if ([System.IO.Path]::DirectorySeparatorChar -eq '\') {
        return "powershell.exe"
    }
    $cmd = Get-Command pwsh -ErrorAction SilentlyContinue
    if ($null -ne $cmd -and -not [string]::IsNullOrWhiteSpace($cmd.Source)) {
        return [string]$cmd.Source
    }
    if (Test-Path "/snap/bin/pwsh") {
        return "/snap/bin/pwsh"
    }
    throw "pwsh executable not found"
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
