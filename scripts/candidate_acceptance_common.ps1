$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

function Load-JsonOrEmpty {
    param([string]$PathValue)
    if ([string]::IsNullOrWhiteSpace($PathValue) -or (-not (Test-Path $PathValue))) {
        return @{}
    }
    $raw = Get-Content -Path $PathValue -Raw -Encoding UTF8
    if ([string]::IsNullOrWhiteSpace($raw)) {
        return @{}
    }
    return $raw | ConvertFrom-Json
}

function Write-JsonFile {
    param(
        [string]$PathValue,
        [Parameter(Mandatory = $false)]$Payload,
        [int]$Depth = 12
    )
    if ([string]::IsNullOrWhiteSpace($PathValue)) {
        return
    }
    $parent = Split-Path -Parent $PathValue
    if (-not [string]::IsNullOrWhiteSpace($parent)) {
        New-Item -ItemType Directory -Path $parent -Force | Out-Null
    }
    ($Payload | ConvertTo-Json -Depth $Depth) | Set-Content -Path $PathValue -Encoding UTF8
}

function Test-IsEffectivelyEmptyObject {
    param([Parameter(Mandatory = $false)]$ObjectValue)
    if ($null -eq $ObjectValue) {
        return $true
    }
    if ($ObjectValue -is [System.Collections.IDictionary]) {
        return $ObjectValue.Count -eq 0
    }
    if ($ObjectValue.PSObject) {
        try {
            $ownProperties = @(
                $ObjectValue.PSObject.Properties |
                    Where-Object { $_.MemberType -in @("NoteProperty", "Property") }
            )
            return $ownProperties.Count -eq 0
        } catch {
            return $false
        }
    }
    return $false
}

function Get-PropValue {
    param(
        [Parameter(Mandatory = $false)]$ObjectValue,
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $false)]$DefaultValue = $null
    )
    if ($null -eq $ObjectValue) {
        return $DefaultValue
    }
    if ($ObjectValue -is [System.Collections.IDictionary]) {
        if ($ObjectValue.Contains($Name)) {
            return $ObjectValue[$Name]
        }
        return $DefaultValue
    }
    try {
        foreach ($property in @($ObjectValue.PSObject.Properties)) {
            if ($null -eq $property) {
                continue
            }
            if ([string]$property.Name -eq $Name) {
                return $property.Value
            }
        }
    } catch {
    }
    return $DefaultValue
}

function Get-OutputPreview {
    param([string]$Text, [int]$MaxLength = 400)
    if ([string]::IsNullOrWhiteSpace($Text)) {
        return ""
    }
    $preview = $Text.Trim() -replace "\r?\n", " | "
    if ($preview.Length -le $MaxLength) {
        return $preview
    }
    return $preview.Substring(0, $MaxLength)
}

function Format-CommandText {
    param([string]$Exe, [string[]]$ArgList)
    return ($Exe + " " + ($ArgList -join " ")).Trim()
}

function Invoke-CommandCapture {
    param(
        [string]$Exe,
        [string[]]$ArgList,
        [switch]$AllowFailure
    )
    $commandText = Format-CommandText -Exe $Exe -ArgList $ArgList
    if ($DryRun) {
        return [PSCustomObject]@{
            ExitCode = 0
            Output = "[dry-run] $commandText"
            Command = $commandText
            DryRun = $true
        }
    }
    $output = & $Exe @ArgList 2>&1
    $exitCode = [int]$LASTEXITCODE
    if ((-not $AllowFailure) -and $exitCode -ne 0) {
        throw ("command failed: " + $commandText)
    }
    return [PSCustomObject]@{
        ExitCode = $exitCode
        Output = ($output -join [Environment]::NewLine)
        Command = $commandText
        DryRun = $false
    }
}

function Resolve-ReportedJsonPathFromText {
    param(
        [string]$TextValue,
        [string]$LogTag
    )
    if ([string]::IsNullOrWhiteSpace($TextValue) -or [string]::IsNullOrWhiteSpace($LogTag)) {
        return ""
    }
    $pattern = '(?m)^\[' + [Regex]::Escape($LogTag) + '\] report=(.+)$'
    $match = [Regex]::Match($TextValue, $pattern)
    if (-not $match.Success) {
        return ""
    }
    $reportedPath = [string]$match.Groups[1].Value
    if ([string]::IsNullOrWhiteSpace($reportedPath)) {
        return ""
    }
    return $reportedPath.Trim()
}

function Resolve-FeaturesBuildReportPathFromText {
    param([string]$TextValue)
    if ([string]::IsNullOrWhiteSpace($TextValue)) {
        return ""
    }
    $pattern = '(?m)^\[features\]\[build\]\[[^\]]+\]\s+report=(.+)$'
    $match = [Regex]::Match($TextValue, $pattern)
    if (-not $match.Success) {
        return ""
    }
    $reportedPath = [string]$match.Groups[1].Value
    if ([string]::IsNullOrWhiteSpace($reportedPath)) {
        return ""
    }
    return $reportedPath.Trim()
}

function Resolve-RunDirFromText {
    param([string]$TextValue)
    if ([string]::IsNullOrWhiteSpace($TextValue)) {
        return ""
    }

    $jsonMatch = [Regex]::Match($TextValue, '(?ms)"run_dir"\s*:\s*"((?:\\.|[^"])*)"')
    if ($jsonMatch.Success) {
        $encodedPath = [string]$jsonMatch.Groups[1].Value
        try {
            return [string](('"' + $encodedPath + '"') | ConvertFrom-Json)
        } catch {
        }
    }

    $lineMatch = [Regex]::Match($TextValue, '(?m)^\[[^\]]+\]\s+run_dir=(.+)$')
    if ($lineMatch.Success) {
        return ([string]$lineMatch.Groups[1].Value).Trim()
    }

    $plainMatch = [Regex]::Match($TextValue, '(?m)^run_dir=(.+)$')
    if ($plainMatch.Success) {
        return ([string]$plainMatch.Groups[1].Value).Trim()
    }
    return ""
}
