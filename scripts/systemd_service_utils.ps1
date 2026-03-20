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

function Expand-DelimitedStringArray {
    param([Parameter(Mandatory = $false)]$Value)
    if ($null -eq $Value) {
        return @()
    }
    $items = @()
    foreach ($rawItem in @($Value)) {
        if ($null -eq $rawItem) {
            continue
        }
        foreach ($candidate in ([string]$rawItem -split ",")) {
            $text = [string]$candidate
            if ([string]::IsNullOrWhiteSpace($text)) {
                continue
            }
            $items += $text.Trim()
        }
    }
    return @($items)
}

function Join-DelimitedStringArray {
    param([Parameter(Mandatory = $false)]$Values)
    $normalized = @(Expand-DelimitedStringArray -Value $Values)
    if ($normalized.Count -eq 0) {
        return ""
    }
    return [string]::Join(",", $normalized)
}

function Resolve-ReportedJsonPath {
    param([string]$OutputText)
    if ([string]::IsNullOrWhiteSpace($OutputText)) {
        return ""
    }
    $regex = [System.Text.RegularExpressions.Regex]::new("(?m)^\[[^\]]+\]\s+report=(.+)$")
    $matches = $regex.Matches([string]$OutputText)
    if ($null -eq $matches -or $matches.Count -eq 0) {
        return ""
    }
    for ($index = $matches.Count - 1; $index -ge 0; $index--) {
        $candidatePath = [string]$matches[$index].Groups[1].Value.Trim()
        if ([string]::IsNullOrWhiteSpace($candidatePath)) {
            continue
        }
        if ([string]::Equals([System.IO.Path]::GetExtension($candidatePath), ".json", [System.StringComparison]::OrdinalIgnoreCase)) {
            return $candidatePath
        }
    }
    return [string]$matches[$matches.Count - 1].Groups[1].Value.Trim()
}

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
        [Parameter(Mandatory = $true)]$Payload
    )
    $parent = Split-Path -Parent $PathValue
    if (-not [string]::IsNullOrWhiteSpace($parent)) {
        New-Item -ItemType Directory -Force -Path $parent | Out-Null
    }
    $Payload | ConvertTo-Json -Depth 10 | Set-Content -Path $PathValue -Encoding UTF8
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

function Test-ObjectHasValues {
    param([Parameter(Mandatory = $false)]$ObjectValue)
    if ($null -eq $ObjectValue) {
        return $false
    }
    if ($ObjectValue -is [System.Collections.IDictionary]) {
        return ($ObjectValue.Count -gt 0)
    }
    if ($ObjectValue.PSObject) {
        return (@($ObjectValue.PSObject.Properties).Count -gt 0)
    }
    return $true
}

function Get-StringArray {
    param([Parameter(Mandatory = $false)]$Value)
    return @(Expand-DelimitedStringArray -Value $Value)
}

function Test-AcceptanceFatalFailure {
    param(
        [int]$ExitCode,
        [Parameter(Mandatory = $false)]$AcceptanceReport
    )
    if ($ExitCode -eq 0) {
        return $false
    }
    if (($ExitCode -ne 2) -or (-not (Test-ObjectHasValues -ObjectValue $AcceptanceReport))) {
        return $true
    }
    $steps = Get-PropValue -ObjectValue $AcceptanceReport -Name "steps" -DefaultValue @{}
    $exceptionStep = Get-PropValue -ObjectValue $steps -Name "exception" -DefaultValue @{}
    if (Test-ObjectHasValues -ObjectValue $exceptionStep) {
        return $true
    }
    $reasons = Get-StringArray -Value (Get-PropValue -ObjectValue $AcceptanceReport -Name "reasons" -DefaultValue @())
    foreach ($reason in $reasons) {
        if (@(
            "UNHANDLED_EXCEPTION",
            "DAILY_PIPELINE_FAILED",
            "TRAIN_OR_CANDIDATE_POINTER_FAILED"
        ) -contains [string]$reason) {
            return $true
        }
    }
    return $false
}

function Resolve-BatchDateValue {
    param([string]$DateText)
    if (-not [string]::IsNullOrWhiteSpace($DateText)) {
        return [DateTime]::ParseExact(
            $DateText,
            "yyyy-MM-dd",
            [System.Globalization.CultureInfo]::InvariantCulture,
            [System.Globalization.DateTimeStyles]::None
        ).ToString("yyyy-MM-dd")
    }
    return (Get-Date).Date.AddDays(-1).ToString("yyyy-MM-dd")
}

function Test-SystemdUnitActive {
    param(
        [string]$UnitName,
        [switch]$IsDryRun
    )
    if ($IsDryRun) {
        return $false
    }
    $systemctl = Get-Command systemctl -ErrorAction SilentlyContinue
    if ($null -eq $systemctl) {
        return $false
    }
    & $systemctl.Source is-active --quiet $UnitName
    return ($LASTEXITCODE -eq 0)
}

function Test-ScoutOnlyBudgetEvidence {
    param([Parameter(Mandatory = $false)]$AcceptanceReport)
    $reasons = Get-StringArray -Value (Get-PropValue -ObjectValue $AcceptanceReport -Name "reasons" -DefaultValue @())
    if ($reasons -contains "SCOUT_ONLY_BUDGET_EVIDENCE") {
        return $true
    }
    $backtestGate = Get-PropValue -ObjectValue (Get-PropValue -ObjectValue $AcceptanceReport -Name "gates" -DefaultValue @{}) -Name "backtest" -DefaultValue @{}
    $budgetReasons = Get-StringArray -Value (Get-PropValue -ObjectValue $backtestGate -Name "budget_contract_reasons" -DefaultValue @())
    return ($budgetReasons -contains "SCOUT_ONLY_BUDGET_EVIDENCE")
}

function Test-BootstrapOnlyAcceptanceReport {
    param([Parameter(Mandatory = $false)]$AcceptanceReport)
    if (-not (Test-ObjectHasValues -ObjectValue $AcceptanceReport)) {
        return $false
    }
    $reasons = Get-StringArray -Value (Get-PropValue -ObjectValue $AcceptanceReport -Name "reasons" -DefaultValue @())
    if ($reasons -contains "BOOTSTRAP_ONLY_POLICY") {
        return $true
    }
    $candidate = Get-PropValue -ObjectValue $AcceptanceReport -Name "candidate" -DefaultValue @{}
    $splitPolicy = Get-PropValue -ObjectValue $AcceptanceReport -Name "split_policy" -DefaultValue @{}
    $laneMode = [string](Get-PropValue -ObjectValue $candidate -Name "lane_mode" -DefaultValue "")
    if ([string]::IsNullOrWhiteSpace($laneMode)) {
        $laneMode = [string](Get-PropValue -ObjectValue $splitPolicy -Name "lane_mode" -DefaultValue "")
    }
    $promotionEligible = [bool](Get-PropValue -ObjectValue $candidate -Name "promotion_eligible" -DefaultValue $true)
    return (($laneMode -eq "bootstrap_latest_inclusive") -and (-not $promotionEligible))
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
