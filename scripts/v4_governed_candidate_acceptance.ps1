# Governed acceptance selects the current promotable lane automatically from the
# latest rank-shadow governance action. It defaults to the cls primary lane.
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
    if ($ObjectValue.PSObject -and $ObjectValue.PSObject.Properties.Name -contains $Name) {
        return $ObjectValue.$Name
    }
    return $DefaultValue
}

function Convert-ToBooleanLike {
    param(
        [Parameter(Mandatory = $false)]$Value,
        [bool]$DefaultValue = $true
    )
    if ($null -eq $Value) {
        return $DefaultValue
    }
    $text = [string]$Value
    if ([string]::IsNullOrWhiteSpace($text)) {
        return $DefaultValue
    }
    switch ($text.Trim().ToLowerInvariant()) {
        "1" { return $true }
        "true" { return $true }
        "yes" { return $true }
        "on" { return $true }
        "0" { return $false }
        "false" { return $false }
        "no" { return $false }
        "off" { return $false }
        default { return $DefaultValue }
    }
}

function Test-RankShadowGovernanceEnabled {
    $envOverride = $env:AUTOBOT_RANK_SHADOW_GOVERNANCE
    if (-not [string]::IsNullOrWhiteSpace([string]$envOverride)) {
        return (Convert-ToBooleanLike -Value $envOverride -DefaultValue $true)
    }
    $systemctl = Get-Command systemctl -ErrorAction SilentlyContinue
    if ($null -eq $systemctl -or [string]::IsNullOrWhiteSpace([string]$systemctl.Source)) {
        return $true
    }
    & $systemctl.Source is-enabled autobot-v4-rank-shadow.timer *> $null
    return ($LASTEXITCODE -eq 0)
}

$projectRoot = Split-Path -Path $PSScriptRoot -Parent
$governancePath = Join-Path $projectRoot "logs/model_v4_rank_shadow_cycle/latest_governance_action.json"
$resolvedGovernancePath = [System.IO.Path]::GetFullPath($governancePath)
$rankShadowGovernanceEnabled = Test-RankShadowGovernanceEnabled
$governance = if ($rankShadowGovernanceEnabled) {
    Load-JsonOrEmpty -PathValue $resolvedGovernancePath
} else {
    @{}
}
$selectedScriptName = [string](Get-PropValue -ObjectValue $governance -Name "selected_acceptance_script" -DefaultValue "")
if ([string]::IsNullOrWhiteSpace($selectedScriptName)) {
    $selectedScriptName = "v4_promotable_candidate_acceptance.ps1"
}
$selectedScriptPath = Join-Path $PSScriptRoot $selectedScriptName
if (-not (Test-Path $selectedScriptPath)) {
    $selectedScriptName = "v4_promotable_candidate_acceptance.ps1"
    $selectedScriptPath = Join-Path $PSScriptRoot $selectedScriptName
}

Write-Host ("[v4-governed] governance_path={0}" -f $resolvedGovernancePath)
Write-Host ("[v4-governed] rank_shadow_governance_enabled={0}" -f ([string]$rankShadowGovernanceEnabled).ToLowerInvariant())
Write-Host ("[v4-governed] selected_acceptance_script={0}" -f $selectedScriptPath)

& $selectedScriptPath @args
exit $LASTEXITCODE
