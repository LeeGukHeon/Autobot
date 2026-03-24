$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

function Load-V4StateJsonOrEmpty {
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

function Write-V4StateJsonFile {
    param(
        [string]$PathValue,
        $Payload,
        [int]$Depth = 20
    )
    if ([string]::IsNullOrWhiteSpace($PathValue)) {
        return
    }
    $parent = Split-Path -Path $PathValue -Parent
    if (-not [string]::IsNullOrWhiteSpace($parent)) {
        New-Item -ItemType Directory -Force -Path $parent | Out-Null
    }
    $json = $Payload | ConvertTo-Json -Depth $Depth
    Set-Content -Path $PathValue -Value $json -Encoding UTF8
}

function Get-V4StatePropValue {
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
    $propertyNames = @(
        $ObjectValue.PSObject.Properties |
            Where-Object { $null -ne $_ -and $null -ne $_.Name } |
            ForEach-Object { [string]$_.Name }
    )
    if ($propertyNames -contains $Name) {
        return $ObjectValue.$Name
    }
    return $DefaultValue
}

function ConvertTo-V4StateBool {
    param(
        [Parameter(Mandatory = $false)]$Value,
        [bool]$DefaultValue = $false
    )
    if ($null -eq $Value) {
        return [bool]$DefaultValue
    }
    if ($Value -is [bool]) {
        return [bool]$Value
    }
    $text = ([string]$Value).Trim()
    if ([string]::IsNullOrWhiteSpace($text)) {
        return [bool]$DefaultValue
    }
    switch -Regex ($text.ToLowerInvariant()) {
        "^(1|true|yes|on)$" { return $true }
        "^(0|false|no|off)$" { return $false }
        default { return [bool]$DefaultValue }
    }
}

function Resolve-V4OptionalBool {
    param(
        [AllowNull()][Nullable[bool]]$Value,
        [bool]$DefaultValue = $false
    )
    if ($null -eq $Value) {
        return [bool]$DefaultValue
    }
    return [bool]$Value
}

function Resolve-V4RegistryPointerPath {
    param(
        [string]$RegistryRoot,
        [string]$Family,
        [string]$PointerName
    )
    return (Join-Path (Join-Path $RegistryRoot $Family) ($PointerName + ".json"))
}

function Update-V4LatestCandidatePointers {
    param(
        [string]$RegistryRoot,
        [string]$Family,
        [string]$RunId,
        [switch]$DryRun
    )
    if ([string]::IsNullOrWhiteSpace($RegistryRoot) -or [string]::IsNullOrWhiteSpace($Family) -or [string]::IsNullOrWhiteSpace($RunId)) {
        return @{}
    }
    $updatedAtUtc = (Get-Date).ToUniversalTime().ToString("o")
    $familyPath = Resolve-V4RegistryPointerPath -RegistryRoot $RegistryRoot -Family $Family -PointerName "latest_candidate"
    $globalPath = Join-Path $RegistryRoot "latest_candidate.json"
    if (-not $DryRun) {
        Write-V4StateJsonFile -PathValue $familyPath -Payload ([ordered]@{
            run_id = $RunId
            updated_at_utc = $updatedAtUtc
        })
        Write-V4StateJsonFile -PathValue $globalPath -Payload ([ordered]@{
            run_id = $RunId
            model_family = $Family
            updated_at_utc = $updatedAtUtc
        })
    }
    return [ordered]@{
        family_path = $familyPath
        global_path = $globalPath
        updated_at_utc = $updatedAtUtc
    }
}

function Clear-V4LatestCandidatePointers {
    param(
        [string]$RegistryRoot,
        [string]$Family,
        [switch]$DryRun
    )
    $familyPath = Resolve-V4RegistryPointerPath -RegistryRoot $RegistryRoot -Family $Family -PointerName "latest_candidate"
    $globalPath = Join-Path $RegistryRoot "latest_candidate.json"
    $removedPaths = New-Object System.Collections.Generic.List[string]
    $missingPaths = New-Object System.Collections.Generic.List[string]
    foreach ($path in @($familyPath, $globalPath)) {
        if ([string]::IsNullOrWhiteSpace($path)) {
            continue
        }
        if (-not (Test-Path $path)) {
            $missingPaths.Add($path) | Out-Null
            continue
        }
        if (-not $DryRun) {
            Remove-Item -Path $path -Force -ErrorAction Stop
        }
        $removedPaths.Add($path) | Out-Null
    }
    return [ordered]@{
        attempted = $true
        removed_paths = @($removedPaths.ToArray())
        missing_paths = @($missingPaths.ToArray())
    }
}

function Resolve-V4ArtifactStatusPath {
    param([string]$RunDir)
    if ([string]::IsNullOrWhiteSpace($RunDir)) {
        return ""
    }
    return (Join-Path $RunDir "artifact_status.json")
}

function Resolve-V4ChampionRunId {
    param([string]$Root)
    $pointerPath = Join-Path $Root "models/registry/train_v4_crypto_cs/champion.json"
    $pointer = Load-V4StateJsonOrEmpty -PathValue $pointerPath
    return [string](Get-V4StatePropValue -ObjectValue $pointer -Name "run_id" -DefaultValue "")
}

function Update-V4RunArtifactStatus {
    param(
        [string]$RunDir,
        [string]$RunId,
        [string]$Status = "",
        [AllowNull()][Nullable[bool]]$AcceptanceCompleted = $null,
        [AllowNull()][Nullable[bool]]$CandidateAdoptable = $null,
        [AllowNull()][Nullable[bool]]$CandidateAdopted = $null,
        [AllowNull()][Nullable[bool]]$Promoted = $null,
        [switch]$DryRun
    )
    if ($DryRun -or [string]::IsNullOrWhiteSpace($RunDir) -or (-not (Test-Path $RunDir))) {
        return @{}
    }
    $artifactStatusPath = Resolve-V4ArtifactStatusPath -RunDir $RunDir
    $existing = Load-V4StateJsonOrEmpty -PathValue $artifactStatusPath
    $resolvedRunId = [string]$RunId
    if ([string]::IsNullOrWhiteSpace($resolvedRunId)) {
        $resolvedRunId = [string](Get-V4StatePropValue -ObjectValue $existing -Name "run_id" -DefaultValue "")
    }
    if ([string]::IsNullOrWhiteSpace($resolvedRunId)) {
        $resolvedRunId = Split-Path -Leaf $RunDir
    }
    $payload = [ordered]@{
        run_id = $resolvedRunId
        status = if ([string]::IsNullOrWhiteSpace($Status)) {
            [string](Get-V4StatePropValue -ObjectValue $existing -Name "status" -DefaultValue "pending")
        } else {
            $Status
        }
        core_saved = ConvertTo-V4StateBool (Get-V4StatePropValue -ObjectValue $existing -Name "core_saved" -DefaultValue $false) $false
        support_artifacts_written = ConvertTo-V4StateBool (Get-V4StatePropValue -ObjectValue $existing -Name "support_artifacts_written" -DefaultValue $false) $false
        execution_acceptance_complete = ConvertTo-V4StateBool (Get-V4StatePropValue -ObjectValue $existing -Name "execution_acceptance_complete" -DefaultValue $false) $false
        runtime_recommendations_complete = ConvertTo-V4StateBool (Get-V4StatePropValue -ObjectValue $existing -Name "runtime_recommendations_complete" -DefaultValue $false) $false
        governance_artifacts_complete = ConvertTo-V4StateBool (Get-V4StatePropValue -ObjectValue $existing -Name "governance_artifacts_complete" -DefaultValue $false) $false
        acceptance_completed = Resolve-V4OptionalBool -Value $AcceptanceCompleted -DefaultValue (ConvertTo-V4StateBool (Get-V4StatePropValue -ObjectValue $existing -Name "acceptance_completed" -DefaultValue $false) $false)
        candidate_adoptable = Resolve-V4OptionalBool -Value $CandidateAdoptable -DefaultValue (ConvertTo-V4StateBool (Get-V4StatePropValue -ObjectValue $existing -Name "candidate_adoptable" -DefaultValue $false) $false)
        candidate_adopted = Resolve-V4OptionalBool -Value $CandidateAdopted -DefaultValue (ConvertTo-V4StateBool (Get-V4StatePropValue -ObjectValue $existing -Name "candidate_adopted" -DefaultValue $false) $false)
        promoted = Resolve-V4OptionalBool -Value $Promoted -DefaultValue (ConvertTo-V4StateBool (Get-V4StatePropValue -ObjectValue $existing -Name "promoted" -DefaultValue $false) $false)
        updated_at_utc = (Get-Date).ToUniversalTime().ToString("o")
    }
    Write-V4StateJsonFile -PathValue $artifactStatusPath -Payload $payload
    return [ordered]@{
        path = $artifactStatusPath
        payload = $payload
    }
}
