param(
    [string]$ProjectRoot = "",
    [string]$PythonExe = "",
    [string]$PaperUnitName = "autobot-paper-v4.service",
    [int]$PaperDurationSec = 0,
    [ValidateSet("live_v3", "live_v4", "live_v4_native", "candidate_v4", "offline_v4", "paired_v4")]
    [string]$PaperPreset = "live_v4",
    [string[]]$PaperCliArgs = @(),
    [string]$PaperRuntimeRole = "",
    [string]$PaperLaneName = "v4",
    [string]$PaperModelRefPinned = "",
    [string]$PaperModelFamilyOverride = "",
    [string]$PaperChampionModelFamilyOverride = "",
    [string]$PaperChallengerModelFamilyOverride = "",
    [switch]$BootstrapChampion,
    [switch]$NoBootstrapChampion,
    [switch]$NoStart,
    [switch]$NoEnable,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

. (Join-Path $PSScriptRoot "systemd_service_utils.ps1")

function Get-PaperRuntimeSpec {
    param([string]$Preset, [string]$UnitName)
    $name = ([string]$Preset).Trim().ToLower()
    switch ($name) {
        "live_v3" {
            return [PSCustomObject]@{
                Description = "Autobot Paper Runtime (v3 live)"
                SyslogIdentifier = "autobot-paper-v3"
                RuntimeModelRef = "champion_v3"
                BootstrapRefs = @("latest_candidate_v3", "latest_v3")
                ModelFamily = "train_v3_mtf_micro"
                RuntimeRole = "champion"
            }
        }
        "live_v4" {
            return [PSCustomObject]@{
                Description = "Autobot Paper Runtime (v4 live)"
                SyslogIdentifier = "autobot-paper-v4"
                RuntimeModelRef = "champion_v4"
                BootstrapRefs = @("latest_candidate_v4", "latest_v4")
                ModelFamily = "train_v4_crypto_cs"
                RuntimeRole = "champion"
            }
        }
        "live_v4_native" {
            return [PSCustomObject]@{
                Description = "Autobot Paper Runtime (v4 live native)"
                SyslogIdentifier = "autobot-paper-v4-native"
                RuntimeModelRef = "champion_v4"
                BootstrapRefs = @("latest_candidate_v4", "latest_v4")
                ModelFamily = "train_v4_crypto_cs"
                RuntimeRole = "champion"
            }
        }
        "offline_v4" {
            return [PSCustomObject]@{
                Description = "Autobot Paper Runtime (v4 offline)"
                SyslogIdentifier = "autobot-paper-v4-offline"
                RuntimeModelRef = "champion_v4"
                BootstrapRefs = @("latest_candidate_v4", "latest_v4")
                ModelFamily = "train_v4_crypto_cs"
                RuntimeRole = "champion"
            }
        }
        "candidate_v4" {
            return [PSCustomObject]@{
                Description = "Autobot Paper Runtime (v4 candidate)"
                SyslogIdentifier = "autobot-paper-v4-candidate"
                RuntimeModelRef = "latest_candidate_v4"
                BootstrapRefs = @()
                ModelFamily = "train_v4_crypto_cs"
                RuntimeRole = "challenger"
            }
        }
        "paired_v4" {
            return [PSCustomObject]@{
                Description = "Autobot Paired Paper Runtime (v4)"
                SyslogIdentifier = "autobot-paper-v4-paired"
                RuntimeModelRef = ""
                BootstrapRefs = @()
                ModelFamily = "train_v4_crypto_cs"
                RuntimeRole = "paired"
            }
        }
        default {
            return [PSCustomObject]@{
                Description = "Autobot Paper Runtime ($Preset)"
                SyslogIdentifier = ($UnitName -replace '\.service$', '')
                RuntimeModelRef = ""
                BootstrapRefs = @()
                ModelFamily = ""
                RuntimeRole = "unspecified"
            }
        }
    }
}

function Test-RegistryPointerExists {
    param(
        [string]$Root,
        [string]$ModelFamily,
        [string]$PointerName
    )
    if ([string]::IsNullOrWhiteSpace($Root) -or [string]::IsNullOrWhiteSpace($ModelFamily) -or [string]::IsNullOrWhiteSpace($PointerName)) {
        return $false
    }
    $pointerPath = Join-Path $Root ("models/registry/" + $ModelFamily + "/" + $PointerName + ".json")
    return (Test-Path $pointerPath)
}

function Invoke-ExternalCommand {
    param(
        [string]$Exe,
        [string[]]$ArgList,
        [string]$ErrorLabel
    )
    & $Exe @ArgList
    if ($LASTEXITCODE -ne 0) {
        throw "$ErrorLabel failed"
    }
}

function Stop-And-Disable-UnitBestEffort {
    param([string]$UnitName)
    if ([string]::IsNullOrWhiteSpace($UnitName)) {
        return
    }
    & sudo systemctl stop $UnitName 2>$null
    & sudo systemctl disable $UnitName 2>$null
}

$resolvedProjectRoot = if ([string]::IsNullOrWhiteSpace($ProjectRoot)) { Resolve-DefaultProjectRoot } else { $ProjectRoot }
$resolvedProjectRoot = [System.IO.Path]::GetFullPath($resolvedProjectRoot)
$resolvedPythonExe = if ([string]::IsNullOrWhiteSpace($PythonExe)) { Resolve-DefaultPythonExe -Root $resolvedProjectRoot } else { $PythonExe }
$runtimeSpec = Get-PaperRuntimeSpec -Preset $PaperPreset -UnitName $PaperUnitName
$effectiveRuntimeRole = if ([string]::IsNullOrWhiteSpace($PaperRuntimeRole)) { [string]$runtimeSpec.RuntimeRole } else { [string]$PaperRuntimeRole }
$normalizedRuntimeRole = ([string]$effectiveRuntimeRole).Trim().ToLowerInvariant()
$effectiveRuntimeModelFamily = if ([string]::IsNullOrWhiteSpace($PaperModelFamilyOverride)) {
    [string]$runtimeSpec.ModelFamily
} else {
    [string]$PaperModelFamilyOverride
}
$effectivePairedChampionModelFamily = if ([string]::IsNullOrWhiteSpace($PaperChampionModelFamilyOverride)) {
    $effectiveRuntimeModelFamily
} else {
    [string]$PaperChampionModelFamilyOverride
}
$effectivePairedChallengerModelFamily = if ([string]::IsNullOrWhiteSpace($PaperChallengerModelFamilyOverride)) {
    $effectiveRuntimeModelFamily
} else {
    [string]$PaperChallengerModelFamilyOverride
}
$defaultsToChampionSource = (
    -not [string]::IsNullOrWhiteSpace($runtimeSpec.RuntimeModelRef) `
    -and $runtimeSpec.RuntimeModelRef.StartsWith("champion_")
)
$defaultsToLatestCandidateSource = (
    -not [string]::IsNullOrWhiteSpace($runtimeSpec.RuntimeModelRef) `
    -and $runtimeSpec.RuntimeModelRef.StartsWith("latest_candidate")
)
$pinnedModelRefPresent = (-not [string]::IsNullOrWhiteSpace($PaperModelRefPinned))
$resolvedPaperCliArgs = @(Expand-DelimitedStringArray -Value $PaperCliArgs)
$requiresPinnedCandidateModel = (
    ($normalizedRuntimeRole -eq "challenger") `
    -and $defaultsToChampionSource `
    -and (-not $pinnedModelRefPresent)
)
if ($requiresPinnedCandidateModel) {
    throw (
        "runtime role '{0}' cannot default to champion source '{1}'; pass -PaperModelRefPinned or use candidate_v4 preset" -f
        $effectiveRuntimeRole,
        $runtimeSpec.RuntimeModelRef
    )
}
$requiresChampionSource = (
    ($normalizedRuntimeRole -eq "champion") `
    -and $defaultsToLatestCandidateSource `
    -and (-not $pinnedModelRefPresent)
)
if ($requiresChampionSource) {
    throw (
        "runtime role '{0}' cannot default to candidate source '{1}'; use a champion preset or pass an explicit pinned champion ref" -f
        $effectiveRuntimeRole,
        $runtimeSpec.RuntimeModelRef
    )
}
$requiresChampionPointer = (
    -not [string]::IsNullOrWhiteSpace($runtimeSpec.RuntimeModelRef) `
    -and $runtimeSpec.RuntimeModelRef.StartsWith("champion_")
)
$championPointerMissing = (
    $requiresChampionPointer `
    -and -not (Test-RegistryPointerExists -Root $resolvedProjectRoot -ModelFamily $effectiveRuntimeModelFamily -PointerName "champion")
)
if ($BootstrapChampion -and $NoBootstrapChampion) {
    throw "BootstrapChampion and NoBootstrapChampion cannot both be set"
}

if (
    -not $DryRun `
    -and `
    $BootstrapChampion `
    -and `
    $championPointerMissing
) {
    $bootstrapCompleted = $false
    foreach ($bootstrapRef in @($runtimeSpec.BootstrapRefs)) {
        if ([string]::IsNullOrWhiteSpace($bootstrapRef)) {
            continue
        }
        try {
            Write-Host ("[paper-install][bootstrap] preset={0} bootstrap_ref={1}" -f $PaperPreset, $bootstrapRef)
            Invoke-ExternalCommand -Exe $resolvedPythonExe -ArgList @(
                "-m", "autobot.cli",
                "model", "promote",
                "--model-ref", $bootstrapRef,
                "--model-family", $effectiveRuntimeModelFamily
            ) -ErrorLabel ("model promote " + $bootstrapRef)
            $bootstrapCompleted = $true
            break
        } catch {
            Write-Warning ("[paper-install][bootstrap] failed ref={0}: {1}" -f $bootstrapRef, $_.Exception.Message)
        }
    }
    if (-not $bootstrapCompleted -and -not (Test-RegistryPointerExists -Root $resolvedProjectRoot -ModelFamily $effectiveRuntimeModelFamily -PointerName "champion")) {
        throw ("runtime preset '{0}' requires champion pointer for family '{1}', but bootstrap failed" -f $PaperPreset, $effectiveRuntimeModelFamily)
    }
}
if (
    -not $DryRun `
    -and `
    -not $NoStart `
    -and `
    $championPointerMissing `
    -and `
    (-not $BootstrapChampion)
) {
    throw ("runtime preset '{0}' requires champion pointer for family '{1}', but install no longer auto-bootstraps. Promote explicitly or rerun with -BootstrapChampion." -f $PaperPreset, $effectiveRuntimeModelFamily)
}

$paperArgList = @()
if ($PaperPreset -eq "paired_v4") {
    $paperArgList = @(
        "-m", "autobot.paper.paired_runtime",
        "run-service",
        "--project-root", $resolvedProjectRoot,
        "--config-dir", "config",
        "--quote", "KRW",
        "--top-n", "20",
        "--tf", "5m",
        "--model-family", $effectiveRuntimeModelFamily,
        "--champion-model-family", $effectivePairedChampionModelFamily,
        "--challenger-model-family", $effectivePairedChallengerModelFamily,
        "--feature-set", "v4",
        "--preset", "live_v4",
        "--paper-feature-provider", "live_v4",
        "--paper-micro-provider", "live_ws",
        "--paper-micro-warmup-sec", "60",
        "--paper-micro-warmup-min-trade-events-per-market", "1",
        "--out-dir", "logs/paired_paper",
        "--min-matched-opportunities", "1"
    ) + $resolvedPaperCliArgs
} else {
    $paperArgList = @(
        "-m", "autobot.cli",
        "paper", "alpha",
        "--duration-sec", [string]([Math]::Max($PaperDurationSec, 0)),
        "--preset", $PaperPreset
    ) + $resolvedPaperCliArgs
}
$paperCommand = ($paperArgList | ForEach-Object { Quote-ShellArg ([string]$_) }) -join " "
$activatePath = Join-Path $resolvedProjectRoot ".venv/bin/activate"
$execStart = "/bin/bash -lc " + (Quote-ShellArg ("source " + $activatePath + " && " + $resolvedPythonExe + " " + $paperCommand))
$conditionPathExistsLine = if ($PaperPreset -eq "paired_v4") {
    "ConditionPathExists=" + (Join-Path $resolvedProjectRoot "logs/model_v4_challenger/current_state.json")
} else {
    ""
}

$paperUnitContent = @"
[Unit]
Description=$($runtimeSpec.Description)
After=network-online.target
Wants=network-online.target
$conditionPathExistsLine

[Service]
Type=simple
User=ubuntu
WorkingDirectory=$resolvedProjectRoot
Environment=PYTHONUNBUFFERED=1
Environment=AUTOBOT_PAPER_PRESET=$PaperPreset
Environment=AUTOBOT_PAPER_UNIT_NAME=$PaperUnitName
Environment=AUTOBOT_PAPER_RUNTIME_ROLE=$effectiveRuntimeRole
Environment=AUTOBOT_PAPER_LANE=$PaperLaneName
Environment=AUTOBOT_PAPER_MODEL_REF_PINNED=$PaperModelRefPinned
Environment=AUTOBOT_RUNTIME_MODEL_REF_SOURCE=$($runtimeSpec.RuntimeModelRef)
Environment=AUTOBOT_RUNTIME_MODEL_FAMILY=$effectiveRuntimeModelFamily
Environment=AUTOBOT_PAIRED_CHAMPION_MODEL_FAMILY=$effectivePairedChampionModelFamily
Environment=AUTOBOT_PAIRED_CHALLENGER_MODEL_FAMILY=$effectivePairedChallengerModelFamily
SyslogIdentifier=$($runtimeSpec.SyslogIdentifier)
ExecStart=$execStart
Restart=always
RestartSec=15
TimeoutStopSec=30
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
"@

if ($DryRun) {
    Write-Host ("[paper-install][dry-run] unit={0}" -f $PaperUnitName)
    Write-Host ("[paper-install][dry-run] bootstrap_champion={0}" -f ([bool]$BootstrapChampion))
    if ($PaperPreset -eq "paired_v4") {
        Write-Host ("[paper-install][dry-run] paired_champion_model_family={0}" -f $effectivePairedChampionModelFamily)
        Write-Host ("[paper-install][dry-run] paired_challenger_model_family={0}" -f $effectivePairedChallengerModelFamily)
    }
    Write-Host $paperUnitContent
    exit 0
}

Install-UnitFile -UnitName $PaperUnitName -Content $paperUnitContent
& sudo systemctl daemon-reload
if ($LASTEXITCODE -ne 0) {
    throw "systemctl daemon-reload failed"
}
if (-not $NoEnable) {
    & sudo systemctl enable $PaperUnitName
    if ($LASTEXITCODE -ne 0) {
        throw "systemctl enable failed: $PaperUnitName"
    }
}
if (-not $NoStart) {
    & sudo systemctl restart $PaperUnitName
    if ($LASTEXITCODE -ne 0) {
        throw "systemctl restart failed: $PaperUnitName"
    }
}

if ($PaperPreset -eq "paired_v4") {
    foreach ($legacyUnit in @("autobot-paper-v4.service", "autobot-paper-v4-challenger.service")) {
        if ($legacyUnit -eq $PaperUnitName) {
            continue
        }
        Stop-And-Disable-UnitBestEffort -UnitName $legacyUnit
    }
}

& systemctl status $PaperUnitName --no-pager
