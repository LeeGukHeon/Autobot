param(
    [string]$ProjectRoot = "",
    [string]$PythonExe = "",
    [string]$DailySplitInstallScript = "",
    [string]$RuntimeInstallScript = "",
    [string]$LiveInstallScript = "",
    [string]$ArchiveRoot = "",
    [switch]$NoInstall,
    [switch]$NoStart,
    [switch]$NoEnable,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

. (Join-Path $PSScriptRoot "systemd_service_utils.ps1")

function Resolve-DefaultDailySplitInstallScript {
    param([string]$Root)
    return (Join-Path $Root "scripts/install_server_daily_v5_split_challenger_services.ps1")
}

function Invoke-ExternalCapture {
    param(
        [string]$Exe,
        [string[]]$ArgList
    )
    & $Exe @ArgList
    if ($LASTEXITCODE -ne 0) {
        throw ("command failed: " + $Exe + " " + ($ArgList -join " "))
    }
}

function Stop-And-Disable-Unit {
    param([string]$UnitName)
    if ([string]::IsNullOrWhiteSpace($UnitName)) {
        return
    }
    if ($DryRun) {
        Write-Host ("[v5-lane-migrate][dry-run] stop_disable=" + $UnitName)
        return
    }
    & sudo systemctl stop $UnitName 2>$null
    & sudo systemctl disable $UnitName 2>$null
}

function Move-ToArchive {
    param(
        [string]$SourcePath,
        [string]$ArchiveBase
    )
    if ([string]::IsNullOrWhiteSpace($SourcePath) -or (-not (Test-Path $SourcePath))) {
        return ""
    }
    $leafName = Split-Path -Leaf $SourcePath
    $destination = Join-Path $ArchiveBase $leafName
    if ($DryRun) {
        Write-Host ("[v5-lane-migrate][dry-run] archive " + $SourcePath + " -> " + $destination)
        return $destination
    }
    Move-Item -LiteralPath $SourcePath -Destination $destination -Force
    return $destination
}

function Copy-StateForward {
    param(
        [string]$LegacyRoot,
        [string]$TargetRoot
    )
    if ([string]::IsNullOrWhiteSpace($LegacyRoot) -or [string]::IsNullOrWhiteSpace($TargetRoot)) {
        return
    }
    foreach ($fileName in @("current_state.json", "latest.json", "latest_candidate_adoption.json", "step_06_promote.json")) {
        $source = Join-Path $LegacyRoot $fileName
        $target = Join-Path $TargetRoot $fileName
        if (-not (Test-Path $source)) {
            continue
        }
        if ($DryRun) {
            Write-Host ("[v5-lane-migrate][dry-run] copy_state " + $source + " -> " + $target)
            continue
        }
        New-Item -ItemType Directory -Force -Path (Split-Path -Parent $target) | Out-Null
        Copy-Item -LiteralPath $source -Destination $target -Force
    }
}

function Copy-FileWithSqliteSidecars {
    param(
        [string]$SourcePath,
        [string]$TargetPath,
        [string]$LogPrefix
    )
    foreach ($suffix in @("", "-wal", "-shm")) {
        $source = $SourcePath + $suffix
        if (-not (Test-Path $source)) {
            continue
        }
        $target = $TargetPath + $suffix
        if ($DryRun) {
            Write-Host ("[{0}][dry-run] copy_state_db {1} -> {2}" -f $LogPrefix, $source, $target)
            continue
        }
        New-Item -ItemType Directory -Force -Path (Split-Path -Parent $target) | Out-Null
        Copy-Item -LiteralPath $source -Destination $target -Force
    }
}

function Copy-StateDbForward {
    param(
        [string]$LegacyDbPath,
        [string]$TargetDbPath
    )
    if ([string]::IsNullOrWhiteSpace($LegacyDbPath) -or [string]::IsNullOrWhiteSpace($TargetDbPath)) {
        return
    }
    if (-not (Test-Path $LegacyDbPath)) {
        return
    }
    if (Test-Path $TargetDbPath) {
        return
    }
    Copy-FileWithSqliteSidecars -SourcePath $LegacyDbPath -TargetPath $TargetDbPath -LogPrefix "v5-lane-migrate"
}

$resolvedProjectRoot = if ([string]::IsNullOrWhiteSpace($ProjectRoot)) { Resolve-DefaultProjectRoot } else { [System.IO.Path]::GetFullPath($ProjectRoot) }
$resolvedPythonExe = if ([string]::IsNullOrWhiteSpace($PythonExe)) { Resolve-DefaultPythonExe -Root $resolvedProjectRoot } else { $PythonExe }
$resolvedDailySplitInstallScript = if ([string]::IsNullOrWhiteSpace($DailySplitInstallScript)) { Resolve-DefaultDailySplitInstallScript -Root $resolvedProjectRoot } else { $DailySplitInstallScript }
$resolvedRuntimeInstallScript = if ([string]::IsNullOrWhiteSpace($RuntimeInstallScript)) { Join-Path $resolvedProjectRoot "scripts/install_server_runtime_services.ps1" } else { $RuntimeInstallScript }
$resolvedLiveInstallScript = if ([string]::IsNullOrWhiteSpace($LiveInstallScript)) { Join-Path $resolvedProjectRoot "scripts/install_server_live_runtime_service.ps1" } else { $LiveInstallScript }
$resolvedArchiveRoot = if ([string]::IsNullOrWhiteSpace($ArchiveRoot)) {
    Join-Path $resolvedProjectRoot ("logs/archive_pre_v5_lane_migration/" + (Get-Date -Format "yyyyMMdd-HHmmss"))
} else {
    $ArchiveRoot
}

$legacyStateRoot = Join-Path $resolvedProjectRoot "logs/model_v4_challenger"
$v5StateRoot = Join-Path $resolvedProjectRoot "logs/model_v5_candidate"
$legacyCandidateDbPath = Join-Path $resolvedProjectRoot "data/state/live_candidate/live_state.db"
$v5CanaryDbPath = Join-Path $resolvedProjectRoot "data/state/live_canary/live_state.db"
$legacyCanarySlug = "autobot_live_alpha_candidate_service"
$legacyRolloutPath = Join-Path $resolvedProjectRoot ("logs/live_rollout/latest." + $legacyCanarySlug + ".json")
$legacyCanaryConfidenceRoot = Join-Path $resolvedProjectRoot ("logs/canary_confidence_sequence/" + $legacyCanarySlug)

if (-not $DryRun) {
    New-Item -ItemType Directory -Force -Path $resolvedArchiveRoot | Out-Null
}

foreach ($unitName in @(
    "autobot-paper-v4-challenger.service",
    "autobot-paper-v4-paired.service",
    "autobot-live-alpha-candidate.service",
    "autobot-v4-challenger-spawn.service",
    "autobot-v4-challenger-promote.service",
    "autobot-v4-challenger-spawn.timer",
    "autobot-v4-challenger-promote.timer"
)) {
    Stop-And-Disable-Unit -UnitName $unitName
}

Copy-StateForward -LegacyRoot $legacyStateRoot -TargetRoot $v5StateRoot
Copy-StateDbForward -LegacyDbPath $legacyCandidateDbPath -TargetDbPath $v5CanaryDbPath
Move-ToArchive -SourcePath $legacyCanaryConfidenceRoot -ArchiveBase $resolvedArchiveRoot | Out-Null
Move-ToArchive -SourcePath $legacyRolloutPath -ArchiveBase $resolvedArchiveRoot | Out-Null
Move-ToArchive -SourcePath $legacyStateRoot -ArchiveBase $resolvedArchiveRoot | Out-Null

if (-not $NoInstall) {
    $pwshExe = Resolve-PwshExe
    $dailyArgs = @(
        "-NoProfile",
        "-ExecutionPolicy", "Bypass",
        "-File", $resolvedDailySplitInstallScript,
        "-ProjectRoot", $resolvedProjectRoot,
        "-PythonExe", $resolvedPythonExe
    )
    if ($NoStart) { $dailyArgs += "-NoStart" }
    if ($NoEnable) { $dailyArgs += "-NoEnable" }
    if ($DryRun) { $dailyArgs += "-DryRun" }
    Invoke-ExternalCapture -Exe $pwshExe -ArgList $dailyArgs

    $paperChampionArgs = @(
        "-NoProfile",
        "-ExecutionPolicy", "Bypass",
        "-File", $resolvedRuntimeInstallScript,
        "-ProjectRoot", $resolvedProjectRoot,
        "-PythonExe", $resolvedPythonExe,
        "-PaperUnitName", "autobot-paper-v5.service",
        "-PaperPreset", "live_v5",
        "-PaperModelFamilyOverride", "train_v5_fusion"
    )
    if ($NoStart) { $paperChampionArgs += "-NoStart" }
    if ($NoEnable) { $paperChampionArgs += "-NoEnable" }
    if ($DryRun) { $paperChampionArgs += "-DryRun" }
    Invoke-ExternalCapture -Exe $pwshExe -ArgList $paperChampionArgs

    $paperPairedArgs = @(
        "-NoProfile",
        "-ExecutionPolicy", "Bypass",
        "-File", $resolvedRuntimeInstallScript,
        "-ProjectRoot", $resolvedProjectRoot,
        "-PythonExe", $resolvedPythonExe,
        "-PaperUnitName", "autobot-paper-v5-paired.service",
        "-PaperPreset", "paired_v5",
        "-PairedPaperTf", "1m",
        "-PaperRuntimeRole", "paired",
        "-PaperModelFamilyOverride", "train_v5_fusion",
        "-PaperChampionModelFamilyOverride", "train_v5_fusion",
        "-PaperChallengerModelFamilyOverride", "train_v5_fusion",
        "-PairedStateRootRelPath", "logs/model_v5_candidate"
    )
    if ($NoStart) { $paperPairedArgs += "-NoStart" }
    if ($NoEnable) { $paperPairedArgs += "-NoEnable" }
    if ($DryRun) { $paperPairedArgs += "-DryRun" }
    Invoke-ExternalCapture -Exe $pwshExe -ArgList $paperPairedArgs

    $liveArgs = @(
        "-NoProfile",
        "-ExecutionPolicy", "Bypass",
        "-File", $resolvedLiveInstallScript,
        "-ProjectRoot", $resolvedProjectRoot,
        "-PythonExe", $resolvedPythonExe,
        "-UnitName", "autobot-live-alpha-canary.service",
        "-RolloutMode", "canary",
        "-RolloutTargetUnit", "autobot-live-alpha-canary.service",
        "-StateDbPath", "data/state/live_canary/live_state.db",
        "-ModelRefSource", "latest_candidate",
        "-ModelFamily", "train_v5_fusion",
        "-StrategyTf", "1m",
        "-StrategyRuntime"
    )
    if ($NoStart) { $liveArgs += "-NoStart" }
    if ($NoEnable) { $liveArgs += "-NoEnable" }
    if ($DryRun) { $liveArgs += "-DryRun" }
    Invoke-ExternalCapture -Exe $pwshExe -ArgList $liveArgs
}

Write-Host ("[v5-lane-migrate] archive_root=" + $resolvedArchiveRoot)
Write-Host ("[v5-lane-migrate] state_root=" + $v5StateRoot)
