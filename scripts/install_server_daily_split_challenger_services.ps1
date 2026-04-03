param(
    [string]$ProjectRoot = "",
    [string]$PythonExe = "",
    [string]$WrapperScript = "",
    [string]$AcceptanceScript = "",
    [string]$RuntimeInstallScript = "",
    [string]$CandidateAdoptionScript = "",
    [string]$ServiceUser = "ubuntu",
    [string]$ModelFamily = "train_v5_fusion",
    [string]$ChampionCompareModelFamily = "",
    [string]$PairedPaperModelFamily = "",
    [string]$ChampionUnitName = "autobot-paper-v5.service",
    [string]$ChallengerUnitName = "autobot-paper-v4-challenger.service",
    [string[]]$PromotionTargetUnits = @(),
    [string[]]$CandidateTargetUnits = @(),
    [string]$PromoteServiceUnitName = "autobot-v4-challenger-promote.service",
    [string]$PromoteTimerUnitName = "autobot-v4-challenger-promote.timer",
    [string]$PromoteOnCalendar = "*-*-* 00:10:00",
    [string]$SpawnServiceUnitName = "autobot-v4-challenger-spawn.service",
    [string]$SpawnTimerUnitName = "autobot-v4-challenger-spawn.timer",
    [string]$SpawnOnCalendar = "*-*-* 00:20:00",
    [string]$LockFile = "/tmp/autobot-train-acceptance.lock",
    [string[]]$DisableLegacyTimerNames = @("autobot-daily-micro.timer", "autobot-daily-v4-accept.timer"),
    [string[]]$DisableLegacyServiceNames = @("autobot-daily-micro.service", "autobot-daily-v4-accept.service", "autobot-paper-v4-replay.service", "autobot-live-alpha-replay-shadow.service"),
    [switch]$NoStart,
    [switch]$NoEnable,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

. (Join-Path $PSScriptRoot "systemd_service_utils.ps1")

function Resolve-DefaultWrapperScript {
    param([string]$Root)
    return (Join-Path $Root "scripts/daily_champion_challenger_v5_for_server.ps1")
}

function New-ServiceContent {
    param(
        [string]$Description,
        [string]$UserName,
        [string]$WorkingRoot,
        [string]$ExecStart
    )
    return @"
[Unit]
Description=$Description
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
User=$UserName
WorkingDirectory=$WorkingRoot
Environment=PYTHONUNBUFFERED=1
ExecStart=$ExecStart
TimeoutStartSec=infinity
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
"@
}

function New-TimerContent {
    param(
        [string]$Description,
        [string]$OnCalendar,
        [string]$UnitName
    )
    return @"
[Unit]
Description=$Description Timer

[Timer]
OnCalendar=$OnCalendar
Persistent=true
Unit=$UnitName

[Install]
WantedBy=timers.target
"@
}

function Build-ExecStart {
    param(
        [string]$PwshExe,
        [string]$WrapperPath,
        [string]$Root,
        [string]$PyExe,
        [string]$ModeName,
        [string]$AcceptanceScriptPath,
        [string]$RuntimeInstallScriptPath,
        [string]$CandidateAdoptionScriptPath,
        [string]$ModelFamilyName,
        [string]$ChampionCompareModelFamilyName,
        [string]$PairedPaperModelFamilyName,
        [string]$ChampionUnit,
        [string]$ChallengerUnit,
        [string[]]$ExtraPromotionUnits,
        [string[]]$ExtraCandidateUnits,
        [string]$SharedLockFile
    )
    $argList = @(
        "-NoProfile",
        "-ExecutionPolicy", "Bypass",
        "-File", $WrapperPath,
        "-ProjectRoot", $Root,
        "-PythonExe", $PyExe,
        "-Mode", $ModeName,
        "-ModelFamily", $ModelFamilyName,
        "-ChampionUnitName", $ChampionUnit,
        "-ChallengerUnitName", $ChallengerUnit
    )
    if (-not [string]::IsNullOrWhiteSpace($ChampionCompareModelFamilyName)) {
        $argList += @("-ChampionCompareModelFamily", $ChampionCompareModelFamilyName)
    }
    if (-not [string]::IsNullOrWhiteSpace($AcceptanceScriptPath)) {
        $argList += @("-AcceptanceScript", $AcceptanceScriptPath)
    }
    if (-not [string]::IsNullOrWhiteSpace($RuntimeInstallScriptPath)) {
        $argList += @("-RuntimeInstallScript", $RuntimeInstallScriptPath)
    }
    if (-not [string]::IsNullOrWhiteSpace($CandidateAdoptionScriptPath)) {
        $argList += @("-CandidateAdoptionScript", $CandidateAdoptionScriptPath)
    }
    if (-not [string]::IsNullOrWhiteSpace($PairedPaperModelFamilyName)) {
        $argList += @("-PairedPaperModelFamily", $PairedPaperModelFamilyName)
    }
    if (@($ExtraPromotionUnits).Count -gt 0) {
        $argList += "-PromotionTargetUnits"
        $argList += @($ExtraPromotionUnits)
    }
    if (($ModeName -eq "spawn_only") -and (@($ExtraCandidateUnits).Count -gt 0)) {
        $argList += "-CandidateTargetUnits"
        $argList += @($ExtraCandidateUnits)
    }
    $command = $PwshExe + " " + (($argList | ForEach-Object { Quote-ShellArg ([string]$_) }) -join " ")
    return (Build-FlockWrappedExecStart `
        -Command $command `
        -LockFile $SharedLockFile `
        -BusyMessage ("[daily-split] lock busy for mode=" + $ModeName + ", skipping"))
}

$resolvedProjectRoot = if ([string]::IsNullOrWhiteSpace($ProjectRoot)) { Resolve-DefaultProjectRoot } else { $ProjectRoot }
$resolvedProjectRoot = [System.IO.Path]::GetFullPath($resolvedProjectRoot)
$resolvedPythonExe = if ([string]::IsNullOrWhiteSpace($PythonExe)) { Resolve-DefaultPythonExe -Root $resolvedProjectRoot } else { $PythonExe }
$resolvedWrapperScript = if ([string]::IsNullOrWhiteSpace($WrapperScript)) { Resolve-DefaultWrapperScript -Root $resolvedProjectRoot } else { $WrapperScript }
$resolvedAcceptanceScript = if ([string]::IsNullOrWhiteSpace($AcceptanceScript)) { "" } else { $AcceptanceScript }
$resolvedRuntimeInstallScript = if ([string]::IsNullOrWhiteSpace($RuntimeInstallScript)) { "" } else { $RuntimeInstallScript }
$resolvedCandidateAdoptionScript = if ([string]::IsNullOrWhiteSpace($CandidateAdoptionScript)) { "" } else { $CandidateAdoptionScript }
$resolvedModelFamily = [string]$ModelFamily
$resolvedModelFamily = $resolvedModelFamily.Trim()
if ([string]::IsNullOrWhiteSpace($resolvedModelFamily)) {
    $resolvedModelFamily = Resolve-PreferredModelFamily -Root $resolvedProjectRoot -PreferredFamily "train_v5_fusion"
}
$resolvedChampionCompareModelFamily = [string]$ChampionCompareModelFamily
$resolvedChampionCompareModelFamily = $resolvedChampionCompareModelFamily.Trim()
if ([string]::IsNullOrWhiteSpace($resolvedChampionCompareModelFamily)) {
    $resolvedChampionCompareModelFamily = $resolvedModelFamily
}
$resolvedPairedPaperModelFamily = [string]$PairedPaperModelFamily
$resolvedPairedPaperModelFamily = $resolvedPairedPaperModelFamily.Trim()
if ([string]::IsNullOrWhiteSpace($resolvedPairedPaperModelFamily)) {
    $resolvedPairedPaperModelFamily = $resolvedModelFamily
}
$resolvedPwshExe = Resolve-PwshExe

$promoteExecStart = Build-ExecStart `
    -PwshExe $resolvedPwshExe `
    -WrapperPath $resolvedWrapperScript `
    -Root $resolvedProjectRoot `
    -PyExe $resolvedPythonExe `
    -ModeName "promote_only" `
    -AcceptanceScriptPath $resolvedAcceptanceScript `
    -RuntimeInstallScriptPath $resolvedRuntimeInstallScript `
    -CandidateAdoptionScriptPath $resolvedCandidateAdoptionScript `
    -ModelFamilyName $resolvedModelFamily `
    -ChampionCompareModelFamilyName $resolvedChampionCompareModelFamily `
    -PairedPaperModelFamilyName $resolvedPairedPaperModelFamily `
    -ChampionUnit $ChampionUnitName `
    -ChallengerUnit $ChallengerUnitName `
    -ExtraPromotionUnits $PromotionTargetUnits `
    -ExtraCandidateUnits $CandidateTargetUnits `
    -SharedLockFile $LockFile
$spawnExecStart = Build-ExecStart `
    -PwshExe $resolvedPwshExe `
    -WrapperPath $resolvedWrapperScript `
    -Root $resolvedProjectRoot `
    -PyExe $resolvedPythonExe `
    -ModeName "spawn_only" `
    -AcceptanceScriptPath $resolvedAcceptanceScript `
    -RuntimeInstallScriptPath $resolvedRuntimeInstallScript `
    -CandidateAdoptionScriptPath $resolvedCandidateAdoptionScript `
    -ModelFamilyName $resolvedModelFamily `
    -ChampionCompareModelFamilyName $resolvedChampionCompareModelFamily `
    -PairedPaperModelFamilyName $resolvedPairedPaperModelFamily `
    -ChampionUnit $ChampionUnitName `
    -ChallengerUnit $ChallengerUnitName `
    -ExtraPromotionUnits $PromotionTargetUnits `
    -ExtraCandidateUnits $CandidateTargetUnits `
    -SharedLockFile $LockFile

$promoteServiceContent = New-ServiceContent `
    -Description "Autobot V4 Challenger Promotion" `
    -UserName $ServiceUser `
    -WorkingRoot $resolvedProjectRoot `
    -ExecStart $promoteExecStart
$spawnServiceContent = New-ServiceContent `
    -Description "Autobot V4 Challenger Spawn" `
    -UserName $ServiceUser `
    -WorkingRoot $resolvedProjectRoot `
    -ExecStart $spawnExecStart
$promoteTimerContent = New-TimerContent `
    -Description "Autobot V4 Challenger Promotion" `
    -OnCalendar $PromoteOnCalendar `
    -UnitName $PromoteServiceUnitName
$spawnTimerContent = New-TimerContent `
    -Description "Autobot V4 Challenger Spawn" `
    -OnCalendar $SpawnOnCalendar `
    -UnitName $SpawnServiceUnitName

if ($DryRun) {
    Write-Host ("[daily-split-install][dry-run] service_user={0}" -f $ServiceUser)
    Write-Host ("[daily-split-install][dry-run] pwsh={0}" -f $resolvedPwshExe)
    Write-Host ("[daily-split-install][dry-run] lock_file={0}" -f $LockFile)
    Write-Host ("[daily-split-install][dry-run] model_family={0}" -f $resolvedModelFamily)
    Write-Host ("[daily-split-install][dry-run] champion_compare_model_family={0}" -f $resolvedChampionCompareModelFamily)
    Write-Host ("[daily-split-install][dry-run] paired_paper_model_family={0}" -f $resolvedPairedPaperModelFamily)
    if (-not [string]::IsNullOrWhiteSpace($resolvedAcceptanceScript)) {
        Write-Host ("[daily-split-install][dry-run] acceptance_script={0}" -f $resolvedAcceptanceScript)
    }
    Write-Host ("[daily-split-install][dry-run] promote_service={0}" -f $PromoteServiceUnitName)
    Write-Host $promoteServiceContent
    Write-Host ("[daily-split-install][dry-run] promote_timer={0}" -f $PromoteTimerUnitName)
    Write-Host $promoteTimerContent
    Write-Host ("[daily-split-install][dry-run] spawn_service={0}" -f $SpawnServiceUnitName)
    Write-Host $spawnServiceContent
    Write-Host ("[daily-split-install][dry-run] spawn_timer={0}" -f $SpawnTimerUnitName)
    Write-Host $spawnTimerContent
    foreach ($timerName in @($DisableLegacyTimerNames)) {
        if ([string]::IsNullOrWhiteSpace($timerName)) {
            continue
        }
        Write-Host ("[daily-split-install][dry-run] disable_legacy_timer={0}" -f $timerName)
    }
    foreach ($serviceName in @($DisableLegacyServiceNames)) {
        if ([string]::IsNullOrWhiteSpace($serviceName)) {
            continue
        }
        Write-Host ("[daily-split-install][dry-run] disable_legacy_service={0}" -f $serviceName)
    }
    exit 0
}

Enable-UserLinger -UserName $ServiceUser
Install-UnitFile -UnitName $PromoteServiceUnitName -Content $promoteServiceContent
Install-UnitFile -UnitName $PromoteTimerUnitName -Content $promoteTimerContent
Install-UnitFile -UnitName $SpawnServiceUnitName -Content $spawnServiceContent
Install-UnitFile -UnitName $SpawnTimerUnitName -Content $spawnTimerContent

& sudo systemctl daemon-reload
if ($LASTEXITCODE -ne 0) {
    throw "systemctl daemon-reload failed"
}

foreach ($timerName in @($DisableLegacyTimerNames)) {
    $trimmed = [string]$timerName
    if ([string]::IsNullOrWhiteSpace($trimmed)) {
        continue
    }
    & sudo systemctl disable --now $trimmed 2>$null
}
foreach ($serviceName in @($DisableLegacyServiceNames)) {
    $trimmed = [string]$serviceName
    if ([string]::IsNullOrWhiteSpace($trimmed)) {
        continue
    }
    & sudo systemctl stop $trimmed 2>$null
}

if (-not $NoEnable) {
    foreach ($timerName in @($PromoteTimerUnitName, $SpawnTimerUnitName)) {
        & sudo systemctl enable $timerName
        if ($LASTEXITCODE -ne 0) {
            throw "systemctl enable failed: $timerName"
        }
    }
}
if (-not $NoStart) {
    foreach ($timerName in @($PromoteTimerUnitName, $SpawnTimerUnitName)) {
        & sudo systemctl restart $timerName
        if ($LASTEXITCODE -ne 0) {
            throw "systemctl restart failed: $timerName"
        }
    }
    & systemctl status $PromoteTimerUnitName --no-pager
    & systemctl status $SpawnTimerUnitName --no-pager
}
