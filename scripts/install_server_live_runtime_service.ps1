param(
    [string]$ProjectRoot = "",
    [string]$PythonExe = "",
    [string]$ServiceUser = "ubuntu",
    [string]$UnitName = "autobot-live-alpha.service",
    [string]$BotId = "",
    [string]$StateDbPath = "",
    [string]$ModelRefSource = "",
    [string]$ModelFamily = "",
    [string]$ModelRegistryRoot = "",
    [string]$SmallAccountMaxPositions = "",
    [string]$SmallAccountMaxOpenOrdersPerMarket = "",
    [string]$StrategyTf = "",
    [ValidateSet("shadow", "canary", "live")]
    [string]$RolloutMode = "shadow",
    [ValidateSet("poll", "private_ws", "executor_ws")]
    [string]$SyncMode = "private_ws",
    [switch]$StrategyRuntime,
    [string]$RolloutTargetUnit = "",
    [switch]$AllowCancelExternal,
    [switch]$NoStart,
    [switch]$NoEnable,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

. (Join-Path $PSScriptRoot "systemd_service_utils.ps1")

function Resolve-AbsolutePathMaybeRelative {
    param(
        [string]$Root,
        [string]$PathValue
    )
    if ([string]::IsNullOrWhiteSpace($PathValue)) {
        return ""
    }
    if ([System.IO.Path]::IsPathRooted($PathValue)) {
        return [System.IO.Path]::GetFullPath($PathValue)
    }
    return [System.IO.Path]::GetFullPath((Join-Path $Root $PathValue))
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
            Write-Host ("[{0}][dry-run] seed_state_db={1} -> {2}" -f $LogPrefix, $source, $target)
            continue
        }
        New-Item -ItemType Directory -Force -Path (Split-Path -Parent $target) | Out-Null
        Copy-Item -LiteralPath $source -Destination $target -Force
    }
}

function Seed-CandidateStateDbIfMissing {
    param(
        [string]$Root,
        [string]$UnitName,
        [string]$StateDbPath
    )
    $normalizedUnitName = ([string]$UnitName).Trim().ToLowerInvariant()
    if ((-not $normalizedUnitName.Contains("candidate")) -and (-not $normalizedUnitName.Contains("canary"))) {
        return
    }
    $targetDbPath = Resolve-AbsolutePathMaybeRelative -Root $Root -PathValue $StateDbPath
    if ([string]::IsNullOrWhiteSpace($targetDbPath) -or (Test-Path $targetDbPath)) {
        return
    }
    $legacyCandidateDbPath = Resolve-AbsolutePathMaybeRelative -Root $Root -PathValue "data/state/live_candidate/live_state.db"
    if ([string]::IsNullOrWhiteSpace($legacyCandidateDbPath) -or (-not (Test-Path $legacyCandidateDbPath))) {
        return
    }
    if ([string]::Equals($legacyCandidateDbPath, $targetDbPath, [System.StringComparison]::OrdinalIgnoreCase)) {
        return
    }
    Copy-FileWithSqliteSidecars -SourcePath $legacyCandidateDbPath -TargetPath $targetDbPath -LogPrefix "live-install"
}

$resolvedProjectRoot = if ([string]::IsNullOrWhiteSpace($ProjectRoot)) { Resolve-DefaultProjectRoot } else { $ProjectRoot }
$resolvedProjectRoot = [System.IO.Path]::GetFullPath($resolvedProjectRoot)
$resolvedPythonExe = if ([string]::IsNullOrWhiteSpace($PythonExe)) { Resolve-DefaultPythonExe -Root $resolvedProjectRoot } else { $PythonExe }
$effectiveTargetUnit = if ([string]::IsNullOrWhiteSpace($RolloutTargetUnit)) { $UnitName } else { $RolloutTargetUnit }
$trimmedUnitName = [string]$UnitName
$trimmedUnitName = $trimmedUnitName.Trim()
$normalizedUnitName = $trimmedUnitName.ToLowerInvariant()
$isCandidateUnit = $normalizedUnitName.Contains("candidate") -or $normalizedUnitName.Contains("canary")
$effectiveModelRefSource = if ([string]::IsNullOrWhiteSpace($ModelRefSource)) {
    if ($isCandidateUnit) { "latest_candidate" } else { "champion" }
} else {
    $ModelRefSource
}
$effectiveModelFamily = if ([string]::IsNullOrWhiteSpace($ModelFamily)) {
    "train_v5_fusion"
} else {
    $ModelFamily
}
$effectiveModelRegistryRoot = if ([string]::IsNullOrWhiteSpace($ModelRegistryRoot)) {
    "models/registry"
} else {
    $ModelRegistryRoot
}
$effectiveStateDbPath = if ([string]::IsNullOrWhiteSpace($StateDbPath)) {
    if ($isCandidateUnit) { "data/state/live_canary/live_state.db" } else { "data/state/live_state.db" }
} else {
    $StateDbPath
}
$effectiveStrategyTf = if ([string]::IsNullOrWhiteSpace($StrategyTf)) {
    ""
} else {
    ([string]$StrategyTf).Trim().ToLowerInvariant()
}
Seed-CandidateStateDbIfMissing -Root $resolvedProjectRoot -UnitName $UnitName -StateDbPath $effectiveStateDbPath

$liveArgList = @(
    "-m", "autobot.cli",
    "live", "run",
    "--duration-sec", "0",
    "--rollout-mode", $RolloutMode,
    "--rollout-target-unit", $effectiveTargetUnit
)
if ($StrategyRuntime) {
    if ($SyncMode -eq "executor_ws") {
        throw "StrategyRuntime currently supports only SyncMode=poll or private_ws"
    }
    $liveArgList += "--strategy-runtime"
}
switch ($SyncMode) {
    "private_ws" { $liveArgList += "--use-private-ws" }
    "executor_ws" { $liveArgList += "--use-executor-ws" }
    default { }
}
if ($AllowCancelExternal) {
    $liveArgList += "--allow-cancel-external"
}

$liveCommand = ($liveArgList | ForEach-Object { Quote-ShellArg ([string]$_) }) -join " "
$activatePath = Join-Path $resolvedProjectRoot ".venv/bin/activate"
$execStart = "/bin/bash -lc " + (Quote-ShellArg ("source " + $activatePath + " && " + $resolvedPythonExe + " " + $liveCommand))
$strategyTfEnvironmentLine = if ([string]::IsNullOrWhiteSpace($effectiveStrategyTf)) {
    ""
} else {
    "Environment=AUTOBOT_LIVE_STRATEGY_TF=$effectiveStrategyTf`n"
}

$unitContent = @"
[Unit]
Description=Autobot Live Runtime
After=network-online.target
Wants=network-online.target
StartLimitIntervalSec=60
StartLimitBurst=4

[Service]
Type=simple
User=$ServiceUser
WorkingDirectory=$resolvedProjectRoot
Environment=PYTHONUNBUFFERED=1
Environment=AUTOBOT_LIVE_BOT_ID=$BotId
Environment=AUTOBOT_LIVE_STATE_DB_PATH=$effectiveStateDbPath
Environment=AUTOBOT_LIVE_MODEL_REF_SOURCE=$effectiveModelRefSource
Environment=AUTOBOT_LIVE_MODEL_FAMILY=$effectiveModelFamily
Environment=AUTOBOT_LIVE_MODEL_REGISTRY_ROOT=$effectiveModelRegistryRoot
Environment=AUTOBOT_LIVE_ROLLOUT_MODE=$RolloutMode
Environment=AUTOBOT_LIVE_TARGET_UNIT=$effectiveTargetUnit
Environment=AUTOBOT_LIVE_SYNC_MODE=$SyncMode
$strategyTfEnvironmentLine
Environment=AUTOBOT_LIVE_SMALL_ACCOUNT_MAX_POSITIONS=$SmallAccountMaxPositions
Environment=AUTOBOT_LIVE_SMALL_ACCOUNT_MAX_OPEN_ORDERS_PER_MARKET=$SmallAccountMaxOpenOrdersPerMarket
SyslogIdentifier=$($UnitName -replace '\.service$', '')
ExecStart=$execStart
Restart=on-failure
RestartPreventExitStatus=2
RestartSec=15
TimeoutStopSec=30
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
"@

if ($DryRun) {
    Write-Host ("[live-install][dry-run] service_user={0}" -f $ServiceUser)
    Write-Host ("[live-install][dry-run] unit={0}" -f $UnitName)
    Write-Host ("[live-install][dry-run] bot_id={0}" -f $BotId)
    Write-Host ("[live-install][dry-run] state_db_path={0}" -f $effectiveStateDbPath)
    Write-Host ("[live-install][dry-run] model_ref_source={0}" -f $effectiveModelRefSource)
    Write-Host ("[live-install][dry-run] model_family={0}" -f $effectiveModelFamily)
    Write-Host ("[live-install][dry-run] strategy_tf={0}" -f $effectiveStrategyTf)
    Write-Host ("[live-install][dry-run] small_account_max_positions={0}" -f $SmallAccountMaxPositions)
    Write-Host $unitContent
    exit 0
}

Enable-UserLinger -UserName $ServiceUser
Install-UnitFile -UnitName $UnitName -Content $unitContent
& sudo systemctl daemon-reload
if ($LASTEXITCODE -ne 0) {
    throw "systemctl daemon-reload failed"
}
if (-not $NoEnable) {
    & sudo systemctl enable $UnitName
    if ($LASTEXITCODE -ne 0) {
        throw "systemctl enable failed: $UnitName"
    }
}
if (-not $NoStart) {
    & sudo systemctl restart $UnitName
    if ($LASTEXITCODE -ne 0) {
        throw "systemctl restart failed: $UnitName"
    }
}

& systemctl status $UnitName --no-pager
