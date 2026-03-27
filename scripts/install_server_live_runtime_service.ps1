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

$resolvedProjectRoot = if ([string]::IsNullOrWhiteSpace($ProjectRoot)) { Resolve-DefaultProjectRoot } else { $ProjectRoot }
$resolvedProjectRoot = [System.IO.Path]::GetFullPath($resolvedProjectRoot)
$resolvedPythonExe = if ([string]::IsNullOrWhiteSpace($PythonExe)) { Resolve-DefaultPythonExe -Root $resolvedProjectRoot } else { $PythonExe }
$effectiveTargetUnit = if ([string]::IsNullOrWhiteSpace($RolloutTargetUnit)) { $UnitName } else { $RolloutTargetUnit }
$trimmedUnitName = [string]$UnitName
$trimmedUnitName = $trimmedUnitName.Trim()
$isCandidateUnit = $trimmedUnitName.ToLowerInvariant().Contains("candidate")
$effectiveModelRefSource = if ([string]::IsNullOrWhiteSpace($ModelRefSource)) {
    if ($isCandidateUnit) { "latest_candidate" } else { "champion" }
} else {
    $ModelRefSource
}
$effectiveModelFamily = if ([string]::IsNullOrWhiteSpace($ModelFamily)) {
    "train_v5_panel_ensemble"
} else {
    $ModelFamily
}
$effectiveModelRegistryRoot = if ([string]::IsNullOrWhiteSpace($ModelRegistryRoot)) {
    "models/registry"
} else {
    $ModelRegistryRoot
}
$effectiveStateDbPath = if ([string]::IsNullOrWhiteSpace($StateDbPath)) {
    if ($isCandidateUnit) { "data/state/live_candidate/live_state.db" } else { "data/state/live_state.db" }
} else {
    $StateDbPath
}

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
