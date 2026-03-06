@echo off
setlocal EnableExtensions EnableDelayedExpansion
set "PAUSE_ON_EXIT=1"
if /I "%~1"=="--no-pause" set "PAUSE_ON_EXIT=0"

rem ===== User config =====
set "KEY_PATH=C:\Users\Administrator\Desktop\OCI_SSH_KEY\ssh-key-2026-03-05.key"
set "SERVER=ubuntu@168.107.44.206"
set "REMOTE_ROOT=/home/ubuntu/MyApps/Autobot"
set "LOCAL_RUNS_DIR=D:\MyApps\Autobot\data\paper\runs"
set "DIGEST_PS1=%~dp0paper_run_digest.ps1"
rem =======================

set "OPENSSH_DIR=C:\Windows\System32\OpenSSH"
if exist "%OPENSSH_DIR%\ssh.exe" (
  set "PATH=%OPENSSH_DIR%;%PATH%"
)

where ssh >nul 2>nul
if errorlevel 1 (
  echo [ERROR] ssh not found.
  echo [ERROR] expected: C:\Windows\System32\OpenSSH\ssh.exe
  goto fail
)

where scp >nul 2>nul
if errorlevel 1 (
  echo [ERROR] scp not found.
  echo [ERROR] expected: C:\Windows\System32\OpenSSH\scp.exe
  goto fail
)

if not exist "%KEY_PATH%" (
  echo [ERROR] KEY_PATH not found: %KEY_PATH%
  goto fail
)

if not exist "%LOCAL_RUNS_DIR%" (
  mkdir "%LOCAL_RUNS_DIR%" >nul 2>nul
)

set "RUN_ID="
for /f "usebackq delims=" %%R in (`ssh -i "%KEY_PATH%" "%SERVER%" "cd %REMOTE_ROOT%/data/paper/runs 2>/dev/null && ls -1dt paper-* 2>/dev/null | head -n 1"`) do (
  if not defined RUN_ID set "RUN_ID=%%R"
)

if not defined RUN_ID (
  echo [ERROR] Could not resolve latest paper run id on server.
  echo [INFO] Remote diagnostics:
  ssh -i "%KEY_PATH%" "%SERVER%" "echo SERVER_PWD=\$PWD; ls -ld %REMOTE_ROOT% 2>/dev/null || echo MISSING:%REMOTE_ROOT%; ls -ld %REMOTE_ROOT%/data/paper/runs 2>/dev/null || echo MISSING:%REMOTE_ROOT%/data/paper/runs; cd %REMOTE_ROOT%/data/paper/runs 2>/dev/null && ls -1dt paper-* 2>/dev/null | head -n 5 || true"
  echo [HINT] Update REMOTE_ROOT at top of this file if server path is different.
  goto fail
)

set "REMOTE_RUN_DIR=%REMOTE_ROOT%/data/paper/runs/%RUN_ID%"
echo [INFO] Pulling: %REMOTE_RUN_DIR%

pushd "%LOCAL_RUNS_DIR%" >nul
if errorlevel 1 (
  echo [ERROR] Could not enter local dir: %LOCAL_RUNS_DIR%
  goto fail
)
scp -i "%KEY_PATH%" -r "%SERVER%:%REMOTE_RUN_DIR%" .
set "SCP_EXIT=%ERRORLEVEL%"
popd >nul
if not "%SCP_EXIT%"=="0" (
  echo [ERROR] scp failed.
  goto fail
)

set "LOCAL_RUN_DIR=%LOCAL_RUNS_DIR%\%RUN_ID%"
echo [OK] Pulled to: %LOCAL_RUN_DIR%

if exist "%DIGEST_PS1%" (
  powershell -NoProfile -ExecutionPolicy Bypass -File "%DIGEST_PS1%" -RunDir "%LOCAL_RUN_DIR%"
) else if exist "%LOCAL_RUN_DIR%\summary.json" (
  powershell -NoProfile -Command ^
    "$s = Get-Content -Raw '%LOCAL_RUN_DIR%\summary.json' | ConvertFrom-Json; " ^
    "Write-Host ('orders_submitted=' + $s.orders_submitted + ' orders_filled=' + $s.orders_filled + ' fill_rate=' + $s.fill_rate + ' realized_pnl=' + $s.realized_pnl_quote + ' unrealized_pnl=' + $s.unrealized_pnl_quote)"
)

if "%PAUSE_ON_EXIT%"=="1" (
  echo.
  echo [INFO] Press any key to close...
  pause >nul
)
exit /b 0

:fail
if "%PAUSE_ON_EXIT%"=="1" (
  echo.
  echo [INFO] Press any key to close...
  pause >nul
)
exit /b 1
