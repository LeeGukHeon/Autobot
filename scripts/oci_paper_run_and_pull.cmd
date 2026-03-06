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

rem runtime knobs
set "UNIT_PREFIX=autobot-paper-alpha"
set "DURATION_MIN_DEFAULT=10"
set "DURATION_SEC=600"
set "DURATION_LABEL=10m"
set "TOP_N=50"
set "TF=5m"
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

echo.
echo [INPUT] Paper run duration
echo [INPUT] value + unit(h/m). Enter keeps default %DURATION_MIN_DEFAULT%m.

:duration_value_prompt
set "DUR_VALUE="
set /p "DUR_VALUE=duration value (number, default=%DURATION_MIN_DEFAULT%): "
if not defined DUR_VALUE set "DUR_VALUE=%DURATION_MIN_DEFAULT%"
echo %DUR_VALUE% | findstr /R "^[0-9][0-9]*$" >nul
if errorlevel 1 (
  echo [ERROR] duration value must be an integer.
  goto duration_value_prompt
)
set /a DUR_VALUE_NUM=%DUR_VALUE%
if %DUR_VALUE_NUM% LEQ 0 (
  echo [ERROR] duration value must be greater than 0.
  goto duration_value_prompt
)

:duration_unit_prompt
set "DUR_UNIT="
set /p "DUR_UNIT=duration unit [m/h] (default=m): "
if not defined DUR_UNIT set "DUR_UNIT=m"
set "DUR_UNIT=%DUR_UNIT: =%"
set "DUR_UNIT=%DUR_UNIT:~0,1%"
if /I "%DUR_UNIT%"=="h" (
  set /a DURATION_SEC=%DUR_VALUE_NUM%*3600
  set "DURATION_LABEL=%DUR_VALUE_NUM%h"
) else if /I "%DUR_UNIT%"=="m" (
  set /a DURATION_SEC=%DUR_VALUE_NUM%*60
  set "DURATION_LABEL=%DUR_VALUE_NUM%m"
) else (
  echo [ERROR] duration unit must be m or h.
  goto duration_unit_prompt
)

set "UNIT_NAME=%UNIT_PREFIX%-%DURATION_LABEL%-%RANDOM%%RANDOM%"
set /a WAIT_TIMEOUT_SEC=%DURATION_SEC%+1200
if %WAIT_TIMEOUT_SEC% LSS 600 set "WAIT_TIMEOUT_SEC=600"
set /a MAX_POLLS=(%WAIT_TIMEOUT_SEC%+9)/10
echo [INFO] duration_sec=%DURATION_SEC% (%DURATION_LABEL%)
echo [INFO] unit_name=%UNIT_NAME%

echo [INFO] Starting remote unit: %UNIT_NAME%
ssh -i "%KEY_PATH%" "%SERVER%" "sudo systemd-run --unit=%UNIT_NAME% --collect --uid=ubuntu --gid=ubuntu --working-directory=%REMOTE_ROOT% -p StandardOutput=journal -p StandardError=journal /bin/bash -lc 'source %REMOTE_ROOT%/.venv/bin/activate && python -m autobot.cli paper run --duration-sec %DURATION_SEC% --quote KRW --top-n %TOP_N% --strategy model_alpha_v1 --model-ref champion_v3 --feature-set v3 --tf %TF% --top-pct 0.20 --min-prob 0.50 --min-cands-per-ts 1 --exit-mode hold --hold-bars 6 --paper-micro-provider live_ws --paper-feature-provider live_v3 --micro-order-policy on --micro-order-policy-mode trade_only --micro-order-policy-on-missing static_fallback'"
if errorlevel 1 (
  echo [ERROR] Failed to start remote systemd unit.
  goto fail
)

echo [INFO] Waiting for completion... (timeout_sec=%WAIT_TIMEOUT_SEC%, polls=%MAX_POLLS%)
set "UNIT_STATE=unknown"
for /L %%I in (1,1,%MAX_POLLS%) do (
  for /f "usebackq delims=" %%S in (`ssh -i "%KEY_PATH%" "%SERVER%" "systemctl is-active %UNIT_NAME% 2>/dev/null || true"`) do (
    set "UNIT_STATE=%%S"
  )
  echo [INFO] poll=%%I state=!UNIT_STATE!
  if /I "!UNIT_STATE!"=="active" (
    timeout /t 10 /nobreak >nul
  ) else if /I "!UNIT_STATE!"=="activating" (
    timeout /t 10 /nobreak >nul
  ) else (
    goto unit_done
  )
)

echo [WARN] Timeout while waiting for completion.
echo [INFO] The run may still be active. Check:
echo [INFO] ssh -i "%KEY_PATH%" "%SERVER%" "systemctl status %UNIT_NAME% --no-pager"
echo [INFO] ssh -i "%KEY_PATH%" "%SERVER%" "journalctl -u %UNIT_NAME% -f"
echo [INFO] Pull later with: %~dp0oci_paper_pull_latest.cmd
goto done_no_pull

:unit_done
echo [INFO] Final unit state: !UNIT_STATE!
if /I "!UNIT_STATE!"=="failed" (
  echo [WARN] Unit failed. Showing tail logs.
  ssh -i "%KEY_PATH%" "%SERVER%" "journalctl -u %UNIT_NAME% -n 120 --no-pager"
  goto fail
)

:fetch_latest
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

echo [INFO] Remote logs command:
echo ssh -i "%KEY_PATH%" "%SERVER%" "journalctl -u %UNIT_NAME% -n 200 --no-pager"
if "%PAUSE_ON_EXIT%"=="1" (
  echo.
  echo [INFO] Press any key to close...
  pause >nul
)
exit /b 0

:done_no_pull
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
