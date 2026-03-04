@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
for %%I in ("%SCRIPT_DIR%..") do set "PROJECT_ROOT=%%~fI"
set "CENTER_PS1=%PROJECT_ROOT%\scripts\autobot_center.ps1"

if not exist "%CENTER_PS1%" (
  echo [AutobotCenter] missing file: "%CENTER_PS1%"
  pause
  exit /b 1
)

powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%CENTER_PS1%" -ProjectRoot "%PROJECT_ROOT%" %*
set "EXIT_CODE=%ERRORLEVEL%"

if not "%EXIT_CODE%"=="0" (
  echo.
  echo [AutobotCenter] exited with code %EXIT_CODE%
  pause
)

exit /b %EXIT_CODE%
