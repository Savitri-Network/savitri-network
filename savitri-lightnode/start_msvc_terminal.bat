@echo off
REM Start MSVC terminal from savitri-lightnode directory
REM This script starts a new terminal with MSVC environment configured

echo ========================================
echo Starting MSVC Build Terminal
echo ========================================
echo.

REM Navigate to project root
cd /d "%~dp0..\"

REM Start MSVC terminal from project root
echo Starting MSVC terminal from project root...
powershell -ExecutionPolicy Bypass -File "scripts\build\start_msvc_terminal.bat"

pause
