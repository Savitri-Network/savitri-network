@echo off
echo Starting all Savitri Lightnodes...

echo.
echo Starting Lightnode 1 on port 4001...
start "Savitri Lightnode 1" cmd /k "target\release\savitri-lightnode-1.exe --config config\lightnode-1.toml --port 4001"

timeout /t 2 >nul

echo.
echo Starting Lightnode 2 on port 4002...
start "Savitri Lightnode 2" cmd /k "target\release\savitri-lightnode-2.exe --config config\lightnode-2.toml --port 4002"

timeout /t 2 >nul

echo.
echo Starting Lightnode 3 on port 4003...
start "Savitri Lightnode 3" cmd /k "target\release\savitri-lightnode-3.exe --config config\lightnode-3.toml --port 4003"

echo.
echo ✅ All lightnodes started!
echo Each lightnode is running in its own window.
echo.

pause
