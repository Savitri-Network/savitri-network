@echo off
echo Starting all Savitri Lightnodes in separate windows...
echo.

echo Starting Lightnode 1 on port 5001...
start "Savitri Lightnode 1" cmd /k "start_lightnode_1.bat"

timeout /t 3 >nul

echo Starting Lightnode 2 on port 5002...
start "Savitri Lightnode 2" cmd /k "start_lightnode_2.bat"

timeout /t 3 >nul

echo Starting Lightnode 3 on port 5003...
start "Savitri Lightnode 3" cmd /k "start_lightnode_3.bat"

timeout /t 3 >nul

echo Starting Lightnode 4 on port 5004...
start "Savitri Lightnode 4" cmd /k "start_lightnode_4.bat"

timeout /t 3 >nul

echo Starting Lightnode 5 on port 5005...
start "Savitri Lightnode 5" cmd /k "start_lightnode_5.bat"

timeout /t 3 >nul

echo Starting Lightnode 6 on port 5006...
start "Savitri Lightnode 6" cmd /k "start_lightnode_6.bat"

timeout /t 3 >nul

echo Starting Lightnode 7 on port 5007...
start "Savitri Lightnode 7" cmd /k "start_lightnode_7.bat"

timeout /t 3 >nul

echo Starting Lightnode 8 on port 5008...
start "Savitri Lightnode 8" cmd /k "start_lightnode_8.bat"

echo.
echo ✅ All lightnodes started!
echo Each lightnode is running in its own window with:
echo - Separate databases
echo - Separate configurations
echo - Separate keys
echo - Different ports (5001-5008)
echo.

pause
