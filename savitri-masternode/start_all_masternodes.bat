@echo off
echo Starting all Savitri Masternodes in separate windows...
echo.

echo Starting Masternode 1 on port 5021...
start "Savitri Masternode 1" cmd /k "start_masternode_1.bat"

timeout /t 3 >nul

echo Starting Masternode 2 on port 5022...
start "Savitri Masternode 2" cmd /k "start_masternode_2.bat"

timeout /t 3 >nul

echo Starting Masternode 3 on port 5023...
start "Savitri Masternode 3" cmd /k "start_masternode_3.bat"

timeout /t 3 >nul

echo Starting Masternode 4 on port 5024...
start "Savitri Masternode 4" cmd /k "start_masternode_4.bat"

timeout /t 3 >nul

echo Starting Masternode 5 on port 5025...
start "Savitri Masternode 5" cmd /k "start_masternode_5.bat"

echo.
echo ✅ All masternodes started!
echo Each masternode is running in its own window with:
echo - Separate storage databases
echo - Separate configurations
echo - Separate keys
echo - Different ports (5021-5025)
echo.

pause
