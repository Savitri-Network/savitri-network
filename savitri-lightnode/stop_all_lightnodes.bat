@echo off
echo Stopping all Savitri Lightnodes...

echo Killing savitri-lightnode processes...
taskkill /f /im savitri-lightnode.exe 2>nul

echo Cleaning up temporary files...
del /q *.db 2>nul
del /q *.key 2>nul

echo.
echo ✅ All lightnodes stopped and cleaned up!
echo.

pause
