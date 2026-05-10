@echo off
echo Stopping all Savitri Masternodes...

echo Killing savitri-masternode processes...
taskkill /f /im savitri-masternode.exe 2>nul

echo Cleaning up temporary files...
rmdir /s /q storage-1 2>nul
rmdir /s /q storage-2 2>nul
rmdir /s /q storage-3 2>nul
rmdir /s /q storage-4 2>nul
rmdir /s /q storage-5 2>nul

del /q *.key 2>nul

echo.
echo ✅ All masternodes stopped and cleaned up!
echo.

pause
