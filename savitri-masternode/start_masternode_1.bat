@echo off
echo Starting Savitri Masternode 1...

set CONFIG_PATH=config/masternode-1.toml
set STORAGE_PATH=storage-1
set NETWORK_KEY=identity-1.key
set MASTERNODE_KEY=masternode-1.key
set P2P_PORT=5021

echo Configuration:
echo - Config: %CONFIG_PATH%
echo - Storage: %STORAGE_PATH%
echo - Network Key: %NETWORK_KEY%
echo - Masternode Key: %MASTERNODE_KEY%
echo - P2P Port: %P2P_PORT%
echo.

target\release\savitri-masternode.exe %CONFIG_PATH%
