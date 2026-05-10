@echo off
echo Starting Savitri Lightnode 8...

set DB_PATH=lightnode-8.db
set CONFIG_PATH=config/lightnode-8.toml
set NETWORK_KEY_PATH=lightnode-8-network.key
set PRODUCER_KEY_PATH=lightnode-8-producer.key
set TX_KEY_PATH=lightnode-8-tx.key
set LISTEN_PORT=5008

echo Configuration:
echo - Database: %DB_PATH%
echo - Config: %CONFIG_PATH%
echo - Network Key: %NETWORK_KEY_PATH%
echo - Producer Key: %PRODUCER_KEY_PATH%
echo - TX Key: %TX_KEY_PATH%
echo - Port: %LISTEN_PORT%
echo.

target\release\savitri-lightnode.exe ^
    --db %DB_PATH% ^
    --config %CONFIG_PATH% ^
    --network-key-path %NETWORK_KEY_PATH% ^
    --producer-key-path %PRODUCER_KEY_PATH% ^
    --tx-key-path %TX_KEY_PATH% ^
    --listen-port %LISTEN_PORT% ^
    --tx-interval-secs 3 ^
    --block-interval-secs 60

pause
