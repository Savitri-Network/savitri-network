@echo off
echo Building all Savitri Masternode executables...

echo.
echo Building savitri-masternode (main)...
cargo build --release --bin savitri-masternode

echo.
echo Building savitri-masternode-1...
cargo build --release --bin savitri-masternode-1

echo.
echo Building savitri-masternode-2...
cargo build --release --bin savitri-masternode-2

echo.
echo Building savitri-masternode-3...
cargo build --release --bin savitri-masternode-3

echo.
echo Building savitri-masternode-4...
cargo build --release --bin savitri-masternode-4

echo.
echo Building savitri-masternode-5...
cargo build --release --bin savitri-masternode-5

echo.
echo ✅ All masternode executables built successfully!
echo.
echo Available executables:
echo - target\release\savitri-masternode.exe
echo - target\release\savitri-masternode-1.exe
echo - target\release\savitri-masternode-2.exe
echo - target\release\savitri-masternode-3.exe
echo - target\release\savitri-masternode-4.exe
echo - target\release\savitri-masternode-5.exe
echo.

pause
