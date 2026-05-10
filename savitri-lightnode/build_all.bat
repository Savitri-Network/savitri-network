@echo off
echo Building all Savitri Lightnode executables...

echo.
echo Building savitri-lightnode (main)...
cargo build --release --bin savitri-lightnode

echo.
echo Building savitri-lightnode-1...
cargo build --release --bin savitri-lightnode-1

echo.
echo Building savitri-lightnode-2...
cargo build --release --bin savitri-lightnode-2

echo.
echo Building savitri-lightnode-3...
cargo build --release --bin savitri-lightnode-3

echo.
echo ✅ All executables built successfully!
echo.
echo Available executables:
echo - target\release\savitri-lightnode.exe
echo - target\release\savitri-lightnode-1.exe
echo - target\release\savitri-lightnode-2.exe
echo - target\release\savitri-lightnode-3.exe
echo.

pause
