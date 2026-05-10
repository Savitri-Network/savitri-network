@echo off
REM Quick MSVC setup and build for savitri-lightnode
REM This script configures MSVC and builds in one step

echo ========================================
echo Quick MSVC Build for Savitri Lightnode
echo ========================================
echo.

REM Navigate to project root
cd /d "%~dp0..\"

echo Configuring MSVC environment...
call "C:\Program Files\Microsoft Visual Studio\2022\Community\VC\Auxiliary\Build\vcvars64.bat" 2>nul

if %errorlevel% neq 0 (
    echo Trying alternative VS paths...
    call "C:\Program Files\Microsoft Visual Studio\2022\Professional\VC\Auxiliary\Build\vcvars64.bat" 2>nul
    if %errorlevel% neq 0 (
        call "C:\Program Files\Microsoft Visual Studio\2022\Enterprise\VC\Auxiliary\Build\vcvars64.bat" 2>nul
        if %errorlevel% neq 0 (
            echo ERROR: Visual Studio not found!
            pause
            exit /b 1
        )
    )
)

echo.
echo Building savitri-lightnode with MSVC...
cd savitri-lightnode

set CC_x86_64_pc_windows_msvc=cl.exe
set CXX_x86_64_pc_windows_msvc=cl.exe
set RUSTFLAGS=-C link-arg=/STACK:8388608 -C link-arg=/IGNORE:4217 -C link-arg=/IGNORE:4099

cargo build --release
if %errorlevel% equ 0 (
    echo.
    echo [SUCCESS] savitri-lightnode built successfully!
    echo Binary location: target\release\
    
    if exist "target\release\*.exe" (
        echo Built executables:
        dir target\release\*.exe /B
    )
) else (
    echo.
    echo [FAILED] Build failed
)

echo.
pause
