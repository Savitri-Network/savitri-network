# Build lightnode SENZA RocksDB (storage in-memory) - per Windows senza MSVC/cl.exe
# Evita lz4-sys, rocksdb e altre dipendenze C native.
#
# Uso: .\build-no-msvc.ps1
# oppure: .\build-no-msvc.ps1 --features "test_simulated_latency"
#
# Per build completa con RocksDB: installa Visual Studio Build Tools con C++

$ErrorActionPreference = "Stop"
$env:CARGO_TARGET_DIR = Join-Path $PSScriptRoot "target"

# --no-default-features disabilita rocksdb; --features desktop mantiene funzionalità base
$baseArgs = @("build", "--release", "--no-default-features", "--features", "desktop")
if ($args.Count -gt 0) {
    $baseArgs += $args
}

& cargo $baseArgs
