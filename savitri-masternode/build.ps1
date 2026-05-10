# Build masternode - evita "Accesso negato" disabilitando CARGO_TARGET_DIR nella cache utente
# Uso: .\build.ps1
# oppure: .\build.ps1 --release --features "full,zkp-arkworks"

# Usa target locale (evita %USERPROFILE%\.cache dove Windows/antivirus può bloccare)
$env:CARGO_TARGET_DIR = Join-Path $PSScriptRoot "target"
& cargo $args
