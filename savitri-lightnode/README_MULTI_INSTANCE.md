# Savitri Lightnode Multi-Instance Setup

Questa configurazione ti permette di eseguire più istanze di Savitri Lightnode contemporaneamente, ognuna con la propria configurazione.

## Struttura dei File

```
savitri-lightnode/
├── config/
│   ├── lightnode-1.toml    # Configurazione Lightnode 1
│   ├── lightnode-2.toml    # Configurazione Lightnode 2
│   ├── lightnode-3.toml    # Configurazione Lightnode 3
│   ├── lightnode-4.toml    # Configurazione Lightnode 4
│   ├── lightnode-5.toml    # Configurazione Lightnode 5
│   ├── lightnode-6.toml    # Configurazione Lightnode 6
│   ├── lightnode-7.toml    # Configurazione Lightnode 7
│   └── lightnode-8.toml    # Configurazione Lightnode 8
├── start_lightnode_1.bat   # Starts Lightnode 1
├── start_lightnode_2.bat   # Starts Lightnode 2
├── start_lightnode_3.bat   # Starts Lightnode 3
├── start_lightnode_4.bat   # Starts Lightnode 4
├── start_lightnode_5.bat   # Starts Lightnode 5
├── start_lightnode_6.bat   # Starts Lightnode 6
├── start_lightnode_7.bat   # Starts Lightnode 7
├── start_lightnode_8.bat   # Starts Lightnode 8
├── start_all_lightnodes_simple.bat  # Starts tutti i lightnode
├── stop_all_lightnodes.bat # Ferma tutti i lightnode
└── target/release/
    └── savitri-lightnode.exe
```

## Configurazione dei Lightnode

### Lightnode 1
- **Porta**: 5001
- **Database**: `lightnode-1.db`
- **Config**: `config/lightnode-1.toml`
- **Chiavi**: `lightnode-1-*.key`
- **TX Interval**: 30 secondi
- **Block Interval**: 60 secondi

### Lightnode 2
- **Porta**: 5002
- **Database**: `lightnode-2.db`
- **Config**: `config/lightnode-2.toml`
- **Chiavi**: `lightnode-2-*.key`
- **TX Interval**: 35 secondi
- **Block Interval**: 65 secondi

### Lightnode 3
- **Porta**: 5003
- **Database**: `lightnode-3.db`
- **Config**: `config/lightnode-3.toml`
- **Chiavi**: `lightnode-3-*.key`
- **TX Interval**: 40 secondi
- **Block Interval**: 70 secondi

### Lightnode 4
- **Porta**: 5004
- **Database**: `lightnode-4.db`
- **Config**: `config/lightnode-4.toml`
- **Chiavi**: `lightnode-4-*.key`
- **TX Interval**: 3 secondi
- **Block Interval**: 60 secondi

### Lightnode 5
- **Porta**: 5005
- **Database**: `lightnode-5.db`
- **Config**: `config/lightnode-5.toml`
- **Chiavi**: `lightnode-5-*.key`
- **TX Interval**: 3 secondi
- **Block Interval**: 60 secondi

### Lightnode 6
- **Porta**: 5006
- **Database**: `lightnode-6.db`
- **Config**: `config/lightnode-6.toml`
- **Chiavi**: `lightnode-6-*.key`
- **TX Interval**: 3 secondi
- **Block Interval**: 60 secondi

### Lightnode 7
- **Porta**: 5007
- **Database**: `lightnode-7.db`
- **Config**: `config/lightnode-7.toml`
- **Chiavi**: `lightnode-7-*.key`
- **TX Interval**: 3 secondi
- **Block Interval**: 60 secondi

### Lightnode 8
- **Porta**: 5008
- **Database**: `lightnode-8.db`
- **Config**: `config/lightnode-8.toml`
- **Chiavi**: `lightnode-8-*.key`
- **TX Interval**: 3 secondi
- **Block Interval**: 60 secondi

## Utilizzo

### 1. Build l'Eseguibile
```bash
cargo build --release
```

### 2. Starts un Singolo Lightnode
```bash
# Lightnode 1
start_lightnode_1.bat

# Lightnode 2
start_lightnode_2.bat

# Lightnode 3
start_lightnode_3.bat
```

### 3. Starts Tutti i Lightnode
```bash
start_all_lightnodes_simple.bat
```

Questo aprirà 3 finestre separate, una per ogni lightnode.

### 4. Ferma Tutti i Lightnode
```bash
stop_all_lightnodes.bat
```

## Personalizzazione

### Modificare le Porte
Modifica le variabili `LISTEN_PORT` negli script `.bat` o nei file di configurazione `.toml`.

### Modificare i Timer
Modifica i parametri `--tx-interval-secs` e `--block-interval-secs` negli script.

### Aggiungere Altri Lightnode
1. Copia uno degli script esistenti
2. Modifica le variabili (porta, database, chiavi, etc.)
3. Creates un nuovo file di configurazione `.toml`
4. Aggiungi il nuovo script allo `start_all_lightnodes_simple.bat`

## Configurazioni di Rete

Ogni lightnode può essere configurato per:
- Connettersi a masternode diversi
- Usare bootstrap peers diversi
- Avere diverse capacità di risorse (bandwidth, CPU, storage)

Vedi i file `config/lightnode-*.toml` per esempi di configurazione.

## Troubleshooting

### Porte Già in Uso
Se ricevi un errore di porta già in uso:
1. Esegui `stop_all_lightnodes.bat`
2. Attendi qualche secondo
3. Riavvia i lightnode

### Database Corrotti
Se un database è corrotto:
1. Ferma i lightnode
2. Elimina i file `.db` corrispondenti
3. Riavvia i lightnode

### Chiavi Mancanti
Le chiavi vengono generate automaticamente al primo avvio se non esistono.

## Vantaggi di Questa Configurazione

1. **Isolamento Completo**: Ogni lightnode ha i propri dati, chiavi e configurazione
2. **Testing Facile**: Puoi testare diversi scenari di rete contemporaneamente
3. **Sviluppo**: Ideale per sviluppo e debugging di reti locali
4. **Scalabilità**: Facile aggiungere o rimuovere istanze
