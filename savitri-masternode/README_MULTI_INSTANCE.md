# Savitri Masternode Multi-Instance Setup

Questa configurazione ti permette di eseguire più istanze di Savitri Masternode contemporaneamente, ognuna con la propria configurazione.

## Struttura dei File

```
savitri-masternode/
├── config/
│   ├── masternode-1.toml    # Configurazione Masternode 1
│   ├── masternode-2.toml    # Configurazione Masternode 2
│   ├── masternode-3.toml    # Configurazione Masternode 3
│   ├── masternode-4.toml    # Configurazione Masternode 4
│   └── masternode-5.toml    # Configurazione Masternode 5
├── start_masternode_1.bat   # Starts Masternode 1
├── start_masternode_2.bat   # Starts Masternode 2
├── start_masternode_3.bat   # Starts Masternode 3
├── start_masternode_4.bat   # Starts Masternode 4
├── start_masternode_5.bat   # Starts Masternode 5
├── start_all_masternodes.bat  # Starts tutti i masternode
├── stop_all_masternodes.bat  # Ferma tutti i masternode
├── build_all.bat             # Build tutti gli eseguibili
└── target/release/
    └── savitri-masternode.exe
```

## Configurazione dei Masternode

### Masternode 1
- **Porta P2P**: 5021
- **Storage**: `storage-1/`
- **Config**: `config/masternode-1.toml`
- **Chiavi**: `identity-1.key`, `masternode-1.key`

### Masternode 2
- **Porta P2P**: 5022
- **Storage**: `storage-2/`
- **Config**: `config/masternode-2.toml`
- **Chiavi**: `identity-2.key`, `masternode-2.key`

### Masternode 3
- **Porta P2P**: 5023
- **Storage**: `storage-3/`
- **Config**: `config/masternode-3.toml`
- **Chiavi**: `identity-3.key`, `masternode-3.key`

### Masternode 4
- **Porta P2P**: 5024
- **Storage**: `storage-4/`
- **Config**: `config/masternode-4.toml`
- **Chiavi**: `identity-4.key`, `masternode-4.key`

### Masternode 5
- **Porta P2P**: 5025
- **Storage**: `storage-5/`
- **Config**: `config/masternode-5.toml`
- **Chiavi**: `identity-5.key`, `masternode-5.key`

## Utilizzo

### 1. Build Tutti gli Eseguibili
```bash
build_all.bat
```

### 2. Starts un Singolo Masternode
```bash
# Masternode 1
start_masternode_1.bat

# Masternode 2
start_masternode_2.bat

# etc...
```

### 3. Starts Tutti i Masternode
```bash
start_all_masternodes.bat
```

Questo aprirà 5 finestre separate, una per ogni masternode.

### 4. Ferma Tutti i Masternode
```bash
stop_all_masternodes.bat
```

## Configurazione di Rete

### Bootstrap Peers
Ogni masternode è configurato per connettersi agli altri masternode:
- Masternode 1 si connette alle porte 5022-5025
- Masternode 2 si connette alle porte 5021, 5023-5025
- etc.

### Validator List
Tutti i masternode condividono la stessa lista di validatori per il consenso.

### Slot Scheduling
Tutti i masternode usano lo stesso `slot_base_ms` per garantire rotazione sincronizzata of the leadership.

## Personalizzazione

### Modificare le Porte
Modifica le variabili `P2P_PORT` negli script `.bat` o i valori `p2p_port` nei file `.toml`.

### Modificare i Timer
Modifica i parametri nei file `.toml`:
- `slot_duration`: durata di ogni slot
- `block_interval_secs`: intervallo produzione blocchi
- `tx_interval_secs`: intervallo generazione transazioni

### Aggiungere Altri Masternode
1. Copia uno degli script esistenti
2. Modifica le variabili (porta, storage, chiavi, etc.)
3. Creates un nuovo file di configurazione `.toml`
4. Aggiungi il nuovo script allo `start_all_masternodes.bat`

## Integrazione con Lightnode

I masternode su queste porte possono essere usati come bootstrap peers per i lightnode:

```toml
# In lightnode config
bootstrap_peers = [
    "12D3KooW...@/ip4/127.0.0.1/tcp/5021",
    "12D3KooW...@/ip4/127.0.0.1/tcp/5022",
    # etc...
]
```

## Testing e Sviluppo

### Test di Consenso
Con 5 masternode puoi testare:
- BFT consensus con tolleranza a 2 fault
- Rotazione of the leadership
- Monolith production
- Propagation dei blocchi

### Test di Rete
- Topologia mesh completa
- Bootstrap automatico
- Riconnessione automatica
- Load balancing

## Troubleshooting

### Porte Già in Uso
```bash
stop_all_masternodes.bat
netstat -an | findstr 502
```

### Database Corrotti
```bash
stop_all_masternodes.bat
rmdir /s /q storage-*
```

### Chiavi Mancanti
Le chiavi vengono generate automaticamente al primo avvio se non esistono.

## Vantaggi di Questa Configurazione

1. **Testing Completo**: Simula una rete reale con 5 validatori
2. **Isolamento**: Ogni masternode ha i propri dati e configurazione
3. **Sviluppo**: Ideale per sviluppo e debugging of the consenso
4. **Scalabilità**: Facile aggiungere o rimuovere validatori
5. **Integrazione**: Perfetto per test con lightnode

## Note Importanti

- Assicurati che le porte 5021-5025 siano libere
- Ogni masternode richiede circa 100MB di storage
- I masternode generano molto traffico di rete in locale
- Usa `stop_all_masternodes.bat` prima di riavviare per evitare conflitti
