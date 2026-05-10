use super::{Storage, RocksDb};
use super::{Storage, CF_VESTING, RocksDb};
use serde::{Deserialize, Serialize};

/// Storage per vesting schedules
///
///
/// **Struttura chiave:** `address_bytes + "_" + schedule_id (little-endian)`
/// - L'address è rappresentato come `Vec<u8>` per flessibilità
/// - Il schedule_id è un u64 in little-endian
///
/// **Value:** `VestingSchedule` serializzato con bincode
/// - Query per address: Usa prefix iteration (O(log n) con RocksDB)
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum VestingType {
    /// Vesting lineare: token rilasciati linearmente nel tempo without cliff period
    Linear,
    /// Vesting with cliff: no tokens released before the cliff period, then linear
    Cliff,
}

/// Schedule di vesting completo
///
/// calcolare l'amount vested e rilasciato nel tempo.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VestingSchedule {
    pub address: Vec<u8>,
    /// Unique schedule id (allowing multiple schedules per address)
    pub schedule_id: u64,
    /// Amount totale da vestire
    pub amount: u128,
    /// Timestamp di inizio of the vesting (Unix timestamp in secondi)
    pub start_time: u64,
    /// Durata totale of the vesting in secondi
    pub duration: u64,
    /// Cliff period in secondi (0 per Linear vesting, >0 per Cliff vesting)
    /// During the cliff period no tokens are released
    pub cliff: u64,
    /// Tipo di vesting (Linear o Cliff)
    pub vesting_type: VestingType,
    /// Amount già vested (calcolato e aggiornato periodicamente)
    pub vested_amount: u128,
    /// Amount già rilasciato al beneficiario
    pub released_amount: u128,
}

impl VestingSchedule {
    ///
    /// # Parametri
    /// - `schedule_id`: Unique schedule id
    /// - `amount`: Amount totale da vestire
    /// - `start_time`: Timestamp di inizio (Unix timestamp in secondi)
    /// - `duration`: Durata totale of the vesting in secondi
    /// - `cliff`: Cliff period in secondi (0 per Linear vesting, >0 per Cliff vesting)
    /// - `vesting_type`: Tipo di vesting (Linear o Cliff)
    ///
    /// # Note
    /// - For `VestingType::Cliff`, if `cliff >= duration`, no tokens are released until the end
    ///
    /// # Esempi
    /// ```
    /// use savitri_node::storage::{VestingSchedule, VestingType};
    ///
    /// // Linear vesting: 1M token rilasciati linearmente in 6 mesi
    /// let linear = VestingSchedule::new(
    ///     address.to_vec(),
    ///     1,
    ///     1_000_000_000_000_000_000, // 1M token (18 decimali)
    ///     1704067200, // start_time
    ///     15768000,   // 6 mesi in secondi
    ///     0,          // no cliff
    ///     VestingType::Linear,
    /// );
    ///
    /// // Cliff vesting: 1M token con cliff di 3 mesi, poi linear per 3 mesi
    /// let cliff = VestingSchedule::new(
    ///     address.to_vec(),
    ///     2,
    ///     1_000_000_000_000_000_000,
    ///     1704067200,
    ///     15768000,   // durata totale: 6 mesi
    ///     7884000,    // cliff: 3 mesi
    ///     VestingType::Cliff,
    /// );
    /// ```
    pub fn new(
        address: Vec<u8>,
        schedule_id: u64,
        amount: u128,
        start_time: u64,
        duration: u64,
        cliff: u64,
        vesting_type: VestingType,
    ) -> Self {
        Self {
            address,
            schedule_id,
            amount,
            start_time,
            duration,
            cliff,
            vesting_type,
            vested_amount: 0,
            released_amount: 0,
        }
    }

    ///
    /// # Ritorna
    /// - `Ok(())` se lo schedule è valido
    /// - `Err` con messaggio di errore se lo schedule è invalido
    ///
    /// # Validazioni
    /// - `released_amount` non può superare `amount`
    /// - `released_amount` non può superare `vested_amount` (se `vested_amount` è aggiornato)
    pub fn validate(&self) -> anyhow::Result<()> {
        // Validazione amount
        if self.amount == 0 {
            anyhow::bail!("Vesting schedule amount must be greater than 0");
        }

        // Validazione per Linear vesting
        if self.vesting_type == VestingType::Linear && self.cliff > 0 {
            // Warning: cliff > 0 per Linear vesting (viene ignorato)
        }

        // Validazione per Cliff vesting
        if self.vesting_type == VestingType::Cliff {
            if self.cliff >= self.duration {
                // Not an error: it just means no tokens are released until the end
            }
        }

        // Validazione released_amount
        if self.released_amount > self.amount {
            anyhow::bail!(
                "released_amount ({}) cannot exceed total amount ({})",
                self.released_amount,
                self.amount
            );
        }

        // Validazione: released_amount non può superare vested_amount (se vested_amount è aggiornato)
        if self.vested_amount > 0 && self.released_amount > self.vested_amount {
            anyhow::bail!(
                "released_amount ({}) cannot exceed vested_amount ({})",
                self.released_amount,
                self.vested_amount
            );
        }

        Ok(())
    }

    ///
    /// # Parametri
    /// - `timestamp`: Timestamp corrente (Unix timestamp in secondi)
    ///
    /// # Ritorna
    pub fn is_completed(&self, timestamp: u64) -> bool {
        let vested = self.calculate_vested(timestamp);
        vested >= self.amount && self.released_amount >= self.amount
    }

    /// Compute l'amount ancora locked (non vested)
    ///
    /// # Parametri
    /// - `timestamp`: Timestamp corrente (Unix timestamp in secondi)
    ///
    /// # Ritorna
    /// L'amount ancora locked (non vested)
    pub fn locked_amount(&self, timestamp: u64) -> u128 {
        let vested = self.calculate_vested(timestamp);
        self.amount.saturating_sub(vested)
    }

    /// Compute l'amount vested fino a un timestamp con precisione migliorata
    ///
    /// La formula per il calcolo è:
    /// - **Linear vesting**: `vested = (elapsed * amount + duration/2) / duration`
    /// - **Cliff vesting**:
    ///   - Se `elapsed < cliff`: `vested = 0`
    ///   - Altrimenti: `vested = ((vesting_period * amount + vesting_duration/2) / vesting_duration`
    ///
    /// # Parametri
    /// - `timestamp`: Timestamp corrente (Unix timestamp in secondi)
    ///
    /// # Ritorna
    /// L'amount vested fino al timestamp specificato (0 se prima di start_time)
    ///
    /// # Note
    /// - If `cliff >= duration` in Cliff vesting, no tokens are released until the end
    /// - Usa aritmetica fixed-point con `u128` per evitare errori di arrotondamento
    pub fn calculate_vested(&self, timestamp: u64) -> u128 {
        // If the timestamp is before start, no tokens are vested
        if timestamp < self.start_time {
            return 0;
        }

        // Compute il tempo trascorso dall'inizio
        let elapsed = timestamp.saturating_sub(self.start_time);

        match self.vesting_type {
            VestingType::Linear => {
                // Gestione caso edge: duration == 0 (vesting istantaneo)
                if self.duration == 0 {
                    return self.amount;
                }

                if elapsed >= self.duration {
                    return self.amount;
                }

                // Linear vesting: (elapsed * amount + duration/2) / duration
                // Usa aritmetica fixed-point con arrotondamento al più vicino
                let elapsed_u128 = elapsed as u128;
                let duration_u128 = self.duration as u128;

                // Calcolo: (elapsed * amount + duration/2) / duration
                let half_duration = duration_u128 / 2;

                elapsed_u128
                    .checked_mul(self.amount)
                    .and_then(|x| x.checked_add(half_duration))
                    .and_then(|x| x.checked_div(duration_u128))
                    .unwrap_or(0)
            }
            VestingType::Cliff => {
                // While still in the cliff period, no tokens are vested
                if elapsed < self.cliff {
                    return 0;
                }

                // Compute il periodo di vesting effettivo (dopo il cliff)
                let vesting_period = elapsed.saturating_sub(self.cliff);
                let vesting_duration = self.duration.saturating_sub(self.cliff);

                // Gestione casi edge:
                // - If cliff >= duration, no tokens are released until the end
                if vesting_duration == 0 {
                    // Cliff period covers the whole duration: no gradual vesting
                    if elapsed >= self.cliff {
                        return self.amount;
                    }
                    return 0;
                }

                if vesting_period >= vesting_duration {
                    return self.amount;
                }

                // Linear after cliff: (vesting_period * amount + vesting_duration/2) / vesting_duration
                // Usa aritmetica fixed-point con arrotondamento al più vicino
                let vesting_period_u128 = vesting_period as u128;
                let vesting_duration_u128 = vesting_duration as u128;

                // Calcolo: (vesting_period * amount + vesting_duration/2) / vesting_duration
                let half_duration = vesting_duration_u128 / 2;

                vesting_period_u128
                    .checked_mul(self.amount)
                    .and_then(|x| x.checked_add(half_duration))
                    .and_then(|x| x.checked_div(vesting_duration_u128))
                    .unwrap_or(0)
            }
        }
    }

    /// Compute l'amount rilasciabile (vested - released)
    ///
    /// L'amount rilasciabile è la differenza tra l'amount vested e l'amount già rilasciato.
    ///
    /// # Parametri
    /// - `timestamp`: Timestamp corrente (Unix timestamp in secondi)
    ///
    /// # Ritorna
    ///
    /// # Note
    /// - Usa `saturating_sub` per evitare underflow
    ///
    /// # Esempio
    /// ```
    /// use savitri_node::storage::{VestingSchedule, VestingType};
    ///
    /// let mut schedule = VestingSchedule::new(
    ///     address.to_vec(),
    ///     1,
    ///     1_000_000_000,
    ///     1704067200,
    ///     15768000,
    ///     0,
    ///     VestingType::Linear,
    /// );
    /// schedule.released_amount = 100_000_000; // già rilasciati
    ///
    /// let current_time = 1705644800; // 1 mese dopo
    /// let releasable = schedule.releasable(current_time);
    /// // releasable = vested - 100_000_000
    /// ```
    pub fn releasable(&self, timestamp: u64) -> u128 {
        let vested = self.calculate_vested(timestamp);

        // Compute l'amount rilasciabile: vested - released
        // Usa saturating_sub per evitare underflow se released > vested
        vested.saturating_sub(self.released_amount)
    }
}

impl Storage<RocksDb> {
    /// Formato: `address_bytes + "_" + schedule_id (little-endian)`
    fn vesting_key(address: &[u8], schedule_id: u64) -> Vec<u8> {
        let mut key = address.to_vec();
        key.push(b'_');
        key.extend_from_slice(&schedule_id.to_le_bytes());
        key
    }

    /// Salva un vesting schedule in the storage.
    /// Se un schedule con lo stesso address e schedule_id esiste già, viene sovrascritto.
    ///
    /// # Esempi
    /// ```no_run
    /// use savitri_node::storage::{Storage, VestingSchedule, VestingType};
    ///
    /// let storage = Storage<RocksDb>::new("./data")?;
    /// let schedule = VestingSchedule::new(
    ///     address.to_vec(),
    ///     1,
    ///     1_000_000_000,
    ///     1704067200, // Unix timestamp
    ///     15768000,   // 6 mesi in secondi
    ///     0,
    ///     VestingType::Linear,
    /// );
    /// storage.put_vesting_schedule(&schedule)?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    pub fn put_vesting_schedule(&self, schedule: &VestingSchedule) -> anyhow::Result<()> {
        let key = Self::vesting_key(&schedule.address, schedule.schedule_id);
        let value = bincode::serialize(schedule)?;
        self.put_cf(CF_VESTING, key, value)
    }

    ///
    /// # Parametri
    /// - `schedule_id`: Unique schedule id
    ///
    /// # Ritorna
    /// - `Some(VestingSchedule)` se trovato
    /// - `None` se non esiste
    ///
    /// # Esempi
    /// ```no_run
    /// use savitri_node::storage::Storage;
    ///
    /// let storage = Storage<RocksDb>::new("./data")?;
    /// if let Some(schedule) = storage.get_vesting_schedule(&address, 1)? {
    ///     println!("Amount: {}", schedule.amount);
    /// }
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    pub fn get_vesting_schedule(
        &self,
        address: &[u8],
        schedule_id: u64,
    ) -> anyhow::Result<Option<VestingSchedule>> {
        let key = Self::vesting_key(address, schedule_id);
        match self.get_cf(CF_VESTING, key)? {
            Some(ref bytes) => Ok(Some(crate::safe_deserialize(&bytes[..])?)),
            None => Ok(None),
        }
    }

    ///
    ///
    /// # Parametri
    ///
    /// # Ritorna
    ///
    /// # Esempi
    /// ```no_run
    /// use savitri_node::storage::Storage;
    ///
    /// let storage = Storage<RocksDb>::new("./data")?;
    /// let schedules = storage.get_vesting_schedules_for_address(&address)?;
    /// println!("Trovati {} schedules", schedules.len());
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    pub fn get_vesting_schedules_for_address(
        &self,
        address: &[u8],
    ) -> anyhow::Result<Vec<VestingSchedule>> {
        let cf = self.cf(CF_VESTING)?;
        let mut prefix = address.to_vec();
        prefix.push(b'_');
        let iter = self.db.iterator_cf(
            &cf,
            rocksdb::IteratorMode::From(&prefix, rocksdb::Direction::Forward),
        );
        let mut schedules = Vec::new();
        for entry in iter {
            let (key, value): (Box<[u8]>, Box<[u8]>) = entry?;
            if !key.starts_with(&prefix) {
                break;
            }
            let schedule: VestingSchedule = crate::safe_deserialize(&value[..])?;
            schedules.push(schedule);
        }
        Ok(schedules)
    }

    ///
    ///
    /// # Esempi
    /// ```no_run
    /// use savitri_node::storage::Storage;
    ///
    /// let storage = Storage<RocksDb>::new("./data")?;
    /// let mut schedule = storage.get_vesting_schedule(&address, 1)?
    /// schedule.released_amount += amount_to_release;
    /// storage.update_vesting_schedule(&schedule)?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    pub fn update_vesting_schedule(&self, schedule: &VestingSchedule) -> anyhow::Result<()> {
        self.put_vesting_schedule(schedule)
    }

    /// Elimina un vesting schedule dallo storage.
    ///
    /// # Parametri
    /// - `schedule_id`: Unique schedule id da eliminare
    ///
    /// # Note
    ///
    /// # Esempi
    /// ```no_run
    /// use savitri_node::storage::Storage;
    ///
    /// let storage = Storage<RocksDb>::new("./data")?;
    /// storage.delete_vesting_schedule(&address, 1)?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    pub fn delete_vesting_schedule(&self, address: &[u8], schedule_id: u64) -> anyhow::Result<()> {
        let key = Self::vesting_key(address, schedule_id);
        self.delete_cf(CF_VESTING, key)
    }
}
