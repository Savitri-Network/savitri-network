//!
//! - Prevenzione overflow con checked arithmetic
//! - Revert se gas insufficiente
//! - Batch gas accounting per migliorare performance

///
/// Valori in gas units.
pub struct GasCosts {
    /// Costo per SLOAD (lettura storage)
    pub sload: u64,
    /// Costo per SSTORE nuovo valore (scrittura in slot vuoto)
    pub sstore_new: u64,
    pub sstore_update: u64,
    /// Costo base per CALL (chiamata cross-contract)
    pub call: u64,
    /// Costo base per CREATE (deploy contract)
    pub create: u64,
    /// Costo per transfer di token
    pub transfer: u64,
    /// Costo base per LOG (emissione evento)
    pub log: u64,
    /// Costo per topic aggiuntivo in LOG
    pub log_topic: u64,
    /// Costo per byte di data in LOG
    pub log_data: u64,
    /// Costo per byte di calldata
    pub calldata_byte: u64,
}

impl Default for GasCosts {
    fn default() -> Self {
        Self {
            // Costi basati su design document e best practices
            sload: 100,
            sstore_new: 20_000,   // Scrittura nuova (slot vuoto)
            sstore_update: 5_000, // Aggiornamento esistente
            call: 2_300,          // Base cost per CALL
            create: 32_000,       // Base cost per CREATE
            transfer: 300,        // Costo per transfer di token
            log: 375,             // Base cost per LOG
            log_topic: 375,       // Costo per topic aggiuntivo
            log_data: 8,          // Costo per byte di data
            calldata_byte: 16,    // Costo per byte di calldata
        }
    }
}

///
/// Traccia il gas consumato durante l'esecuzione e previene overflow.
/// Usa checked arithmetic per prevent overflow/underflow.
/// Supporta batch accounting per ridurre lock contention.
pub struct GasMeter {
    gas_limit: u64,
    /// Gas consumato finora
    gas_used: u64,
    costs: GasCosts,
    batch_accumulator: u64,
    batch_count: usize,
    batch_limit: usize,
}

impl GasMeter {
    ///
    /// # Arguments
    /// * `gas_limit` - Limit massimo di gas per l'esecuzione
    pub fn new(gas_limit: u64) -> Self {
        Self {
            gas_limit,
            gas_used: 0,
            costs: GasCosts::default(),
            batch_accumulator: 0,
            batch_count: 0,
            batch_limit: 100, // Configurabile
        }
    }

    pub fn with_batch_limit(gas_limit: u64, batch_limit: usize) -> Self {
        Self {
            gas_limit,
            gas_used: 0,
            costs: GasCosts::default(),
            batch_accumulator: 0,
            batch_count: 0,
            batch_limit,
        }
    }

    ///
    /// # Arguments
    /// * `amount` - Quantità di gas da consumare
    ///
    /// # Returns
    /// * `Err(String)` se c'è overflow o gas insufficiente
    ///
    /// # Performance
    /// Usa batch accumulator per ridurre aggiornamenti frequenti
    pub fn consume(&mut self, amount: u64) -> Result<(), String> {
        // Aggiungi al batch accumulator
        self.batch_accumulator = self
            .batch_accumulator
            .checked_add(amount)
            .ok_or_else(|| "Gas overflow: batch accumulator would exceed u64::MAX".to_string())?;

        self.batch_count += 1;

        // Commit batch se raggiunge il limit
        if self.batch_count >= self.batch_limit {
            self.commit_batch()?;
        }

        Ok(())
    }

    /// Forza il commit of the batch corrente
    ///
    /// Utile alla fine dell'esecuzione per assicurarsi che tutto il gas
    /// accumulato venga contabilizzato.
    pub fn commit_batch(&mut self) -> Result<(), String> {
        if self.batch_accumulator > 0 {
            // Check overflow con checked arithmetic
            self.gas_used = self
                .gas_used
                .checked_add(self.batch_accumulator)
                .ok_or_else(|| "Gas overflow: addition would exceed u64::MAX".to_string())?;

            // Check che non si superi il limit
            if self.gas_used > self.gas_limit {
                return Err(format!(
                    "Out of gas: used {} > limit {}",
                    self.gas_used, self.gas_limit
                ));
            }

            // Resetta batch accumulator
            self.batch_accumulator = 0;
            self.batch_count = 0;
        }

        Ok(())
    }

    /// Consuma gas immediatamente (without batch)
    ///
    pub fn consume_immediate(&mut self, amount: u64) -> Result<(), String> {
        // Prima fa il commit of the batch corrente
        self.commit_batch()?;

        // Poi consuma immediatamente
        self.batch_accumulator = amount;
        self.batch_count = 1;
        self.commit_batch()
    }

    /// Consuma gas per SLOAD (lettura storage)
    pub fn consume_sload(&mut self) -> Result<(), String> {
        self.consume(self.costs.sload)
    }

    /// Consuma gas per SSTORE (scrittura storage)
    ///
    /// # Arguments
    /// * `is_new` - `true` se si sta scrivendo in uno slot vuoto, `false` se si sta aggiornando
    pub fn consume_sstore(&mut self, is_new: bool) -> Result<(), String> {
        let cost = if is_new {
            self.costs.sstore_new
        } else {
            self.costs.sstore_update
        };
        self.consume(cost)
    }

    /// Consuma gas per CALL (chiamata cross-contract)
    ///
    /// # Arguments
    /// * `calldata_len` - Lunghezza dei calldata in bytes (opzionale, per calcolo preciso)
    pub fn consume_call(&mut self, calldata_len: Option<usize>) -> Result<(), String> {
        let mut cost = self.costs.call;

        // Aggiungi costo per calldata se specificato
        if let Some(len) = calldata_len {
            let calldata_cost = (len as u64)
                .checked_mul(self.costs.calldata_byte)
                .ok_or_else(|| "Gas overflow: calldata cost calculation".to_string())?;
            cost = cost
                .checked_add(calldata_cost)
                .ok_or_else(|| "Gas overflow: call cost calculation".to_string())?;
        }

        self.consume(cost)
    }

    /// Consuma gas per CREATE (deploy contract)
    ///
    /// # Arguments
    /// * `bytecode_len` - Lunghezza of the bytecode in bytes
    pub fn consume_create(&mut self, bytecode_len: usize) -> Result<(), String> {
        let mut cost = self.costs.create;

        // Aggiungi costo per bytecode (simile a calldata)
        let bytecode_cost = (bytecode_len as u64)
            .checked_mul(self.costs.calldata_byte)
            .ok_or_else(|| "Gas overflow: bytecode cost calculation".to_string())?;
        cost = cost
            .checked_add(bytecode_cost)
            .ok_or_else(|| "Gas overflow: create cost calculation".to_string())?;

        self.consume(cost)
    }

    /// Consuma gas per LOG (emissione evento)
    ///
    /// # Arguments
    /// * `data_len` - Lunghezza dei data in bytes
    pub fn consume_log(&mut self, topics: usize, data_len: usize) -> Result<(), String> {
        let mut cost = self.costs.log;

        // Aggiungi costo per topics aggiuntivi (il primo è incluso nel base cost)
        if topics > 0 {
            let topics_cost = ((topics - 1) as u64)
                .checked_mul(self.costs.log_topic)
                .ok_or_else(|| "Gas overflow: log topics cost calculation".to_string())?;
            cost = cost
                .checked_add(topics_cost)
                .ok_or_else(|| "Gas overflow: log cost calculation".to_string())?;
        }

        // Aggiungi costo per data
        let data_cost = (data_len as u64)
            .checked_mul(self.costs.log_data)
            .ok_or_else(|| "Gas overflow: log data cost calculation".to_string())?;
        cost = cost
            .checked_add(data_cost)
            .ok_or_else(|| "Gas overflow: log cost calculation".to_string())?;

        self.consume(cost)
    }

    /// Consuma gas per transfer di token
    pub fn consume_transfer(&mut self) -> Result<(), String> {
        self.consume(self.costs.transfer)
    }

    /// Ottiene il gas rimanente
    ///
    /// # Returns
    /// Gas rimanente (0 se esaurito o superato)
    pub fn gas_remaining(&self) -> u64 {
        self.gas_limit.saturating_sub(self.gas_used)
    }

    /// Ottiene il gas used (include batch accumulator)
    pub fn gas_used(&self) -> u64 {
        self.gas_used + self.batch_accumulator
    }

    /// Ottiene il gas limit
    pub fn gas_limit(&self) -> u64 {
        self.gas_limit
    }

    /// Check se c'è gas sufficiente per una quantità specifica (include batch)
    ///
    /// # Arguments
    /// * `amount` - Quantità di gas richiesta
    ///
    /// # Returns
    /// `true` se c'è gas sufficiente, `false` altrimenti
    pub fn has_gas(&self, amount: u64) -> bool {
        let projected_used = self.gas_used + self.batch_accumulator;
        projected_used
            .checked_add(amount)
            .map_or(false, |total| total <= self.gas_limit)
    }

    ///
    /// # Arguments
    /// * `gas_limit` - Nuovo limit di gas
    pub fn reset(&mut self, gas_limit: u64) {
        // Prima fa commit di qualsiasi batch pendente
        let _ = self.commit_batch();

        self.gas_limit = gas_limit;
        self.gas_used = 0;
        self.batch_accumulator = 0;
        self.batch_count = 0;
    }

    /// Ottiene statistiche of the batch accounting
    pub fn batch_stats(&self) -> (usize, u64) {
        (self.batch_count, self.batch_accumulator)
    }
}
