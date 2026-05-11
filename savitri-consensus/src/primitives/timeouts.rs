//! Centralized timeout / threshold tunables.
//!
//! All magic numbers that were previously inline `const FOO_MS: u64 = 2000`
//! at random callsites live here, with documented rationale (network model
//! assumed) + env override hooks.
//!
//! See `memory/architectural_debt.md` Tier 7.
//!
//! Phase 1: this module only introduces the struct + env override hooks.
//! Existing callers still use their inline `const`s; migration to read
//! values from `Timeouts` happens in Phase 2.

#[derive(Debug, Clone, Copy)]
pub struct Timeouts {
    /// How long a pending block stays in the proposer's pipeline before
    /// being evicted as stale. Previously hardcoded as 300s, raised to
    /// 900s after the geo-distributed BFT cert timing analysis.
    pub pending_block_ttl_secs: u64,
    /// Backup-cert generation timeout; promotes a fallback proposer when
    /// the primary fails to produce a cert in time. 300ms was too tight
    /// for the testnet's cross-region RTTs.
    pub backup_cert_timeout_ms: u64,
    /// Width of the per-account nonce window the mempool admits ahead of
    /// the current chain nonce.
    pub nonce_window: u64,
    /// Maximum tolerated epoch drift between local view and a peer's
    /// announced epoch before the peer is considered desynced.
    pub max_epoch_drift: u64,
    /// Maximum number of in-flight proposed blocks the proposer may have
    /// pipelined before pausing for finalization to catch up.
    pub max_pipeline_depth: u64,
    /// Acceptable absolute delta (seconds) between a cert's timestamp and
    pub cert_timestamp_tolerance_secs: u64,
    /// Maximum gap (in nonce units) between an admitted TX's nonce and
    /// the current main-pool head before the TX is routed to the queued
    /// pool instead of the main pool.
    pub admission_max_main_pool_nonce_gap: u64,
    /// Interval (seconds) at which the in-flight TX restore task scans
    /// for orphaned/expired entries.
    pub inflight_restore_interval_secs: u64,
    /// Maximum age (seconds) of an in-flight TX before it is considered
    /// orphaned and re-queued or evicted.
    pub inflight_restore_max_age_secs: u64,
}

impl Default for Timeouts {
    fn default() -> Self {
        Self {
            pending_block_ttl_secs: 900,
            backup_cert_timeout_ms: 2000,
            nonce_window: 3500,
            max_epoch_drift: 100,
            max_pipeline_depth: 16,
            cert_timestamp_tolerance_secs: 900,
            admission_max_main_pool_nonce_gap: 3000,
            inflight_restore_interval_secs: 10,
            inflight_restore_max_age_secs: 30,
        }
    }
}

impl Timeouts {
    /// Override defaults from environment variables. Each field has a
    /// `SAVITRI_<UPPER>` env. Missing/invalid values keep the default.
    pub fn from_env() -> Self {
        let mut t = Self::default();
        macro_rules! env_or {
            ($field:ident, $env:expr) => {
                if let Ok(s) = std::env::var($env) {
                    if let Ok(v) = s.parse() {
                        t.$field = v;
                    }
                }
            };
        }
        env_or!(pending_block_ttl_secs, "SAVITRI_PENDING_BLOCK_TTL_SECS");
        env_or!(backup_cert_timeout_ms, "SAVITRI_BACKUP_CERT_TIMEOUT_MS");
        env_or!(nonce_window, "SAVITRI_NONCE_WINDOW");
        env_or!(max_epoch_drift, "SAVITRI_MAX_EPOCH_DRIFT");
        env_or!(max_pipeline_depth, "SAVITRI_MAX_PIPELINE_DEPTH");
        env_or!(
            cert_timestamp_tolerance_secs,
            "SAVITRI_CERT_TIMESTAMP_TOLERANCE_SECS"
        );
        env_or!(
            admission_max_main_pool_nonce_gap,
            "SAVITRI_ADMISSION_MAX_MAIN_POOL_NONCE_GAP"
        );
        env_or!(
            inflight_restore_interval_secs,
            "SAVITRI_INFLIGHT_RESTORE_INTERVAL_SECS"
        );
        env_or!(
            inflight_restore_max_age_secs,
            "SAVITRI_INFLIGHT_RESTORE_MAX_AGE_SECS"
        );
        t
    }
}
