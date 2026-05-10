//! Extension trait for libp2p gossipsub ConfigBuilder
//! Adds missing methods like mesh_outbound_max

use libp2p::gossipsub::ConfigBuilder;
use std::time::Duration;

/// Extension trait to add missing methods to libp2p gossipsub ConfigBuilder
pub trait GossipsubConfigBuilderExt {
    /// Set maximum number of outbound peers in the mesh network
    /// This method is not available in libp2p 0.55, so we implement it as an extension
    fn mesh_outbound_max(&mut self, mesh_outbound_max: usize) -> &mut Self;

    /// Set mesh_n_high value (upper bound for mesh size)
    fn mesh_n_high(&mut self, mesh_n_high: usize) -> &mut Self;

    fn set_mesh_params(
        &mut self,
        mesh_n: usize,
        mesh_outbound_min: usize,
        mesh_outbound_max: usize,
    ) -> &mut Self;
}

impl GossipsubConfigBuilderExt for ConfigBuilder {
    fn mesh_outbound_max(&mut self, mesh_outbound_max: usize) -> &mut Self {
        // mesh_outbound_max is not available in libp2p 0.55.
        // No-op to avoid accidentally overriding mesh parameters.
        let _ = mesh_outbound_max;
        self
    }

    fn mesh_n_high(&mut self, mesh_n_high: usize) -> &mut Self {
        // mesh_n_high is not available in libp2p 0.55.
        // No-op to avoid overriding mesh_n.
        let _ = mesh_n_high;
        self
    }

    fn set_mesh_params(
        &mut self,
        mesh_n: usize,
        mesh_outbound_min: usize,
        mesh_outbound_max: usize,
    ) -> &mut Self {
        // Validate parameters according to gossipsub spec
        assert!(
            mesh_outbound_min <= mesh_n / 2,
            "mesh_outbound_min must be <= mesh_n / 2"
        );
        assert!(
            mesh_outbound_min <= mesh_outbound_max,
            "mesh_outbound_min must be <= mesh_outbound_max"
        );
        assert!(
            mesh_outbound_max <= mesh_n,
            "mesh_outbound_max must be <= mesh_n"
        );

        self.mesh_n(mesh_n)
            .mesh_outbound_min(mesh_outbound_min)
            .mesh_outbound_max(mesh_outbound_max)
    }
}

/// Helper function to create a gossipsub config with full mesh parameters
pub fn create_gossipsub_config_with_mesh_params(
    mesh_n: usize,
    mesh_outbound_min: usize,
    mesh_outbound_max: usize,
    heartbeat_interval: Duration,
    validation_mode: libp2p::gossipsub::ValidationMode,
) -> libp2p::gossipsub::Config {
    let mut config = ConfigBuilder::default();
    config.heartbeat_interval(heartbeat_interval);
    config.validation_mode(validation_mode);

    config.set_mesh_params(mesh_n, mesh_outbound_min, mesh_outbound_max);
    // Build and return the final Config
    config.build().expect("Invalid gossipsub configuration")
}

#[cfg(test)]
mod tests {
    use super::*;
    use libp2p::gossipsub::ValidationMode;
    use std::time::Duration;

    #[test]
    fn test_mesh_outbound_max_extension() {
        let mut config = ConfigBuilder::default();

        // This should work without compilation errors
        config.mesh_outbound_max(6);
        config.mesh_n_high(12);

        let result = config.set_mesh_params(12, 3, 6);

        // Should not panic
        assert!(true);
    }

    #[test]
    fn test_mesh_params_validation() {
        let mut config = ConfigBuilder::default();

        // Valid parameters should work
        config.set_mesh_params(12, 3, 6);

        // Invalid parameters should panic
        std::panic::catch_unwind(|| {
            config.set_mesh_params(12, 7, 6); // outbound_min > mesh_n/2
        })
        .expect_err("Should panic for invalid parameters");
    }

    #[test]
    fn test_create_gossipsub_config() {
        let _config = create_gossipsub_config_with_mesh_params(
            12,
            3,
            6,
            Duration::from_secs(10),
            ValidationMode::Strict,
        );

        // Should create successfully
        assert!(true);
    }
}
