//! Integration tests for ZKP implementation
//!
//! These tests verify that the ZKP system works end-to-end
//! with different backends and configurations.

use savitri_zkp::verifier::{ArkworksVerifier, MockVerifier, PlonkVerifier};
use savitri_zkp::{Statement, ZkProof, ZkpBackend, ZkpConfig, ZkpVerifierFactory};

#[test]
fn test_zkp_backend_selection() {
    // Test Mock backend
    let mock_config = ZkpConfig {
        backend: ZkpBackend::Mock,
        ..Default::default()
    };
    let mock_verifier = ZkpVerifierFactory::create(mock_config);
    assert!(mock_verifier
        .verify(&Statement::default(), &ZkProof::default())
        .unwrap());

    // Test PLONK backend (if available)
    #[cfg(feature = "plonk")]
    {
        let plonk_config = ZkpConfig {
            backend: ZkpBackend::Plonk,
            ..Default::default()
        };
        let plonk_verifier = ZkpVerifierFactory::create(plonk_config);
        assert!(plonk_verifier
            .verify(&Statement::default(), &Zkp::default())
            .unwrap());
    }

    // Test Arkworks backend (if available)
    #[cfg(feature = "arkworks")]
    {
        let arkworks_config = ZkpConfig {
            backend: ZkpBackend::Arkworks,
            ..Default::default()
        };
        let arkworks_verifier = ZkpVerifierFactory::create(arkworks_config);
        assert!(arkworks_verifier
            .verify(&Statement::default(), &Zkp::default())
            .unwrap());
    }
}

#[test]
fn test_real_zkp_proof_generation() {
    // Create a real statement
    let statement = Statement {
        a: [1u8; 32],
        b: [2u8; 32],
        c: [3u8; 32],
        d: [4u8; 32],
        e: 100,
        f: 200,
    };

    // Generate proof with Mock backend
    let mock_config = ZkpConfig {
        backend: ZkpBackend::Mock,
        ..Default::default()
    };
    let mock_verifier = ZkpVerifierFactory::create(mock_config);
    let mock_proof = ZkpProof {
        proof: vec![5, 6, 7, 8, 9, 10, 11, 12],
        public_inputs: vec![1, 2, 3, 4, 5, 6],
        verification_key: vec![7, 8, 9, 10, 11, 12],
    };

    let mock_result = mock_verifier.verify(&statement, &mock_proof).unwrap();
    assert!(mock_result);

    // Test batch verification
    let statements = vec![
        statement.clone(),
        Statement {
            a: [10u8; 32],
            b: [20u8; 32],
            c: [30u8; 32],
            d: [40u8; 32],
            e: 1000,
            f: 2000,
        },
    ];
    let proofs = vec![
        mock_proof.clone(),
        ZkpProof {
            proof: vec![50, 60, 70, 80, 90, 100, 110, 120],
            public_inputs: vec![10, 20, 30, 40, 50, 60],
            verification_key: vec![70, 80, 90, 100, 110, 120],
        },
    ];

    let batch_results = mock_verifier.batch_verify(&statements, &proofs).unwrap();
    assert_eq!(batch_results.len(), 2);
    assert!(batch_results[0]);
    assert!(batch_results[1]);
}

#[test]
fn test_zkp_proof_serialization() {
    // Test that ZKP proofs can be serialized and deserialized
    let original_proof = ZkProof {
        proof: vec![1, 2, 3, 4, 5, 6, 7, 8],
        public_inputs: vec![9, 10, 11, 12, 13, 14],
        verification_key: vec![15, 16, 17, 18, 19, 20],
    };

    // Serialize
    let serialized = bincode::serialize(&original_proof).unwrap();
    assert!(!serialized.is_empty());

    // Deserialize
    let deserialized: ZkProof = bincode::deserialize(&serialized).unwrap();
    assert_eq!(original_proof.proof, deserialized.proof);
    assert_eq!(original_proof.public_inputs, deserialized.public_inputs);
    assert_eq!(
        original_proof.verification_key,
        deserialized.verification_key
    );
}

#[test]
fn test_zkp_performance() {
    use std::time::Instant;

    let config = ZkpConfig {
        backend: ZkpBackend::Mock,
        max_proof_size: 1024 * 1024,
        verification_timeout_ms: 1000,
    };
    let verifier = ZkpVerifierFactory::create(config);

    // Create test data
    let statement = Statement {
        a: [1u8; 32],
        b: [2u8; 32],
        c: [3u8; 32],
        d: [4u8; 32],
        e: 100,
        f: 200,
    };
    let proof = ZkpProof {
        proof: vec![5; 1000], // Large proof
        public_inputs: vec![1; 1000],
        verification_key: vec![2; 1000],
    };

    // Test single verification performance
    let start = Instant::now();
    let result = verifier.verify(&statement, &proof).unwrap();
    let duration = start.elapsed();

    assert!(result);
    assert!(duration.as_millis() < 100); // Should be fast

    // Test batch verification performance
    let statements = vec![statement; 100];
    let proofs = vec![proof; 100];

    let start = Instant::now();
    let results = verifier.batch_verify(&statements, &proofs).unwrap();
    let duration = start.elapsed();

    assert_eq!(results.len(), 100);
    assert!(results.iter().all(|&r| r));
    assert!(duration.as_millis() < 1000); // Should still be fast
}

#[cfg(feature = "plonk")]
#[test]
fn test_plonk_integration() {
    use plonk::halo2curves::bn256::Fr;

    // Test PLONK-specific features
    let config = ZkpConfig {
        backend: ZkpBackend::Plonk,
        ..Default::default()
    };
    let verifier = ZkpVerifierFactory::create(config);

    // Test with PLONK-compatible statement
    let statement = Statement {
        a: [1u8; 32],
        b: [2u8; 32],
        c: [3u8; 32],
        d: [4u8; 32],
        e: 100,
        f: 200,
    };

    let plonk_proof = ZkpProof {
        proof: vec![1, 2, 3, 4],
        public_inputs: vec![5, 6, 7, 8, 9, 10],
        verification_key: vec![11, 12, 13, 14],
    };

    let result = verifier.verify(&statement, &plonk_proof).unwrap();
    assert!(result);

    // Test that PLONK verifier has different behavior from Mock
    let mock_verifier = ZkpVerifierFactory::create(ZkpConfig {
        backend: ZkpBackend::Mock,
        ..Default::default()
    });

    let mock_result = mock_verifier.verify(&statement, &plonk_proof).unwrap();

    // Results should be different (deterministic but different algorithms)
    assert_ne!(result, mock_result);
}

#[cfg(feature = "arkworks")]
#[test]
fn test_arkworks_integration() {
    use ark_bn254::Fr;

    // Test Arkworks-specific features
    let config = ZkpConfig {
        backend: ZkpBackend::Arkworks,
        ..Default::default()
    };
    let verifier = ZkpVerifierFactory::create(config);

    // Test with Arkworks-compatible statement
    let statement = Statement {
        a: [1u8; 32],
        b: [2u8; 32],
        c: [3u8; 32],
        d: [4u8; 32],
        e: 100,
        f: 200,
    };

    let arkworks_proof = ZkpProof {
        proof: vec![1, 2, 3, 4],
        public_inputs: vec![5, 6, 7, 8, 9, 10],
        verification_key: vec![11, 12, 13, 14],
    };

    let result = verifier.verify(&statement, &arkworks_proof).unwrap();
    assert!(result);

    // Test that Arkworks verifier has different behavior
    let mock_verifier = ZkpVerifierFactory::create(ZkpConfig {
        backend: ZkpBackend::Mock,
        ..Default::default()
    });

    let mock_result = mock_verifier.verify(&statement, &arkworks_proof).unwrap();

    // Results should be different (deterministic but different algorithms)
    assert_ne!(result, mock_result);
}

#[test]
fn test_zkp_error_handling() {
    let config = ZkpConfig {
        backend: ZkpBackend::Mock,
        max_proof_size: 100,
        verification_timeout_ms: 100,
    };
    let verifier = ZkpVerifierFactory::create(config);

    let statement = Statement::default();

    // Test with oversized proof
    let oversized_proof = ZkpProof {
        proof: vec![0; 200], // Exceeds max size
        public_inputs: vec![],
        verification_key: vec![],
    };

    let result = verifier.verify(&statement, &oversized_proof);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("exceeds maximum limit"));

    // Test with empty proof
    let empty_proof = ZkpProof {
        proof: vec![],
        public_inputs: vec![],
        verification_key: vec![],
    };

    let result = verifier.verify(&statement, &empty_proof);
    assert!(result.is_err());
}

#[test]
fn test_zkp_factory_fallback() {
    // Test fallback when features are not enabled

    // This should work even without PLONK feature
    let plonk_verifier = ZkpVerifierFactory::create_with_backend(ZkpBackend::Plonk);
    assert!(plonk_verifier
        .verify(&Statement::default(), &ZkProof::default())
        .is_ok());

    // This should work even without Arkworks feature
    let arkworks_verifier = ZkpVerifierFactory::create_with_backend(ZkpBackend::Arkworks);
    assert!(arkworks_verifier
        .verify(&Statement::default(), &ZkpProof::default())
        .is_ok());
}

#[test]
fn test_zkp_config_validation() {
    let config = ZkpConfig {
        backend: ZkpBackend::Mock,
        max_proof_size: 1024,
        verification_timeout_ms: 5000,
    };

    assert_eq!(config.backend, ZkpBackend::Mock);
    assert_eq!(config.max_proof_size, 1024);
    assert_eq!(config.verification_timeout_ms, 5000);

    // Test default configuration
    let default_config = ZkpConfig::default();
    assert_eq!(default_config.backend, ZkpBackend::Mock);
    assert_eq!(default_config.max_proof_size, 1024 * 1024);
    assert_eq!(default_config.verification_timeout_ms, 5000);
}
