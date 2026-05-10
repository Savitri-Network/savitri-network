#[cfg(test)]
mod tests {
    use super::*;
    use crate::monolith::headers_commit;
    use crate::monolith::monolith_zkp;
    use crate::monolith::MonolithHeader;
    use crate::verifier::{MockVerifier, Statement, ZkProof, ZkVerifier};
    use crate::zkp::proof;
    use crate::{create_verifier, ZkpBackend, ZkpConfig};

    #[test]
    fn test_mock_verifier() {
        let verifier = MockVerifier::new();
        let proof = proof::generate_mock_proof(&crate::zkp::Statement {
            a: [1; 32],
            b: [2; 32],
            c: [3; 32],
            d: [4; 32],
            e: 100,
            f: 200,
        });

        let result = verifier
            .verify(
                &crate::zkp::Statement {
                    a: [1; 32],
                    b: [2; 32],
                    c: [3; 32],
                    d: [4; 32],
                    e: 100,
                    f: 200,
                },
                &proof,
            )
            .unwrap();
        assert!(result);
    }

    #[test]
    fn test_mock_verifier_always_false() {
        let verifier = MockVerifier::always_valid(false);
        let proof = proof::generate_mock_proof(&crate::zkp::Statement {
            a: [1; 32],
            b: [2; 32],
            c: [3; 32],
            d: [4; 32],
            e: 100,
            f: 200,
        });

        let result = verifier
            .verify(
                &crate::zkp::Statement {
                    a: [1; 32],
                    b: [2; 32],
                    c: [3; 32],
                    d: [4; 32],
                    e: 100,
                    f: 200,
                },
                &proof,
            )
            .unwrap();
        assert!(!result);
    }

    #[test]
    fn test_batch_verify() {
        let verifier = MockVerifier::new();
        let statements = vec![
            crate::zkp::Statement {
                a: [1; 32],
                b: [2; 32],
                c: [3; 32],
                d: [4; 32],
                e: 100,
                f: 200,
            },
            crate::zkp::Statement {
                a: [5; 32],
                b: [6; 32],
                c: [7; 32],
                d: [8; 32],
                e: 300,
                f: 400,
            },
        ];
        let proofs = statements
            .iter()
            .map(proof::generate_mock_proof)
            .collect::<Vec<_>>();

        let results = verifier.batch_verify(&statements, &proofs).unwrap();
        assert_eq!(results.len(), 2);
        assert!(results[0]);
        assert!(results[1]);
    }

    #[test]
    fn test_proof_validation() {
        let valid_proof = proof::generate_mock_proof(&crate::zkp::Statement {
            a: [1; 32],
            b: [2; 32],
            c: [3; 32],
            d: [4; 32],
            e: 100,
            f: 200,
        });

        assert!(proof::validate_proof_structure(&valid_proof).is_ok());

        let invalid_proof = ZkProof {
            proof: vec![],
            public_inputs: vec![1, 2, 3],
            verification_key: vec![4, 5, 6],
        };
        assert!(proof::validate_proof_structure(&invalid_proof).is_err());
    }

    #[test]
    fn test_statement_hash() {
        let hash = crate::zkp::utils::hash_statement(&crate::zkp::Statement {
            a: [1; 32],
            b: [2; 32],
            c: [3; 32],
            d: [4; 32],
            e: 100,
            f: 200,
        });
        assert_ne!(hash, [0; 32]);
    }

    #[test]
    fn test_statement_serialization() {
        let serialized = crate::zkp::utils::serialize_statement(&crate::zkp::Statement {
            a: [1; 32],
            b: [2; 32],
            c: [3; 32],
            d: [4; 32],
            e: 100,
            f: 200,
        });
        assert!(!serialized.is_empty());
    }

    #[test]
    fn test_monolith_zkp_verification() {
        let verifier = MockVerifier::new();
        let header = MonolithHeader {
            headers_commit: [1; 64],
            state_commit: [2; 64],
            exec_height: 1000,
            epoch_id: 5,
        };
        let prev_state_root = Some([3; 64]);
        let prev_epoch_id = Some(4);

        let result =
            monolith_zkp::verify_monolith_proof(&header, prev_state_root, prev_epoch_id, &verifier)
                .unwrap();
        assert!(result);
    }

    #[test]
    fn test_epoch_regression_protection() {
        let verifier = MockVerifier::new();
        let header = MonolithHeader {
            headers_commit: [1; 64],
            state_commit: [2; 64],
            exec_height: 1000,
            epoch_id: 3, // Lower than previous epoch
        };
        let prev_state_root = Some([3; 64]);
        let prev_epoch_id = Some(5); // Higher than current epoch

        let result =
            monolith_zkp::verify_monolith_proof(&header, prev_state_root, prev_epoch_id, &verifier);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("epoch regression"));
    }

    #[test]
    fn test_headers_commit_verification() {
        let leaves = vec![[1; 64], [2; 64], [3; 64]];
        let header = MonolithHeader {
            headers_commit: headers_commit::headers_commit_from_hashes(&leaves),
            state_commit: [2; 64],
            exec_height: 1000,
            epoch_id: 5,
        };

        let result = headers_commit::verify_headers_commit(&leaves, &header);
        assert!(result.is_ok());
    }

    #[test]
    fn test_headers_commit_mismatch() {
        let leaves = vec![[1; 64], [2; 64], [3; 64]];
        let header = MonolithHeader {
            headers_commit: [0; 64], // Wrong commit
            state_commit: [2; 64],
            exec_height: 1000,
            epoch_id: 5,
        };

        let result = headers_commit::verify_headers_commit(&leaves, &header);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("mismatch"));
    }

    #[test]
    fn test_compression_64_to_32() {
        let root_64 = [1; 64];
        let compressed = monolith_zkp::compress_root_64_to_32(&root_64);
        assert_ne!(compressed, [0; 32]);
        assert_eq!(compressed.len(), 32);
    }

    #[test]
    fn test_zkp_config_default() {
        let config = ZkpConfig::default();
        assert!(matches!(config.backend, ZkpBackend::Mock));
        assert_eq!(config.max_proof_size, 1024 * 1024);
        assert_eq!(config.verification_timeout_ms, 5000);
    }

    #[test]
    fn test_create_verifier_mock() {
        let config = ZkpConfig::default();
        let verifier = create_verifier(config);

        let proof = proof::generate_mock_proof(&crate::zkp::Statement {
            a: [1; 32],
            b: [2; 32],
            c: [3; 32],
            d: [4; 32],
            e: 100,
            f: 200,
        });

        let result = verifier
            .verify(
                &crate::zkp::Statement {
                    a: [1; 32],
                    b: [2; 32],
                    c: [3; 32],
                    d: [4; 32],
                    e: 100,
                    f: 200,
                },
                &proof,
            )
            .unwrap();
        assert!(result);
    }

    #[test]
    fn test_production_config() {
        let config = ZkpConfig::production();
        assert!(matches!(config.backend, ZkpBackend::Arkworks));
        assert_eq!(config.max_proof_size, 4 * 1024 * 1024);
        assert_eq!(config.verification_timeout_ms, 15000);
    }

    #[test]
    fn test_development_config() {
        let config = ZkpConfig::development();
        assert!(matches!(config.backend, ZkpBackend::Mock));
        assert_eq!(config.max_proof_size, 1024 * 1024);
        assert_eq!(config.verification_timeout_ms, 5000);
    }

    #[test]
    fn test_testing_config() {
        let config = ZkpConfig::testing();
        assert!(matches!(config.backend, ZkpBackend::Mock));
        assert_eq!(config.max_proof_size, 512 * 1024);
        assert_eq!(config.verification_timeout_ms, 1000);
    }

    #[cfg(feature = "arkworks")]
    #[test]
    fn test_arkworks_verifier_creation() {
        let config = ZkpConfig::production();
        let verifier = create_verifier(config);

        let statement = Statement {
            a: [1; 32],
            b: [2; 32],
            c: [3; 32],
            d: [4; 32],
            e: 100,
            f: 200,
        };

        // Create a valid Groth16-style proof
        let mut public_inputs = Vec::new();
        public_inputs.extend_from_slice(&[1; 32]); // a
        public_inputs.extend_from_slice(&[2; 32]); // b
        public_inputs.extend_from_slice(&[3; 32]); // c
        public_inputs.extend_from_slice(&[4; 32]); // d
        public_inputs.extend_from_slice(&100u64.to_le_bytes()); // e
        public_inputs.extend_from_slice(&200u64.to_le_bytes()); // f

        let proof = ZkProof {
            proof: vec![1; 48 * 3], // Groth16 proof size (3 points)
            public_inputs,
            verification_key: vec![1, 2, 3, 4],
        };

        let result = verifier.verify(&statement, &proof);
        assert!(result.is_ok());
    }

    #[test]
    fn test_feature_flag_compatibility() {
        // Test that the library works with default features (Mock)
        let config = ZkpConfig::default();
        let verifier = create_verifier(config);

        let statement = crate::zkp::Statement {
            a: [5; 32],
            b: [6; 32],
            c: [7; 32],
            d: [8; 32],
            e: 300,
            f: 400,
        };
        let proof = proof::generate_mock_proof(&statement);

        let result = verifier.verify(&statement, &proof).unwrap();
        assert!(result);
    }
}
