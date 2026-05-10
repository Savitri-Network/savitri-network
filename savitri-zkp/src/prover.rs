//! ZKP proof generation.
//!
//! Provides the `ZkProver` trait and implementations for generating
//! real cryptographic proofs from witness values.

use anyhow::Result;
use crate::zkp::{ZkProof, Statement};

/// Trait for generating ZK proofs from statements.
pub trait ZkProver: Send + Sync {
    fn prove(&self, statement: &Statement) -> Result<ZkProof>;
}

/// Mock prover for testing — produces deterministic placeholder proofs.
pub struct MockProver;

impl ZkProver for MockProver {
    fn prove(&self, _statement: &Statement) -> Result<ZkProof> {
        Ok(ZkProof {
            proof: vec![1, 2, 3, 4],
            public_inputs: vec![5, 6, 7, 8],
            verification_key: vec![9, 10, 11, 12],
        })
    }
}

/// Arkworks Groth16 prover — generates real Groth16 proofs over BN254.
#[cfg(feature = "arkworks")]
pub struct ArkworksProver {
    proving_key: ark_groth16::ProvingKey<ark_bn254::Bn254>,
    verification_key_bytes: Vec<u8>,
}

#[cfg(feature = "arkworks")]
impl ArkworksProver {
    /// Run trusted setup and create a prover.
    ///
    /// Uses a deterministic seed derived from the circuit description
    /// for reproducible key generation. For production, replace with
    /// an MPC ceremony.
    pub fn from_setup() -> Result<Self> {
        use ark_bn254::{Bn254, Fr};
        use ark_groth16::Groth16;
        use ark_serialize::CanonicalSerialize;
        use ark_snark::SNARK;
        use ark_std::rand::SeedableRng;
        use sha2::Digest;

        // Deterministic seed for reproducible setup
        let seed: [u8; 32] = sha2::Sha256::digest(
            b"savitri-monolith-sum-circuit-setup-v1",
        )
        .into();
        let mut rng = ark_std::rand::rngs::StdRng::from_seed(seed);

        // Empty circuit for setup (witnesses are None)
        let circuit =
            crate::circuits::monolith_circuit::MonolithSumCircuit::<Fr> {
                w1: None,
                w2: None,
                w3: None,
            };

        let (pk, vk) =
            Groth16::<Bn254>::circuit_specific_setup(circuit, &mut rng)
                .map_err(|e| {
                    anyhow::anyhow!("Groth16 trusted setup failed: {:?}", e)
                })?;

        // Serialize verification key for embedding in proofs
        let mut vk_bytes = Vec::new();
        vk.serialize_compressed(&mut vk_bytes).map_err(|e| {
            anyhow::anyhow!("Verification key serialization failed: {:?}", e)
        })?;

        tracing::info!(
            vk_size_bytes = vk_bytes.len(),
            "Groth16 trusted setup complete"
        );

        Ok(Self {
            proving_key: pk,
            verification_key_bytes: vk_bytes,
        })
    }

    /// Create a prover from pre-existing serialized keys.
    pub fn from_keys(pk_bytes: &[u8], vk_bytes: &[u8]) -> Result<Self> {
        use ark_serialize::CanonicalDeserialize;

        let pk = ark_groth16::ProvingKey::<ark_bn254::Bn254>::deserialize_compressed(pk_bytes)
            .map_err(|e| {
                anyhow::anyhow!("Proving key deserialization failed: {:?}", e)
            })?;

        Ok(Self {
            proving_key: pk,
            verification_key_bytes: vk_bytes.to_vec(),
        })
    }

    pub fn verification_key_bytes(&self) -> &[u8] {
        &self.verification_key_bytes
    }

    /// Serialize the proving key for storage.
    pub fn proving_key_bytes(&self) -> Result<Vec<u8>> {
        use ark_serialize::CanonicalSerialize;
        let mut bytes = Vec::new();
        self.proving_key
            .serialize_compressed(&mut bytes)
            .map_err(|e| {
                anyhow::anyhow!(
                    "Proving key serialization failed: {:?}",
                    e
                )
            })?;
        Ok(bytes)
    }
}

#[cfg(feature = "arkworks")]
impl ZkProver for ArkworksProver {
    fn prove(&self, statement: &Statement) -> Result<ZkProof> {
        use ark_bn254::{Bn254, Fr};
        use ark_ff::PrimeField;
        use ark_groth16::Groth16;
        use ark_serialize::CanonicalSerialize;
        use ark_snark::SNARK;

        let w1 = Fr::from_le_bytes_mod_order(&statement.a);
        let w2 = Fr::from_le_bytes_mod_order(&statement.b);
        let w3 = Fr::from_le_bytes_mod_order(&statement.c);

        let circuit =
            crate::circuits::monolith_circuit::MonolithSumCircuit {
                w1: Some(w1),
                w2: Some(w2),
                w3: Some(w3),
            };

        // SECURITY (HIGH-04): Use OsRng for Groth16 proof generation.
        // The RNG provides the zero-knowledge blinding factors; using a weak
        // PRNG could allow an attacker to reconstruct witness values from proofs.
        let mut rng = rand::rngs::OsRng;
        let proof =
            Groth16::<Bn254>::prove(&self.proving_key, circuit, &mut rng)
                .map_err(|e| {
                    anyhow::anyhow!("Groth16 proof generation failed: {:?}", e)
                })?;

        // Serialize proof
        let mut proof_bytes = Vec::new();
        proof.serialize_compressed(&mut proof_bytes).map_err(|e| {
            anyhow::anyhow!("Proof serialization failed: {:?}", e)
        })?;

        // Serialize public input (the sum)
        let sum = w1 + w2 + w3;
        let mut sum_bytes = Vec::new();
        sum.serialize_compressed(&mut sum_bytes).map_err(|e| {
            anyhow::anyhow!("Public input serialization failed: {:?}", e)
        })?;

        Ok(ZkProof {
            proof: proof_bytes,
            public_inputs: sum_bytes,
            verification_key: self.verification_key_bytes.clone(),
        })
    }
}
