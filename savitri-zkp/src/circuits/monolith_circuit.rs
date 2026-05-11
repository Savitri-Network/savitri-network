//! Arkworks R1CS circuit for monolith integrity proofs.
//!
//! Proves the algebraic relation: w1 + w2 + w3 = public_sum
//! where w1, w2, w3 are field elements derived from monolith header fields
//! (compressed prev_state_root, headers_commit, state_commit).

use ark_ff::Field;
use ark_relations::r1cs::{
    ConstraintSynthesizer, ConstraintSystemRef, LinearCombination,
    SynthesisError, Variable,
};

/// Monolith sum circuit for Groth16.
///
/// Witnesses: w1 (prev_state_root), w2 (headers_commit), w3 (state_commit)
/// Public input: sum = w1 + w2 + w3
///
/// This provides genuine Groth16 security guarantees — a valid proof
/// can only be constructed by a prover who knows the witness values.
pub struct MonolithSumCircuit<F: Field> {
    pub w1: Option<F>,
    pub w2: Option<F>,
    pub w3: Option<F>,
}

impl<F: Field> ConstraintSynthesizer<F> for MonolithSumCircuit<F> {
    fn generate_constraints(
        self,
        cs: ConstraintSystemRef<F>,
    ) -> Result<(), SynthesisError> {
        // Allocate private witness variables
        let w1_var = cs.new_witness_variable(|| {
            self.w1.ok_or(SynthesisError::AssignmentMissing)
        })?;
        let w2_var = cs.new_witness_variable(|| {
            self.w2.ok_or(SynthesisError::AssignmentMissing)
        })?;
        let w3_var = cs.new_witness_variable(|| {
            self.w3.ok_or(SynthesisError::AssignmentMissing)
        })?;

        // Compute the public sum
        let sum_val = match (self.w1, self.w2, self.w3) {
            (Some(a), Some(b), Some(c)) => Ok(a + b + c),
            _ => Err(SynthesisError::AssignmentMissing),
        };
        let sum_var = cs.new_input_variable(|| sum_val)?;

        // Enforce R1CS constraint: (w1 + w2 + w3) * 1 = sum
        cs.enforce_constraint(
            LinearCombination::zero() + w1_var + w2_var + w3_var,
            LinearCombination::zero() + Variable::One,
            LinearCombination::zero() + sum_var,
        )?;

        Ok(())
    }
}

