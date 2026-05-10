//! halo2 PLONK circuit for monolith integrity proofs.
//!
//! Proves the same algebraic relation as the Arkworks circuit:
//! w1 + w2 + w3 = public_sum
//!
//! Uses halo2's PLONKish arithmetization with custom gates.

use halo2_proofs::{
    circuit::{Layouter, SimpleFloorPlanner, Value},
    plonk::{
        Advice, Circuit, Column, ConstraintSystem, Error as PlonkError,
        Instance, Selector,
    },
    poly::Rotation,
};

/// Configuration for the sum circuit.
#[derive(Clone, Debug)]
pub struct SumConfig {
    pub advice: [Column<Advice>; 4],
    pub instance: Column<Instance>,
    pub selector: Selector,
}

/// PLONK circuit proving w1 + w2 + w3 = public_sum.
#[derive(Clone, Debug)]
pub struct MonolithSumCircuit<F: halo2_proofs::arithmetic::Field> {
    pub w1: Value<F>,
    pub w2: Value<F>,
    pub w3: Value<F>,
}

impl<F: halo2_proofs::arithmetic::Field> Default for MonolithSumCircuit<F> {
    fn default() -> Self {
        Self {
            w1: Value::unknown(),
            w2: Value::unknown(),
            w3: Value::unknown(),
        }
    }
}

impl<F: halo2_proofs::arithmetic::Field> Circuit<F> for MonolithSumCircuit<F> {
    type Config = SumConfig;
    type FloorPlanner = SimpleFloorPlanner;

    fn without_witnesses(&self) -> Self {
        Self::default()
    }

    fn configure(meta: &mut ConstraintSystem<F>) -> Self::Config {
        // Three advice columns for witnesses, one for the computed sum
        let advice = [
            meta.advice_column(),
            meta.advice_column(),
            meta.advice_column(),
            meta.advice_column(),
        ];
        let instance = meta.instance_column();
        let selector = meta.selector();

        // Enable equality constraints for copy constraints
        for col in &advice {
            meta.enable_equality(*col);
        }
        meta.enable_equality(instance);

        // Custom gate: s * (w1 + w2 + w3 - sum) = 0
        meta.create_gate("sum_check", |meta| {
            let s = meta.query_selector(selector);
            let w1 = meta.query_advice(advice[0], Rotation::cur());
            let w2 = meta.query_advice(advice[1], Rotation::cur());
            let w3 = meta.query_advice(advice[2], Rotation::cur());
            let sum = meta.query_advice(advice[3], Rotation::cur());

            vec![s * (w1 + w2 + w3 - sum)]
        });

        SumConfig {
            advice,
            instance,
            selector,
        }
    }

    fn synthesize(
        &self,
        config: Self::Config,
        mut layouter: impl Layouter<F>,
    ) -> Result<(), PlonkError> {
        let sum_cell = layouter.assign_region(
            || "sum_computation",
            |mut region| {
                config.selector.enable(&mut region, 0)?;

                region.assign_advice(
                    || "w1",
                    config.advice[0],
                    0,
                    || self.w1,
                )?;
                region.assign_advice(
                    || "w2",
                    config.advice[1],
                    0,
                    || self.w2,
                )?;
                region.assign_advice(
                    || "w3",
                    config.advice[2],
                    0,
                    || self.w3,
                )?;

                let sum_val = self.w1.and_then(|w1| {
                    self.w2
                        .and_then(|w2| self.w3.map(|w3| w1 + w2 + w3))
                });

                let sum_cell = region.assign_advice(
                    || "sum",
                    config.advice[3],
                    0,
                    || sum_val,
                )?;

                Ok(sum_cell)
            },
        )?;

        // Constrain sum advice cell to equal the public instance value
        layouter.constrain_instance(
            sum_cell.cell(),
            config.instance,
            0,
        )?;

        Ok(())
    }
}
