//! Randomized data generation for Ironwood types.

use proptest::prelude::*;

use crate::{ironwood, orchard};

impl Arbitrary for ironwood::Nullifier {
    type Parameters = ();

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        any::<orchard::Nullifier>()
            .prop_map(ironwood::Nullifier::from)
            .boxed()
    }

    type Strategy = BoxedStrategy<Self>;
}

impl Arbitrary for ironwood::ShieldedData {
    type Parameters = ();

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        any::<orchard::ShieldedData>()
            .prop_map(orchard::ShieldedDataV6::new)
            .prop_map(ironwood::ShieldedData::new)
            .boxed()
    }

    type Strategy = BoxedStrategy<Self>;
}
