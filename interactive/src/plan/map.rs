//! Projection expression plan.

use std::hash::Hash;

use timely::dataflow::Scope;

use differential_dataflow::{Collection, ExchangeData};
use plan::{Plan, Render, Stash};
use crate::{Diff, Datum};

/// A plan which retains values at specified locations.
///
/// The plan does not ascribe meaning to specific locations (e.g. bindings)
/// to variable names, and simply selects out the indicated sequence of values,
/// panicking if some input record is insufficiently long.
#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Map<V: Datum> {
    /// Sequence (and order) of indices to be retained.
    pub expressions: Vec<V::Expression>,
    /// Plan for the data source.
    pub plan: Box<Plan<V>>,
}

impl<V: ExchangeData+Hash+Datum> Render for Map<V> {
    type Value = V;

    fn render<S: Scope>(
        &self,
        scope: &mut S,
        stash: &mut Stash<S, Self::Value>,
    ) -> Collection<S, Vec<Self::Value>, Diff>
    where
        S::Timestamp: differential_dataflow::lattice::Lattice+timely::progress::timestamp::Refines<crate::Time>,
    {
        let expressions = self.expressions.clone();

        // TODO: re-use `tuple` allocation.
        self.plan
            .render(scope, stash)
            .map(move |tuple|
                expressions
                    .iter()
                    .map(|expr| V::subject_to(&tuple[..], expr))
                    .collect()
            )
    }
}
