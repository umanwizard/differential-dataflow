//! Equijoin expression plan.

use std::hash::Hash;

use timely::dataflow::Scope;

use differential_dataflow::operators::JoinCore;

use differential_dataflow::{Collection, ExchangeData};
use plan::{Plan, Render, Stash};
use crate::{Diff, Datum};

/// A plan stage joining two source relations on the specified
/// symbols. Throws if any of the join symbols isn't bound by both
/// sources.
#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Join<Value: Datum> {
    /// Pairs of indices whose values must be equal.
    pub keys: Vec<(usize, usize)>,
    /// Plan for the left input.
    pub plan1: Box<Plan<Value>>,
    /// Plan for the right input.
    pub plan2: Box<Plan<Value>>,
}

impl<V: ExchangeData+Hash+Datum> Render for Join<V> {

    type Value = V;

    fn render<S: Scope>(
        &self,
        scope: &mut S,
        stash: &mut Stash<S, Self::Value>,
    ) -> Collection<S, Vec<Self::Value>, Diff>
    where
        S::Timestamp: differential_dataflow::lattice::Lattice+timely::progress::timestamp::Refines<crate::Time>,
    {
        use differential_dataflow::operators::arrange::ArrangeByKey;

        // acquire arrangements for each input.
        let keys1 = self.keys.iter().map(|key| key.0).collect::<Vec<_>>();
        let vals1 = (0 .. self.plan1.arity).filter(|i| !keys1.contains(i)).collect::<Vec<_>>();
        if stash.get_local(&self.plan1, Some(&keys1[..])).is_none() {
            let keys = keys1.clone();
            let vals = vals1.clone();
            let arrangement =
            self.plan1
                .render(scope, stash)
                .map(move |tuple|
                    (
                        // TODO: Re-use `tuple` for values.
                        keys.iter().map(|index| tuple[*index].clone()).collect::<Vec<_>>(),
                        vals.iter().map(|index| tuple[*index].clone()).collect::<Vec<_>>(),
                    )
                )
                .arrange_by_key();

            stash.set_local((*self.plan1).clone(), Some(&keys1[..]), arrangement);
        };

        // extract relevant fields for each index.
        let keys2 = self.keys.iter().map(|key| key.1).collect::<Vec<_>>();
        let vals2 = (0 .. self.plan2.arity).filter(|i| !keys2.contains(i)).collect::<Vec<_>>();
        if stash.get_local(&self.plan2, Some(&keys2[..])).is_none() {
            let keys = keys2.clone();
            let vals = vals2.clone();
            let arrangement =
            self.plan2
                .render(scope, stash)
                .map(move |tuple|
                    (
                        // TODO: Re-use `tuple` for values.
                        keys.iter().map(|index| tuple[*index].clone()).collect::<Vec<_>>(),
                        vals.iter().map(|index| tuple[*index].clone()).collect::<Vec<_>>(),
                    )
                )
                .arrange_by_key();

            stash.set_local((*self.plan2).clone(), Some(&keys2[..]), arrangement);
        };

        let arrange1 = stash.get_local(&self.plan1, Some(&keys1[..])).expect("We just installed this");
        let arrange2 = stash.get_local(&self.plan2, Some(&keys2[..])).expect("We just installed this");

        arrange1
            .join_core(&arrange2, |keys, vals1, vals2| {
                Some(
                    keys.iter().cloned()
                        .chain(vals1.iter().cloned())
                        .chain(vals2.iter().cloned())
                        .collect()
                )
            })
    }
}
