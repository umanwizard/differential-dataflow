//! Types and traits for implementing query plans.

use std::hash::Hash;
use std::collections::HashMap;

use timely::dataflow::Scope;
use differential_dataflow::{Collection, ExchangeData};
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::lattice::Lattice;

use crate::Diff;
use crate::manager::TraceValHandle;

// pub mod count;
pub mod filter;
pub mod join;
pub mod map;
pub mod sfw;

use crate::Datum;

// pub use self::count::Count;
pub use self::filter::{Filter, Predicate};
pub use self::join::Join;
pub use self::sfw::MultiwayJoin;
pub use self::map::Map;

/// Dataflow-local collections and arrangements.
pub struct Stash<S: Scope, V: ExchangeData+Datum>
where
    S::Timestamp: Lattice,
{
    collections: HashMap<Plan<V>, Collection<S, Vec<V>, Diff>>,
    local: HashMap<Plan<V>, HashMap<Vec<usize>, Arranged<S, TraceValHandle<Vec<V>, Vec<V>, S::Timestamp, Diff>>>>,
    trace: HashMap<Plan<V>, HashMap<Vec<usize>, Arranged<S, TraceValHandle<Vec<V>, Vec<V>, S::Timestamp, Diff>>>>,
}

impl<S: Scope, V: ExchangeData+Datum> Stash<S, V>
where
    S::Timestamp: Lattice,
{
    /// Retrieves an arrangement from a plan and keys.
    pub fn get_local(&self, plan: &Plan<V>, keys: Option<&[usize]>) -> Option<&Arranged<S, TraceValHandle<Vec<V>, Vec<V>, S::Timestamp, Diff>>> {
        if let Some(keys) = keys {
            self.local.get(plan).and_then(|x| x.get(keys))
        }
        else {
            let keys = (0 .. plan.arity).collect::<Vec<_>>();
            self.local.get(plan).and_then(|x| x.get(&keys))
        }
    }
    /// Binds a plan and keys to an arrangement.
    pub fn set_local(&mut self, plan: Plan<V>, keys: Option<&[usize]>, arranged: Arranged<S, TraceValHandle<Vec<V>, Vec<V>, S::Timestamp, Diff>>) {
        let keys = keys.map(|k| k.to_vec()).unwrap_or((0 .. plan.arity).collect());
        self.local
            .entry(plan)
            .or_insert_with(|| HashMap::new())
            .insert(keys, arranged);
    }


    /// Retrieves an arrangement from a plan and keys.
    pub fn get_trace(&self, plan: &Plan<V>, keys: Option<&[usize]>) -> Option<&Arranged<S, TraceValHandle<Vec<V>, Vec<V>, S::Timestamp, Diff>>> {
        if let Some(keys) = keys {
            self.trace.get(plan).and_then(|x| x.get(keys))
        }
        else {
            let keys = (0 .. plan.arity).collect::<Vec<_>>();
            self.trace.get(plan).and_then(|x| x.get(&keys))
        }
    }
    /// Binds a plan and keys to an arrangement.
    pub fn set_trace(&mut self, plan: Plan<V>, keys: Option<&[usize]>, arranged: Arranged<S, TraceValHandle<Vec<V>, Vec<V>, S::Timestamp, Diff>>) {
        let keys = keys.map(|k| k.to_vec()).unwrap_or((0 .. plan.arity).collect());
        self.trace
            .entry(plan)
            .or_insert_with(|| HashMap::new())
            .insert(keys, arranged);
    }

    /// Creates a new empty stash.
    pub fn new() -> Self {
        Self {
            collections: HashMap::new(),
            local: HashMap::new(),
            trace: HashMap::new(),
        }
    }
}

/// A type that can be rendered as a collection.
pub trait Render : Sized {

    /// Value type produced.
    type Value: ExchangeData+Datum;

    /// Renders the instance as a collection in the supplied scope.
    ///
    /// This method has access to arranged data, and may rely on and update the set
    /// of arrangements based on the needs and offerings of the rendering process.
    fn render<S: Scope>(
        &self,
        scope: &mut S,
        stash: &mut Stash<S, Self::Value>,
    ) -> Collection<S, Vec<Self::Value>, Diff> where S::Timestamp: Lattice;
}

/// Possible query plan types.
#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Plan<V: Datum> {
    /// Number of columns of `V`.
    pub arity: usize,
    /// Description of data source.
    pub node: PlanNode<V>,
}

/// Possible query plan types.
#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum PlanNode<V: Datum> {
    /// Map
    Map(Map<V>),
    /// Distinct
    Distinct(Box<Plan<V>>),
    /// Concat
    Concat(Vec<Plan<V>>),
    /// Consolidate
    Consolidate(Box<Plan<V>>),
    /// Equijoin
    Join(Join<V>),
    /// MultiwayJoin
    MultiwayJoin(MultiwayJoin<V>),
    /// Negation
    Negate(Box<Plan<V>>),
    /// Filters bindings by one of the built-in predicates
    Filter(Filter<V>),
    /// Sources data from outside the dataflow.
    Source(String),
    /// A dataflow-local bound collection.
    Local(String),
    /// Prints resulting updates.
    Inspect(String, Box<Plan<V>>),
}

impl<V: ExchangeData+Hash+Datum> Plan<V> {
    /// Retains only the values at the indicated indices.
    pub fn project(self, indices: Vec<usize>) -> Self {
        Plan {
            arity: indices.len(),
            node: PlanNode::Map(Map {
                expressions: indices.into_iter().map(|i| V::projection(i)).collect(),
                plan: Box::new(self),
            }),
        }
    }
    /// Reduces a collection to distinct tuples.
    pub fn distinct(self) -> Self {
        Plan {
            arity: self.arity,
            node: PlanNode::Distinct(Box::new(self)),
        }
    }
    /// Merges two collections.
    pub fn concat(self, other: Self) -> Self {
        assert_eq!(self.arity, other.arity);
        Plan {
            arity: self.arity,
            node: PlanNode::Concat(vec![self, other]),
        }
    }
    /// Merges multiple collections.
    pub fn concatenate(plans: Vec<Self>) -> Self {
        assert!(!plans.is_empty());
        assert!(plans.iter().all(|p| p.arity == plans[0].arity));
        Plan {
            arity: plans[0].arity,
            node: PlanNode::Concat(plans),
        }
    }
    /// Merges multiple collections.
    pub fn consolidate(self) -> Self {
        Plan {
            arity: self.arity,
            node: PlanNode::Consolidate(Box::new(self))
        }
    }
    /// Equi-joins two collections using the specified pairs of keys.
    pub fn join(self, other: Plan<V>, keys: Vec<(usize, usize)>) -> Self {
        assert!(keys.iter().all(|(x,y)| x <= &self.arity && y <= &other.arity));
        Plan {
            arity: self.arity + other.arity - keys.len(),   // TODO: Not obviously general
            node: PlanNode::Join(Join {
                keys,
                plan1: Box::new(self),
                plan2: Box::new(other),
            }),
        }
    }
    /// Equi-joins multiple collections using lists of equality constraints.
    ///
    /// The list `equalities` should contain equivalence classes of pairs of
    /// attribute index and source index, and the `multiway_join` method will
    /// ensure that each equivalence class has equal values in each attribute.
    pub fn multiway_join(
        sources: Vec<Self>,
        equalities: Vec<Vec<(usize, usize)>>,
        results: Vec<(usize, usize)>
    ) -> Self {
        Plan {
            arity: results.len(),
            node: PlanNode::MultiwayJoin(MultiwayJoin {
                results,
                sources,
                equalities,
            }),
        }
    }
    /// Negates a collection (negating multiplicities).
    pub fn negate(self) -> Self {
        Plan {
            arity: self.arity,
            node: PlanNode::Negate(Box::new(self)),
        }
    }
    /// Restricts collection to tuples satisfying the predicate.
    pub fn filter(self, predicate: Predicate<V>) -> Self {
        Plan {
            arity: self.arity,
            node: PlanNode::Filter(Filter { predicate, plan: Box::new(self) } ),
        }
    }
    /// Loads a source of data by name.
    pub fn source(name: &str, arity: usize) -> Self {
        Plan {
            arity: arity,
            node: PlanNode::Source(name.to_string()),
        }
    }
    /// Loads a source of data by name.
    pub fn local(name: &str, arity: usize) -> Self {
        Plan {
            arity: arity,
            node: PlanNode::Local(name.to_string()),
        }
    }
    /// Prints each tuple prefixed by `text`.
    pub fn inspect(self, text: &str) -> Self {
        Plan {
            arity: self.arity,
            node: PlanNode::Inspect(text.to_string(), Box::new(self)),
        }
    }
    /// Convert the plan into a named rule.
    pub fn into_rule(self, name: &str) -> crate::Rule<V> {
        crate::Rule {
            name: name.to_string(),
            plan: self,
        }
    }
}

impl<V: ExchangeData+Hash+Datum> Render for Plan<V> {

    type Value = V;

    fn render<S: Scope>(
        &self,
        scope: &mut S,
        stash: &mut Stash<S, Self::Value>,
    ) -> Collection<S, Vec<Self::Value>, Diff>
    where
        S::Timestamp: differential_dataflow::lattice::Lattice,
    {
        if stash.collections.get(self).is_none() {

            let collection =
            match self.node {
                PlanNode::Map(ref expressions) => expressions.render(scope, stash),
                PlanNode::Distinct(ref input) => {

                    use differential_dataflow::operators::reduce::ReduceCore;
                    use differential_dataflow::operators::arrange::ArrangeByKey;
                    use differential_dataflow::trace::implementations::ord::OrdValSpine;

                    let output =
                    if let Some(mut arrangement) = stash.get_local(&input, None).cloned() {
                        arrangement.reduce_abelian::<_,OrdValSpine<_,_,_,_>>(move |_,_,t| t.push((vec![], 1)))
                    }
                    else if let Some(mut arrangement) = stash.get_trace(&input, None).cloned() {
                        arrangement.reduce_abelian::<_,OrdValSpine<_,_,_,_>>(move |_,_,t| t.push((vec![], 1)))
                    }
                    else {
                        let arranged = input.render(scope, stash).map(|x| (x, vec![])).arrange_by_key();
                        let clone: Box<Plan<V>> = input.clone();
                        let output = arranged.reduce_abelian::<_,OrdValSpine<_,_,_,_>>(move |_,_,t| t.push((vec![], 1)));
                        stash.set_local(*clone, None, arranged);
                        output
                    };

                    let result = output.as_collection(|k,_| k.clone());
                    stash.set_local(self.clone(), None, output);
                    result
                },
                PlanNode::Concat(ref concat) => {

                    use timely::dataflow::operators::Concatenate;
                    use differential_dataflow::AsCollection;

                    let plans =
                    concat
                        .iter()
                        .map(|plan| plan.render(scope, stash).inner)
                        .collect::<Vec<_>>();

                    scope
                        .concatenate(plans)
                        .as_collection()
                }
                PlanNode::Consolidate(ref input) => {
                    // if let Some(mut trace) = arrangements.get_unkeyed(&self) {
                    //     trace.import(scope).as_collection(|k,&()| k.clone())
                    // }
                    // else {
                        use differential_dataflow::operators::Consolidate;
                        input.render(scope, stash).consolidate()
                    // }
                },
                PlanNode::Join(ref join) => join.render(scope, stash),
                PlanNode::MultiwayJoin(ref join) => join.render(scope, stash),
                PlanNode::Negate(ref negate) => {
                    negate.render(scope, stash).negate()
                },
                PlanNode::Filter(ref filter) => filter.render(scope, stash),
                PlanNode::Source(ref source) => {
                    stash
                        .get_trace(self, None)
                        .expect(&format!("Failed to find source collection: {:?}", source))
                        .as_collection(|k,_| k.to_vec())
                },
                PlanNode::Local(ref source) => {
                    stash
                        .get_trace(self, None)
                        .expect(&format!("Failed to find local collection: {:?}", source))
                        .as_collection(|k,_| k.to_vec())
                },
                PlanNode::Inspect(ref text, ref plan) => {
                    let text = text.clone();
                    plan.render(scope, stash)
                        .inspect(move |x| println!("{}\t{:?}", text, x))
                },
            };

            stash.collections.insert(self.clone(), collection);
        }

        stash.collections.get(self).expect("We just installed this").clone()
    }
}
