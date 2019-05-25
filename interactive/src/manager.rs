//! Management of inputs and traces.

use std::collections::HashMap;
use std::hash::Hash;

use timely::communication::Allocate;
use timely::worker::Worker;
use timely::logging::TimelyEvent;

use differential_dataflow::ExchangeData;
use differential_dataflow::trace::implementations::ord::{OrdKeySpine, OrdValSpine};
use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::input::InputSession;

use differential_dataflow::logging::DifferentialEvent;

use crate::{Time, Diff, Plan, Datum};

/// A trace handle for key-only data.
pub type TraceKeyHandle<K, T, R> = TraceAgent<OrdKeySpine<K, T, R>>;
/// A trace handle for key-value data.
pub type TraceValHandle<K, V, T, R> = TraceAgent<OrdValSpine<K, V, T, R>>;
/// A key-only trace handle binding `Time` and `Diff` using `Vec<V>` as data.
pub type KeysOnlyHandle<V> = TraceKeyHandle<Vec<V>, Time, Diff>;
/// A key-value trace handle binding `Time` and `Diff` using `Vec<V>` as data.
pub type KeysValsHandle<V> = TraceValHandle<Vec<V>, Vec<V>, Time, Diff>;

/// Manages inputs and traces.
pub struct Manager<V: ExchangeData+Datum> {
    /// Manages input sessions.
    pub inputs: InputManager<V>,
    /// Manages maintained traces.
    pub traces: TraceManager<V>,
}

impl<V: ExchangeData+Datum> Manager<V> {

    /// Creates a new empty manager.
    pub fn new() -> Self {
        Manager {
            inputs: InputManager::new(),
            traces: TraceManager::new(),
        }
    }

    /// True iff any maintained trace is less than `time`.
    pub fn less_than(&mut self, time: &Time) -> bool {
        let mut antichain = timely::progress::Antichain::new();
        self.traces.arrangements.values_mut().any(|v| v.values_mut().any(|t| {
            use differential_dataflow::trace::TraceReader;
            t.read_upper(&mut antichain);
            antichain.less_than(time)
        }))
    }

    // /// Enables logging of timely and differential events.
    // pub fn enable_logging<A: Allocate>(&mut self, worker: &mut Worker<A>) {

    //     use std::rc::Rc;
    //     use timely::dataflow::operators::capture::event::link::EventLink;
    //     use timely::logging::BatchLogger;

    //     let timely_events = Rc::new(EventLink::new());
    //     let differential_events = Rc::new(EventLink::new());

    //     self.publish_timely_logging(worker, Some(timely_events.clone()));
    //     self.publish_differential_logging(worker, Some(differential_events.clone()));

    //     let mut timely_logger = BatchLogger::new(timely_events.clone());
    //     worker
    //         .log_register()
    //         .insert::<TimelyEvent,_>("timely", move |time, data| timely_logger.publish_batch(time, data));

    //     let mut differential_logger = BatchLogger::new(differential_events.clone());
    //     worker
    //         .log_register()
    //         .insert::<DifferentialEvent,_>("differential/arrange", move |time, data| differential_logger.publish_batch(time, data));

    // }

    /// Clear the managed inputs and traces.
    pub fn shutdown<A: Allocate>(&mut self, worker: &mut Worker<A>) {
        self.inputs.sessions.clear();
        // self.traces.inputs.clear();
        self.traces.arrangements.clear();

        // Deregister loggers, so that the logging dataflows can shut down.
        worker
            .log_register()
            .insert::<TimelyEvent,_>("timely", move |_time, _data| { });

        worker
            .log_register()
            .insert::<DifferentialEvent,_>("differential/arrange", move |_time, _data| { });
    }

    /// Inserts a new input session by name.
    pub fn insert_input(
        &mut self,
        name: String,
        arity: usize,
        input: InputSession<Time, Vec<V>, Diff>,
        trace: KeysValsHandle<V>)
    {
        self.inputs.sessions.insert(name.clone(), input);
        self.traces.set(&Plan::source(&name, arity), None, &trace);
    }

    /// Advances inputs and traces to `time`.
    pub fn advance_time(&mut self, time: &Time) {
        self.inputs.advance_time(time);
        self.traces.advance_time(time);
    }

    // /// Timely logging capture and arrangement.
    // pub fn publish_timely_logging<A, I>(&mut self, worker: &mut Worker<A>, events: I)
    // where
    //     A: Allocate,
    //     I : IntoIterator,
    //     <I as IntoIterator>::Item: EventIterator<Duration, (Duration, usize, TimelyEvent)>+'static
    // {
    //     crate::logging::publish_timely_logging(self, worker, 1, "interactive", events)
    // }

    // /// Timely logging capture and arrangement.
    // pub fn publish_differential_logging<A, I>(&mut self, worker: &mut Worker<A>, events: I)
    // where
    //     A: Allocate,
    //     I : IntoIterator,
    //     <I as IntoIterator>::Item: EventIterator<Duration, (Duration, usize, DifferentialEvent)>+'static
    // {
    //     crate::logging::publish_differential_logging(self, worker, 1, "interactive", events)
    // }
}

/// Manages input sessions.
pub struct InputManager<V: ExchangeData> {
    /// Input sessions by name.
    pub sessions: HashMap<String, InputSession<Time, Vec<V>, Diff>>,
}

impl<V: ExchangeData> InputManager<V> {

    /// Creates a new empty input manager.
    pub fn new() -> Self { Self { sessions: HashMap::new() } }

    /// Advances the times of all managed inputs.
    pub fn advance_time(&mut self, time: &Time) {
        for session in self.sessions.values_mut() {
            session.advance_to(time.clone());
            session.flush();
        }
    }

}

/// Root handles to maintained collections.
///
/// Manages a map from plan (describing a collection)
/// to various arranged forms of that collection.
pub struct TraceManager<V: ExchangeData+Datum> {
    /// Arrangements of collections by key.
    arrangements: HashMap<Plan<V>, HashMap<Vec<usize>, KeysValsHandle<V>>>,
}

impl<V: ExchangeData+Hash+Datum> TraceManager<V> {

    /// Creates a new empty trace manager.
    pub fn new() -> Self {
        Self {
            // inputs: HashMap::new(),
            arrangements: HashMap::new()
        }
    }

    /// Advances the frontier of each maintained trace.
    pub fn advance_time(&mut self, time: &Time) {
        use differential_dataflow::trace::TraceReader;
        let frontier = &[time.clone()];
        for map in self.arrangements.values_mut() {
            for trace in map.values_mut() {
                trace.advance_by(frontier);
                trace.distinguish_since(frontier);
            }
        }
    }

    /// Recover an arrangement by plan and keys, if it is cached.
    pub fn get(&self, plan: &Plan<V>, keys: Option<&[usize]>) -> Option<KeysValsHandle<V>> {
        if let Some(keys) = keys {
            self.arrangements
                .get(plan)
                .and_then(|map| map.get(keys).map(|x| x.clone()))
        }
        else {
            let keys = (0 .. plan.arity).collect::<Vec<_>>();
            self.arrangements
                .get(plan)
                .and_then(|map| map.get(&keys).map(|x| x.clone()))
        }
    }

    /// Installs a keyed arrangement for a specified plan and sequence of keys.
    pub fn set(&mut self, plan: &Plan<V>, keys: Option<&[usize]>, handle: &KeysValsHandle<V>) {
        let keys = keys.map(|x| x.to_vec()).unwrap_or((0 .. plan.arity).collect::<Vec<_>>());
        self.arrangements
            .entry(plan.clone())
            .or_insert(HashMap::new())
            .insert(keys, handle.clone());
    }
}