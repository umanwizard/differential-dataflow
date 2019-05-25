//! Commands accepted by the system.

use std::hash::Hash;
use std::io::Write;

use timely::communication::Allocate;
use timely::worker::Worker;

use timely::logging::TimelyEvent;
use differential_dataflow::logging::DifferentialEvent;

use differential_dataflow::ExchangeData;

use super::{Query, Rule, Plan, Time, Diff, Manager, Datum};
use crate::logging::LoggingValue;

/// Commands accepted by the system.
#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum Command<V: Datum> {
    /// Installs the query and publishes public rules.
    Query(Query<V>),
    /// Advances all inputs and traces to `time`, and advances computation.
    AdvanceTime(Time),
    /// Creates a new named input, with initial input.
    CreateInput(String, usize, Vec<Vec<usize>>),
    /// Introduces updates to a specified input.
    UpdateInput(String, Vec<(Vec<V>, Time, Diff)>),
    /// Closes a specified input.
    CloseInput(String),
    /// Attaches a logging source. (address, flavor, number, granularity, name_as)
    SourceLogging(String, String, usize, u64, String),
    /// Terminates the system.
    Shutdown,
}

impl<V: Datum> From<Query<V>> for Command<V> {
    fn from(query: Query<V>) -> Self { Command::Query(query) }
}

impl<V: Datum> From<Rule<V>> for Command<V> {
    fn from(rule: Rule<V>) -> Self { Command::Query(Query::new().add_rule(rule)) }
}

impl<V: Datum> Command<V>
where
    V: ExchangeData+Hash+LoggingValue+From<usize>,
{
    /// Executes a command.
    pub fn execute<A: Allocate>(self, manager: &mut Manager<V>, worker: &mut Worker<A>) {

        match self {

            Command::Query(mut query) => {

                // Query construction requires a bit of guff to allow us to
                // re-use as much stuff as possible. It *seems* we need to
                // be able to cache and re-use:
                //
                //   1. Collections.
                //   2. Arrangements.
                //   3. External traces.
                //
                // Although (2.) and (3.) look pretty similar, arrangements
                // provide better progress tracking information than imported
                // traces, and the types present in imported traces are not
                // the same as those in arrangements.

                worker.dataflow(move |scope| {

                    use differential_dataflow::operators::arrange::ArrangeByKey;
                    use plan::Render;

                    // Import traces into `scope`.
                    let mut imports = Vec::new();
                    for (plan, keys) in query.imports.drain(..) {

                        let name: String = if let crate::plan::PlanNode::Source(name) = &plan.node { format!("Import: {}", name) } else { "arranged".to_owned() };

                        let import =
                        manager
                            .traces
                            .get(&plan, Some(&keys[..]))
                            .expect("Failed to find import")
                            .import_named(scope, &name);

                        imports.push((plan, keys, import));
                    }

                    // Build iterative subscope.
                    use timely::dataflow::Scope;
                    scope.iterative::<usize,_,_>(move |inner| {

                        let mut stash = crate::plan::Stash::new();
                        let mut local = std::collections::HashMap::new(); // map from name -> Variable

                        // Complete import process.
                        for (plan, keys, trace) in imports.into_iter() {
                            stash.set_trace(plan, Some(&keys[..]), trace.enter(inner));
                        }

                        // Create a `Variable` for each named rule.
                        for Rule { name, plan } in query.rules.iter() {
                            use differential_dataflow::operators::iterate::MonoidVariable;
                            use timely::order::Product;
                            let variable = MonoidVariable::new(inner, Product::new(Default::default(), 1));
                            stash.collections.insert(Plan::source(name, plan.arity), variable.clone());
                            local.insert(name.to_owned(), variable);
                        }

                        // Render each rule and bind variable definition.
                        for Rule { name, plan } in query.rules.drain(..) {
                            let collection = plan.render(inner, &mut stash);
                            local.remove(&name)
                                 .expect("Variable missing!")
                                 .set(&collection);
                        }

                        // Fish out collections that should be published.
                        for (plan, keys) in query.publish.drain(..) {
                            let keys_clone = keys.to_vec();
                            let vals = (0 .. plan.arity).filter(|i| !keys.contains(i)).collect::<Vec<_>>();
                            let trace =
                            plan.render(inner, &mut stash)
                                .leave()
                                .map(move |tuple|
                                    (
                                        keys.iter().map(|i| tuple[*i].clone()).collect::<Vec<_>>(),
                                        vals.iter().map(|i| tuple[*i].clone()).collect::<Vec<_>>(),
                                    )
                                )
                                .arrange_by_key()
                                .trace;

                            manager.traces.set(&plan, Some(&keys_clone[..]), &trace);
                        }

                        // Avoid non-deterministic drop order.
                        assert!(local.is_empty());
                    });
                });
            },

            Command::AdvanceTime(time) => {
                manager.advance_time(&time);
                while manager.less_than(&time) {
                    worker.step();
                }
            },

            Command::CreateInput(name, arity, keys) => {

                use differential_dataflow::input::Input;
                use differential_dataflow::operators::arrange::ArrangeByKey;

                // let (input, trace) =
                worker.dataflow(|scope| {

                    let (input, collection) = scope.new_collection();

                    for keys in keys.into_iter() {
                        let keys_clone = keys.clone();
                        let vals = (0 .. arity).filter(|i| !keys.contains(i)).collect::<Vec<_>>();
                        let trace =
                        collection
                            .map(move |tuple: Vec<V>|
                                (
                                    keys.iter().map(|index| tuple[*index].clone()).collect::<Vec<_>>(),
                                    vals.iter().map(|index| tuple[*index].clone()).collect::<Vec<_>>(),
                                )
                            )
                            .arrange_by_key()
                            .trace;

                        manager.traces.set(&Plan::source(&name, arity), Some(&keys_clone[..]), &trace);
                    }

                    let trace = collection.map(|x| (x,vec![])).arrange_by_key().trace;
                    manager.insert_input(name, arity, input, trace);

                });

                // manager.insert_input(name, arity, input, trace);
            },

            Command::UpdateInput(name, updates) => {
                if let Some(input) = manager.inputs.sessions.get_mut(&name) {
                    for (data, time, diff) in updates.into_iter() {
                        input.update_at(data, time, diff);
                    }
                }
                else {
                    println!("Input not found: {:?}", name);
                }
            },

            Command::CloseInput(name) => {
                manager.inputs.sessions.remove(&name);
            },

            Command::SourceLogging(address, flavor, number, granularity, name_as) => {

                match flavor.as_str() {
                    "timely" => {

                        let mut streams = Vec::new();

                        // Only one worker can bind to listen.
                        if worker.index() == 0 {

                            use std::time::Duration;
                            use std::net::TcpListener;
                            use timely::dataflow::operators::capture::EventReader;

                            println!("Awaiting timely logging connections ({})", number);

                            // e.g. "127.0.0.1:8000"
                            let listener = TcpListener::bind(address).unwrap();
                            for index in 0 .. number {
                                println!("\tTimely logging connection {} of {}", index, number);
                                let socket = listener.incoming().next().unwrap().unwrap();
                                socket.set_nonblocking(true).expect("failed to set nonblocking");
                                streams.push(EventReader::<Duration, (Duration, usize, TimelyEvent),_>::new(socket));
                            }

                            println!("\tAll logging connections established");
                        }
                        crate::logging::publish_timely_logging(manager, worker, granularity, &name_as, streams);
                    },
                    "differential" => {

                        let mut streams = Vec::new();

                        // Only one worker can bind to listen.
                        if worker.index() == 0 {

                            use std::time::Duration;
                            use std::net::TcpListener;
                            use timely::dataflow::operators::capture::EventReader;

                            // "127.0.0.1:8000"
                            let listener = TcpListener::bind(address).unwrap();
                            for _ in 0 .. number {
                                let socket = listener.incoming().next().unwrap().unwrap();
                                socket.set_nonblocking(true).expect("failed to set nonblocking");
                                streams.push(EventReader::<Duration, (Duration, usize, DifferentialEvent),_>::new(socket));
                            }
                        }
                        crate::logging::publish_differential_logging(manager, worker, granularity, &name_as, streams);
                    },
                    _ => { println!("{}", format!("Unknown logging flavor: {}", flavor)); }
                }
            },

            Command::Shutdown => {
                println!("Shutdown received");
                manager.shutdown(worker);
            },
        }
    }

    /// Serialize the command at a writer.
    pub fn serialize_into<W: Write>(&self, writer: W) {
        bincode::serialize_into(writer, self).expect("bincode: serialization failed");
    }
}