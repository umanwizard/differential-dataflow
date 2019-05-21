extern crate rand;
extern crate timely;
extern crate differential_dataflow;

use rand::{Rng, SeedableRng, StdRng};

use timely::dataflow::*;
use timely::dataflow::operators::probe::Handle;

use differential_dataflow::input::Input;
use differential_dataflow::Collection;
use differential_dataflow::operators::*;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::logging::DifferentialEvent;

type Dist = u32;
type Node = u32;
type Edge = (Node, (Node, (Dist, Dist)));

fn main() {

    let mut args = std::env::args();
    args.next();

    let nodes: u32 = args.next().unwrap().parse().unwrap();
    let edges: u32 = args.next().unwrap().parse().unwrap();
    let min_w: u32 = args.next().unwrap().parse().unwrap();
    let max_w: u32 = args.next().unwrap().parse().unwrap();
    let batch: u32 = args.next().unwrap().parse().unwrap();
    let rounds: u32 = args.next().unwrap().parse().unwrap();
    let inspect: bool = args.next().unwrap() == "inspect";

    // define a new computational scope, in which to run sswp
    timely::execute_from_args(std::env::args(), move |worker| {

        let timer = ::std::time::Instant::now();

        // define sswp dataflow; return handles to roots and edges inputs
        let mut probe = Handle::new();
        let (mut roots, mut graph) = worker.dataflow(|scope| {

            let (root_input, roots) = scope.new_collection();
            let (edge_input, graph) = scope.new_collection();

            let mut result = spwp(&graph, &roots);

            if !inspect {
                result = result.filter(|_| false);
            }

            result.map(|(_,l)| l)
                  .consolidate()
                  .inspect(|x| println!("\t{:?}", x))
                  .probe_with(&mut probe);

            (root_input, edge_input)
        });

        let seed: &[_] = &[1, 2, 3, 4];
        let mut rng1: StdRng = SeedableRng::from_seed(seed);    // rng for edge additions
        let mut rng2: StdRng = SeedableRng::from_seed(seed);    // rng for edge deletions

        roots.insert(0);
        roots.close();

        println!("performing sswp on {} nodes, {} edges:", nodes, edges);

        if worker.index() == 0 {
            for _ in 0 .. edges {
                graph.insert((rng1.gen_range(0, nodes), (rng1.gen_range(0, nodes), (rng1.gen_range(min_w, max_w), rng1.gen_range(min_w, max_w)))));
            }
        }

        println!("{:?}\tloaded", timer.elapsed());

        if batch > 0 {

            graph.advance_to(1);
            graph.flush();
            worker.step_while(|| probe.less_than(graph.time()));

            println!("{:?}\tstable", timer.elapsed());

            for round in 0 .. rounds {
                for element in 0 .. batch {
                    if worker.index() == 0 {
                        graph.insert((rng1.gen_range(0, nodes), (rng1.gen_range(0, nodes), (rng1.gen_range(min_w, max_w), rng1.gen_range(min_w, max_w)))));
                        graph.remove((rng2.gen_range(0, nodes), (rng2.gen_range(0, nodes), (rng2.gen_range(min_w, max_w), rng2.gen_range(min_w, max_w)))));
                    }
                    graph.advance_to(2 + round * batch + element);
                }
                graph.flush();

                // let timer2 = ::std::time::Instant::now();
                worker.step_while(|| probe.less_than(&graph.time()));

                // if worker.index() == 0 {
                //     let elapsed = timer2.elapsed();
                //     println!("{:?}\tround {:?}:\t{}", timer.elapsed(), round, elapsed.as_secs() * 1000000000 + (elapsed.subsec_nanos() as u64));
                // }
            }
        }
        else {
            graph.close();
            while worker.step() { }
        }
        println!("{:?}\tfinished", timer.elapsed());
    }).unwrap();
}

// returns pairs (n, s) indicating node n can be reached from a root in s steps.
fn spwp<G: Scope>(edges: &Collection<G, Edge>, roots: &Collection<G, Node>) -> Collection<G, (Node, (u32, u32))>
where G::Timestamp: Lattice+Ord {

    // initialize roots as reaching themselves at distance 0
    let nodes = roots.map(|x| (x, (0, 0)));

    // repeatedly update minimal distances each node can be reached from each root
    nodes.iterate(|inner| {

        use timely::dataflow::operators::Map;
        use timely::dataflow::operators::Delay;
        use differential_dataflow::AsCollection;

        let edges = edges.enter(&inner.scope());
        let nodes = nodes.enter(&inner.scope());

        // inner.join_map(&edges, |_k,(l1,w1),(d,l2,w2)| (*d, (*l1 + *l2, ::std::cmp::max(*w1, *w2))))
        inner
            .join(&edges)
            .map(|(_k,((l1,w1),(d,(l2,w2))))| (d, (l1 + l2, std::cmp::max(w1, w2))))
            .concat(&nodes)
            .inner
            .map_in_place(|((_,(l,w)),t,_)|
                // t.inner = ::std::cmp::max(t.inner, *l as u64)
                t.inner = ::std::cmp::max(t.inner, 1024 * *w as u64)
            )
            .delay(|(_,t,_),_| t.clone())
            .as_collection()
            .reduce(|_, s, t: &mut Vec<((Dist, Dist), isize)>| {
                // things are sorted by (l, w).
                // t.push((*s[0].0, 1));
                for index in 0 .. s.len() {
                    let last = t.last().map(|x| (x.0).1).unwrap_or(u32::max_value());
                    if last > (s[index].0).1 {
                        t.push((*s[index].0, 1));
                    }
                }

                // println!("{:?} -> {:?}", s, t);
            })
     })
}