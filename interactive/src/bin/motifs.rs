extern crate interactive;

use std::time::Duration;
use interactive::{Command, Plan};
use interactive::concrete::{Session, Value};

fn main() {

    let socket = std::net::TcpStream::connect("127.0.0.1:8000".to_string()).expect("failed to connect");
    let mut session = Session::new(socket);

    // Create initially empty set of edges.
    session.issue(Command::CreateInput("Edges".to_string(), 2, Vec::new()));

    let nodes = std::env::args().nth(1).expect("must supply number of nodes").parse().expect("couldn't parse number of nodes");

    for node_0 in 0 .. (nodes / 2) {
        println!("Inserting node: {}", node_0);
        let updates =
        (0 .. nodes)
            .map(|x| vec![Value::Usize(node_0), Value::Usize(x)])
            .map(|e| (e, Duration::from_secs(node_0 as u64), 1))
            .collect::<Vec<_>>();
        session.issue(Command::UpdateInput("Edges".to_string(), updates));
        session.issue(Command::AdvanceTime(Duration::from_secs(node_0 as u64 + 1)));
    }

    session.issue(
        Plan::multiway_join(
            vec![
                Plan::source("Edges", 2),
                Plan::source("Edges", 2),
                Plan::source("Edges", 2),
            ],
            vec![
                vec![(0,1), (1,0)], // b == b
                vec![(0,0), (0,2)], // a == a
                vec![(1,1), (1,2)], // c == c
            ],
            vec![(0,0), (1,0), (1,1)],
        )
        .project(vec![])
        .consolidate()
        .inspect("triangles")
        .into_rule("triangles")
        .into_query()
        .add_import(Plan::source("Edges", 2), vec![0,1])
    );


    for node_0 in (nodes / 2) .. nodes {
        let updates =
        (0 .. nodes)
            .map(|x| vec![Value::Usize(node_0), Value::Usize(x)])
            .map(|e| (e, Duration::from_secs(node_0 as u64), 1))
            .collect::<Vec<_>>();
        session.issue(Command::UpdateInput("Edges".to_string(), updates));
        session.issue(Command::AdvanceTime(Duration::from_secs(node_0 as u64 + 1)));
    }

    session.issue(
        Plan::multiway_join(
            vec![
                Plan::source("Edges", 2),  // R0(a,b)
                Plan::source("Edges", 2),  // R1(a,c)
                Plan::source("Edges", 2),  // R2(a,d)
                Plan::source("Edges", 2),  // R3(b,c)
                Plan::source("Edges", 2),  // R4(b,d)
                Plan::source("Edges", 2),  // R5(c,d)
            ],
            vec![
                vec![(0,0), (0,1), (0,2)], // a
                vec![(1,0), (0,3), (0,4)], // b
                vec![(1,1), (1,3), (0,5)], // c
                vec![(1,2), (1,4), (1,5)], // d
            ],
            vec![(0,0), (1,0), (1,1), (1,2)], // (a, b, c, d)
        )
        .project(vec![])
        .consolidate()
        .inspect("4cliques")
        .into_rule("4cliques")
        .into_query()
        .add_import(Plan::source("Edges", 2), vec![0,1])
    );

    session.issue(Command::Shutdown);
}