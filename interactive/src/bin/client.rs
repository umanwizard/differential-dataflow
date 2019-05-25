extern crate interactive;

use std::time::Duration;
use interactive::{Command, Plan};
use interactive::concrete::{Session, Value};

fn main() {

    let socket = std::net::TcpStream::connect("127.0.0.1:8000".to_string()).expect("failed to connect");
    let mut session = Session::new(socket);

    let nodes: usize = std::env::args().nth(1).expect("must supply nodes").parse().expect("could not parse nodes");

    // Create initially empty set of edges.
    session.issue(Command::CreateInput("Edges".to_string(), 2, Vec::new()));

    for node in 0 .. nodes {
        let edge = vec![Value::Usize(node), Value::Usize(node+1)];
        session.issue(Command::UpdateInput("Edges".to_string(), vec![(edge, Duration::from_secs(0), 1)]));
    }

    // Create initially empty set of edges.
    session.issue(Command::CreateInput("Nodes".to_string(), 1, Vec::new()));

    session.issue(
        Plan::source("Nodes", 1)
            .join(Plan::source("Edges", 2), vec![(0, 0)])
            .project(vec![1])
            .inspect("one-hop")
            .into_rule("One-hop")
            .into_query()
            .add_import(Plan::source("Nodes", 1), vec![0])
            .add_import(Plan::source("Edges", 2), vec![0,1])
    );

    session.issue(Command::AdvanceTime(Duration::from_secs(1)));
    session.issue(Command::UpdateInput("Nodes".to_string(), vec![(vec![Value::Usize(0)], Duration::from_secs(1), 1)]));
    session.issue(Command::AdvanceTime(Duration::from_secs(2)));

    session.issue(
        Plan::source("Nodes", 1)
            .join(Plan::source("Edges", 2), vec![(0, 0)])
            .project(vec![1])
            .join(Plan::source("Edges", 2), vec![(0, 0)])
            .project(vec![1])
            .join(Plan::source("Edges", 2), vec![(0, 0)])
            .project(vec![1])
            .join(Plan::source("Edges", 2), vec![(0, 0)])
            .project(vec![1])
            .join(Plan::source("Edges", 2), vec![(0, 0)])
            .project(vec![1])
            .join(Plan::source("Edges", 2), vec![(0, 0)])
            .project(vec![1])
            .join(Plan::source("Edges", 2), vec![(0, 0)])
            .project(vec![1])
            .join(Plan::source("Edges", 2), vec![(0, 0)])
            .project(vec![1])
            .join(Plan::source("Edges", 2), vec![(0, 0)])
            .project(vec![1])
            .join(Plan::source("Edges", 2), vec![(0, 0)])
            .project(vec![1])
            .inspect("ten-hop")
            .into_rule("Ten-hop")
            .into_query()
            .add_import(Plan::source("Nodes", 1), vec![0])
            .add_import(Plan::source("Edges", 2), vec![0,1])
    );

    session.issue(Command::AdvanceTime(Duration::from_secs(3)));

    session.issue(
        Plan::source("Reach", 1)
            .join(Plan::source("Edges", 2), vec![(0, 0)])
            .project(vec![1])
            .concat(Plan::source("Nodes", 1))
            .distinct()
            .inspect("reach")
            .into_rule("Reach")
            .into_query()
            .add_import(Plan::source("Nodes", 1), vec![0])
            .add_import(Plan::source("Edges", 2), vec![0,1])
    );


    session.issue(Command::Shutdown);
}