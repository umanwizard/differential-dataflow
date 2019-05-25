extern crate interactive;

use std::time::Duration;
use interactive::{Command, Plan, Query};
use interactive::concrete::Session;

fn main() {

    let timer = std::time::Instant::now();

    let socket = std::net::TcpStream::connect("127.0.0.1:8000".to_string()).expect("failed to connect");
    let socket = std::io::BufWriter::new(socket);
    let mut session = Session::new(socket);

    // Create initially empty set of edges.
    session.issue(Command::CreateInput("N".to_string(), 2, Vec::new()));
    session.issue(Command::CreateInput("E".to_string(), 2, Vec::new()));

    // // Load up nodes and edges.
    // let mut nodes = Vec::new();
    // let mut edges = Vec::new();

    // use std::io::{BufRead, BufReader};
    // use std::fs::File;

    // let filename = std::env::args().nth(1).unwrap();
    // let file = BufReader::new(File::open(filename).unwrap());
    // for (index, readline) in file.lines().enumerate() {
    //     if (index % 1000) == 0 { println!("{:?}\tread line {}", timer.elapsed(), index); }
    //     let line = readline.ok().expect("read error");
    //     if !line.starts_with('#') && line.len() > 0 {
    //         let mut elts = line[..].split_whitespace();
    //         let src: usize = elts.next().unwrap().parse().ok().expect("malformed src");
    //         let dst: usize = elts.next().unwrap().parse().ok().expect("malformed dst");
    //         let typ: &str = elts.next().unwrap();

    //         let update = (vec![src.into(), dst.into()], Duration::from_secs(0), 1);

    //         match typ {
    //             "n" => { nodes.push(update); },
    //             "e" => { edges.push(update); },
    //             unk => { panic!("unknown type: {}", unk)},
    //         }

    //         if nodes.len() > 1000 {
    //             let send = std::mem::replace(&mut nodes, Vec::with_capacity(1000));
    //             session.issue(Command::UpdateInput("N".to_string(), send));
    //         }
    //         if edges.len() > 1000 {
    //             let send = std::mem::replace(&mut edges, Vec::with_capacity(1000));
    //             session.issue(Command::UpdateInput("E".to_string(), send));
    //         }
    //     }
    // }

    // session.issue(Command::UpdateInput("N".to_string(), nodes));
    // session.issue(Command::UpdateInput("E".to_string(), edges));

    let rule1 =
    Plan::source("Reach", 2)
        .join(Plan::source("E", 2), vec![(1,0)])
        .project(vec![1,2])
        .concat(Plan::source("N", 2))
        .distinct()
        // .inspect("reach")
        .into_rule("Reach");

    // let rule2 =
    // Plan::source("Reach", 2)
    //     .project(vec![])
    //     .consolidate()
    //     .inspect("reach")
    //     .into_rule("Count");

    session.issue(
        Query::new()
            .add_rule(rule1)
            // .add_rule(rule2)
            .add_import(Plan::source("N", 2), vec![0,1])
            .add_import(Plan::source("E", 2), vec![0,1])
    );

    session.issue(Command::Shutdown);
}