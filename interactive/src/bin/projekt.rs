extern crate interactive;

use std::time::Duration;
use interactive::{Command, Plan};
use interactive::concrete::{Session, Value};

fn main() {

    let socket = std::net::TcpStream::connect("127.0.0.1:8000".to_string()).expect("failed to connect");
    let mut session = Session::new(socket);

    session.issue(Command::CreateInput("XYZ".to_string(), 3, Vec::new()));
    session.issue(Command::CreateInput("XYGoal".to_string(), 2, Vec::new()));
    session.issue(Command::CreateInput("XZGoal".to_string(), 2, Vec::new()));

    // Determine errors in the xy plane.
    session.issue(
        Plan::source("XYZ", 3)
            .project(vec![0,1])
            .distinct()
            .negate()
            .concat(Plan::source("XYGoal", 2))
            .consolidate()
            // .inspect("xy error")
            .into_rule("XYErrors")
            .into_query()
            .add_import(Plan::source("XYZ", 3), vec![0,1,2])
            .add_import(Plan::source("XYGoal", 2), vec![0,1])
            .add_publish(Plan::source("XYErrors", 2), vec![0,1])
    );

    // Determine errors in the xy plane.
    session.issue(
        Plan::source("XYZ", 3)
            .project(vec![0,2])
            .distinct()
            .negate()
            .concat(Plan::source("XZGoal", 2))
            .consolidate()
            // .inspect("xz error")
            .into_rule("XZErrors")
            .into_query()
            .add_import(Plan::source("XYZ", 3), vec![0,1,2])
            .add_import(Plan::source("XZGoal", 2), vec![0,1])
            .add_publish(Plan::source("XZErrors", 2), vec![0,1])
    );

    session.issue(Command::AdvanceTime(Duration::from_secs(1)));

    session.issue(
        Command::UpdateInput(
            "XYGoal".to_string(),
            vec![
                (vec![Value::Usize(0), Value::Usize(0)], Duration::from_secs(1), 1),
                (vec![Value::Usize(0), Value::Usize(1)], Duration::from_secs(1), 1),
                (vec![Value::Usize(0), Value::Usize(3)], Duration::from_secs(1), 1),
                (vec![Value::Usize(0), Value::Usize(4)], Duration::from_secs(1), 1),
                (vec![Value::Usize(1), Value::Usize(1)], Duration::from_secs(1), 1),
                (vec![Value::Usize(1), Value::Usize(3)], Duration::from_secs(1), 1),
                (vec![Value::Usize(2), Value::Usize(1)], Duration::from_secs(1), 1),
                (vec![Value::Usize(2), Value::Usize(2)], Duration::from_secs(1), 1),
                (vec![Value::Usize(3), Value::Usize(2)], Duration::from_secs(1), 1),
                (vec![Value::Usize(3), Value::Usize(3)], Duration::from_secs(1), 1),
                (vec![Value::Usize(3), Value::Usize(4)], Duration::from_secs(1), 1),
                (vec![Value::Usize(4), Value::Usize(0)], Duration::from_secs(1), 1),
                (vec![Value::Usize(4), Value::Usize(1)], Duration::from_secs(1), 1),
                (vec![Value::Usize(4), Value::Usize(2)], Duration::from_secs(1), 1),
            ],
        ));

    session.issue(
        Command::UpdateInput(
            "XZGoal".to_string(),
            vec![
                (vec![Value::Usize(0), Value::Usize(2)], Duration::from_secs(1), 1),
                (vec![Value::Usize(0), Value::Usize(3)], Duration::from_secs(1), 1),
                (vec![Value::Usize(0), Value::Usize(4)], Duration::from_secs(1), 1),
                (vec![Value::Usize(1), Value::Usize(2)], Duration::from_secs(1), 1),
                (vec![Value::Usize(1), Value::Usize(4)], Duration::from_secs(1), 1),
                (vec![Value::Usize(2), Value::Usize(1)], Duration::from_secs(1), 1),
                (vec![Value::Usize(2), Value::Usize(2)], Duration::from_secs(1), 1),
                (vec![Value::Usize(2), Value::Usize(3)], Duration::from_secs(1), 1),
                (vec![Value::Usize(3), Value::Usize(0)], Duration::from_secs(1), 1),
                (vec![Value::Usize(3), Value::Usize(1)], Duration::from_secs(1), 1),
                (vec![Value::Usize(3), Value::Usize(3)], Duration::from_secs(1), 1),
                (vec![Value::Usize(3), Value::Usize(4)], Duration::from_secs(1), 1),
                (vec![Value::Usize(4), Value::Usize(1)], Duration::from_secs(1), 1),
                (vec![Value::Usize(4), Value::Usize(4)], Duration::from_secs(1), 1),
            ],
        ));

    // Determine errors in the xy plane.
    session.issue(
        Plan::source("XYErrors", 2)
            .distinct()
            .project(vec![])
            .concat(Plan::source("XZErrors", 2).distinct().project(vec![]))
            .consolidate()
            .inspect("error")
            .into_rule("Errors")
            .into_query()
            .add_import(Plan::source("XYErrors", 2), vec![0,1])
            .add_import(Plan::source("XZErrors", 2), vec![0,1])
    );

    session.issue(Command::AdvanceTime(Duration::from_secs(2)));

    session.issue(
        Command::UpdateInput(
            "XYZ".to_string(),
            vec![
                (vec![Value::Usize(0), Value::Usize(0), Value::Usize(2)], Duration::from_secs(2), 1),
                (vec![Value::Usize(0), Value::Usize(1), Value::Usize(3)], Duration::from_secs(2), 1),
                (vec![Value::Usize(0), Value::Usize(3), Value::Usize(4)], Duration::from_secs(2), 1),
                (vec![Value::Usize(0), Value::Usize(4), Value::Usize(4)], Duration::from_secs(2), 1),
                (vec![Value::Usize(1), Value::Usize(1), Value::Usize(2)], Duration::from_secs(2), 1),
                (vec![Value::Usize(1), Value::Usize(3), Value::Usize(4)], Duration::from_secs(2), 1),
                (vec![Value::Usize(2), Value::Usize(1), Value::Usize(1)], Duration::from_secs(2), 1),
                (vec![Value::Usize(2), Value::Usize(2), Value::Usize(2)], Duration::from_secs(2), 1),
                (vec![Value::Usize(2), Value::Usize(2), Value::Usize(3)], Duration::from_secs(2), 1),
                (vec![Value::Usize(3), Value::Usize(2), Value::Usize(0)], Duration::from_secs(2), 1),
                (vec![Value::Usize(3), Value::Usize(3), Value::Usize(1)], Duration::from_secs(2), 1),
                (vec![Value::Usize(3), Value::Usize(4), Value::Usize(3)], Duration::from_secs(2), 1),
                (vec![Value::Usize(3), Value::Usize(4), Value::Usize(4)], Duration::from_secs(2), 1),
                (vec![Value::Usize(4), Value::Usize(0), Value::Usize(1)], Duration::from_secs(2), 1),
                (vec![Value::Usize(4), Value::Usize(1), Value::Usize(4)], Duration::from_secs(2), 1),
                (vec![Value::Usize(4), Value::Usize(2), Value::Usize(4)], Duration::from_secs(2), 1),
            ],
        ));

    session.issue(Command::AdvanceTime(Duration::from_secs(2)));
    session.issue(Command::Shutdown);
}