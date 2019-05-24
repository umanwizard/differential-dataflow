extern crate interactive;

use interactive::{Command, Plan};
use interactive::concrete::Session;

fn main() {

    let frequency_ns: u64 = std::env::args().nth(1).expect("must supply frequency in nanoseconds").parse().expect("could not parse frequency in nanoseconds as u64");

    let socket = std::net::TcpStream::connect("127.0.0.1:8000".to_string()).expect("failed to connect");
    let mut session = Session::new(socket);

    session.issue(
    Command::SourceLogging(
        "127.0.0.1:9000".to_string(),   // port the server should listen on.
        "timely".to_string(),           // flavor of logging (of "timely", "differential").
        1,                              // number of worker connections to await.
        frequency_ns,                   // maximum granularity in nanoseconds.
        "remote".to_string()            // name to use for publication.
    ));

    session.issue(
        Plan::source("logs/remote/timely/operates", 3)
            .inspect("operates")
            .into_rule("operates"));

    session.issue(
        Plan::source("logs/remote/timely/shutdown", 2)
            .inspect("shutdown")
            .into_rule("shutdown"));

    session.issue(
        Plan::source("logs/remote/timely/channels", 6)
            .inspect("channels")
            .into_rule("channels"));

    // session.issue(
    //     Plan::source("logs/remote/timely/schedule", 2)
    //         .inspect("schedule")
    //         .into_rule("schedule"));

    // session.issue(
    //     Plan::source("logs/remote/timely/schedule/elapsed", 1)
    //         .inspect("schedule/elapsed")
    //         .into_rule("schedule/elapsed"));

    session.issue(
        Plan::source("logs/remote/timely/schedule/elapsed", 1)
            .join(Plan::source("logs/remote/timely/operates", 3), vec![(0, 0)])
            .inspect("joined")
            .into_rule("joined"));

    // session.issue(
    //     Plan::source("logs/remote/timely/messages", 6)
    //         .inspect("messages")
    //         .into_rule("messages"));

    session.issue(Command::Shutdown);
}