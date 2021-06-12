#![allow(unknown_lints)]
extern crate client;
extern crate env_logger;
extern crate futures;
extern crate getopts;
extern crate tokio;
#[macro_use]
extern crate log;

use client::{Configuration, LogServerClient};
use getopts::Options;
use std::env;
use std::process::exit;
use tokio::io::{stdin, stdout, AsyncBufReadExt, AsyncWriteExt, BufReader};

const MAX_READ_BYTES: u32 = 4096;
const USAGE: &str = "
    append [payload...]
        Appends a message to the log.

    latest
        Queries the log for the latest offset.

    read [offset]
        Reads the log from the starting offset.

    help
        Shows this menu.

    tail
        Grabs the tail replies.

    quit
        Quits the application.
";

fn parse_opts() -> String {
    let args: Vec<String> = env::args().collect();
    let program = args[0].clone();

    let mut opts = Options::new();
    opts.optopt(
        "a",
        "head-address",
        "address of the management server",
        "HOST:PORT",
    );
    opts.optflag("h", "help", "print this help menu");

    let matches = match opts.parse(&args[1..]) {
        Ok(m) => m,
        Err(f) => panic!("{}", f),
    };

    if matches.opt_present("h") {
        let brief = format!("Usage: {} [options]", program);
        print!("{}", opts.usage(&brief));
        exit(1);
    }

    matches.opt_str("a").unwrap_or_else(|| "127.0.0.1:5000".to_string())
}

async fn write_output(value: String) {
    let mut stdout = stdout();
    stdout.write_all(value.as_bytes()).await.unwrap();
    stdout.flush().await.unwrap();
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    env_logger::init();

    let management_server_addr = parse_opts();
    let mut client_config = Configuration::default();
    client_config
        .management_server(&management_server_addr)
        .unwrap();
    let client = LogServerClient::new(client_config);

    let mut conn = client.new_connection().await.unwrap();

    // write the first header
    info!("Connected");

    write_output("Connected".to_string()).await;
    let mut lines = BufReader::new(stdin()).lines();
    loop {
        // write the header
        write_output("> ".to_string()).await;

        // read the next command
        let line = if let Some(line) = lines.next_line().await.unwrap() {
            line
        } else {
            continue;
        };

        let (cmd, rest) = {
            let word_pos = line.find(' ').unwrap_or_else(|| line.len());
            let (x, y) = line.split_at(word_pos);
            (x.trim().to_lowercase().to_owned(), y.trim().to_owned())
        };

        match cmd.as_str() {
            "append" => conn.append(rest.into()).await.unwrap(),
            "latest" => {
                let off = conn.latest_offset().await.unwrap();
                trace!("GOT OFFSET: {:?}", off);
                if let Some(offset) = off {
                    write_output(format!("{}", offset)).await;
                }
            }
            "read" => match rest.parse::<u64>() {
                Ok(offset) => {
                    for msg in conn.read(offset, MAX_READ_BYTES).await.unwrap() {
                        let mut s = String::new();
                        s.push_str(format!(":{} => ", msg.0).as_str());
                        match std::str::from_utf8(&msg.1) {
                            Ok(v) => s.push_str(v),
                            Err(_) => {
                                for &byte in msg.1.iter() {
                                    s.push_str(&format!("{:X} ", byte));
                                }
                            }
                        }
                        s.push('\n');
                        write_output(s).await;
                    }
                }
                Err(_) => write_output("ERROR: Invalid offset".to_owned()).await,
            },
            "help" => write_output(USAGE.to_owned()).await,
            "quit" | "exit" => {
                return;
            }
            other => write_output(format!("Unknown command: {}\n{}", other, USAGE)).await,
        }
        write_output("\n".to_string()).await;
    }
}
