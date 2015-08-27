#![feature(drain, deque_extras)]

#[macro_use]
extern crate log;
extern crate docopt;
extern crate env_logger;
extern crate histogram;
extern crate mio;
extern crate rand;
extern crate rustc_serialize;
extern crate time;

mod server;
mod bench;

use docopt::Docopt;
use server::Server;
use bench::Bench;

const USAGE: &'static str = "
A simple in-memory Key Value store.

Commands:

  server        Starts a rust-db instance.

  bench-get     Starts a read benchmark against a rust-db instance.

  bench-put     Starts a write benchmark against a rust-db instance.

Usage:
  simple-kv server [--port=<port>]
  simple-kv bench  <pid> [--port=<port> --concurrency=<concurrency> --key-size=<key-size> --val-size=<val-size> --batch-size=<batch-size> --report-duration=<report-duration> --count=<count>]

Options:
  -h --help                             Show a help message.
  --port=<port>                         Set the listening port [default: 5555].
  --concurrency=<concurrency>           Number of concurrent benchmark connections [default: 16].
  --val-size=<val-size>                 Size of values in bytes [default: 48].
  --batch-size=<batch-size>             How many key-value pairs to write per connection per event-loop tick [default: 10].
  --report-duration=<report-duration>   How often to report results, in ms [default: 1000].
  --count=<count>                       Number of KV entries to write, or unlimited if 0 [default: 0].
";

#[derive(Debug, RustcDecodable)]
struct Args {
    cmd_server: bool,
    cmd_bench: bool,

    arg_pid: i32,

    flag_port: u32,
    flag_concurrency: u32,
    flag_val_size: usize,
    flag_batch_size: usize,
    flag_report_duration: u64,
    flag_count: usize,
}


fn main() {
    let _ = env_logger::init();

    let args: Args = Docopt::new(USAGE)
                            .and_then(|d| d.decode())
                            .unwrap_or_else(|e| e.exit());

    if args.cmd_server {
        Server::start(args.flag_port).unwrap();
    } else if args.cmd_bench {
        Bench::start(args.flag_port,
                     args.arg_pid,
                     args.flag_concurrency,
                     args.flag_val_size,
                     args.flag_batch_size,
                     args.flag_count,
                     args.flag_report_duration).unwrap();
    }
}
