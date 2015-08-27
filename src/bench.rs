use std::collections::VecDeque;
use std::io::{Error, ErrorKind, Read, Result, Write};
use std::net::SocketAddr;
use std::str::FromStr;
use std::iter;
use std::sync::mpsc;
use std::mem;
use std::thread;
use std::process::exit;

use time;
use histogram::{Histogram, HistogramConfig};
use mio::{PollOpt, EventLoop, EventSet, Handler, Token};
use mio::tcp::TcpStream;
use mio::util::Slab;
use rand::{Rng, XorShiftRng};

/// Initial read and write buffer size.
const BUF_SIZE: usize = 4096;

pub struct Bench {
    connections: Slab<Connection>,
    pid: i32,
    concurrency: u32,
    entries_written: usize,
    val_size: usize,
    batch_size: usize,
    count: usize,
    report_duration: u64,
    hist: Histogram,
    hist_send: mpsc::Sender<Histogram>,
    rand: XorShiftRng,
}

impl Bench {
    pub fn start(port: u32,
                 pid: i32,
                 concurrency: u32,
                 val_size: usize,
                 batch_size: usize,
                 count: usize,
                 report_duration: u64) -> Result<()> {
        info!("Starting benchmark of simple-kv server with listening port {} and pid {}", port, pid);
        info!("concurrency: {}, val-size: {}b, batch-size: {}, count: {}, report-duration: {:?}",
              concurrency, val_size, batch_size, count, report_duration);
        let addr = SocketAddr::from_str(&format!("127.0.0.1:{}", port)).unwrap();

        let mut event_loop = try!(EventLoop::<Bench>::new());
        let mut connections = Slab::new(concurrency as usize);

        for _ in 0..concurrency {
            let mut connection = Connection::new(try!(TcpStream::connect(&addr)));
            let token = connections.insert(connection).unwrap();
            try!(event_loop.register_opt(&connections[token].socket,
                                         token,
                                         EventSet::all(),
                                         PollOpt::edge() | PollOpt::oneshot()));
        }

        event_loop.timeout_ms((), report_duration).unwrap();

        let (hist_send, hist_recv) = mpsc::channel();

        thread::spawn(move || {
            reporter(hist_recv);
        });

        let mut bench = Bench {
            connections: connections,
            pid: pid,
            concurrency: concurrency,
            entries_written: 0,
            count: count,
            val_size: val_size,
            batch_size: batch_size,
            report_duration: report_duration,
            hist: create_hist(),
            hist_send: hist_send,
            rand: XorShiftRng::new_unseeded(),
        };

        event_loop.run(&mut bench)
    }

    /// Called when the connection's socket is writable.
    fn writable(&mut self, token: Token) -> Result<()> {
        let &mut Bench { ref mut connections, ref mut rand,
                         batch_size, ref mut entries_written, val_size, .. } = self;
        let connection = &mut connections[token];

        let message_size = 22 + val_size; // 'PUT' + 16 byte key + 2 spaces + newline

        while connection.write_buf.len() < message_size * batch_size {
            connection.write_buf.extend(b"PUT ".iter());
            write!(&mut connection.write_buf, "{:016X}", *entries_written);
            connection.write_buf.push(' ' as u8);
            connection.write_buf.extend(rand.gen_ascii_chars().map(|c| c as u8).take(val_size as usize));
            connection.write_buf.push('\n' as u8);
            *entries_written += 1;
        }

        let mut idx = 0;
        while idx < connection.write_buf.len() {
            match connection.socket.write(&connection.write_buf[idx..]) {
                Ok(0) => return Err(Error::new(ErrorKind::WriteZero,
                                               "unable to write to socket")),
                Ok(n) => idx += n,
                Err(ref error) if error.kind() == ErrorKind::WouldBlock => break,
                Err(error) => return Err(error),
            }
        }
        let send_time: u64 = time::precise_time_ns();

        let messages_sent = (connection.bytes_sent + idx) / message_size
                          - connection.bytes_sent / message_size;

        debug!("sent {} messages from {:?}", messages_sent, token);

        connection.send_times.extend(iter::repeat(send_time).take(messages_sent));
        connection.write_buf.drain(..idx).count();
        Ok(())
    }

    /// Receive responses, and return a vector of response latencies.
    fn readable(&mut self, token: Token) -> Result<()> {
        let recv_time: u64 = time::precise_time_ns();

        let &mut Bench { ref mut connections, ref mut hist, .. } = self;
        let connection = &mut connections[token];

        match connection.socket.read_to_end(&mut connection.read_buf) {
            Ok(_) => (),
            Err(ref error) if error.kind() == ErrorKind::WouldBlock => (),
            Err(error) => return Err(error),
        }

        assert!(connection.read_buf.chunks(3).all(|response| response == b"OK\n"));
        let responses = connection.read_buf.len() / 3;
        connection.read_buf.drain(..responses * 3);

        debug!("received {} responses to {:?}", responses, token);

        for send_time in connection.send_times.iter().rev().take(responses) {
            hist.increment(recv_time - send_time);
        }

        let len = connection.send_times.len() - responses;
        connection.send_times.truncate(len);
        Ok(())
    }
}

impl Handler for Bench {
    type Timeout=();
    type Message=();

    fn ready(&mut self, event_loop: &mut EventLoop<Bench>, token: Token, events: EventSet) {
        debug!("ready, token: {:?}, events: {:?}", token, events);

        if self.count > 0 && self.entries_written > self.count {
            exit(0);
        }

        if events.is_error() {
            panic!("connection error: {:?}", self.connections[token]);
        }
        if events.is_hup() {
            panic!("connection hangup: {:?}", self.connections[token]);
        }

        if events.is_readable() {
            self.readable(token).unwrap();
        }

        if events.is_writable() {
            self.writable(token).unwrap();
        };

        let socket = &self.connections[token].socket;
        event_loop.reregister(socket, token, EventSet::all(),
                              PollOpt::edge() | PollOpt::oneshot()).unwrap();
    }

    fn timeout(&mut self, event_loop: &mut EventLoop<Bench>, _timeout: ()) {
        self.hist_send.send(mem::replace(&mut self.hist, create_hist())).unwrap();
        event_loop.timeout_ms((), self.report_duration).unwrap();
    }
}

#[derive(Debug)]
struct Connection {
    socket: TcpStream,
    /// Holds bytes being read from the socket before deserialization.
    read_buf: Vec<u8>,
    /// Holds bytes being written to the connection.
    write_buf: Vec<u8>,
    bytes_sent: usize,
    send_times: VecDeque<u64>,
}

impl Connection {

    /// Creates a new connection with the provided socket.
    fn new(socket: TcpStream) -> Connection {
        Connection { socket: socket,
                     read_buf: Vec::with_capacity(BUF_SIZE),
                     write_buf: Vec::with_capacity(BUF_SIZE),
                     bytes_sent: 0,
                     send_times: VecDeque::new() }
    }
}

fn create_hist() -> Histogram {
    Histogram::new(HistogramConfig {
        max_value: 1_000_000_000,
        precision: 4,
        max_memory: 0,
    }).unwrap()
}

fn reporter(recv: mpsc::Receiver<Histogram>) {
    println!("time, count, p50, p90, p99");
    let mut mark: u64 = time::precise_time_ns();
    for mut hist in recv.into_iter() {
        let now = time::precise_time_ns();
        println!("{}, {}, {}, {}, {}",
                 now - mark,
                 hist.entries(),
                 hist.percentile(0.5).unwrap_or(0),
                 hist.percentile(0.9).unwrap_or(0),
                 hist.percentile(0.99).unwrap_or(0));
        mark = now;
    }
}
