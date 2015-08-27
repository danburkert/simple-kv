use std::collections::HashMap;
use std::io::{Error, ErrorKind, Read, Result, Write};
use std::net::SocketAddr;
use std::str::{self, FromStr};

use mio::{PollOpt, EventLoop, EventSet, Handler, Token};
use mio::tcp::{TcpListener, TcpStream};
use mio::util::Slab;

const LISTENER: Token = Token(0);

/// Initial read and write buffer size.
const BUF_SIZE: usize = 128;
/// Maximum number of concurrent clients.
const SLAB_SIZE: usize = 4096;

/// A simple Key/Value database server.
///
/// The server listens for text-based TCP messages in the following formats:
///
/// * 'PUT <key> <value>'
/// * 'GET <key>'
pub struct Server {
    listener: TcpListener,
    connections: Slab<Connection>,
    db: HashMap<String, String>,
}

impl Server {
    pub fn start(port: u32) -> Result<()> {
        let mut event_loop = try!(EventLoop::<Server>::new());
        info!("Starting simple-kv Rust server with listening port {}", port);
        let addr = SocketAddr::from_str(&format!("127.0.0.1:{}", port)).unwrap();
        let listener = try!(TcpListener::bind(&addr));
        try!(event_loop.register(&listener, LISTENER));

        let mut server = Server { listener: listener,
                                  connections: Slab::new_starting_at(Token(1), SLAB_SIZE),
                                  db: HashMap::new() };

        event_loop.run(&mut server)
    }

    /// Called when the TCP listener accepts a new socket connection.
    fn accept_connections(&mut self, event_loop: &mut EventLoop<Server>) {
        while let Ok(Some(socket)) = self.listener.accept() {
            info!("new connection accepted from {}", socket.peer_addr().unwrap());
            if let Ok(token) = self.connections.insert(Connection::new(socket)) {
                event_loop.register_opt(&self.connections[token].socket,
                                        token,
                                        EventSet::readable(),
                                        PollOpt::edge() | PollOpt::oneshot())
                          .unwrap_or_else(|error| {
                              warn!("unable to register accepted socket: {}", error);
                              self.connections.remove(token);
                          });
            }
        }
    }

    /// Called when a connection is readable.
    fn connection_readable(&mut self, token: Token) -> Result<()> {
        let &mut Server { ref mut connections, ref mut db, .. } = self;
        let connection = &mut connections[token];

        for message in try!(connection.readable()) {
            debug!("received message from {:?}: {:?}", token, message);
            match message {
                Message::Get(key) => {
                    let val = db.get(&key).map(|s| &s[..]).unwrap_or("NONE");
                    connection.send(val);
                },
                Message::Put(key, value) => {
                    db.insert(key.to_owned(), value.to_owned());
                    connection.send("OK");
                },
                Message::Error => {
                    connection.send("ERR");
                },
            }
        };
        Ok(())
    }
}

impl Handler for Server {
    type Timeout=();
    type Message=();

    fn ready(&mut self, event_loop: &mut EventLoop<Server>, token: Token, events: EventSet) {
        debug!("ready, token: {:?}, events: {:?}", token, events);

        if token == LISTENER {
            assert!(events == EventSet::readable(),
                    "unexpected events for listener: {:?}", events);
            self.accept_connections(event_loop);
        } else {

            if events.is_error() {
                warn!("connection error: {:?}", token);
                self.connections.remove(token);
                return;
            }

            if events.is_hup() {
                debug!("connection hangup: {:?}", token);
                self.connections.remove(token);
                return;
            }

            if events.is_readable() {
                if let Err(error) = self.connection_readable(token) {
                    warn!("error while reading from {:?}: {}", token, error);
                    self.connections.remove(token);
                    return;
                }
            }

            if events.is_writable() {
                if let Err(error) = self.connections[token].writable() {
                    warn!("error while writing to {:?}: {}", token, error);
                    self.connections.remove(token);
                    return
                }
            }

            let events = self.connections[token].events;
            if let Err(error) = event_loop.reregister(&mut self.connections[token].socket, token,
                                                      events, PollOpt::edge() | PollOpt::oneshot()) {
                warn!("error while reregistering connection: {}", error);
                self.connections.remove(token);
            }
        }
    }
}

#[derive(Debug)]
enum Message {
    /// A get message, including the key to look up.
    Get(String),
    /// A put message, including the key and value.
    Put(String, String),
    /// Unable to decode the message
    Error,
}

impl Message {
    fn from_bytes(bytes: &[u8]) -> Message {
        let line = match str::from_utf8(bytes) {
            Ok(chars) => chars,
            Err(..) => { info!("error: decode"); return Message::Error},
        };

        let words: Vec<&str> = line.split_whitespace().collect();
        let len = words.len();
        if len < 2 { info!("len < 2"); return Message::Error }
        match words[0] {
            "GET" => {
                if len != 2 { info!("error: len != 2"); Message::Error }
                else { Message::Get(words[1].to_owned()) }
            },
            "PUT" => {
                if len != 3 { info!("error: len != 3, '{}'", str::from_utf8(bytes).unwrap()); Message::Error }
                else { Message::Put(words[1].to_owned(), words[2].to_owned()) }
            },
            _ => {info!("unknown command: {}", words[0]); Message::Error},
        }
    }
}

struct Connection {
    socket: TcpStream,
    /// Holds bytes being read from the socket before deserialization.
    read_buf: Vec<u8>,
    /// Holds bytes being written to the connection.
    write_buf: Vec<u8>,
    /// The set of events which the connection is registerd to handle.
    events: EventSet,
}

impl Connection {

    /// Creates a new connection with the provided socket.
    fn new(socket: TcpStream) -> Connection {
        Connection { socket: socket,
                     read_buf: Vec::with_capacity(BUF_SIZE),
                     write_buf: Vec::with_capacity(BUF_SIZE),
                     events: EventSet::readable() | EventSet::hup() | EventSet::error() }
    }

    /// Called when there are bytes available to read on the connection's socket.
    ///
    /// Returns the messages read.
    fn readable(&mut self) -> Result<Vec<Message>> {
        let mut messages = Vec::new();
        let read_buf = &mut self.read_buf;

        let read_from = read_buf.len();
        match self.socket.read_to_end(read_buf) {
            Ok(0) => return Ok(messages),
            Ok(_) => (),
            Err(ref error) if error.kind() == ErrorKind::WouldBlock => (),
            Err(error) => return Err(error),
        }

        let mut lo = 0;
        // Check the newly read bytes for line seperators. For each line, decode it
        // into a message and add it to the messages list.
        for (hi, &c) in read_buf[read_from..].iter().enumerate() {
            if c == '\n' as u8 {
                let line = &read_buf[lo..hi];
                messages.push(Message::from_bytes(line));
                lo = hi + 1;
            }
        }

        // Remove bytes that have been decoded into lines.
        read_buf.drain(..lo).count();
        Ok(messages)
    }

    /// Adds the message to the connection's send buffer, to be sent the next time the socket is
    /// writable.
    fn send(&mut self, message: &str) {
        // Ensure we are listening for writable events on this connection.
        self.events.insert(EventSet::writable());

        // Write the message into the buffer.
        self.write_buf.extend(message.as_bytes().iter());
        self.write_buf.push('\n' as u8);
    }

    /// Called when the connection's socket is writeable.
    fn writable(&mut self) -> Result<()> {
        let mut idx = 0; // index of last written byte in the write buffer.
        while idx < self.write_buf.len() {
            match self.socket.write(&self.write_buf[idx..]) {
                Ok(0) => return Err(Error::new(ErrorKind::WriteZero,
                                               "unable to write to socket")),
                Ok(n) => idx += n,
                Err(ref error) if error.kind() == ErrorKind::WouldBlock => {
                    // Socket is full; drain the bytes we have written from the write buffer.
                    self.write_buf.drain(0..idx).count();
                    return Ok(());
                },
                Err(error) => return Err(error),
            }
        }
        self.events.remove(EventSet::writable());
        self.write_buf.clear();
        Ok(())
    }
}
