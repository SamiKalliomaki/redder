use std::{
    io::{Read, Write},
    net::TcpListener,
};

use anyhow::Context;
use nix::sys::epoll::{Epoll, EpollCreateFlags, EpollEvent, EpollFlags, EpollTimeout};

struct Connection {
    stream: std::net::TcpStream,
    read_buffer: [u8; 4096],
}

impl Connection {
    fn from(stream: std::net::TcpStream) -> Self {
        Self {
            stream,
            read_buffer: [0; 4096],
        }
    }
}

const LISTENER_DATA: u64 = u64::MAX;

struct ProgramContext {
    epoll: Epoll,
    listener: TcpListener,
    connections: Vec<Option<Connection>>,
}

impl ProgramContext {
    fn new() -> anyhow::Result<Self> {
        let epoll =
            Epoll::new(EpollCreateFlags::EPOLL_CLOEXEC).context("Failed to create epoll")?;
        let listener = TcpListener::bind("127.0.0.1:6379").context("Failed to bind listener")?;

        Ok(Self {
            epoll,
            listener,
            connections: vec![],
        })
    }

    fn handle_epollin(&mut self, data: u64) -> anyhow::Result<()> {
        if data == LISTENER_DATA {
            let (stream, _) = self
                .listener
                .accept()
                .context("Failed to accept connection")?;
            stream
                .set_nonblocking(true)
                .context("Failed to set stream nonblocking")?;

            let connection_index =
                if let Some(index) = self.connections.iter().position(|c| c.is_none()) {
                    index
                } else {
                    self.connections.push(None);
                    self.connections.len() - 1
                };

            self.epoll
                .add(
                    &stream,
                    EpollEvent::new(EpollFlags::EPOLLIN, connection_index as u64),
                )
                .context("Failed to add connection to epoll")?;
            self.connections[connection_index] = Some(Connection::from(stream));
        } else {
            let connection = self
                .connections
                .get_mut(data as usize)
                .context("Invalid connection")?;
            let connection = connection.as_mut().context("Invalid connection")?;

            let n = connection
                .stream
                .read(&mut connection.read_buffer)
                .context("Failed to read on connection")?;
            if n == 0 {
                self.epoll
                    .delete(&connection.stream)
                    .context("Failed to delete connection from epoll")?;
                self.connections[data as usize] = None;
                return Ok(());
            }

            connection
                .stream
                .write(b"+PONG\r\n")
                .context("Failed to write on connection")?;
        }
        Ok(())
    }

    fn run(&mut self) -> anyhow::Result<()> {
        println!("Hello, world!");

        self.listener
            .set_nonblocking(true)
            .context("Failed to set listener nonblocking")?;
        self.epoll
            .add(
                &self.listener,
                EpollEvent::new(EpollFlags::EPOLLIN, LISTENER_DATA),
            )
            .context("Failed to add listener to epoll")?;

        let mut events = [EpollEvent::empty(); 16];
        while let Ok(event_count) = self.epoll.wait(&mut events, EpollTimeout::NONE) {
            for event in events.iter().take(event_count) {
                if event.events().contains(EpollFlags::EPOLLIN) {
                    self.handle_epollin(event.data())
                        .context("Failed to handle EPOLLIN")?;
                }
            }
        }

        Ok(())
    }
}

fn main() {
    ProgramContext::new().unwrap().run().unwrap();
}
