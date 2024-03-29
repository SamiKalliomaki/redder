use std::io;

use monoio::{
    net::{TcpListener, TcpStream},
    spawn,
};

use anyhow::Context;
use protocol::RedisReadExt;

use crate::protocol::{RedisBufStream, RedisWrite};

mod protocol;

async fn handle_connection(stream: TcpStream) -> anyhow::Result<()> {

    let mut stream = RedisBufStream::new(stream);
    loop {
        let _ = match stream.read_array().await {
            Ok(array) => array,
            Err(e) => {
                if e.kind() == io::ErrorKind::UnexpectedEof {
                    return Ok(());
                }
                return Err(e).context("Failed to read next command");
            }
        };
        let command = stream.read_string().await?.to_lowercase();

        match command.as_str() {
            "ping" => {
                stream.write_simple_string("PONG".to_owned()).await?;
            }
            _ => {
                eprintln!("Unknown command: {}", command);
                continue;
            }
        }
    }
}

async fn handle_connection_spawn(stream: TcpStream) {
    let addr = stream.peer_addr().unwrap();
    println!("New connection from {}", addr);

    let result = handle_connection(stream);
    if let Err(e) = result.await {
        println!("Connection to {} closed with error: {:?}", addr, e);
    } else {
        println!("Connection to {} closed", addr);
    }
}

async fn run() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").context("Failed to bind")?;
    loop {
        let (stream, _) = listener
            .accept()
            .await
            .context("Failed to accept connection")?;
        spawn(handle_connection_spawn(stream));
    }
}

#[monoio::main(driver = "legacy")]
async fn main() {
    run().await.unwrap();
}
