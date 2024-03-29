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
        let command = stream.read_string_array().await?;

        match command[0].to_lowercase().as_str() {
            "ping" => {
                stream.write_simple_string("PONG".to_owned()).await?;
            },
            "echo" => {
                stream.write_bulk_string(command[1].clone()).await?;
            },
            v => {
                eprintln!("Unknown command: {}", v);
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
