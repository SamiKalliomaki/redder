use std::{thread, sync::Arc};

use clap::Parser;
use database::Database;
use monoio::{
    net::{TcpListener, TcpStream},
    spawn,
};

use anyhow::Context;

use crate::connection::Connection;

mod connection;
mod database;
mod protocol;

#[derive(Parser)]
struct Cli {
    /// RDB storage directory
    #[clap(long)]
    dir: Option<String>,

    /// RDB storage file name
    #[clap(long)]
    dbfilename: Option<String>,
}

async fn handle_connection_spawn(db: Arc<Database>, stream: TcpStream) {
    let addr = stream.peer_addr().unwrap();
    println!("New connection from {} on {:?}", addr, thread::current().id());

    let result = Connection::new(db.as_ref(), stream)
        .handle_connection()
        .await;
    if let Err(e) = result {
        println!("Connection to {} closed with error: {:?}", addr, e);
    } else {
        println!("Connection to {} closed", addr);
    }
}

async fn run() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let db = Arc::new(Database::new());
    if let Some(dir) = cli.dir {
        db.set_config("dir".to_owned(), dir);
    }
    if let Some(dbfilename) = cli.dbfilename {
        db.set_config("dbfilename".to_owned(), dbfilename);
    }

    let listener = TcpListener::bind("127.0.0.1:6379").context("Failed to bind")?;
    loop {
        let (stream, _) = listener
            .accept()
            .await
            .context("Failed to accept connection")?;
        spawn(handle_connection_spawn(db.clone(), stream));
    }
}

#[monoio::main(driver = "legacy")]
async fn main() {
    run().await.unwrap();
}
