use std::{thread, sync::Arc, path::{self, Path}};

use clap::Parser;
use database::Database;
use monoio::{
    net::{TcpListener, TcpStream},
    spawn, fs,
};

use anyhow::Context;
use rdb::read_rdb;

use crate::connection::Connection;

mod buf_reader;
mod connection;
mod database;
mod protocol;
mod rdb;

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


async fn read_db(db: &mut Database, dir: &str, dbfilename: &str) -> anyhow::Result<()> {
    let path = path::Path::join(&Path::new(dir), &dbfilename);
    let file = match fs::File::open(path).await {
        Ok(file) => file,
        Err(e) => {
            if e.kind() == std::io::ErrorKind::NotFound {
                println!("RDB file not found, starting with empty database");
                return Ok(());
            } else {
                return Err(e).context("Failed to open RDB file");
            }
        }
    };

    db.swap_datasets(read_rdb(file).await?);

    Ok(())
}

async fn run() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let mut db = Database::new();
    if let (Some(ref dir), Some(ref dbfilename)) = (&cli.dir, &cli.dbfilename) {
        read_db(&mut db, dir, dbfilename).await?;
    }
    let db = Arc::new(db);

    if let Some(dir) = cli.dir {
        db.set_config(b"dir", dir);
    }
    if let Some(dbfilename) = cli.dbfilename {
        db.set_config(b"dbfilename", dbfilename);
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
