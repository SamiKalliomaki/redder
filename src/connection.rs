use monoio::io::{AsyncReadRent, AsyncWriteRent};

use crate::{protocol::{RedisBufStream, RedisReadExt, RedisWrite}, database::Database};

pub(crate) struct Connection<'db, Stream: AsyncReadRent + AsyncWriteRent> {
    db: &'db Database,
    stream: RedisBufStream<Stream>,
}

impl<'db, Stream: AsyncReadRent + AsyncWriteRent> Connection<'db, Stream> {
    pub(crate) fn new(db: &'db Database, stream: Stream) -> Self {
        Self {
            db,
            stream: RedisBufStream::new(stream),
        }
    }

    pub(crate) async fn handle_connection(&mut self) -> anyhow::Result<()> {
        loop {
            let command = self.stream.read_string_array().await?;

            match command[0].to_lowercase().as_str() {
                "ping" => {
                    self.stream.write_simple_string("PONG").await?;
                },
                "echo" => {
                    self.stream.write_bulk_string(command[1].clone().into_bytes()).await?;
                },
                "get" => {
                    let key = &command[1];
                    let value = self.db.get(key);
                    match value {
                        Some(value) => {
                            self.stream.write_bulk_string(value.into_bytes()).await?;
                        },
                        None => {
                            self.stream.write_null_bulk_string().await?;
                        },
                    }
                },
                "set" => {
                    let key = command[1].clone();
                    let value = command[2].clone();
                    self.db.set(key, value);
                    self.stream.write_simple_string("OK").await?;
                },
                v => {
                    eprintln!("Unknown command: {}", v);
                    continue;
                }
            }
        }
    }
}
