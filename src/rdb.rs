use std::{
    io,
    time::{Duration, UNIX_EPOCH},
};

use bytes::BytesMut;
use monoio::fs::File;

use crate::{
    buf_reader::{BufReader, BufReaderExt, FileBufReader},
    database::{Dataset, Value},
};

const FLAG_BITS: u8 = 0b1100_0000;
const REMAINING_BITS: u8 = 0b0011_1111;

#[derive(Debug, PartialEq)]
enum Length {
    Normal(u32),
    Special(u8),
}

async fn read_length<R: BufReader>(reader: &mut R) -> io::Result<Length> {
    let first = reader.read_u8().await?;
    let flag = first & FLAG_BITS;

    match flag {
        0b0000_0000 => {
            return Ok(Length::Normal(first as u32));
        }
        0b0100_0000 => {
            let first = (first & REMAINING_BITS) as u32;
            let second = reader.read_u8().await? as u32;
            return Ok(Length::Normal(first << 8 | second));
        }
        0b1000_0000 => {
            return Ok(Length::Normal(reader.read_u32().await?));
        }
        0b1100_0000 => {
            return Ok(Length::Special(first & REMAINING_BITS));
        }
        _ => unreachable!(),
    }
}

async fn read_string<R: BufReader>(reader: &mut R) -> io::Result<BytesMut> {
    let length = read_length(reader).await?;
    match length {
        Length::Normal(len) => Ok(reader.read_bytes(len as usize).await?),
        Length::Special(mode) => {
            // TODO: Byte order?
            let bytes = match mode {
                0 => reader.read_bytes(1).await?,
                1 => reader.read_bytes(2).await?,
                2 => reader.read_bytes(4).await?,
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Invalid string length mode",
                    ))
                }
            };
            Ok(bytes)
        }
    }
}

pub(crate) async fn read_rdb(file: File) -> anyhow::Result<Vec<Dataset>> {
    let mut reader = FileBufReader::new(file);

    let magic = reader.read_bytes(5).await?;
    anyhow::ensure!(
        magic.as_ref() == b"REDIS",
        "Invalid RDB file, invalid magic: {:?}, expected REDIS.",
        magic
    );

    let version = reader.read_bytes(4).await?;
    anyhow::ensure!(
        version.as_ref() == b"0003",
        "Invalid RDB file, invalid version: {:?}, expected 0003.",
        version
    );

    let mut datasets = vec![Dataset::new()];
    let current_db = 0;

    loop {
        let opcode = reader.read_u8().await?;
        match opcode {
            0x00 => {
                // String
                let key = read_string(&mut reader).await?.to_vec().into_boxed_slice();
                let value = read_string(&mut reader).await?.to_vec();

                datasets[current_db].set(key, Value::String(value));
            }
            0xFA => {
                // Auxiallary data
                let _key = read_string(&mut reader).await?;
                let _value = read_string(&mut reader).await?;
            }
            0xFB => {
                let _hash_table_size = read_length(&mut reader).await?;
                let _expiry_hash_table_size = read_length(&mut reader).await?;
            }
            0xFC => {
                let expiry = reader.read_u64().await?;
                anyhow::ensure!(
                    reader.read_u8().await? == 0,
                    "Only string values are supported"
                );
                let key = read_string(&mut reader).await?.to_vec().into_boxed_slice();
                let value = read_string(&mut reader).await?.to_vec();

                datasets[current_db]
                    .set_expiry(key.clone(), UNIX_EPOCH + Duration::from_millis(expiry));
                datasets[current_db].set(key, Value::String(value));
            }
            0xFD => {
                let expiry = reader.read_u32().await?;
                anyhow::ensure!(
                    reader.read_u8().await? == 0,
                    "Only string values are supported"
                );
                let key = read_string(&mut reader).await?.to_vec().into_boxed_slice();
                let value = read_string(&mut reader).await?.to_vec();

                datasets[current_db]
                    .set_expiry(key.clone(), UNIX_EPOCH + Duration::from_secs(expiry as u64));
                datasets[current_db].set(key, Value::String(value));
            }
            0xFE => {
                // Select dataset
                let db = read_length(&mut reader).await?;
                anyhow::ensure!(
                    db == Length::Normal(0),
                    "Only DB 0 is supported right now, got: {:?}",
                    db
                );
            }
            0xFF => {
                // EOF
                break;
            }
            _ => {
                anyhow::bail!("Invalid RDB file, unknown opcode: {}", opcode);
            }
        }
    }

    return Ok(datasets);
}
