use std::io;

use bytes::BytesMut;
use monoio::{io::{AsyncWriteRent, AsyncWriteRentExt}, buf::IoBuf};

use crate::buf_reader::{TcpBufReader, BufReader, BufReaderExt};

#[derive(Debug)]
pub(crate) enum RedisValue {
    String(BytesMut),
    Array(i64),
}

pub(crate) trait RedisRead {
    async fn read_value(&mut self) -> io::Result<RedisValue>;
}

pub(crate) trait RedisReadExt {
    async fn read_string(&mut self) -> io::Result<BytesMut>;
    async fn read_array(&mut self) -> io::Result<i64>;
    async fn read_string_array(&mut self) -> io::Result<Vec<BytesMut>>;
}

impl<T: RedisRead> RedisReadExt for T {
    async fn read_string(&mut self) -> io::Result<BytesMut> {
        let value = self.read_value().await?;
        match value {
            RedisValue::String(s) => Ok(s),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Expected string, got: {:?}", value),
            )),
        }
    }

    async fn read_array(&mut self) -> io::Result<i64> {
        let value = self.read_value().await?;
        match value {
            RedisValue::Array(length) => Ok(length),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Expected array, got: {:?}", value),
            )),
        }
    }

    async fn read_string_array(&mut self) -> io::Result<Vec<BytesMut>> {
        let length = self.read_array().await?;
        if length < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Expected non-negative array length, got: {}", length),
            ));
        }

        let mut values: Vec<BytesMut> = Vec::with_capacity(length as usize);
        for _ in 0..length {
            values.push(self.read_string().await?);
        }
        Ok(values)
    }
}

trait FooBar {
    async fn read_line(&mut self) -> io::Result<BytesMut>;
    async fn parse_line(&mut self) -> io::Result<String>;
    async fn parse_bulk_string(&mut self) -> io::Result<BytesMut>;
    async fn parse_int<T>(&mut self) -> io::Result<T>
    where
        T: std::str::FromStr;
}

impl<R: BufReader> FooBar for R {
    async fn read_line(&mut self) -> io::Result<BytesMut> {
        let mut buf = self.read_until(b"\r\n").await?;
        buf.truncate(buf.len() - 2);
        return Ok(buf);
    }

    async fn parse_line(&mut self) -> io::Result<String> {
        let line = self.read_line().await?;
        Ok(String::from_utf8(line.to_vec()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "Invalid UTF-8 data")
        })?)
    }

    async fn parse_bulk_string(&mut self) -> io::Result<BytesMut> {
        let len: usize = self.parse_int().await?;
        let line = self.read_bytes(len).await?;
        if self.read_bytes(2).await?.as_ref() != b"\r\n" {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Expected CRLF at end of bulk string",
            ));
        }

        Ok(line)
    }

    async fn parse_int<T>(&mut self) -> io::Result<T>
    where
        T: std::str::FromStr,
    {
        let line = self.parse_line().await?;
        line.parse::<T>().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Failed to parse integer from line: {}", line),
            )
        })
    }
}

impl<R: BufReader> RedisRead for R {
    async fn read_value(&mut self) -> io::Result<RedisValue> {
        Ok(match self.read_u8().await? {
            // Simple string
            b'+' => RedisValue::String(self.read_line().await?),
            // Bulk string
            b'$' => RedisValue::String(self.parse_bulk_string().await?),
            // Array
            b'*' => RedisValue::Array(self.parse_int().await?),
            c => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Unknown / invalid value type: {}", c),
                ))
            }
        })
    }
}

pub(crate) trait RedisWrite {
    async fn write_simple_string<T: IoBuf + 'static>(&mut self, s: T) -> io::Result<()>;
    async fn write_bulk_string<T: IoBuf + 'static>(&mut self, s: T) -> io::Result<()>;
    async fn write_null_bulk_string(&mut self) -> io::Result<()>;
    async fn write_bulk_string_opt<T: IoBuf + 'static>(&mut self, s: Option<T>) -> io::Result<()>;
    async fn write_array(&mut self, size: i64) -> io::Result<()>;
}

impl<W: AsyncWriteRent> RedisWrite for TcpBufReader<W>
{
    async fn write_simple_string<T: IoBuf + 'static>(&mut self, s: T) -> io::Result<()> {

        self.inner.write_all(b"+").await.0?;
        self.inner.write_all(s).await.0?;
        self.inner.write_all(b"\r\n").await.0?;

        Ok(())
    }

    async fn write_bulk_string<T: IoBuf + 'static>(&mut self, s: T) -> io::Result<()> {
        let size = s.bytes_init().to_string().into_bytes();

        self.inner.write_all(b"$").await.0?;
        self.inner.write_all(size).await.0?;
        self.inner.write_all(b"\r\n").await.0?;
        self.inner.write_all(s).await.0?;
        self.inner.write_all(b"\r\n").await.0?;

        Ok(())
    }

    async fn write_null_bulk_string(&mut self) -> io::Result<()> {
        self.inner.write_all(b"$-1\r\n").await.0?;
        Ok(())
    }

    async fn write_bulk_string_opt<T: IoBuf + 'static>(&mut self, s: Option<T>) -> io::Result<()> {
        match s {
            Some(s) => self.write_bulk_string(s).await,
            None => self.write_null_bulk_string().await,
        }
    }

    async fn write_array(&mut self, size: i64) -> io::Result<()> {
        let size = size.to_string().into_bytes();

        self.inner.write_all(b"*").await.0?;
        self.inner.write_all(size).await.0?;
        self.inner.write_all(b"\r\n").await.0?;

        Ok(())
    }
}
