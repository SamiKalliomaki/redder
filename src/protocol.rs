use std::io;

use bytes::{Buf, BytesMut};
use monoio::io::{AsyncReadRent, AsyncWriteRent, AsyncWriteRentExt};

pub(crate) trait RedisRead {
    async fn read_value(&mut self) -> io::Result<RedisValue>;
}

pub(crate) trait RedisReadExt {
    async fn read_array(&mut self) -> io::Result<usize>;
    async fn read_string(&mut self) -> io::Result<String>;
}

impl<T: RedisRead> RedisReadExt for T {
    async fn read_array(&mut self) -> io::Result<usize>
    {
        let value = self.read_value().await?;
        match value {
            RedisValue::Array(length) => Ok(length),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Expected array, got: {:?}", value),
            )),
        }
    }

    async fn read_string(&mut self) -> io::Result<String>
    {
        let value = self.read_value().await?;
        match value {
            RedisValue::String(s) => Ok(s),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Expected string, got: {:?}", value),
            )),
        }
    }
}

#[derive(Debug)]
pub(crate) enum RedisValue {
    String(String),
    Array(usize),
}

struct RedisArrayStream<'a, T: RedisRead> {
    stream: &'a mut T,
    len: usize,
}

impl<'a, T: RedisRead> RedisRead for RedisArrayStream<'a, T> {
    async fn read_value(&mut self) -> io::Result<RedisValue> {
        if self.len == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Unexpected end of array",
            ));
        }

        self.len -= 1;
        self.stream.read_value().await
    }
}

pub(crate) struct RedisBufStream<T> {
    stream: T,
    buffer: BytesMut,
}

impl<T> RedisBufStream<T> {
    pub fn new(stream: T) -> Self {
        Self {
            stream,
            buffer: BytesMut::new(),
        }
    }
}

impl<Read: AsyncReadRent> RedisRead for RedisBufStream<Read> {
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

impl<Read: AsyncReadRent> RedisBufStream<Read> {
    async fn fill_buf(&mut self) -> io::Result<usize> {
        if self.buffer.capacity() < 1024 {
            self.buffer.reserve(1024 * 1024);
        }

        let (n, buffer) = self.stream.read(self.buffer.split_off(0)).await;
        self.buffer = buffer;

        let n = n?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Unexpected EOF",
            ));
        }

        Ok(n)
    }

    async fn read_u8(&mut self) -> io::Result<u8> {
        if !self.buffer.has_remaining() {
            self.fill_buf().await?;
        }
        Ok(self.buffer.get_u8())
    }

    async fn read_line_bytes(&mut self) -> io::Result<BytesMut> {
        let mut line = BytesMut::new();
        loop {
            match self.buffer.windows(2).position(|bytes| bytes == b"\r\n") {
                Some(pos) => {
                    line.unsplit(self.buffer.split_to(pos));
                    self.buffer.advance(2);
                    return Ok(line);
                }
                None => {
                    line.unsplit(self.buffer.split());
                    self.fill_buf().await?;
                }
            }
        }
    }

    async fn read_line(self: &mut Self) -> io::Result<String> {
        let line = self.read_line_bytes().await?;
        match String::from_utf8(line.into()) {
            Ok(s) => Ok(s),
            Err(_) => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Stream did not contain valid UTF-8",
            )),
        }
    }

    async fn read_bytes(&mut self, len: usize) -> io::Result<BytesMut> {
        while self.buffer.len() < len {
            self.fill_buf().await?;
        }

        Ok(self.buffer.split_to(len))
    }

    async fn parse_bulk_string(&mut self) -> io::Result<String> {
        let len: usize = self.parse_int().await?;
        let line = self.read_bytes(len).await?;
        if self.read_bytes(2).await?.as_ref() != b"\r\n" {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Expected CRLF at end of bulk string",
            ));
        }

        match String::from_utf8(line.into()) {
            Ok(s) => Ok(s),
            Err(_) => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Stream did not contain valid UTF-8",
            )),
        }
    }

    async fn parse_int<T>(&mut self) -> io::Result<T>
    where
        T: std::str::FromStr,
    {
        let line = self.read_line().await?;
        line.parse::<T>().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Failed to parse integer from line: {}", line),
            )
        })
    }
}

pub(crate) trait RedisWrite {
    async fn write_simple_string(self: &mut Self, s: String) -> io::Result<()>;
    async fn write_bulk_string(self: &mut Self, s: String) -> io::Result<()>;
}

impl<T> RedisWrite for RedisBufStream<T>
where
    T: AsyncWriteRent,
{
    async fn write_simple_string(self: &mut Self, s: String) -> io::Result<()> {
        let s = s.into_bytes();

        self.stream.write_all(b"+").await.0?;
        self.stream.write_all(s).await.0?;
        self.stream.write_all(b"\r\n").await.0?;

        Ok(())
    }

    async fn write_bulk_string(self: &mut Self, s: String) -> io::Result<()> {
        let s = s.into_bytes();
        let size = s.len().to_string().into_bytes();

        self.stream.write_all(b"$").await.0?;
        self.stream.write_all(size).await.0?;
        self.stream.write_all(b"\r\n").await.0?;
        self.stream.write_all(s).await.0?;
        self.stream.write_all(b"\r\n").await.0?;

        Ok(())
    }
}
