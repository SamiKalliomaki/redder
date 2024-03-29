use std::{future::Future, io, pin::Pin};

use bytes::{Buf, BytesMut};
use monoio::io::{AsyncReadRent, AsyncWriteRent, AsyncWriteRentExt};

type BoxedFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;
type RedisStreamItem<'a> = BoxedFuture<'a, Option<io::Result<RedisValue<'a>>>>;

pub(crate) trait RedisValueRead {
    async fn read_value(&mut self) -> io::Result<RedisValue>;

    async fn read_array<'a>(&'a mut self) -> io::Result<Box<dyn RedisValueStream + 'a>>
    where
        Self: Sized,
    {
        let value = self.read_value().await?;
        match value {
            RedisValue::Array(stream) => Ok(stream),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Expected array type",
            )),
        }
    }
}

pub(crate) trait RedisValueStream {
    fn read_value_dyn(&mut self) -> RedisStreamItem;
    fn consume_rest<'a>(&'a mut self) -> BoxedFuture<'a, io::Result<()>>;
}

pub(crate) trait RedisValueStreamExt {
    async fn read_string_dyn(&mut self) -> io::Result<String>;
}

impl<T: RedisValueStream + ?Sized> RedisValueStreamExt for T {
    async fn read_string_dyn(&mut self) -> io::Result<String> {
        match self.read_value_dyn().await {
            Some(Ok(RedisValue::String(s))) => Ok(s),
            Some(Ok(_)) => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Expected string value",
            )),
            Some(Err(e)) => Err(e),
            None => Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Unexpected end of stream",
            )),
        }
    }
}

pub(crate) enum RedisValue<'a> {
    String(String),
    Array(Box<dyn RedisValueStream + 'a>),
}

struct RedisArrayStream<'a, T: RedisValueRead> {
    stream: &'a mut T,
    len: usize,
}

impl<'a, T: RedisValueRead> RedisValueRead for RedisArrayStream<'a, T> {
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

impl<'a, T: RedisValueRead> RedisValueStream for RedisArrayStream<'a, T> {
    fn read_value_dyn<'b>(&'b mut self) -> RedisStreamItem<'b> {
        Box::pin(async {
            if self.len == 0 {
                return None;
            }

            self.len -= 1;
            Some(self.stream.read_value().await)
        })
    }

    fn consume_rest<'b>(&'b mut self) -> BoxedFuture<'b, io::Result<()>> {
        Box::pin(async move {
            while self.len > 0 {
                self.len -= 1;
                self.stream.read_value().await?;
            }

            Ok(())
        })
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

impl<Read: AsyncReadRent> RedisValueRead for RedisBufStream<Read> {
    async fn read_value(&mut self) -> io::Result<RedisValue> {
        Ok(match self.read_u8().await? {
            // Simple string
            b'+' => RedisValue::String(self.read_line().await?),
            // Bulk string
            b'$' => RedisValue::String(self.parse_bulk_string().await?),
            // Array
            b'*' => RedisValue::Array(Box::new(self.parse_array().await?)),
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
        while !self.buffer.has_remaining() {
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

    async fn parse_array<'a>(&'a mut self) -> io::Result<RedisArrayStream<'a, Self>> {
        let len = self.parse_int().await?;
        return Ok(RedisArrayStream { len, stream: self });
    }
}

impl<Read: AsyncReadRent> RedisValueStream for RedisBufStream<Read> {
    fn read_value_dyn(&mut self) -> RedisStreamItem {
        Box::pin(async { Some(self.read_value().await) })
    }

    fn consume_rest<'a>(&'a mut self) -> BoxedFuture<'a, io::Result<()>> {
        unimplemented!()
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
