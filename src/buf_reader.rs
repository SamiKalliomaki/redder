use std::io;

use bytes::{BytesMut, Buf};
use monoio::{io::AsyncReadRent, fs::File};

pub(crate) trait BufReader {
    async fn try_fill_buf(&mut self) -> io::Result<usize>;
    fn buffer(&self) -> &BytesMut;
    fn buffer_mut(&mut self) -> &mut BytesMut;
}

pub(crate) trait BufReaderExt {
    async fn fill_buf(&mut self) -> io::Result<usize>;
    async fn read_u8(&mut self) -> io::Result<u8>;
    async fn read_u16(&mut self) -> io::Result<u16>;
    async fn read_u32(&mut self) -> io::Result<u32>;
    async fn read_u64(&mut self) -> io::Result<u64>;
    async fn read_bytes(&mut self, len: usize) -> io::Result<BytesMut>;
    async fn read_until(&mut self, delimeter: &[u8]) -> io::Result<BytesMut>;
}

/// A buffered reader that reads bytes from an underlying reader.
/// Uses a BytesMut buffer to store read bytes.
pub(crate) struct TcpBufReader<R> {
    pub inner: R,
    buffer: BytesMut
}

impl<R: AsyncReadRent> TcpBufReader<R> {
    pub fn new(inner: R) -> Self {
        Self {
            inner,
            buffer: BytesMut::new()
        }
    }
}

impl<R: AsyncReadRent> BufReader for TcpBufReader<R> {
    async fn try_fill_buf(&mut self) -> io::Result<usize> {
        if self.buffer.capacity() < 1024 {
            self.buffer.reserve(1024 * 1024);
        }

        let (n, buffer) = self.inner.read(self.buffer.split_off(0)).await;
        self.buffer = buffer;
        Ok(n?)
    }

    fn buffer(&self) -> &BytesMut {
        &self.buffer
    }

    fn buffer_mut(&mut self) -> &mut BytesMut {
        &mut self.buffer
    }
}

pub(crate) struct FileBufReader {
    inner: File,
    pointer: u64,
    buffer: BytesMut
}

impl FileBufReader {
    pub fn new(inner: File) -> Self {
        Self {
            inner,
            pointer: 0,
            buffer: BytesMut::new()
        }
    }
}

impl BufReader for FileBufReader {
    async fn try_fill_buf(&mut self) -> io::Result<usize> {
        if self.buffer.capacity() < 1024 {
            self.buffer.reserve(1024 * 1024);
        }

        let (n, buffer) = self.inner.read_at(self.buffer.split_off(0), self.pointer).await;
        self.buffer = buffer;
        let n = n?;

        self.pointer += n as u64;
        Ok(n)
    }

    fn buffer(&self) -> &BytesMut {
        &self.buffer
    }

    fn buffer_mut(&mut self) -> &mut BytesMut {
        &mut self.buffer
    }
}

impl<R: BufReader> BufReaderExt for R {
    async fn fill_buf(&mut self) -> io::Result<usize> {
        let n = self.try_fill_buf().await?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Unexpected EOF",
            ));
        }
        Ok(n)
    }

    async fn read_u8(&mut self) -> io::Result<u8> {
        if !self.buffer().has_remaining() {
            self.fill_buf().await?;
        }
        Ok(self.buffer_mut().get_u8())
    }

    async fn read_u16(&mut self) -> io::Result<u16> {
        while self.buffer().len() < 2 {
            self.fill_buf().await?;
        }
        Ok(self.buffer_mut().get_u16_le())
    }

    async fn read_u32(&mut self) -> io::Result<u32> {
        while self.buffer().len() < 4 {
            self.fill_buf().await?;
        }
        Ok(self.buffer_mut().get_u32_le())
    }

    async fn read_u64(&mut self) -> io::Result<u64> {
        while self.buffer().len() < 8 {
            self.fill_buf().await?;
        }
        Ok(self.buffer_mut().get_u64_le())
    }

    async fn read_bytes(&mut self, len: usize) -> io::Result<BytesMut> {
        while self.buffer().len() < len {
            self.fill_buf().await?;
        }

        Ok(self.buffer_mut().split_to(len))
    }

    async fn read_until(&mut self, delimeter: &[u8]) -> io::Result<BytesMut> {
        let mut line = BytesMut::new();
        loop {
            match self.buffer().windows(delimeter.len()).position(|bytes| bytes == delimeter) {
                Some(pos) => {
                    line.unsplit(self.buffer_mut().split_to(pos + delimeter.len()));
                    return Ok(line);
                }
                None => {
                    line.unsplit(self.buffer_mut().split());
                    self.fill_buf().await?;
                }
            }
        }
    }
}
