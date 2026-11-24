//! The implementations of the traits of `futures-io`

use core::pin::Pin;
use core::task::{Context, Poll};
use std::io;

use futures_io::{AsyncRead, AsyncWrite};

use crate::{Closed, Producer, Reader, Writer};

impl From<Closed> for io::Error {
    fn from(_: Closed) -> Self {
        io::ErrorKind::BrokenPipe.into()
    }
}

impl<F, E> AsyncRead for Reader<F, E>
where
    F: Producer<E>,
    E: Into<io::Error>,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        Reader::poll_read(self, cx, buf).map_err(Into::into)
    }
}

/// [`poll_flush`](AsyncWrite::poll_flush) waits until the reader has returned what was
/// written, see [`Writer::poll_flush`]. [`poll_close`](AsyncWrite::poll_close) does nothing:
/// the reader only reaches its end when the producer's future completes, which is also when
/// it returns what was written last.
impl AsyncWrite for Writer<'_> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Writer::poll_write(self.get_mut(), cx, buf).map_err(Into::into)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.get_mut()
            .poll_write_all_of(cx, bufs.iter().map(|buf| &**buf))
            .map_err(Into::into)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Writer::poll_flush(self.get_mut(), cx).map(Ok)
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
