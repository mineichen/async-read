use core::fmt;
use core::marker::PhantomData;
use core::ptr::NonNull;
use core::task::{Context, Poll};

use crate::shared::{NoRoom, Shared};

/// The reader no longer accepts data.
///
/// [`Writer::poll_write`] fails with this error after the [`Reader`](crate::Reader) was
/// [closed](crate::Reader::poll_close), and while the reader is being dropped.
///
/// With the `futures-io` feature it converts into an [`std::io::Error`] of kind
/// [`BrokenPipe`](std::io::ErrorKind::BrokenPipe).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Closed;

impl fmt::Display for Closed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the reader no longer accepts data")
    }
}

impl core::error::Error for Closed {}

/// The writing half of a [`Reader`](crate::Reader).
///
/// A writer only exists as the argument of the producer passed to
/// [`Reader::new`](crate::Reader::new). It can be moved freely within the producer's future,
/// but its lifetime keeps it from leaving that future.
///
/// Nothing is buffered: bytes are copied straight into the buffer the reader is currently
/// filling. When that buffer is full, writing is pending until the reader is read from again.
///
/// With the `futures-io` feature (enabled by default) this implements
/// [`AsyncWrite`](futures_io::AsyncWrite). [`poll_write`](Self::poll_write) is the same
/// operation without `std`, failing with [`Closed`] instead of an [`std::io::Error`].
pub struct Writer<'w> {
    shared: NonNull<Shared>,
    /// Covariant, like a reference to the reader
    _reader: PhantomData<&'w ()>,
}

// SAFETY: There is one writer per `Shared`, and it only accesses the `Shared` through `&mut
// self`. The reader accesses the `Shared` as well, but never while it polls or drops the
// future, and neither of those returns before everything in the future that could reach the
// writer is done with it. So all accesses happen one after another. What the `Shared` contains
// and points to is `Send` and `Sync`.
unsafe impl Send for Writer<'_> {}
// SAFETY: Nothing can be done with a `&Writer`. This matters for the producer's future: if it
// holds a reference to something containing the writer across an `.await`, it is only `Send`
// if the writer is `Sync`.
unsafe impl Sync for Writer<'_> {}

impl<'w> Writer<'w> {
    /// # Safety
    ///
    /// `shared` must be valid for `'w`, and there must be no other writer for it.
    pub(crate) unsafe fn new(shared: NonNull<Shared>) -> Self {
        Self {
            shared,
            _reader: PhantomData,
        }
    }

    fn shared(&mut self) -> &Shared {
        // SAFETY: Valid for `'w`, see `new`
        unsafe { self.shared.as_ref() }
    }

    /// Writes as much of `buf` as fits into the buffer the reader is filling.
    ///
    /// Returns the number of bytes written, which is only 0 if `buf` is empty.
    ///
    /// This is pending while the reader's buffer is full, until the reader is read from again
    /// or closed.
    ///
    /// # Wakers
    ///
    /// Reading polls the producer's future, so a pending write is usually polled again without
    /// being woken: if the waker of `cx` [would wake](core::task::Waker::will_wake) whoever is
    /// polling the reader, it is neither kept nor woken. Whoever calls this with the waker it
    /// was polled with therefore has to call it again whenever it is polled itself, as
    /// `.await`, `join!` and `select!` do.
    ///
    /// Any other waker, like the ones `FuturesUnordered` polls its futures with, is kept and
    /// woken the next time the reader is read from or closed, before the future is polled. As
    /// with other `poll_*` methods, only the waker of the most recent call is kept.
    ///
    /// # Errors
    ///
    /// Fails with [`Closed`] if the reader was closed or is being dropped.
    pub fn poll_write(&mut self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, Closed>> {
        self.poll_write_all_of(cx, [buf])
    }

    /// Writes as much of `bufs` as fits, as if they were one buffer.
    pub(crate) fn poll_write_all_of<'a>(
        &mut self,
        cx: &mut Context<'_>,
        bufs: impl IntoIterator<Item = &'a [u8]>,
    ) -> Poll<Result<usize, Closed>> {
        let shared = self.shared();
        let mut written = 0;
        for buf in bufs.into_iter().filter(|buf| !buf.is_empty()) {
            match shared.write(buf) {
                Ok(len) => {
                    written += len;
                    if len < buf.len() {
                        break;
                    }
                }
                Err(_) if written != 0 => break,
                Err(NoRoom::Full) => {
                    shared.park(cx);
                    return Poll::Pending;
                }
                Err(NoRoom::Closed) => return Poll::Ready(Err(Closed)),
            }
        }
        Poll::Ready(Ok(written))
    }
}

impl fmt::Debug for Writer<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Writer").finish_non_exhaustive()
    }
}
