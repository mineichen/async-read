use core::cell::UnsafeCell;
use core::fmt;
use core::future::Future;
use core::mem;
use core::pin::Pin;
use core::ptr::NonNull;
use core::task::{Context, Poll};

use crate::Writer;
use crate::shared::{Aliased, Shared};

/// A callable accepted by [`Reader::new`]: it takes a [`Writer<'w>`](Writer) and returns a
/// future that resolves to a `Result<(), E>`.
///
/// This is implemented for every async closure and `async fn` of that shape, and for every `fn`
/// that returns such a future, like `fn(Writer<'_>) -> MyFuture<'_>`. The trait itself cannot
/// be implemented by hand. A bound on it has to hold for every lifetime of the writer:
/// `F: for<'w> Producer<'w, E>`.
///
/// The trait only exists because the future returned through [`AsyncFnOnce`] cannot be named
/// yet, which the reader has to do in order to store it.
pub trait Producer<'w, E>: sealed::Sealed<'w, E> {}

impl<'w, E, F> Producer<'w, E> for F where F: sealed::Sealed<'w, E> {}

mod sealed {
    use core::future::Future;

    use crate::Writer;

    // `AsyncFnOnce` is what makes a closure infer that it takes a writer of every lifetime
    pub trait Sealed<'w, E>: AsyncFnOnce(Writer<'w>) -> Result<(), E> {
        type Future: Future<Output = Result<(), E>>;

        fn produce(self, writer: Writer<'w>) -> Self::Future;
    }

    impl<'w, E, F, Fut> Sealed<'w, E> for F
    where
        F: AsyncFnOnce(Writer<'w>) -> Result<(), E> + FnOnce(Writer<'w>) -> Fut,
        Fut: Future<Output = Result<(), E>>,
    {
        type Future = Fut;

        fn produce(self, writer: Writer<'w>) -> Fut {
            self(writer)
        }
    }
}

/// The future of a producer.
///
/// Its writer points into the reader the future is a field of, so the writer's true lifetime is
/// the future's own, which has no name. `'static` stands in for it.
///
/// That is sound as long as the future is only used in ways that work for every lifetime.
/// Polling and dropping do. Nothing else may be done with it, and in particular nothing that
/// depends on a trait other than [`Future`]: such a trait could be implemented for `'static`
/// alone and hand the writer out.
type FutureOf<F, E> = <F as sealed::Sealed<'static, E>>::Future;

/// Reads what a future writes to a [`Writer`].
///
/// See the [crate documentation](crate) for an overview.
///
/// With the `futures-io` feature (enabled by default) this implements
/// [`AsyncRead`](futures_io::AsyncRead), provided the producer's error converts into an
/// [`std::io::Error`]. [`poll_read`](Self::poll_read) is the same operation without `std`,
/// failing with the producer's own error.
///
/// A reader must be pinned before it can be read from, for example with
/// [`pin!`](core::pin::pin) or `Box::pin`.
///
/// # Panics
///
/// A panic of the producer or its future propagates to whoever reads. Reading again polls the
/// future again, which panics again if it is an `async` block. If the producer itself panicked,
/// or if it or its future panicked while being dropped, reading again panics as well.
#[must_use = "readers do nothing unless polled"]
pub struct Reader<F, E>
where
    // The writer points into the reader, so it must not leave the future. A producer that
    // works for every lifetime cannot have put it anywhere that outlives the future, nor into
    // `E`, which is the same for all of them.
    F: for<'w> Producer<'w, E>,
{
    // Dropped before `shared`, which the future points to.
    //
    // In an `UnsafeCell`, so that a `&Reader` does not claim that the future is immutable. The
    // future of an `async` block borrows from itself, mutably as well, and a `Pin<&mut Reader>`
    // dereferences to a `&Reader` between two polls. Without the cell, Stacked Borrows takes
    // that reference as the end of those borrows. This is a gap which every self-referential
    // future has today, and which `UnsafePinned` is going to close for them.
    state: UnsafeCell<State<F, FutureOf<F, E>, E>>,
    shared: Aliased<Shared>,
}

enum State<F, Fut, E> {
    /// Not polled yet
    Start(F),
    Running {
        future: Fut,
        /// Whether [`Reader::poll_close`] was called. It is here, rather than in the reader,
        /// to share a word with the discriminant.
        closed: bool,
    },
    /// The future failed after writing something, which had to be returned first
    Failed(E),
    Done,
    /// The producer panicked, or it or its future did while being dropped
    Poisoned,
}

impl<F, Fut, E> State<F, Fut, E> {
    /// Drops what the state owns and continues with `next`. If dropping panics, the state is
    /// [`State::Poisoned`].
    fn set(&mut self, next: Self) {
        *self = Self::Poisoned;
        *self = next;
    }
}

// SAFETY: Nothing can be done with a `&Reader`: no method takes `&self`, and `Debug` reads no
// field. It has to stay that way.
unsafe impl<F, E> Sync for Reader<F, E> where F: for<'w> Producer<'w, E> {}

impl<F, E> Reader<F, E>
where
    F: for<'w> Producer<'w, E>,
{
    /// Creates a reader that yields what `producer` writes.
    ///
    /// `producer` is called on the first read, and its future is polled on every read. The
    /// reader reaches its end when that future completes, and fails with the future's error if
    /// it returns one.
    ///
    /// `producer` is usually an async closure; an `async fn` that takes a `Writer<'_>` works
    /// too, and so does a `fn` that returns a future of its own, like
    /// `fn(Writer<'_>) -> MyFuture<'_>`. Two forms are not accepted: a closure returning an
    /// async block (`|w| async move { .. }`), and an `async fn` that is generic over the type
    /// of the writer. Wrap the latter as `async |w| generic(w).await`.
    ///
    #[cfg_attr(feature = "futures-io", doc = "```")]
    #[cfg_attr(not(feature = "futures-io"), doc = "```ignore")]
    /// use std::io;
    /// use std::pin::pin;
    ///
    /// use async_read::Reader;
    /// use futures::{AsyncReadExt, AsyncWriteExt};
    ///
    /// # futures::executor::block_on(async {
    /// let reader = Reader::new(async |mut writer| {
    ///     writer.write_all(b"Hello ").await?;
    ///     writer.write_all(b"World").await?;
    ///     io::Result::Ok(())
    /// });
    ///
    /// let mut content = String::new();
    /// pin!(reader).read_to_string(&mut content).await?;
    /// assert_eq!("Hello World", content);
    /// # io::Result::Ok(())
    /// # }).unwrap();
    /// ```
    pub fn new(producer: F) -> Self {
        Self {
            state: UnsafeCell::new(State::Start(producer)),
            shared: Aliased::new(Shared::new()),
        }
    }

    /// Reads into `buf` and returns the number of bytes read.
    ///
    /// This polls the producer's future once and returns what it wrote, as soon as that is at
    /// least one byte. So `buf` is only filled completely if the future writes that much
    /// without becoming pending.
    ///
    /// Returns `Ok(0)` if `buf` is empty, without doing anything else, or if the future has
    /// completed.
    ///
    /// A pending read has consumed nothing, so it does not have to be polled again.
    ///
    /// # Errors
    ///
    /// Fails with the error the future completed with. If the future wrote something in the
    /// same poll, that is returned first and the error by the next call. After the error, the
    /// reader is at its end.
    pub fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize, E>> {
        self.poll(cx, Some(buf))
    }

    /// Stops reading and waits for the producer's future to complete.
    ///
    /// From the first call on, every write fails with [`Closed`](crate::Closed). Unlike
    /// dropping the reader, this lets the future run to completion, so it can clean up
    /// asynchronously. Returns what the future completed with, which is usually the error of
    /// the write that failed.
    ///
    /// A producer that was not called yet is dropped without being called. Closing cannot be
    /// undone: later reads return no data, but still report the future's error.
    pub fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), E>> {
        self.poll(cx, None).map_ok(|_| ())
    }

    /// Polls the future while it can write to `buf`, and returns how much it wrote. Without a
    /// buffer, the reader is closed.
    fn poll(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: Option<&mut [u8]>,
    ) -> Poll<Result<usize, E>> {
        if let Some([]) = buf {
            return Poll::Ready(Ok(0));
        }
        // SAFETY: Only the future is pinned, and it is never moved: once the state is
        // `State::Running`, it is only replaced by assigning to it, which drops the future in
        // place.
        let this = unsafe { self.get_unchecked_mut() };
        let state = this.state.get_mut();
        // No `&mut Shared`: the writer points to it
        let shared = this.shared.get();

        let buf = match *state {
            State::Start(_) if buf.is_none() => {
                // Closed before the producer was called: there is nothing to wait for
                state.set(State::Done);
                return Poll::Ready(Ok(0));
            }
            State::Start(_) => buf,
            State::Running { ref mut closed, .. } => {
                *closed |= buf.is_none();
                buf.filter(|_| !*closed)
            }
            State::Failed(_) => {
                let State::Failed(error) = mem::replace(state, State::Done) else {
                    unreachable!()
                };
                return Poll::Ready(Err(error));
            }
            State::Done => return Poll::Ready(Ok(0)),
            State::Poisoned => panic!("`Reader` polled after a panic"),
        };

        // Before the buffer is published: if this panics, nothing is published
        shared.wake_parked();
        // SAFETY: `published` is withdrawn or dropped before this returns
        let published = unsafe { shared.publish(buf, cx.waker()) };

        if let State::Start(_) = *state {
            let State::Start(producer) = mem::replace(state, State::Poisoned) else {
                unreachable!()
            };
            // SAFETY: `shared` is valid until the reader is dropped or its memory is reused.
            // The reader is pinned, so neither happens before the future, and with it the
            // writer, is dropped; see `FutureOf` for why the writer cannot be anywhere else.
            // This is the only place that creates a writer, and it is reached once.
            let writer = unsafe { Writer::new(NonNull::from(shared)) };
            *state = State::Running {
                future: producer.produce(writer),
                closed: false,
            };
        }
        let State::Running { future, .. } = state else {
            unreachable!()
        };
        // SAFETY: The future is pinned because the reader is, see above
        let result = unsafe { Pin::new_unchecked(future) }.poll(cx);
        // Before the future is dropped below: its destructors cannot write anymore
        let written = published.withdraw();

        let Poll::Ready(result) = result else {
            return match written {
                0 => Poll::Pending,
                _ => Poll::Ready(Ok(written)),
            };
        };
        let result = match result {
            Err(error) if written == 0 => {
                state.set(State::Done);
                Err(error)
            }
            Err(error) => {
                state.set(State::Failed(error));
                Ok(written)
            }
            Ok(()) => {
                state.set(State::Done);
                Ok(written)
            }
        };
        shared.forget_parked();
        Poll::Ready(result)
    }
}

impl<F, E> fmt::Debug for Reader<F, E>
where
    F: for<'w> Producer<'w, E>,
{
    // Must not read any field, see the implementation of `Sync`
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Reader").finish_non_exhaustive()
    }
}
