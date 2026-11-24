//! What happens if the reader does not read until the end, or if something panics
#![cfg(feature = "futures-io")]

mod util;

use std::cell::Cell;
use std::future::{Future, poll_fn};
use std::io;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::{Pin, pin};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};

use async_read::{Closed, Reader, Writer};
use futures::AsyncWriteExt;
use util::{close, pattern, poll_once, read, read_chunks, yield_now};

struct CountDrops<'a>(&'a Cell<usize>);

impl Drop for CountDrops<'_> {
    fn drop(&mut self) {
        self.0.set(self.0.get() + 1);
    }
}

/// Tries to write when it is dropped
struct WriteOnDrop<'a, 'w> {
    writer: Writer<'w>,
    result: &'a Cell<Option<Poll<Result<usize, Closed>>>>,
}

impl Drop for WriteOnDrop<'_, '_> {
    fn drop(&mut self) {
        // Writing nothing always works
        let empty = poll_once(|cx| self.writer.poll_write(cx, b""));
        assert_eq!(Poll::Ready(Ok(0)), empty);
        let result = poll_once(|cx| self.writer.poll_write(cx, b"x"));
        self.result.set(Some(result));
    }
}

/// The message of the panic caused by `f`
fn panic_message<T>(f: impl FnOnce() -> T) -> String {
    let Err(payload) = catch_unwind(AssertUnwindSafe(f)) else {
        panic!("didn't panic");
    };
    match payload.downcast::<String>() {
        Ok(message) => *message,
        Err(payload) => payload.downcast::<&str>().unwrap().to_string(),
    }
}

#[test]
fn dropping_the_reader_drops_the_future() {
    let drops = Cell::new(0);
    let completed = Cell::new(false);
    {
        let guard = CountDrops(&drops);
        let mut reader = pin!(Reader::new(async |mut w| {
            let _guard = guard;
            w.write_all(&pattern(100)).await?;
            completed.set(true);
            io::Result::Ok(())
        }));
        assert_eq!(8, read(&mut reader, 8).unwrap().len());
        assert_eq!(0, drops.get());
    }
    assert_eq!(1, drops.get());
    assert!(!completed.get());
}

#[test]
fn dropping_the_reader_drops_the_producer_if_it_was_never_called() {
    let drops = Cell::new(0);
    {
        let guard = CountDrops(&drops);
        let _reader = Reader::new(async move |_w| {
            let _guard = guard;
            io::Result::Ok(())
        });
        assert_eq!(0, drops.get());
    }
    assert_eq!(1, drops.get());
}

#[test]
fn future_is_dropped_as_soon_as_it_completes() {
    static DROPS: AtomicUsize = AtomicUsize::new(0);
    struct Counted<'w>(Writer<'w>);
    impl Future for Counted<'_> {
        type Output = io::Result<()>;
        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            self.0
                .poll_write(cx, b"ab")
                .map_ok(|_| ())
                .map_err(Into::into)
        }
    }
    impl Drop for Counted<'_> {
        fn drop(&mut self) {
            DROPS.fetch_add(1, Ordering::SeqCst);
        }
    }
    fn produce(writer: Writer<'_>) -> Counted<'_> {
        Counted(writer)
    }

    let mut reader = pin!(Reader::new(produce));
    assert_eq!(0, DROPS.load(Ordering::SeqCst));
    assert_eq!(b"ab", &read(&mut reader, 8).unwrap()[..]);
    // Not only when the reader is dropped
    assert_eq!(1, DROPS.load(Ordering::SeqCst));
    assert_eq!(b"", &read(&mut reader, 8).unwrap()[..]);
}

#[test]
fn writing_fails_while_the_reader_is_dropped() {
    let result = Cell::new(None);
    {
        let mut reader = pin!(Reader::new(async |w| {
            let mut guard = WriteOnDrop {
                writer: w,
                result: &result,
            };
            guard.writer.write_all(&pattern(100)).await?;
            io::Result::Ok(())
        }));
        assert_eq!(8, read(&mut reader, 8).unwrap().len());
        assert_eq!(None, result.get());
    }
    assert_eq!(Some(Poll::Ready(Err(Closed))), result.get());
}

/// The variables of an async block are dropped by the poll which completes it, while the
/// buffer is still there.
#[test]
fn writing_still_works_while_an_async_block_completes() {
    let result = Cell::new(None);
    let mut reader = pin!(Reader::new(async |w| {
        let mut guard = WriteOnDrop {
            writer: w,
            result: &result,
        };
        guard.writer.write_all(b"ab").await?;
        io::Result::Ok(())
    }));
    assert_eq!(b"abx", &read(&mut reader, 8).unwrap()[..]);
    assert_eq!(Some(Poll::Ready(Ok(1))), result.get());
}

#[test]
fn closing_lets_the_future_finish() {
    let cleaned_up = Cell::new(false);
    let mut reader = pin!(Reader::new(async |mut w| {
        let result = w.write_all(&pattern(100)).await;
        yield_now().await;
        cleaned_up.set(true);
        result
    }));
    assert_eq!(8, read(&mut reader, 8).unwrap().len());
    assert!(!cleaned_up.get());

    let error = close(&mut reader).unwrap_err();
    assert_eq!(io::ErrorKind::BrokenPipe, error.kind());
    assert!(cleaned_up.get());

    assert_eq!(b"", &read(&mut reader, 8).unwrap()[..]);
    close(&mut reader).unwrap();
}

#[test]
fn closing_is_final() {
    let (sender, receiver) = futures::channel::oneshot::channel::<()>();
    let second = Cell::new(None);
    let mut reader = pin!(Reader::new(async |mut w| {
        w.write_all(b"a").await?;
        receiver.await.unwrap();
        second.set(Some(w.write(b"b").await.map_err(|e| e.kind())));
        io::Result::Ok(())
    }));
    assert_eq!(b"a", &read(&mut reader, 8).unwrap()[..]);
    assert!(poll_once(|cx| reader.as_mut().poll_close(cx)).is_pending());
    sender.send(()).unwrap();

    // Reading does not make it possible to write again
    assert_eq!(b"", &read(&mut reader, 8).unwrap()[..]);
    assert_eq!(Some(Err(io::ErrorKind::BrokenPipe)), second.get());
    close(&mut reader).unwrap();
}

#[test]
fn closing_before_reading_does_not_call_the_producer() {
    let drops = Cell::new(0);
    let called = Cell::new(false);
    let guard = CountDrops(&drops);
    let mut reader = pin!(Reader::new(async |_w| {
        let _guard = guard;
        called.set(true);
        io::Result::Ok(())
    }));
    close(&mut reader).unwrap();
    assert!(!called.get());
    assert_eq!(1, drops.get());
    assert_eq!(b"", &read(&mut reader, 8).unwrap()[..]);
    assert!(!called.get());
}

#[test]
fn closing_returns_the_error_of_the_future() {
    let mut reader = pin!(Reader::new(async |mut w| {
        w.write_all(b"abc").await?;
        Err(io::Error::other("failed"))
    }));
    assert_eq!(b"abc", &read(&mut reader, 8).unwrap()[..]);
    assert_eq!("failed", close(&mut reader).unwrap_err().to_string());
    close(&mut reader).unwrap();
}

#[test]
fn closing_a_future_which_ignores_it() {
    let mut reader = pin!(Reader::new(async |mut w| {
        for _ in 0..10 {
            let _ = w.write_all(b"abc").await;
            yield_now().await;
        }
        io::Result::Ok(())
    }));
    assert_eq!(b"abc", &read(&mut reader, 8).unwrap()[..]);
    close(&mut reader).unwrap();
}

#[test]
fn error_is_returned_by_the_next_read_whatever_its_buffer_is() {
    let mut reader = pin!(Reader::new(async |mut w| {
        w.write_all(b"abc").await?;
        Err(io::Error::other("failed"))
    }));
    assert_eq!(b"abc", &read(&mut reader, 8).unwrap()[..]);
    assert_eq!(
        "failed",
        read_chunks(&mut reader, 1).unwrap_err().to_string()
    );
}

#[test]
fn panic_of_the_future_is_repeated_when_it_is_polled_again() {
    let drops = Cell::new(0);
    {
        let guard = CountDrops(&drops);
        let mut reader = pin!(Reader::new(async |mut w| {
            let _guard = guard;
            w.write_all(b"ab").await?;
            yield_now().await;
            if drops.get() == 0 {
                panic!("failed");
            }
            io::Result::Ok(())
        }));
        assert_eq!(b"ab", &read(&mut reader, 8).unwrap()[..]);
        assert_eq!("failed", panic_message(|| read(&mut reader, 8)));
        assert_eq!(1, drops.get());
        // Not the end: what was read is incomplete. The future is polled again, and an
        // async block does not accept that.
        assert!(panic_message(|| read(&mut reader, 8)).contains("resumed after panicking"));
        assert!(panic_message(|| close(&mut reader)).contains("resumed after panicking"));
        // Nothing is polled for an empty buffer
        assert_eq!(b"", &read(&mut reader, 0).unwrap()[..]);
    }
    assert_eq!(1, drops.get());
}

#[test]
fn panic_of_the_producer_poisons_the_reader() {
    static DROPS: AtomicUsize = AtomicUsize::new(0);
    struct Never;
    impl Future for Never {
        type Output = io::Result<()>;
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            unreachable!()
        }
    }
    struct Captured;
    impl Drop for Captured {
        fn drop(&mut self) {
            DROPS.fetch_add(1, Ordering::SeqCst);
        }
    }

    {
        let captured = Captured;
        // Not an async closure, so this is what panics when it is called
        let mut reader = pin!(Reader::new(move |_w: Writer<'_>| -> Never {
            let _captured = &captured;
            panic!("failed")
        }));
        assert_eq!("failed", panic_message(|| read(&mut reader, 8)));
        assert_eq!(1, DROPS.load(Ordering::SeqCst));
        assert_eq!(
            "`Reader` polled after a panic",
            panic_message(|| read(&mut reader, 8))
        );
    }
    assert_eq!(1, DROPS.load(Ordering::SeqCst));
}

#[test]
fn panic_while_the_future_is_dropped_does_not_drop_it_twice() {
    static DROPS: AtomicUsize = AtomicUsize::new(0);
    struct PanicOnDrop<'w>(Writer<'w>);
    impl Future for PanicOnDrop<'_> {
        type Output = io::Result<()>;
        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            self.0
                .poll_write(cx, b"ab")
                .map_ok(|_| ())
                .map_err(Into::into)
        }
    }
    impl Drop for PanicOnDrop<'_> {
        fn drop(&mut self) {
            DROPS.fetch_add(1, Ordering::SeqCst);
            panic!("failed to drop");
        }
    }
    fn produce(writer: Writer<'_>) -> PanicOnDrop<'_> {
        PanicOnDrop(writer)
    }

    {
        let mut reader = pin!(Reader::new(produce));
        assert_eq!("failed to drop", panic_message(|| read(&mut reader, 8)));
        assert_eq!(1, DROPS.load(Ordering::SeqCst));
        assert_eq!(
            "`Reader` polled after a panic",
            panic_message(|| read(&mut reader, 8))
        );
    }
    assert_eq!(1, DROPS.load(Ordering::SeqCst));
}

/// A future which panicked is still dropped with the reader, and may write then. The buffer of
/// the read it panicked in is gone by then.
#[test]
fn panic_of_the_future_does_not_leave_the_buffer_behind() {
    static ON_DROP: Mutex<Option<Poll<Result<usize, Closed>>>> = Mutex::new(None);
    /// Unlike an async block, this still has its writer after it panicked
    struct PanicAfterWrite<'w>(Writer<'w>);
    impl Future for PanicAfterWrite<'_> {
        type Output = io::Result<()>;
        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            // There is still room afterwards
            assert_eq!(Poll::Ready(Ok(2)), self.0.poll_write(cx, b"ab"));
            panic!("failed");
        }
    }
    impl Drop for PanicAfterWrite<'_> {
        fn drop(&mut self) {
            let result = poll_once(|cx| self.0.poll_write(cx, b"x"));
            *ON_DROP.lock().unwrap() = Some(result);
        }
    }
    fn produce(writer: Writer<'_>) -> PanicAfterWrite<'_> {
        PanicAfterWrite(writer)
    }

    let mut reader = Box::pin(Reader::new(produce));
    {
        let mut buf = vec![0; 4];
        let message = panic_message(|| poll_once(|cx| reader.as_mut().poll_read(cx, &mut buf)));
        assert_eq!("failed", message);
    }
    assert_eq!(None, *ON_DROP.lock().unwrap());
    drop(reader);
    // Would have written to the buffer which does not exist anymore
    assert_eq!(Some(Poll::Ready(Err(Closed))), *ON_DROP.lock().unwrap());
}

/// Waking must not happen while the writer can reach the buffer: if it panics, the buffer would
/// stay reachable after it is gone.
#[test]
fn panic_of_a_waker_does_not_leave_the_buffer_behind() {
    struct Bomb;
    impl Wake for Bomb {
        fn wake(self: Arc<Self>) {
            panic!("failed to wake");
        }
    }

    let result = Cell::new(None);
    let bomb = Waker::from(Arc::new(Bomb));
    {
        let mut reader = Box::pin(Reader::new(async |w| {
            let mut guard = WriteOnDrop {
                writer: w,
                result: &result,
            };
            let mut remaining: &[u8] = b"abcdef";
            poll_fn(|_| {
                let mut cx = Context::from_waker(&bomb);
                while !remaining.is_empty() {
                    match guard.writer.poll_write(&mut cx, remaining) {
                        Poll::Ready(Ok(written)) => remaining = &remaining[written..],
                        Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                        Poll::Pending => return Poll::Pending,
                    }
                }
                Poll::Ready(Ok(()))
            })
            .await?;
            io::Result::Ok(())
        }));
        assert_eq!(b"abcd", &read(&mut reader.as_mut(), 4).unwrap()[..]);
        {
            let mut buf = vec![0; 4];
            let message = panic_message(|| poll_once(|cx| reader.as_mut().poll_read(cx, &mut buf)));
            assert_eq!("failed to wake", message);
        }
        assert_eq!(None, result.get());
    }
    // Would have written to the buffer which does not exist anymore
    assert_eq!(Some(Poll::Ready(Err(Closed))), result.get());
}
