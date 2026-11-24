//! That whoever has to continue is polled again
#![cfg(feature = "futures-io")]

mod util;

use std::cell::Cell;
use std::future::poll_fn;
use std::io;
use std::pin::pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_read::Reader;
use futures::future::{FutureExt, LocalBoxFuture};
use futures::stream::{FuturesUnordered, StreamExt};
use futures::{AsyncReadExt, AsyncWriteExt};
use util::{close, counting_waker, pattern, read, read_chunks, strings, yield_now};

fn len() -> usize {
    if cfg!(miri) { 100 } else { 10_000 }
}

/// `FuturesUnordered` only polls those of its futures again which were woken through the waker
/// it has passed to them.
#[test]
fn writer_within_futures_unordered() {
    let content = pattern(len());
    let mut reader = pin!(Reader::new(async |mut w| {
        let mut tasks = FuturesUnordered::<LocalBoxFuture<'_, io::Result<()>>>::new();
        tasks.push(Box::pin(w.write_all(&content)));
        tasks.push(Box::pin(async {
            for _ in 0..3 {
                yield_now().await;
            }
            Ok(())
        }));
        while let Some(result) = tasks.next().await {
            result?;
        }
        io::Result::Ok(())
    }));
    assert_eq!(content, read_chunks(&mut reader, 7).unwrap().concat());
}

#[test]
fn closing_wakes_writer_within_futures_unordered() {
    let content = pattern(100);
    let mut reader = pin!(Reader::new(async |mut w| {
        let mut tasks = FuturesUnordered::<LocalBoxFuture<'_, io::Result<()>>>::new();
        tasks.push(Box::pin(w.write_all(&content)));
        while let Some(result) = tasks.next().await {
            result?;
        }
        io::Result::Ok(())
    }));
    assert_eq!(7, read(&mut reader, 7).unwrap().len());
    let error = close(&mut reader).unwrap_err();
    assert_eq!(io::ErrorKind::BrokenPipe, error.kind());
}

#[test]
fn flush_within_futures_unordered() {
    let mut reader = pin!(Reader::new(async |mut w| {
        let mut tasks = FuturesUnordered::<LocalBoxFuture<'_, io::Result<()>>>::new();
        tasks.push(Box::pin(async {
            w.write_all(b"ab").await?;
            w.flush().await?;
            w.write_all(b"cd").await?;
            w.flush().await
        }));
        while let Some(result) = tasks.next().await {
            result?;
        }
        io::Result::Ok(())
    }));
    assert_eq!(
        vec!["ab", "cd"],
        strings(read_chunks(&mut reader, 8).unwrap())
    );
}

#[test]
fn writer_next_to_other_futures() {
    let content = pattern(len());
    let mut reader = pin!(Reader::new(async |mut w| {
        let (written, ()) = futures::join!(w.write_all(&content), async {
            for _ in 0..5 {
                yield_now().await;
            }
        });
        written
    }));
    assert_eq!(content, read_chunks(&mut reader, 3).unwrap().concat());
}

#[test]
fn writes_which_are_dropped_while_pending() {
    let content = pattern(len());
    let ticks = Cell::new(0);
    let mut reader = pin!(Reader::new(async |mut w| {
        let mut remaining = &content[..];
        while !remaining.is_empty() {
            let mut tick = pin!(yield_now().fuse());
            futures::select_biased! {
                () = tick => ticks.set(ticks.get() + 1),
                written = w.write(remaining).fuse() => remaining = &remaining[written?..],
            }
        }
        io::Result::Ok(())
    }));
    assert_eq!(content, read_chunks(&mut reader, 5).unwrap().concat());
    assert!(ticks.get() > 0);
}

/// The common case does not need the waker at all
#[test]
fn waker_of_the_reader_is_neither_kept_nor_woken() {
    let mut reader = pin!(Reader::new(async |mut w| {
        w.write_all(b"abcdefgh").await?;
        io::Result::Ok(())
    }));
    let mut buf = [0; 4];
    let (first, first_wakeups) = counting_waker();
    let (second, second_wakeups) = counting_waker();

    let mut cx = Context::from_waker(&first);
    assert!(matches!(
        reader.as_mut().poll_read(&mut cx, &mut buf),
        Poll::Ready(Ok(4))
    ));
    // Whoever polls the reader does not have to stay the same
    let mut cx = Context::from_waker(&second);
    assert!(matches!(
        reader.as_mut().poll_read(&mut cx, &mut buf),
        Poll::Ready(Ok(4))
    ));
    let mut cx = Context::from_waker(&first);
    assert!(matches!(
        reader.as_mut().poll_read(&mut cx, &mut buf),
        Poll::Ready(Ok(0))
    ));

    assert_eq!((0, 0), (first_wakeups.count(), second_wakeups.count()));
    // Only the waker itself and `*_wakeups`
    assert_eq!(2, Arc::strong_count(&first_wakeups));
    assert_eq!(2, Arc::strong_count(&second_wakeups));
}

/// Like whatever polls its futures with a waker of its own
#[test]
fn waker_of_the_writer_is_woken_when_there_is_room_again() {
    let (inner, inner_wakeups) = counting_waker();
    let mut reader = pin!(Reader::new(async |mut w| {
        let mut remaining: &[u8] = b"abcdefghi";
        poll_fn(|_| {
            let mut cx = Context::from_waker(&inner);
            while !remaining.is_empty() {
                match w.poll_write(&mut cx, remaining) {
                    Poll::Ready(Ok(written)) => remaining = &remaining[written..],
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Pending => {
                        // Only the most recent waker is kept, and it is not cloned again
                        assert!(w.poll_write(&mut cx, remaining).is_pending());
                        return Poll::Pending;
                    }
                }
            }
            Poll::Ready(Ok(()))
        })
        .await?;
        io::Result::Ok(())
    }));
    let (outer, outer_wakeups) = counting_waker();
    let mut cx = Context::from_waker(&outer);
    let mut buf = [0; 4];

    for (expected, wakeups) in [(b"abcd", 0), (b"efgh", 1)] {
        assert!(matches!(
            reader.as_mut().poll_read(&mut cx, &mut buf),
            Poll::Ready(Ok(4))
        ));
        assert_eq!(expected, &buf);
        // Only woken once there is room again for what did not fit before
        assert_eq!(wakeups, inner_wakeups.count());
        // The waker, `inner_wakeups` and the one which is kept for what does not fit now
        assert_eq!(3, Arc::strong_count(&inner_wakeups));
    }
    assert!(matches!(
        reader.as_mut().poll_read(&mut cx, &mut buf),
        Poll::Ready(Ok(1))
    ));
    assert_eq!(2, inner_wakeups.count());
    assert_eq!(2, Arc::strong_count(&inner_wakeups));
    assert_eq!(0, outer_wakeups.count());
}

#[test]
fn waker_of_the_writer_is_dropped_when_the_future_completes() {
    let (inner, inner_wakeups) = counting_waker();
    let mut reader = pin!(Reader::new(async |mut w| {
        w.write_all(b"abcd").await?;
        // Waits for room, but the future does not wait for it
        let mut cx = Context::from_waker(&inner);
        assert!(w.poll_write(&mut cx, b"e").is_pending());
        assert_eq!(3, Arc::strong_count(&inner_wakeups));
        io::Result::Ok(())
    }));
    assert_eq!(b"abcd", &read(&mut reader, 4).unwrap()[..]);
    assert_eq!(2, Arc::strong_count(&inner_wakeups));
    assert_eq!(0, inner_wakeups.count());
}

#[test]
fn nothing_is_lost_if_a_read_is_dropped_while_pending() {
    let (sender, receiver) = futures::channel::oneshot::channel::<()>();
    let mut reader = pin!(Reader::new(async |mut w| {
        w.write_all(b"ab").await?;
        receiver.await.unwrap();
        w.write_all(b"cd").await?;
        io::Result::Ok(())
    }));
    let mut buf = [0; 8];
    let mut read = |buf: &mut [u8]| reader.read(buf).now_or_never().map(Result::unwrap);

    assert_eq!(Some(2), read(&mut buf));
    assert_eq!(None, read(&mut buf));
    assert_eq!(None, read(&mut buf));
    sender.send(()).unwrap();
    assert_eq!(Some(2), read(&mut buf));
    assert_eq!(b"cd", &buf[..2]);
    assert_eq!(Some(0), read(&mut buf));
}
