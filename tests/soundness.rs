//! What the compiler accepts, and what therefore has to be fine when it runs. Meant for Miri.
#![cfg(feature = "futures-io")]

mod util;

use std::future::{Future, poll_fn};
use std::io;
use std::pin::{Pin, pin};
use std::task::{Context, Poll};

use async_read::{Reader, Writer};
use futures::{AsyncReadExt, AsyncWriteExt};
use util::{drive, pattern, poll_once, read, read_chunks, strings, yield_now};

/// A writer cannot be swapped with the one of a reader which lives longer (see the examples
/// which must not compile in `src/lib.rs`), unless both are treated as living only as long as
/// the one which lives shorter. That is harmless: both are still alive for that long.
#[test]
fn nested_readers_with_swapped_writers() {
    let mut outer = pin!(Reader::new(async |outer| {
        let inner = Reader::new(async move |inner| {
            let (mut inner, mut outer) = (outer, inner);
            std::mem::swap(&mut inner, &mut outer);
            inner.write_all(b"in").await?;
            outer.write_all(b"out").await?;
            yield_now().await;
            inner.write_all(b"IN").await?;
            outer.write_all(b"OUT").await?;
            io::Result::Ok(())
        });
        let mut inner = pin!(inner);
        let mut buf = [0; 8];
        let mut chunks = Vec::new();
        loop {
            let len = inner.read(&mut buf).await?;
            if len == 0 {
                break;
            }
            chunks.push(buf[..len].to_vec());
        }
        assert_eq!(vec!["in", "IN"], strings(chunks));
        io::Result::Ok(())
    }));
    assert_eq!(vec!["outOUT"], strings(read_chunks(&mut outer, 8).unwrap()));
}

#[test]
fn reader_within_the_future_of_a_reader() {
    let content = pattern(100);
    let mut outer = pin!(Reader::new(async |mut outer| {
        let mut inner = pin!(Reader::new(async |mut inner| {
            for chunk in content.chunks(7) {
                inner.write_all(chunk).await?;
                yield_now().await;
            }
            io::Result::Ok(())
        }));
        let mut buf = [0; 5];
        loop {
            let len = inner.read(&mut buf).await?;
            if len == 0 {
                break io::Result::Ok(());
            }
            outer.write_all(&buf[..len]).await?;
        }
    }));
    assert_eq!(content, read_chunks(&mut outer, 3).unwrap().concat());
}

/// The writer is `Send`, so it can be used by a thread the future waits for
#[test]
fn writer_on_another_thread() {
    let mut reader = pin!(Reader::new(async |mut w| {
        let written = std::thread::scope(|scope| {
            scope
                .spawn(|| poll_once(|cx| w.poll_write(cx, b"abc")))
                .join()
                .unwrap()
        });
        assert_eq!(Poll::Ready(Ok(3)), written);
        w.write_all(b"def").await?;
        io::Result::Ok(())
    }));
    assert_eq!(b"abcdef", &read(&mut reader, 8).unwrap()[..]);
}

#[test]
fn reader_moved_to_another_thread_while_it_is_read() {
    let content = pattern(100);
    let expected = content.clone();
    let mut reader = Box::pin(Reader::new(async move |mut w| {
        for chunk in content.chunks(11) {
            w.write_all(chunk).await?;
            yield_now().await;
        }
        io::Result::Ok(())
    }));
    let mut read = read(&mut reader.as_mut(), 8).unwrap();
    let rest = std::thread::spawn(move || read_chunks(&mut reader.as_mut(), 8).unwrap().concat())
        .join()
        .unwrap();
    read.extend(rest);
    assert_eq!(expected, read);
}

#[test]
fn reader_shared_with_another_thread() {
    let mut reader = pin!(Reader::new(async |mut w| {
        w.write_all(b"abc").await?;
        io::Result::Ok(())
    }));
    assert_eq!(b"ab", &read(&mut reader, 2).unwrap()[..]);
    let shared = &reader;
    let debug = std::thread::scope(|scope| scope.spawn(|| format!("{shared:?}")).join().unwrap());
    assert_eq!("Reader { .. }", debug);
    assert_eq!(b"c", &read(&mut reader, 2).unwrap()[..]);
}

#[test]
fn reader_moved_before_it_is_pinned() {
    fn create() -> impl futures::AsyncRead {
        let reader = Reader::new(async |mut w| {
            w.write_all(b"abc").await?;
            io::Result::Ok(())
        });
        let boxed = Box::new(reader);
        *boxed
    }
    let reader = create();
    let future = async {
        let mut content = Vec::new();
        pin!(reader).read_to_end(&mut content).await?;
        io::Result::Ok(content)
    };
    // The reader is pinned as a part of this future
    let mut future = Box::pin(future);
    let content = drive(|cx| future.as_mut().poll(cx)).unwrap();
    assert_eq!(b"abc", &content[..]);
}

#[test]
fn reader_dropped_at_every_point() {
    let content = pattern(40);
    for reads in 0..20 {
        let mut reader = pin!(Reader::new(async |mut w| {
            for chunk in content.chunks(6) {
                w.write_all(chunk).await?;
                yield_now().await;
            }
            io::Result::Ok(())
        }));
        for _ in 0..reads {
            let _ = read(&mut reader, 4).unwrap();
        }
    }
}

/// An async block is never `Unpin`, so the reader is not either. This future is, and so is
/// everything else in the reader. It still has to be fine that the writer points into it.
#[test]
fn future_which_is_unpin() {
    struct Chunks<'w> {
        writer: Writer<'w>,
        remaining: &'static [u8],
    }
    impl Future for Chunks<'_> {
        type Output = io::Result<()>;
        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let this = self.get_mut();
            while !this.remaining.is_empty() {
                match this.writer.poll_write(cx, this.remaining) {
                    Poll::Ready(Ok(written)) => this.remaining = &this.remaining[written..],
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e.into())),
                    Poll::Pending => return Poll::Pending,
                }
            }
            Poll::Ready(Ok(()))
        }
    }
    impl Drop for Chunks<'_> {
        fn drop(&mut self) {
            let _ = poll_once(|cx| self.writer.poll_write(cx, b"x"));
        }
    }
    fn produce(writer: Writer<'_>) -> Chunks<'_> {
        fn assert_unpin<T: Unpin>(_: &T) {}
        let chunks = Chunks {
            writer,
            remaining: b"abcdefghij",
        };
        assert_unpin(&chunks);
        chunks
    }

    // On the stack, where the reader is moved from one variable to another before. Unlike
    // the variables of an async block, this future is dropped after the poll which completes
    // it, when the buffer is gone.
    let reader = Reader::new(produce);
    let moved = reader;
    let mut reader = pin!(moved);
    assert_eq!(
        vec!["abc", "def", "ghi", "j"],
        strings(read_chunks(&mut reader, 3).unwrap())
    );

    // On the heap, and dropped before the end
    let mut reader = Box::pin(Reader::new(produce));
    assert_eq!(b"abc", &read(&mut reader.as_mut(), 3).unwrap()[..]);
    assert_eq!(b"def", &read(&mut reader.as_mut(), 3).unwrap()[..]);
    drop(reader);
}

/// The reader of a reader's future can read directly into what the writer would write to, if
/// it is polled by hand.
#[test]
fn reader_polled_from_within_poll_fn() {
    let mut outer = pin!(Reader::new(async |mut outer| {
        let mut inner = pin!(Reader::new(async |mut inner| {
            inner.write_all(b"abcdef").await?;
            io::Result::Ok(())
        }));
        let mut buf = [0; 4];
        loop {
            let len = poll_fn(|cx| inner.as_mut().poll_read(cx, &mut buf)).await?;
            if len == 0 {
                break io::Result::Ok(());
            }
            outer.write_all(&buf[..len]).await?;
        }
    }));
    assert_eq!(
        vec!["abcd", "ef"],
        strings(read_chunks(&mut outer, 4).unwrap())
    );
}

/// Between two polls, safe code can look at the reader through a shared reference: a
/// `Pin<&mut Reader>` dereferences to one. That must not affect a future which borrows from
/// itself, as every async block does which holds a reference across an `.await`.
#[test]
fn reader_referenced_while_its_future_borrows_from_itself() {
    let mut reader = pin!(Reader::new(async |mut w| {
        let mut local = [0; 2];
        let borrowed = &mut local;
        borrowed[0] = b'a';
        w.write_all(&borrowed[..1]).await?;
        yield_now().await;
        borrowed[1] = b'b';
        w.write_all(&borrowed[..]).await?;
        io::Result::Ok(())
    }));
    assert_eq!(b"a", &read(&mut reader, 8).unwrap()[..]);

    let shared: &Reader<_, _> = &reader;
    assert_eq!("Reader { .. }", format!("{shared:?}"));

    assert_eq!(b"ab", &read(&mut reader, 8).unwrap()[..]);
}
