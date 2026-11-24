//! What is read for what is written
#![cfg(feature = "futures-io")]

mod util;

use std::cell::Cell;
use std::io;
use std::pin::pin;
use std::task::{Context, Poll};

use async_read::Reader;
use futures::executor::block_on;
use futures::{AsyncReadExt, AsyncWriteExt};
use util::{pattern, read, read_chunks, strings, yield_now};

#[test]
fn nothing_written_is_the_end() {
    let mut reader = pin!(Reader::new(async |_w| io::Result::Ok(())));
    assert_eq!(b"", &read(&mut reader, 8).unwrap()[..]);
    // And it stays there
    assert_eq!(b"", &read(&mut reader, 8).unwrap()[..]);
}

#[test]
fn producer_only_runs_when_read() {
    let called = Cell::new(false);
    let mut reader = pin!(Reader::new(async |_w| {
        called.set(true);
        io::Result::Ok(())
    }));
    assert!(!called.get());
    // Nothing can be read into an empty buffer, so there is no reason to do anything
    assert_eq!(b"", &read(&mut reader, 0).unwrap()[..]);
    assert!(!called.get());
    assert_eq!(b"", &read(&mut reader, 1).unwrap()[..]);
    assert!(called.get());
}

#[test]
fn writes_are_read_at_once() {
    let mut reader = pin!(Reader::new(async |mut w| {
        w.write_all(b"ab").await?;
        w.write_all(b"").await?;
        w.write_all(b"c").await?;
        io::Result::Ok(())
    }));
    assert_eq!(vec!["abc"], strings(read_chunks(&mut reader, 8).unwrap()));
}

#[test]
fn write_is_split_if_it_does_not_fit() {
    let mut reader = pin!(Reader::new(async |mut w| {
        assert_eq!(4, w.write(b"abcdefghij").await?);
        assert_eq!(4, w.write(b"efghij").await?);
        assert_eq!(2, w.write(b"ij").await?);
        assert_eq!(0, w.write(b"").await?);
        io::Result::Ok(())
    }));
    assert_eq!(
        vec!["abcd", "efgh", "ij"],
        strings(read_chunks(&mut reader, 4).unwrap())
    );
}

#[test]
fn read_byte_by_byte() {
    let mut reader = pin!(Reader::new(async |mut w| {
        w.write_all(b"abc").await?;
        io::Result::Ok(())
    }));
    assert_eq!(
        vec!["a", "b", "c"],
        strings(read_chunks(&mut reader, 1).unwrap())
    );
}

#[test]
fn buffers_of_different_sizes() {
    let mut reader = pin!(Reader::new(async |mut w| {
        w.write_all(b"abcdefgh").await?;
        io::Result::Ok(())
    }));
    assert_eq!(b"ab", &read(&mut reader, 2).unwrap()[..]);
    assert_eq!(b"", &read(&mut reader, 0).unwrap()[..]);
    assert_eq!(b"c", &read(&mut reader, 1).unwrap()[..]);
    assert_eq!(b"defgh", &read(&mut reader, 100).unwrap()[..]);
    assert_eq!(b"", &read(&mut reader, 100).unwrap()[..]);
}

#[test]
fn what_is_written_is_read_when_the_future_is_pending() {
    let mut reader = pin!(Reader::new(async |mut w| {
        w.write_all(b"ab").await?;
        yield_now().await;
        w.write_all(b"cd").await?;
        yield_now().await;
        yield_now().await;
        w.write_all(b"ef").await?;
        io::Result::Ok(())
    }));
    assert_eq!(
        vec!["ab", "cd", "ef"],
        strings(read_chunks(&mut reader, 8).unwrap())
    );
}

#[test]
fn what_is_written_is_read_when_the_writer_is_flushed() {
    let mut reader = pin!(Reader::new(async |mut w| {
        // Nothing was written yet
        w.flush().await?;
        w.write_all(b"ab").await?;
        w.flush().await?;
        // Nothing was written since
        w.flush().await?;
        w.write_all(b"cd").await?;
        w.write_all(b"ef").await?;
        w.flush().await?;
        w.write_all(b"g").await?;
        io::Result::Ok(())
    }));
    assert_eq!(
        vec!["ab", "cdef", "g"],
        strings(read_chunks(&mut reader, 8).unwrap())
    );
}

#[test]
fn flush_of_a_full_buffer() {
    let mut reader = pin!(Reader::new(async |mut w| {
        w.write_all(b"abcd").await?;
        w.flush().await?;
        w.write_all(b"efgh").await?;
        w.flush().await
    }));
    assert_eq!(
        vec!["abcd", "efgh"],
        strings(read_chunks(&mut reader, 4).unwrap())
    );
}

#[test]
fn close_of_the_writer_does_nothing() {
    let mut reader = pin!(Reader::new(async |mut w| {
        w.write_all(b"ab").await?;
        w.close().await?;
        w.write_all(b"cd").await?;
        io::Result::Ok(())
    }));
    assert_eq!(vec!["abcd"], strings(read_chunks(&mut reader, 8).unwrap()));
}

#[test]
fn error_without_data() {
    let mut reader = pin!(Reader::new(async |_w| Err(io::Error::other("failed"))));
    assert_eq!("failed", read(&mut reader, 8).unwrap_err().to_string());
    // It is at its end afterwards
    assert_eq!(b"", &read(&mut reader, 8).unwrap()[..]);
}

#[test]
fn error_after_data() {
    let mut reader = pin!(Reader::new(async |mut w| {
        w.write_all(b"abc").await?;
        Err(io::Error::other("failed"))
    }));
    assert_eq!(b"", &read(&mut reader, 0).unwrap()[..]);
    assert_eq!(b"abc", &read(&mut reader, 8).unwrap()[..]);
    // The error isn't lost if it can't be returned yet
    assert_eq!(b"", &read(&mut reader, 0).unwrap()[..]);
    assert_eq!("failed", read(&mut reader, 8).unwrap_err().to_string());
    assert_eq!(b"", &read(&mut reader, 8).unwrap()[..]);
}

#[test]
fn error_which_is_not_returned_is_dropped() {
    struct Error<'a>(&'a Cell<usize>);
    impl Drop for Error<'_> {
        fn drop(&mut self) {
            self.0.set(self.0.get() + 1);
        }
    }

    let dropped = Cell::new(0);
    {
        let mut reader = pin!(Reader::new(async |mut w| {
            w.write_all(b"abc").await.unwrap();
            Err(Error(&dropped))
        }));
        assert_eq!(b"abc", &read(&mut reader, 8).ok().unwrap()[..]);
        assert_eq!(0, dropped.get());
    }
    assert_eq!(1, dropped.get());

    {
        let mut reader = pin!(Reader::new(async |mut w| {
            w.write_all(b"abc").await.unwrap();
            Err(Error(&dropped))
        }));
        assert_eq!(b"abc", &read(&mut reader, 8).ok().unwrap()[..]);
        drop(read(&mut reader, 8).err().unwrap());
        assert_eq!(2, dropped.get());
        assert_eq!(b"", &read(&mut reader, 8).ok().unwrap()[..]);
    }
    assert_eq!(2, dropped.get());
}

#[test]
fn waits_for_what_the_future_waits_for() {
    let (sender, receiver) = futures::channel::oneshot::channel::<()>();
    let mut reader = pin!(Reader::new(async |mut w| {
        receiver.await.unwrap();
        w.write_all(b"abc").await?;
        io::Result::Ok(())
    }));
    let (waker, wakeups) = util::counting_waker();
    let mut cx = Context::from_waker(&waker);
    let mut buf = [0; 8];

    assert!(reader.as_mut().poll_read(&mut cx, &mut buf).is_pending());
    assert!(reader.as_mut().poll_read(&mut cx, &mut buf).is_pending());
    assert_eq!(0, wakeups.count());
    sender.send(()).unwrap();
    assert_eq!(1, wakeups.count());
    assert!(matches!(
        reader.as_mut().poll_read(&mut cx, &mut buf),
        Poll::Ready(Ok(3))
    ));
    assert_eq!(b"abc", &buf[..3]);
}

#[test]
fn write_vectored_writes_all_buffers() {
    let mut reader = pin!(Reader::new(async |mut w| {
        let bufs = [b"ab" as &[u8], b"", b"cde", b"fg"].map(io::IoSlice::new);
        assert_eq!(4, w.write_vectored(&bufs).await?);
        assert_eq!(4, w.write_vectored(&bufs).await?);
        assert_eq!(0, w.write_vectored(&[]).await?);
        assert_eq!(0, w.write_vectored(&[io::IoSlice::new(b"")]).await?);
        assert_eq!(2, w.write_vectored(&bufs[3..]).await?);
        // Exactly as much as there is room for, and then everything again
        assert_eq!(2, w.write_vectored(&bufs[..1]).await?);
        assert_eq!(4, w.write_vectored(&bufs[1..]).await?);
        io::Result::Ok(())
    }));
    assert_eq!(
        vec!["abcd", "abcd", "fgab", "cdef"],
        strings(read_chunks(&mut reader, 4).unwrap())
    );
}

#[test]
fn works_with_the_helpers_of_futures() {
    let content = pattern(if cfg!(miri) { 3_000 } else { 300_000 });
    let reader = Reader::new(async |w| {
        let mut w = futures::io::BufWriter::with_capacity(1000, w);
        for chunk in content.chunks(777) {
            w.write_all(chunk).await?;
        }
        w.flush().await?;
        w.close().await
    });
    let mut copied = Vec::new();
    block_on(futures::io::copy(reader, &mut copied)).unwrap();
    assert_eq!(content, copied);

    let reader = pin!(Reader::new(async |mut w| w.write_all(&content).await));
    let mut reader = futures::io::BufReader::with_capacity(100, reader);
    let mut read = vec![0; content.len()];
    block_on(reader.read_exact(&mut read)).unwrap();
    assert_eq!(content, read);
    assert_eq!(0, block_on(reader.read(&mut read)).unwrap());
}
