//! What compiles, and which traits are implemented.
#![cfg(feature = "futures-io")]

use std::io;
use std::pin::pin;

use async_read::{Producer, Reader, Writer};
use futures::executor::block_on;
use futures::{AsyncRead, AsyncReadExt, AsyncWriteExt};

fn assert_send<T: Send>(value: T) -> T {
    value
}
fn assert_sync<T: Sync>(value: T) -> T {
    value
}
fn assert_static<T: 'static>(value: T) -> T {
    value
}
fn assert_unpin<T: Unpin>(_: &T) {}

fn read_all(reader: impl AsyncRead) -> Vec<u8> {
    block_on(async {
        let mut content = Vec::new();
        pin!(reader).read_to_end(&mut content).await?;
        io::Result::Ok(content)
    })
    .unwrap()
}

#[test]
fn async_closure() {
    let reader = Reader::new(async |mut w| {
        w.write_all(b"abc").await?;
        io::Result::Ok(())
    });
    assert_eq!(b"abc", &read_all(reader)[..]);
}

#[test]
fn async_closure_with_return_type() {
    let reader = Reader::new(async |mut w| -> io::Result<()> {
        w.write_all(b"abc").await?;
        Ok(())
    });
    assert_eq!(b"abc", &read_all(reader)[..]);
}

#[test]
fn async_fn() {
    async fn produce(mut w: Writer<'_>) -> io::Result<()> {
        w.write_all(b"abc").await
    }
    assert_eq!(b"abc", &read_all(Reader::new(produce))[..]);
}

#[test]
fn generic_async_fn_has_to_be_wrapped() {
    async fn produce<W: futures::AsyncWrite + Unpin>(mut w: W) -> io::Result<()> {
        w.write_all(b"abc").await
    }
    let reader = Reader::new(async |w| produce(w).await);
    assert_eq!(b"abc", &read_all(reader)[..]);
}

#[test]
fn writer_is_passed_by_value_and_by_reference() {
    async fn by_value(mut w: Writer<'_>) -> io::Result<()> {
        w.write_all(b"c").await
    }
    async fn by_reference(w: &mut Writer<'_>) -> io::Result<()> {
        w.write_all(b"b").await
    }
    let reader = Reader::new(async |mut w| {
        w.write_all(b"a").await?;
        by_reference(&mut w).await?;
        by_value(w).await
    });
    assert_eq!(b"abc", &read_all(reader)[..]);
}

#[test]
fn reader_is_named_by_its_producer_and_error() {
    struct Body<F, E>
    where
        F: for<'w> Producer<'w, E>,
    {
        reader: Reader<F, E>,
    }
    fn body() -> Body<impl for<'w> Producer<'w, io::Error>, io::Error> {
        Body {
            reader: Reader::new(async |mut w| {
                w.write_all(b"abc").await?;
                io::Result::Ok(())
            }),
        }
    }
    assert_eq!(b"abc", &read_all(body().reader)[..]);
}

#[test]
fn reader_is_send_sync_and_static() {
    let content = b"abc".to_vec();
    let reader = Reader::new(async move |mut w| {
        w.write_all(&content).await?;
        futures::future::ready(()).await;
        w.write_all(&content).await?;
        io::Result::Ok(())
    });
    let reader = assert_send(assert_sync(assert_static(reader)));
    assert_eq!(b"abcabc", &read_all(reader)[..]);
}

#[test]
fn reader_which_borrows_is_send_within_a_future() {
    let future = assert_send(async {
        let content = b"abc".to_vec();
        let content = &content;
        let reader = Reader::new(async |mut w| {
            w.write_all(content).await?;
            futures::future::ready(()).await;
            w.write_all(content).await?;
            io::Result::Ok(())
        });
        let mut read = Vec::new();
        pin!(reader).read_to_end(&mut read).await?;
        io::Result::Ok(read)
    });
    assert_eq!(b"abcabc", &block_on(future).unwrap()[..]);
}

/// The reason for the writer to be `Sync`: Without it, this reader is not `Send`.
#[test]
fn reader_is_send_if_its_future_shares_the_writer() {
    struct Archive<'w>(Writer<'w>);
    async fn inspect(_: &Archive<'_>) {
        futures::future::ready(()).await;
    }
    let reader = assert_send(Reader::new(async |w| {
        let mut archive = Archive(w);
        inspect(&archive).await;
        archive.0.write_all(b"abc").await?;
        io::Result::Ok(())
    }));
    assert_eq!(b"abc", &read_all(reader)[..]);
}

#[test]
fn reader_which_is_not_send_is_sync() {
    // That it isn't `Send` is checked by an example in `src/lib.rs`
    let rc = std::rc::Rc::new(*b"abc");
    let reader = assert_sync(Reader::new(async move |mut w| {
        w.write_all(&*rc).await?;
        io::Result::Ok(())
    }));
    assert_eq!(b"abc", &read_all(reader)[..]);
}

#[test]
fn writer_is_unpin_send_and_sync() {
    let reader = Reader::new(async |w| {
        assert_unpin(&w);
        let _ = assert_send(assert_sync(w));
        io::Result::Ok(())
    });
    assert_eq!(b"", &read_all(reader)[..]);
}
