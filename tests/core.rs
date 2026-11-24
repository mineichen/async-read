//! What is available without any feature
mod util;

use std::future::poll_fn;
use std::pin::pin;

use async_read::{Closed, Reader, Writer};
use util::{close, read, read_chunks, strings, write_all, yield_now};

#[derive(Debug, PartialEq)]
enum Error {
    Closed,
    Failed,
}

impl From<Closed> for Error {
    fn from(_: Closed) -> Self {
        Self::Closed
    }
}

#[test]
fn read_what_is_written() {
    let mut reader = pin!(Reader::new(async |mut w| -> Result<(), Error> {
        write_all(&mut w, b"ab").await?;
        assert_eq!(Ok(2), poll_fn(|cx| w.poll_write(cx, b"cd")).await);
        assert_eq!(Ok(0), poll_fn(|cx| w.poll_write(cx, b"")).await);
        yield_now().await;
        write_all(&mut w, b"efghi").await?;
        Ok(())
    }));
    assert_eq!(
        vec!["abcd", "efgh", "i"],
        strings(read_chunks(&mut reader, 4).unwrap())
    );
}

#[test]
fn async_fn_as_producer() {
    async fn produce(mut w: Writer<'_>) -> Result<(), Closed> {
        write_all(&mut w, b"abc").await
    }
    let mut reader = pin!(Reader::new(produce));
    assert_eq!(vec!["abc"], strings(read_chunks(&mut reader, 8).unwrap()));
}

#[test]
fn error_of_the_future_is_returned_as_it_is() {
    let mut reader = pin!(Reader::new(async |mut w| -> Result<(), Error> {
        write_all(&mut w, b"abc").await?;
        Err(Error::Failed)
    }));
    assert_eq!(Ok(b"abc".to_vec()), read(&mut reader, 8));
    assert_eq!(Err(Error::Failed), read(&mut reader, 8));
    assert_eq!(Ok(Vec::new()), read(&mut reader, 8));
}

#[test]
fn closing_fails_writes() {
    let mut reader = pin!(Reader::new(async |mut w| -> Result<(), Error> {
        loop {
            write_all(&mut w, b"abc").await?;
        }
    }));
    assert_eq!(Ok(b"abca".to_vec()), read(&mut reader, 4));
    assert_eq!(Err(Error::Closed), close(&mut reader));
    assert_eq!(Ok(()), close(&mut reader));
}

/// Besides the future, a reader is five pointers, and one more for its state.
#[test]
fn size_of_a_reader() {
    use std::future::Future;
    use std::mem::{size_of, size_of_val};
    use std::pin::Pin;
    use std::task::{Context, Poll};

    /// Nothing the state of the reader could be squeezed into
    struct Plain {
        _payload: [usize; 3],
    }
    impl Future for Plain {
        type Output = Result<(), Error>;
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Ready(Ok(()))
        }
    }
    fn produce(_: Writer<'_>) -> Plain {
        Plain { _payload: [0; 3] }
    }

    let reader = Reader::new(produce);
    assert_eq!(
        size_of::<Plain>() + 6 * size_of::<usize>(),
        size_of_val(&reader)
    );
    assert_eq!(size_of::<usize>(), size_of::<Writer<'_>>());
}
