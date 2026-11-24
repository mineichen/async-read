# async-read

Read what a future writes.

`Reader` turns a future that writes to a `Writer` into something to read from. Use it when you
have code that only knows how to write, such as a compressor, an archiver or a serializer, and
an API that wants something to read from, such as the body of a request or a response.

```rust
use std::io;
use std::pin::pin;

use async_read::Reader;
use futures::{AsyncReadExt, AsyncWriteExt};

futures::executor::block_on(async {
    let reader = Reader::new(async |mut writer| {
        writer.write_all(b"Hello ").await?;
        writer.write_all(b"World").await?;
        io::Result::Ok(())
    });

    let mut content = String::new();
    pin!(reader).read_to_string(&mut content).await?;
    assert_eq!("Hello World", content);
    io::Result::Ok(())
})
.unwrap();
```

`Reader` implements `futures::AsyncRead`, and `Writer` implements `futures::AsyncWrite`.

## How it works

Reading polls the future, and the future writes straight into the buffer that is being read
into. There is no buffer in between, nothing is allocated, no task is spawned and nothing is
synchronized.

| When | Then |
|---|---|
| The future is pending | The read returns what the future has written so far |
| The buffer is full | The writer waits for the next read |
| The writer is flushed | The read returns what was written, and the future continues with the next read |
| The future completes | The reader is at its end |
| The future fails | What it wrote is read first, then the read fails with its error |
| The reader is dropped | The future is dropped |

## What to keep in mind

- **The future only runs while the reader is polled.** Whoever reads must not wait for
  something the future only does after it was able to write.
- **Do not block on the writer.** Blocking the thread inside the future until a write
  completes is a deadlock as soon as the reader's buffer is full.
- **A reader must be pinned**, with `pin!` or `Box::pin`, because the future refers to it.
- **The producer must be an async closure, an `async fn` or a `fn` returning a future**, each
  taking a `Writer<'_>`. A closure that returns an async block (`|w| async move { .. }`) is
  not accepted.
- **Flush the writer if what was written should be read right away.** Otherwise it is only
  read once the future is pending or completes, or the buffer is full.
- **Closing the writer does nothing.** The reader only reaches its end when the future
  completes.

## Stopping early

Dropping the reader drops the future. If the future should get the chance to clean up
instead, close the reader with `Reader::poll_close`: from then on every write fails, and the
future is polled until it completes.

```rust
use std::future::poll_fn;
use std::io;
use std::pin::pin;

use async_read::Reader;
use futures::{AsyncReadExt, AsyncWriteExt};

futures::executor::block_on(async {
    let mut reader = pin!(Reader::new(async |mut writer| {
        loop {
            writer.write_all(b"tick ").await?;
        }
    }));

    let mut buf = [0; 5];
    reader.read_exact(&mut buf).await?;

    let result: io::Result<()> = poll_fn(|cx| reader.as_mut().poll_close(cx)).await;
    assert_eq!(io::ErrorKind::BrokenPipe, result.unwrap_err().kind());
    io::Result::Ok(())
})
.unwrap();
```

## As the body of a response

With `tokio-util` (features `compat` and `io`), a reader is a body for `axum`. This example
is not compiled as part of this crate.

```rust,ignore
use std::io;

use async_read::Reader;
use axum::body::Body;
use futures::AsyncWriteExt;
use tokio_util::compat::FuturesAsyncReadCompatExt;
use tokio_util::io::ReaderStream;

fn body(name: String) -> Body {
    let reader = Reader::new(async move |mut writer| {
        // Or pass `writer` to whatever writes to a `futures::AsyncWrite`
        writer.write_all(b"Hello ").await?;
        writer.write_all(name.as_bytes()).await?;
        io::Result::Ok(())
    });
    Body::from_stream(ReaderStream::new(reader.compat()))
}
```

## Without `std`

`futures-io` only provides its traits with `std`. Without the `futures-io` feature, which is
enabled by default, this crate is `no_std` and has no dependencies. `Reader::poll_read` and
`Writer::poll_write` are the same operations, with an error type of your choice:

```rust
use core::future::poll_fn;
use core::pin::pin;

use async_read::{Closed, Reader};

#[derive(Debug, PartialEq)]
enum Error {
    Closed,
    Sensor,
}

impl From<Closed> for Error {
    fn from(_: Closed) -> Self {
        Self::Closed
    }
}

futures::executor::block_on(async {
    let mut reader = pin!(Reader::new(async |mut writer| -> Result<(), Error> {
        let written = poll_fn(|cx| writer.poll_write(cx, b"42")).await?;
        assert_eq!(2, written);
        Err(Error::Sensor)
    }));

    let mut buf = [0; 8];
    let read = poll_fn(|cx| reader.as_mut().poll_read(cx, &mut buf)).await;
    assert_eq!(Ok(2), read);
    assert_eq!(b"42", &buf[..2]);
    let read = poll_fn(|cx| reader.as_mut().poll_read(cx, &mut buf)).await;
    assert_eq!(Err(Error::Sensor), read);
});
```

## Requirements

Rust 1.85, for async closures.

## License

MIT
