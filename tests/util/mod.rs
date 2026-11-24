//! Helpers shared by the tests
#![allow(dead_code)]

use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};

use async_read::{Closed, Producer, Reader, Writer};

/// Counts how often its waker is woken.
#[derive(Default)]
pub struct Wakeups(AtomicUsize);

impl Wakeups {
    pub fn count(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }
}

impl Wake for Wakeups {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

pub fn counting_waker() -> (Waker, Arc<Wakeups>) {
    let wakeups = Arc::new(Wakeups::default());
    (Waker::from(wakeups.clone()), wakeups)
}

/// Polls until it is ready.
///
/// Fails if it is pending without having been woken in the meantime: nobody would ever poll it
/// again. So this only works for what does not wait for something from the outside.
pub fn drive<T>(mut poll: impl FnMut(&mut Context<'_>) -> Poll<T>) -> T {
    let (waker, wakeups) = counting_waker();
    let mut cx = Context::from_waker(&waker);
    loop {
        let before = wakeups.count();
        if let Poll::Ready(value) = poll(&mut cx) {
            return value;
        }
        assert!(wakeups.count() > before, "pending, but never woken");
    }
}

/// Polls once with a waker that does nothing.
pub fn poll_once<T>(poll: impl FnOnce(&mut Context<'_>) -> Poll<T>) -> Poll<T> {
    poll(&mut Context::from_waker(Waker::noop()))
}

/// Is pending once, but asks to be polled again right away.
pub fn yield_now() -> impl Future<Output = ()> {
    let mut yielded = false;
    poll_fn(move |cx| {
        if yielded {
            Poll::Ready(())
        } else {
            yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    })
}

/// Writes all of `buf`, like `AsyncWriteExt::write_all`, but without `futures-io`.
pub async fn write_all(writer: &mut Writer<'_>, mut buf: &[u8]) -> Result<(), Closed> {
    while !buf.is_empty() {
        let written = poll_fn(|cx| writer.poll_write(cx, buf)).await?;
        buf = &buf[written..];
    }
    Ok(())
}

/// Reads once into a buffer of the given length.
pub fn read<F, E>(reader: &mut Pin<&mut Reader<F, E>>, buf_len: usize) -> Result<Vec<u8>, E>
where
    F: Producer<E>,
{
    let mut buf = vec![0; buf_len];
    let len = drive(|cx| reader.as_mut().poll_read(cx, &mut buf))?;
    buf.truncate(len);
    Ok(buf)
}

/// Reads with a buffer of the given length until the end, and returns what each read got.
pub fn read_chunks<F, E>(
    reader: &mut Pin<&mut Reader<F, E>>,
    buf_len: usize,
) -> Result<Vec<Vec<u8>>, E>
where
    F: Producer<E>,
{
    let mut chunks = Vec::new();
    loop {
        let chunk = read(reader, buf_len)?;
        if chunk.is_empty() {
            return Ok(chunks);
        }
        chunks.push(chunk);
    }
}

pub fn close<F, E>(reader: &mut Pin<&mut Reader<F, E>>) -> Result<(), E>
where
    F: Producer<E>,
{
    drive(|cx| reader.as_mut().poll_close(cx))
}

/// `[b"ab", b"c"]` as `["ab", "c"]`, which is easier to compare
pub fn strings(chunks: Vec<Vec<u8>>) -> Vec<String> {
    chunks
        .into_iter()
        .map(|chunk| String::from_utf8(chunk).unwrap())
        .collect()
}

/// The first `len` bytes of a sequence in which a mistake of any kind is obvious
pub fn pattern(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

/// A small deterministic random number generator
pub struct Random(u64);

impl Random {
    pub fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    pub fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    /// A number in `0..end`
    pub fn below(&mut self, end: usize) -> usize {
        (self.next() % end as u64) as usize
    }
}
