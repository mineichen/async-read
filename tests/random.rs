//! Random sequences of writes and reads have to transfer everything unchanged
#![cfg(feature = "futures-io")]

mod util;

use std::future::poll_fn;
use std::io;
use std::pin::pin;

use async_read::Reader;
use futures::AsyncWriteExt;
use util::{Random, drive, pattern, yield_now};

#[test]
fn random_writes_and_reads() {
    let (seeds, len) = if cfg!(miri) { (5, 300) } else { (500, 3000) };
    for seed in 0..seeds {
        run(seed, len);
    }
}

fn run(seed: u64, len: usize) {
    let content = pattern(len);
    let mut random = Random::new(seed);
    let mut reader = pin!(Reader::new(async |mut w| {
        let mut remaining = &content[..];
        while !remaining.is_empty() {
            let max = remaining.len().min(1 + random.below(40));
            match random.below(6) {
                0 => {
                    let written = w.write(&remaining[..max]).await?;
                    remaining = &remaining[written..];
                }
                1 => {
                    w.write_all(&remaining[..max]).await?;
                    remaining = &remaining[max..];
                }
                2 => {
                    let written = poll_fn(|cx| w.poll_write(cx, &remaining[..max])).await?;
                    remaining = &remaining[written..];
                }
                3 => {
                    let (first, second) = remaining[..max].split_at(random.below(max + 1));
                    let bufs = [
                        io::IoSlice::new(first),
                        io::IoSlice::new(&[]),
                        io::IoSlice::new(second),
                    ];
                    let written = w.write_vectored(&bufs).await?;
                    remaining = &remaining[written..];
                }
                4 => w.flush().await?,
                _ => yield_now().await,
            }
        }
        io::Result::Ok(())
    }));

    let mut random = Random::new(!seed);
    let mut read = Vec::new();
    loop {
        let mut buf = vec![0xAA; random.below(50)];
        let len = drive(|cx| reader.as_mut().poll_read(cx, &mut buf)).unwrap();
        read.extend_from_slice(&buf[..len]);
        // Reading nothing into something is the end
        if len == 0 && !buf.is_empty() {
            break;
        }
    }
    assert_eq!(content, read, "seed {seed}");
}
