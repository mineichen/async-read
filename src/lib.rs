#![no_std]
#![warn(missing_docs)]
// The examples need `futures-io`. Without it, `tests/core.rs` covers what is left.
#![cfg_attr(feature = "futures-io", doc = include_str!("../README.md"))]
#![cfg_attr(
    not(feature = "futures-io"),
    doc = "Read what a future writes, see [`Reader`]. The documentation is written for the \
           `futures-io` feature."
)]
#![cfg_attr(not(feature = "futures-io"), allow(rustdoc::broken_intra_doc_links))]

#[cfg(feature = "futures-io")]
extern crate std;

#[cfg(feature = "futures-io")]
mod futures_io;
mod reader;
mod shared;
mod writer;

pub use reader::{Producer, Reader};
pub use writer::{Closed, Writer};

/// What must not compile. Each example is followed by the same code without the part which is
/// rejected, to make sure that it is rejected for that reason.
///
/// # The writer cannot leave the future
///
/// Into a variable outside of it:
/// ```compile_fail
/// use std::io;
///
/// let mut kept = None;
/// let reader = async_read::Reader::new(async |writer| {
///     kept = Some(writer);
///     io::Result::Ok(())
/// });
/// ```
/// ```
/// use std::io;
///
/// let mut kept = None;
/// let reader = async_read::Reader::new(async |writer| {
///     kept = Some(1);
///     drop(writer);
///     io::Result::Ok(())
/// });
/// ```
///
/// Through a channel:
/// ```compile_fail
/// use std::io;
///
/// let (sender, receiver) = std::sync::mpsc::channel();
/// let reader = async_read::Reader::new(async move |writer| {
///     sender.send(writer).unwrap();
///     io::Result::Ok(())
/// });
/// ```
/// ```
/// use std::io;
///
/// let (sender, receiver) = std::sync::mpsc::channel();
/// let reader = async_read::Reader::new(async move |writer| {
///     sender.send(1).unwrap();
///     drop(writer);
///     io::Result::Ok(())
/// });
/// # drop(receiver);
/// ```
///
/// To a thread which outlives it:
/// ```compile_fail,E0521
/// use std::io;
///
/// let reader = async_read::Reader::new(async |writer| {
///     std::thread::spawn(move || drop(writer));
///     io::Result::Ok(())
/// });
/// ```
/// ```
/// use std::io;
///
/// let reader = async_read::Reader::new(async |writer| {
///     std::thread::spawn(move || drop(1));
///     drop(writer);
///     io::Result::Ok(())
/// });
/// ```
///
/// As the error:
/// ```compile_fail
/// let reader = async_read::Reader::new(async |writer| Err(writer));
/// ```
/// ```
/// let reader = async_read::Reader::new(async |writer| {
///     drop(writer);
///     Err(1)
/// });
/// ```
///
/// As the payload of a panic:
/// ```compile_fail,E0521
/// use std::io;
///
/// let reader = async_read::Reader::new(async |writer| {
///     if false {
///         std::panic::panic_any(writer);
///     }
///     io::Result::Ok(())
/// });
/// ```
/// ```
/// use std::io;
///
/// let reader = async_read::Reader::new(async |writer| {
///     if false {
///         std::panic::panic_any(1);
///     }
///     drop(writer);
///     io::Result::Ok(())
/// });
/// ```
///
/// By requiring a lifetime for it:
/// ```compile_fail
/// use std::io;
///
/// async fn produce(writer: async_read::Writer<'static>) -> io::Result<()> {
///     Ok(())
/// }
/// let reader = async_read::Reader::new(produce);
/// ```
/// ```
/// use std::io;
///
/// async fn produce(writer: async_read::Writer<'_>) -> io::Result<()> {
///     Ok(())
/// }
/// let reader = async_read::Reader::new(produce);
/// ```
///
/// In exchange for the writer of a reader which lives longer:
/// ```compile_fail
/// use std::io;
///
/// let outer = async_read::Reader::new(async |mut outer| {
///     let inner = async_read::Reader::new(async |mut inner| {
///         std::mem::swap(&mut outer, &mut inner);
///         io::Result::Ok(())
///     });
///     io::Result::Ok(())
/// });
/// ```
/// ```
/// use std::io;
///
/// let outer = async_read::Reader::new(async |mut outer| {
///     let inner = async_read::Reader::new(async |mut inner| {
///         drop((&mut outer, &mut inner));
///         io::Result::Ok(())
///     });
///     io::Result::Ok(())
/// });
/// ```
///
/// # The producer
///
/// A closure which returns a future is not accepted, it has to be an async closure:
/// ```compile_fail,E0282
/// use std::io;
///
/// use futures::AsyncWriteExt;
///
/// let reader = async_read::Reader::new(|mut writer| async move {
///     writer.write_all(b"abc").await?;
///     io::Result::Ok(())
/// });
/// ```
/// ```
/// use std::io;
///
/// use futures::AsyncWriteExt;
///
/// let reader = async_read::Reader::new(async |mut writer| {
///     writer.write_all(b"abc").await?;
///     io::Result::Ok(())
/// });
/// ```
///
/// # The reader
///
/// It has to be pinned:
/// ```compile_fail,E0277
/// use std::io;
///
/// fn assert<T: Unpin>(_: T) {}
/// assert(async_read::Reader::new(async |writer| io::Result::Ok(())));
/// ```
/// ```
/// use std::io;
///
/// fn assert<T>(_: T) {}
/// assert(async_read::Reader::new(async |writer| io::Result::Ok(())));
/// ```
///
/// It is not `Send` if the future is not:
/// ```compile_fail,E0277
/// use std::io;
/// use std::rc::Rc;
///
/// fn assert<T: Send>(_: T) {}
/// let shared = Rc::new(1);
/// assert(async_read::Reader::new(async move |writer| {
///     let _shared = &shared;
///     io::Result::Ok(())
/// }));
/// ```
/// ```
/// use std::io;
/// use std::sync::Arc;
///
/// fn assert<T: Send>(_: T) {}
/// let shared = Arc::new(1);
/// assert(async_read::Reader::new(async move |writer| {
///     let _shared = &shared;
///     io::Result::Ok(())
/// }));
/// ```
#[cfg(all(doctest, feature = "futures-io"))]
mod must_not_compile {}
