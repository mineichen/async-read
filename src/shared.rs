//! The state shared between a [`Reader`](crate::Reader) and its [`Writer`](crate::Writer).
//!
//! The reader *publishes* the buffer it was asked to fill, polls the future, and *withdraws* the
//! buffer again, all within one call. The writer can only write while a buffer is published.
//! Outside of that window every pointer in here is null.
//!
//! [`Shared`] is a field of the pinned reader. The writer lives in the future, which is another
//! field of the same reader, and points to it. That is why everything is in a [`Cell`], and why
//! the reader must not create a `&mut Shared` while the future exists.

use core::cell::Cell;
use core::marker::{PhantomData, PhantomPinned};
use core::ops::Range;
use core::ptr;
use core::task::{Context, Waker};

/// Exempts `T` from the uniqueness of `&mut`, because something points to it while it is pinned.
///
/// The writer keeps pointing to the [`Shared`] of its reader while new `Pin<&mut Reader>` are
/// created. That is only allowed because rustc and Miri do not treat a `&mut T` as unique
/// unless `T` is `Unpin`. The language does not guarantee this, but every self-referential
/// future relies on it, including the ones the compiler generates for `async` blocks.
///
/// `UnsafePinned` (RFC 3467) is the guaranteed replacement. Once it is stable, this becomes a
/// wrapper around it, which is why the marker sits next to the value it is about.
pub(crate) struct Aliased<T> {
    value: T,
    _pin: PhantomPinned,
}

impl<T> Aliased<T> {
    pub(crate) const fn new(value: T) -> Self {
        Self {
            value,
            _pin: PhantomPinned,
        }
    }

    pub(crate) const fn get(&self) -> &T {
        &self.value
    }
}

pub(crate) struct Shared {
    /// Where the next byte is written. Null if nothing can be written.
    cursor: Cell<*mut u8>,
    /// The end of the published buffer.
    end: Cell<*mut u8>,
    /// The waker the reader is being polled with. Null if it is not being polled.
    reader: Cell<*const Waker>,
    /// The waker of a writer that waits for room and is not woken by `reader`.
    parked: Cell<Option<Waker>>,
}

// SAFETY: The pointers are null unless the reader is being polled, which it is not while it
// moves to another thread, and `Waker` is `Send`.
unsafe impl Send for Shared {}

/// Why nothing can be written right now.
pub(crate) enum NoRoom {
    /// The published buffer is full. There is room again the next time the reader is polled.
    Full,
    /// No buffer is published.
    Closed,
}

impl Shared {
    pub(crate) const fn new() -> Self {
        Self {
            cursor: Cell::new(ptr::null_mut()),
            end: Cell::new(ptr::null_mut()),
            reader: Cell::new(ptr::null()),
            parked: Cell::new(None),
        }
    }

    /// Wakes the writer that waits for room.
    ///
    /// Waking runs foreign code, which might panic. Call this before [`publish`](Self::publish).
    pub(crate) fn wake_parked(&self) {
        if let Some(waker) = self.parked.take() {
            waker.wake();
        }
    }

    /// Drops the waker of the writer that waits for room, without waking it.
    pub(crate) fn forget_parked(&self) {
        drop(self.parked.take());
    }

    /// Lets the writer write to `buf` until the returned guard is dropped. Without a buffer,
    /// writing fails with [`NoRoom::Closed`].
    ///
    /// # Safety
    ///
    /// The guard must be dropped, not leaked: the pointers stay published until then.
    pub(crate) unsafe fn publish<'a>(
        &'a self,
        buf: Option<&'a mut [u8]>,
        waker: &'a Waker,
    ) -> Published<'a> {
        let Range { start, end } = match buf {
            Some(buf) => buf.as_mut_ptr_range(),
            None => ptr::null_mut()..ptr::null_mut(),
        };
        // The guard exists before anything is published
        let published = Published {
            shared: self,
            start,
            _buf: PhantomData,
        };
        self.cursor.set(start);
        self.end.set(end);
        self.reader.set(waker);
        published
    }

    /// Copies as much of `buf` as there is room for, which is at least one byte unless `buf`
    /// is empty.
    pub(crate) fn write(&self, buf: &[u8]) -> Result<usize, NoRoom> {
        let cursor = self.cursor.get();
        // Both are null if nothing is published
        let room = self.end.get().addr() - cursor.addr();
        if room == 0 {
            return Err(if cursor.is_null() {
                NoRoom::Closed
            } else {
                NoRoom::Full
            });
        }
        let len = room.min(buf.len());
        // SAFETY: `cursor..end` is the unwritten rest of the published buffer. `Published`
        // borrows that buffer mutably for as long as it is published, so it is valid for
        // writes, nothing else accesses it, and `buf` cannot overlap with it.
        unsafe {
            cursor.copy_from_nonoverlapping(buf.as_ptr(), len);
            self.cursor.set(cursor.add(len));
        }
        Ok(len)
    }

    /// Arranges for the writer to be polled again once there is room.
    ///
    /// Only the waker of the most recent call is kept.
    pub(crate) fn park(&self, cx: &Context<'_>) {
        let waker = cx.waker();
        // SAFETY: If it is not null, it is published, and `Published` borrows the waker for as
        // long as it is.
        if let Some(reader) = unsafe { self.reader.get().as_ref() } {
            // The reader polls the future again on its own. That reaches the writer, because
            // everything in between was polled with this very waker and therefore cannot tell
            // which of its children was woken.
            if waker.will_wake(reader) {
                return;
            }
        }
        // Something in between tracks which of its children was woken and only polls those
        let parked = match self.parked.take() {
            Some(parked) if parked.will_wake(waker) => parked,
            _ => waker.clone(),
        };
        self.parked.set(Some(parked));
    }
}

/// Withdraws the published buffer when dropped.
pub(crate) struct Published<'a> {
    shared: &'a Shared,
    start: *mut u8,
    _buf: PhantomData<&'a mut [u8]>,
}

impl Published<'_> {
    /// Withdraws the buffer and returns the number of bytes written to it.
    pub(crate) fn withdraw(self) -> usize {
        self.shared.cursor.get().addr() - self.start.addr()
    }
}

impl Drop for Published<'_> {
    fn drop(&mut self) {
        self.shared.cursor.set(ptr::null_mut());
        self.shared.end.set(ptr::null_mut());
        self.shared.reader.set(ptr::null());
    }
}
