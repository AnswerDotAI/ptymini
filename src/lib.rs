//! Pty sessions over one offset-tracked output ring.
//!
//! The crate is the sync, runtime-free core of the `ptymini` Python package:
//! `Ring` (bounded byte buffer with absolute offsets) and `PtyCore` (one pty
//! process with a reader thread, blocking waiters, and a change hook for
//! async embeddings). The asyncio surface lives in the Python layer; a tokio
//! embedding wraps the same two primitives (`read_from` plus the change
//! hook).

#[cfg(feature = "python")]
mod python;
mod ring;
mod session;

pub use ring::Ring;
pub use session::PtyCore;
