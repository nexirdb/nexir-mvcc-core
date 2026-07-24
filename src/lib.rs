//! Standalone, deterministic Multi-Version Concurrency Control (MVCC) core library.
//!
//! This library provides the core primitives needed to build durable, transactional
//! key-value databases without imposing a specific runtime or storage engine.
//! It is completely independent of Tokio, networking, or specific consensus implementations.
//!
//! Key components:
//! - [`MvccEngine`]: The main entry point for executing read, write, and batch operations.
//! - [`Backend`]: A trait that must be implemented by the durable storage layer.
//! - `types`: Core data structures like `Intent`, `CommittedVersion`, `ReadGuard`, and `PhysicalWrite`.
//! - `error`: Precision typed error enums for handling validation and conflict states.
//!
//! When the `conformance` feature is enabled, a test macro is exposed for external `Backend` implementers.

/// Storage backend trait and implementations.
pub mod backend;
/// Deterministic binary serialization.
pub mod codec;
/// The core MVCC engine logic.
pub mod engine;
/// Explicit error types.
pub mod error;
/// Core types used throughout the library.
pub mod types;

#[cfg(feature = "conformance")]
pub mod conformance;

pub use backend::{Backend, InMemoryBackend};
pub use codec::{decode_committed, decode_intent, encode_committed, encode_intent};
pub use engine::{
    GcBudget, GcOptions, GcStats, IncrementalGcCursor, IncrementalGcResult, KeyGcOptions,
    KeyGcPlan, MvccEngine,
};
pub use error::{
    AbortError, BatchAbortError, BatchCommitError, BatchError, BatchPrewriteError, CodecError,
    CommitError, GcError, PrewriteError, ReadError,
};
pub use types::{
    CommittedVersion, Intent, Key, Mutation, PhysicalWrite, ReadGuard, Timestamp, TxnId, Value,
};
