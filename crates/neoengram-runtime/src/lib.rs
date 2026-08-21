//! Shared NeoEngram execution kernel and local storage adapters.
//!
//! The runtime owns the data-plane implementation used by standalone repositories and managed
//! agents. The `engine` and `fs` modules are intentionally siblings so callers can substitute
//! storage, transport, and state adapters without crossing back into the domain or service
//! layers.

pub mod engine;
pub mod fs;
pub mod object_backend;

// The local repository application is part of the runtime boundary.  Keeping the composition
// root here means the CLI and managed agent share the same storage/worktree implementation while
// the runtime remains the only crate that owns local execution details.
mod app;
mod local;

pub use engine::*;
pub use fs::*;
pub use object_backend::*;

include!("standalone_api.rs");
