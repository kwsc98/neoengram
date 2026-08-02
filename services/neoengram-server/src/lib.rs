//! HTTP server for the NeoEngram central control plane.
//!
//! This crate provides a Fusen-based HTTP JSON server that exposes the managed-Add job
//! lifecycle. It is deliberately separate from `neoengramd` (which remains transport-agnostic).
//!
//! # Scope
//!
//! This server supports local development and backend integration testing. It does **not**
//! implement authentication, authorization, TLS, or production readiness.

pub mod app_state;
pub(crate) mod clock;
pub mod config;
pub mod dto;
pub(crate) mod error;
pub mod job_api;
pub mod system_api;
