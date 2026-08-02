use neoengram_protocol::UnixMillis;
use neoengramd::Clock;
use std::time::{SystemTime, UNIX_EPOCH};

/// Production clock backed by the operating system wall clock.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> UnixMillis {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        let clamped = millis.min(u64::MAX as u128) as u64;
        UnixMillis::new(clamped)
    }
}
