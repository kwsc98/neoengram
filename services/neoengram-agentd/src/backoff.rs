use std::time::Duration;

const INITIAL_DELAY_SECS: u64 = 1;
const MAX_DELAY_SECS: u64 = 30;
const JITTER_PERCENT: i32 = 20;

/// Enrollment polling backoff. The base sequence is 1, 2, 4, 8, 16, then 30 seconds.
#[derive(Debug, Clone)]
pub struct EnrollmentBackoff {
    next_base_secs: u64,
    max_delay_secs: u64,
}

impl Default for EnrollmentBackoff {
    fn default() -> Self {
        Self::new()
    }
}

impl EnrollmentBackoff {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            next_base_secs: INITIAL_DELAY_SECS,
            max_delay_secs: MAX_DELAY_SECS,
        }
    }

    #[must_use]
    pub fn with_max_delay_seconds(max_delay_secs: u64) -> Self {
        Self {
            next_base_secs: INITIAL_DELAY_SECS,
            max_delay_secs: max_delay_secs.clamp(INITIAL_DELAY_SECS, MAX_DELAY_SECS),
        }
    }

    /// Returns the next delay with a caller-supplied jitter percentage in `-20..=20`.
    /// Production supplies a random value; tests can exercise the bounds deterministically.
    #[must_use]
    pub fn next_delay_with_jitter(&mut self, jitter_percent: i32) -> Duration {
        let jitter_percent = jitter_percent.clamp(-JITTER_PERCENT, JITTER_PERCENT);
        let base_millis = self.next_base_secs.saturating_mul(1_000);
        let adjusted =
            i128::from(base_millis).saturating_mul(i128::from(100 + jitter_percent)) / 100;
        self.next_base_secs = self
            .next_base_secs
            .saturating_mul(2)
            .min(self.max_delay_secs);
        let adjusted = u64::try_from(adjusted)
            .unwrap_or(base_millis)
            .min(self.max_delay_secs.saturating_mul(1_000));
        Duration::from_millis(adjusted)
    }

    pub fn reset(&mut self) {
        self.next_base_secs = INITIAL_DELAY_SECS;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exponential_sequence_caps_at_thirty_seconds() {
        let mut backoff = EnrollmentBackoff::new();
        let delays = (0..8)
            .map(|_| backoff.next_delay_with_jitter(0).as_secs())
            .collect::<Vec<_>>();
        assert_eq!(delays, [1, 2, 4, 8, 16, 30, 30, 30]);
    }

    #[test]
    fn jitter_is_bounded() {
        let mut low = EnrollmentBackoff::new();
        let mut high = EnrollmentBackoff::new();
        assert_eq!(low.next_delay_with_jitter(-100).as_millis(), 800);
        assert_eq!(high.next_delay_with_jitter(100).as_millis(), 1_200);
    }

    #[test]
    fn configured_and_jittered_delay_never_exceeds_the_cap() {
        let mut backoff = EnrollmentBackoff::with_max_delay_seconds(5);
        let delays = (0..8)
            .map(|_| backoff.next_delay_with_jitter(20).as_millis())
            .collect::<Vec<_>>();
        assert_eq!(
            delays,
            [1_200, 2_400, 4_800, 5_000, 5_000, 5_000, 5_000, 5_000]
        );
    }
}
