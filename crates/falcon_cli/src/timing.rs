use std::time::Instant;

/// Execution timer for SQL statements.
pub struct Timer {
    start: Instant,
}

impl Timer {
    pub fn start() -> Self {
        Self {
            start: Instant::now(),
        }
    }

    /// Returns elapsed time in milliseconds with 3 decimal places.
    pub fn elapsed_ms(&self) -> f64 {
        self.start.elapsed().as_secs_f64() * 1000.0
    }

    /// Format the elapsed time for display.
    pub fn format(&self) -> String {
        format!("Time: {:.3} ms", self.elapsed_ms())
    }
}

/// Timing state — tracks whether timing is enabled.
#[derive(Debug, Default)]
pub struct TimingState {
    pub enabled: bool,
}

impl TimingState {
    pub fn toggle(&mut self) -> bool {
        self.enabled = !self.enabled;
        self.enabled
    }

    /// Start a timer if timing is enabled.
    pub fn maybe_start(&self) -> Option<Timer> {
        if self.enabled {
            Some(Timer::start())
        } else {
            None
        }
    }

    /// Print timing if a timer was started.
    pub fn maybe_print(&self, timer: Option<Timer>) {
        if let Some(t) = timer {
            println!("{}", t.format());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_timer_elapsed_positive() {
        let t = Timer::start();
        thread::sleep(Duration::from_millis(5));
        assert!(
            t.elapsed_ms() >= 1.0,
            "elapsed should be >= 1ms after sleep"
        );
    }

    #[test]
    fn test_timer_format_contains_ms() {
        let t = Timer::start();
        let s = t.format();
        assert!(s.contains("Time:"), "format must contain 'Time:'");
        assert!(s.contains("ms"), "format must contain 'ms'");
    }

    #[test]
    fn test_timing_state_default_disabled() {
        let ts = TimingState::default();
        assert!(!ts.enabled);
    }

    #[test]
    fn test_timing_state_toggle() {
        let mut ts = TimingState::default();
        assert!(ts.toggle(), "first toggle enables");
        assert!(!ts.toggle(), "second toggle disables");
    }

    #[test]
    fn test_maybe_start_returns_none_when_disabled() {
        let ts = TimingState::default();
        assert!(ts.maybe_start().is_none());
    }

    #[test]
    fn test_maybe_start_returns_some_when_enabled() {
        let mut ts = TimingState::default();
        ts.toggle();
        assert!(ts.maybe_start().is_some());
    }
}
