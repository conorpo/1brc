#![warn(missing_docs)]

//! Little timer buddy
//! Makes it easy to time parts of your code in a quick and intuitive way.
//! It saves durations in a queue, which can be iterated over later, and it also returns results immediately so you can choose how to output results.

use std::{collections::VecDeque, time::{Duration, Instant}};
use std::collections::vec_deque::IntoIter as IntoIter;

/// Saves a sequence of durations as you call `.lap()`
/// Results can be read immediately or iterated over later
pub struct TimerBuddy {
    last_lap: Instant,
    times: VecDeque<Duration>
}

impl TimerBuddy {
    /// Starts a new timer at `Instant::now()` with an empty VecDeque of durations.
    pub fn start() -> Self {
        Self {
            last_lap: Instant::now(),
            times: VecDeque::new()
        }
    }

    /// Gets the duration at the front of the queue. 
    /// Its up to you to keep track of queue, but I would reccoemnd using multiple minitimers to compose and group functions.
    pub fn pop_front(&mut self) -> Option<Duration> {
        self.times.pop_front()
    }

    /// Saves the duration since the last lap
    /// Also returns a copy of duration to you to use when duration is needed immediately after timing.
    pub fn lap(&mut self) -> Duration {
        let now = Instant::now();
        let since = now - self.last_lap;
        self.last_lap = now;
        self.times.push_back(since);
        since
    }

    /// Consumes the timer, returning an iterator over the rest of the durations
    pub fn times(self) -> IntoIter<Duration> {
        self.times.into_iter()
    }
}
