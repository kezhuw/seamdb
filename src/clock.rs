// Copyright 2023 The SeamDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::ops::{Add, Sub};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use jiff::Timestamp as JiffTimestamp;
use static_assertions::{assert_impl_all, assert_not_impl_any};

pub use crate::protos::Timestamp;

#[derive(Clone)]
pub struct Clock {
    inner: Arc<SystemTimeClock>,
}

impl Clock {
    pub fn new() -> Self {
        Self { inner: Arc::new(SystemTimeClock::new()) }
    }

    pub fn now(&self) -> Timestamp {
        self.inner.now()
    }

    pub fn update(&self, timestamp: Timestamp) {
        self.inner.update(timestamp)
    }
}

impl Default for Clock {
    fn default() -> Self {
        Self::new()
    }
}

fn system_time_now() -> Timestamp {
    let now = SystemTime::now();
    let elapsed = now.duration_since(SystemTime::UNIX_EPOCH).unwrap();
    let seconds = elapsed.as_secs();
    let nanoseconds = elapsed.subsec_nanos();
    Timestamp { seconds, nanoseconds, logical: 0 }
}

struct SystemTimeClock {
    mutex: spin::Mutex<Timestamp>,
}

assert_impl_all!(SystemTimeClock: Send, Sync);
assert_not_impl_any!(SystemTimeClock: Clone, Copy);

impl SystemTimeClock {
    fn new() -> Self {
        let now = system_time_now();
        Self { mutex: spin::Mutex::new(now) }
    }

    fn now(&self) -> Timestamp {
        let mut now = system_time_now();
        let mut cache = self.mutex.lock();
        if now <= *cache {
            cache.logical += 1;
            now = *cache;
        } else {
            *cache = now;
        }
        now
    }

    fn update(&self, timestamp: Timestamp) {
        let mut cache = self.mutex.lock();
        if timestamp > *cache {
            *cache = timestamp;
        }
    }
}

impl std::fmt::Display for Timestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(sequence) = self.get_txn_sequence() {
            return write!(f, "txn-seq-{sequence}");
        }
        let ts = JiffTimestamp::new(self.seconds as i64, self.nanoseconds as i32).unwrap();
        if self.logical == 0 {
            write!(f, "{ts}")
        } else {
            write!(f, "{ts}L{}", self.logical)
        }
    }
}

impl FromStr for Timestamp {
    type Err = anyhow::Error;

    fn from_str(mut s: &str) -> Result<Self, anyhow::Error> {
        if let Some(sequence) = s.strip_prefix("txn-seq-") {
            let sequence: u32 = sequence.parse()?;
            return Ok(Self::txn_sequence(sequence));
        }
        let logical = match s.rfind('L') {
            None => 0,
            Some(i) => {
                s = &s[..i];
                s[i + 1..].parse()?
            },
        };
        let ts = JiffTimestamp::from_str(s)?;
        Ok(Timestamp { seconds: ts.as_second() as u64, nanoseconds: ts.subsec_nanosecond() as u32, logical })
    }
}

impl Timestamp {
    pub const EPSILON: Timestamp = Self { seconds: 0, nanoseconds: 0, logical: 1 };
    pub const MAX: Timestamp = Self { seconds: u64::MAX, nanoseconds: u32::MAX, logical: u32::MAX };
    pub const ZERO: Timestamp = Self { seconds: 0, nanoseconds: 0, logical: 0 };

    pub const fn is_zero(&self) -> bool {
        self.seconds == 0 && self.nanoseconds == 0 && self.logical == 0
    }

    pub const fn get_txn_sequence(&self) -> Option<u32> {
        match self.seconds & 0x8000000000000000 != 0 {
            true => Some(self.seconds as u32),
            false => None,
        }
    }

    pub const fn txn_sequence(sequence: u32) -> Self {
        Self { seconds: 0x8000000000000000 + sequence as u64, nanoseconds: 0, logical: 0 }
    }

    pub fn forward(&mut self, ts: Timestamp) {
        if *self < ts {
            *self = ts;
        }
    }

    pub fn into_physical(self) -> Self {
        Self { logical: 0, ..self }
    }

    pub fn next(self) -> Self {
        if self.logical < u32::MAX {
            return Self { logical: self.logical + 1, ..self };
        }
        let duration = Duration::new(self.seconds, self.nanoseconds) + Duration::new(0, 1);
        Self { seconds: duration.as_secs(), nanoseconds: duration.subsec_nanos(), logical: 0 }
    }
}

impl Add<Duration> for Timestamp {
    type Output = Self;

    fn add(self, rhs: Duration) -> Self {
        let sum = Duration::new(self.seconds, self.nanoseconds) + rhs;
        Self { seconds: sum.as_secs(), nanoseconds: sum.subsec_nanos(), logical: self.logical }
    }
}

impl Sub<Duration> for Timestamp {
    type Output = Self;

    fn sub(self, rhs: Duration) -> Self {
        let sub = Duration::new(self.seconds, self.nanoseconds) - rhs;
        Self { seconds: sub.as_secs(), nanoseconds: sub.subsec_nanos(), logical: self.logical }
    }
}

impl Sub<Timestamp> for Timestamp {
    type Output = Duration;

    fn sub(self, rhs: Timestamp) -> Self::Output {
        Duration::new(self.seconds, self.nanoseconds).saturating_sub(Duration::new(rhs.seconds, rhs.nanoseconds))
    }
}

#[cfg(test)]
mod tests {
    use assertor::*;

    use super::*;

    #[test]
    fn test_timestamp() {
        assert_that!(Timestamp::ZERO.is_zero()).is_equal_to(true);
        assert_that!(Timestamp::EPSILON.is_zero()).is_equal_to(false);
        assert_that!(Timestamp::MAX.is_zero()).is_equal_to(false);

        assert_that!(Timestamp::ZERO).is_less_than(Timestamp::EPSILON);
        assert_that!(Timestamp::EPSILON).is_less_than(Timestamp::MAX);

        let ts0 = Timestamp::ZERO + Duration::from_secs(50);
        let ts1 = ts0 + Duration::from_nanos(51);
        assert_that!(ts1 - Duration::from_nanos(51)).is_equal_to(ts0);
    }

    #[test]
    fn test_clock_monotonic() {
        let clock = Clock::new();
        let mut old = clock.now();
        for i in 0..5000 {
            clock.update(old - Duration::from_secs(i));
            let now = clock.now();
            assert_that!(now).is_greater_than(old);
            old = now;
        }
    }

    #[test]
    fn test_clock_advance() {
        let clock = Clock::new();
        let future = clock.now() + Duration::from_secs(3000);
        clock.update(future);
        let now = clock.now();
        assert_that!(now).is_greater_than(future);
    }
}
