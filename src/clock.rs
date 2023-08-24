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
use std::sync::Arc;
use std::time::{Duration, SystemTime};

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
        write!(f, "{:?}", self)
    }
}

impl Timestamp {
    pub const fn is_zero(&self) -> bool {
        self.seconds == 0 && self.nanoseconds == 0 && self.logical == 0
    }

    pub const fn zero() -> Self {
        Self { seconds: 0, nanoseconds: 0, logical: 0 }
    }

    pub const fn epsilon() -> Self {
        Self { seconds: 0, nanoseconds: 0, logical: 1 }
    }

    pub const fn infinite() -> Self {
        Self { seconds: u64::MAX, nanoseconds: u32::MAX, logical: u32::MAX }
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

#[cfg(test)]
mod tests {
    use assertor::*;

    use super::*;

    #[test]
    fn test_timestamp() {
        assert_that!(Timestamp::zero().is_zero()).is_equal_to(true);
        assert_that!(Timestamp::epsilon().is_zero()).is_equal_to(false);
        assert_that!(Timestamp::infinite().is_zero()).is_equal_to(false);

        assert_that!(Timestamp::zero()).is_less_than(Timestamp::epsilon());
        assert_that!(Timestamp::epsilon()).is_less_than(Timestamp::infinite());

        let ts0 = Timestamp::zero() + Duration::from_secs(50);
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
