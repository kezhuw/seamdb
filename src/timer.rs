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

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use async_io::Timer as IoTimer;

enum Inner {
    Ready(Instant),
    Never,
    Timer(IoTimer),
}

pub struct Timer {
    inner: Inner,
}

impl Timer {
    pub fn never() -> Timer {
        Self { inner: Inner::Never }
    }

    pub fn ready() -> Timer {
        Self { inner: Inner::Ready(Instant::now()) }
    }

    pub fn after(duration: Duration) -> Timer {
        Self { inner: Inner::Timer(IoTimer::after(duration)) }
    }

    pub fn interval(period: Duration) -> Timer {
        Self { inner: Inner::Timer(IoTimer::interval(period)) }
    }
}

impl Future for Timer {
    type Output = Instant;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match &mut self.inner {
            Inner::Ready(instant) => Poll::Ready(*instant),
            Inner::Never => Poll::Pending,
            Inner::Timer(timer) => {
                let timer = unsafe { Pin::new_unchecked(timer) };
                timer.poll(cx)
            },
        }
    }
}
