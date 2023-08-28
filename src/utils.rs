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

use tokio::sync::watch;

#[derive(Debug)]
pub struct DropOwner {
    sender: watch::Sender<()>,
}

#[derive(Clone, Debug)]
pub struct DropWatcher {
    receiver: watch::Receiver<()>,
}

impl DropWatcher {
    pub async fn dropped(&mut self) {
        self.receiver.changed().await.unwrap_err();
    }
}

impl DropOwner {
    pub fn watch(&self) -> DropWatcher {
        DropWatcher { receiver: self.sender.subscribe() }
    }
}

pub fn drop_watcher() -> (DropOwner, DropWatcher) {
    let (sender, receiver) = watch::channel(());
    (DropOwner { sender }, DropWatcher { receiver })
}

pub trait WatchConsumer<T> {
    fn consume(&mut self) -> T;
}

impl<T: Clone> WatchConsumer<T> for watch::Receiver<T> {
    fn consume(&mut self) -> T {
        let borrowed = self.borrow_and_update();
        borrowed.clone()
    }
}

impl<T: Clone> WatchConsumer<T> for watch::Sender<T> {
    fn consume(&mut self) -> T {
        self.borrow().clone()
    }
}
