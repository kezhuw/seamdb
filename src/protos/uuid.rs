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

use std::fmt::{self, Display, Formatter};

use super::Uuid;

impl Uuid {
    pub fn new_random() -> Self {
        let id = uuid::Uuid::new_v4();
        id.into()
    }
}

impl From<uuid::Uuid> for Uuid {
    fn from(id: uuid::Uuid) -> Self {
        let (msb, lsb) = id.as_u64_pair();
        Self { msb, lsb }
    }
}

impl From<Uuid> for uuid::Uuid {
    fn from(id: Uuid) -> Self {
        uuid::Uuid::from_u64_pair(id.msb, id.lsb)
    }
}

impl Display for Uuid {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        uuid::Uuid::from(*self).fmt(f)
    }
}
