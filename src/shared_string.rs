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

use std::borrow::Cow;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::ops::{Deref, Index, RangeBounds};

use arcstr::{ArcStr, Substr};
use imstr::ImString;

#[derive(Clone, Debug, Eq)]
pub enum RefedString<'a> {
    Ref(&'a str),
    Static(&'static str),
    Arcstr(ArcStr),
    Substr(Substr),
    String(ImString),
}

pub type SharedString = RefedString<'static>;

impl<'a> RefedString<'a> {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Ref(str) => str,
            Self::Static(str) => str,
            Self::Arcstr(str) => str.as_str(),
            Self::Substr(str) => str.as_str(),
            Self::String(str) => str.as_str(),
        }
    }

    pub fn slice(&self, range: impl RangeBounds<usize>) -> RefedString<'_> {
        let range = (range.start_bound().cloned(), range.end_bound().cloned());
        match self {
            Self::Ref(str) => Self::Ref(str.index(range)),
            Self::Static(str) => Self::Static(str.index(range)),
            Self::Arcstr(str) => Self::Substr(str.substr(range)),
            Self::Substr(str) => Self::Substr(str.substr(range)),
            Self::String(str) => Self::String(str.slice(range)),
        }
    }

    pub fn into_string(self) -> String {
        match self {
            Self::Ref(str) => str.to_string(),
            Self::Static(str) => str.to_string(),
            Self::Arcstr(str) => str.to_string(),
            Self::Substr(str) => str.to_string(),
            Self::String(str) => str.into_std_string(),
        }
    }

    pub fn to_owned(&self) -> SharedString {
        match self {
            Self::Ref(str) => SharedString::Arcstr(ArcStr::try_alloc(str).unwrap()),
            // Safety: owned uri
            _ => unsafe { std::mem::transmute(self.clone()) },
        }
    }

    pub fn into_owned(self) -> SharedString {
        match self {
            Self::Ref(str) => SharedString::Arcstr(ArcStr::try_alloc(str).unwrap()),
            // Safety: owned uri
            _ => unsafe { std::mem::transmute(self) },
        }
    }
}

impl Deref for RefedString<'_> {
    type Target = str;

    fn deref(&self) -> &str {
        self.as_str()
    }
}

impl Display for RefedString<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.write_str(self.as_str())
    }
}

impl<'a> From<&'a str> for RefedString<'a> {
    fn from(str: &'a str) -> Self {
        Self::Ref(str)
    }
}

impl<'a> From<&'a String> for RefedString<'a> {
    fn from(str: &'a String) -> Self {
        Self::Ref(str.as_str())
    }
}

impl From<String> for RefedString<'static> {
    fn from(str: String) -> Self {
        Self::String(str.into())
    }
}

impl From<ArcStr> for RefedString<'static> {
    fn from(str: ArcStr) -> Self {
        Self::Arcstr(str)
    }
}

impl From<Substr> for RefedString<'static> {
    fn from(str: Substr) -> Self {
        Self::Substr(str)
    }
}

impl<'a> From<Cow<'a, str>> for RefedString<'a> {
    fn from(str: Cow<'a, str>) -> Self {
        match str {
            Cow::Owned(str) => Self::String(str.into()),
            Cow::Borrowed(str) => Self::Ref(str),
        }
    }
}

impl PartialEq<RefedString<'_>> for RefedString<'_> {
    fn eq(&self, other: &RefedString<'_>) -> bool {
        self.as_str() == other.as_str()
    }
}

impl PartialEq<str> for RefedString<'_> {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl Hash for RefedString<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_str().hash(state)
    }
}
