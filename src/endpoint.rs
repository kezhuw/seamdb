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

//! Defines textual endpoint for service and resource.

use std::borrow::Cow;
use std::convert::TryFrom;
use std::fmt::{Display, Formatter, Write as _};
use std::hash::{Hash, Hasher};
use std::ops::Deref;

use anyhow::{anyhow, Error, Result};
use compact_str::{CompactString, ToCompactString};
use hashbrown::Equivalent;
use hashlink::LinkedHashMap;
use uriparse::{Authority, Query, Scheme, SchemeError, Segment};

/// Service endpoint for cluster.
///
/// It has shape `schema://host1[:port1][,host2]`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Endpoint<'a> {
    scheme: &'a str,
    address: &'a str,
}

impl<'a> Endpoint<'a> {
    /// # Safety
    /// It is caller's duty to provide valid arguments.
    pub const unsafe fn new_unchecked(scheme: &'static str, address: &'static str) -> Endpoint<'static> {
        Endpoint { scheme, address }
    }

    pub fn scheme(&self) -> &'a str {
        self.scheme
    }

    /// Comma separated servers.
    pub fn address(&self) -> &'a str {
        self.address
    }

    pub fn to_owned(&self) -> OwnedEndpoint {
        OwnedEndpoint { scheme: self.scheme.into(), address: self.address.into() }
    }

    pub fn split(self) -> impl Iterator<Item = Endpoint<'a>> + 'a {
        self.split_with_scheme(self.scheme)
    }

    pub fn split_once(self) -> Option<(Endpoint<'a>, Endpoint<'a>)> {
        let address = self.address;
        let (server, remainings) = address.split_once(',')?;
        let scheme = self.scheme;
        Some((Self { scheme, address: server }, Self { scheme, address: remainings }))
    }

    pub fn split_with_scheme(self, scheme: &'a str) -> impl Iterator<Item = Endpoint<'a>> + 'a {
        let address = self.address;
        address.split(',').map(move |s| Self { scheme, address: s })
    }
}

impl Hash for Endpoint<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // `str` hash is not stablized and volatile according to `Hasher`s, that is sad! As far as I know, `aHash` does
        // lenght-prefixing for bytes. So, we have to be conservative.
        //
        // * `aHash`: https://github.com/tkaitchuck/aHash/blob/f9acd508bd89e7c5b2877a9510098100f9018d64/src/fallback_hash.rs#L171
        // * `Hasher::write` should clarify its "whole unit" behaviour: https://github.com/rust-lang/rust/issues/94026
        // * Add a dedicated length-prefixing method to `Hasher`: https://github.com/rust-lang/rust/pull/94598
        // * Tracking Issue for `#![feature(hasher_prefixfree_extras)]`: https://github.com/rust-lang/rust/issues/96762
        //
        // ```
        // state.write(self.scheme.as_bytes());
        // state.write(b"://");
        // // state.write_str(self.address);
        // self.address().hash(state);
        // ```

        let str = self.to_compact_string();
        str.hash(state);
    }
}

impl PartialEq<str> for Endpoint<'_> {
    fn eq(&self, other: &str) -> bool {
        let n = self.scheme.len() + 3 + self.address.len();
        if n != other.len() {
            return false;
        }
        let scheme = &other[..self.scheme.len()];
        let separator = &other[self.scheme.len()..self.scheme.len() + 3];
        let address = &other[self.scheme.len() + 3..];
        (scheme, separator, address) == (self.scheme, "://", self.address)
    }
}

impl PartialEq<&str> for Endpoint<'_> {
    fn eq(&self, other: &&str) -> bool {
        self == *other
    }
}

impl<'a> TryFrom<&'a str> for Endpoint<'a> {
    type Error = Error;

    fn try_from(s: &'a str) -> Result<Self> {
        let uri = ServiceUri::parse(s)?;
        if !uri.path().is_empty() {
            return Err(anyhow!("endpoint expect no path: {s}"));
        } else if !uri.params.is_empty() {
            return Err(anyhow!("endpoint expect no params: {s}"));
        }
        // Safety: they are pointing to string argument.
        unsafe { Ok(std::mem::transmute(uri.endpoint())) }
    }
}

impl Display for Endpoint<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.write_str(self.scheme)?;
        f.write_str("://")?;
        f.write_str(self.address)?;
        Ok(())
    }
}

/// Owned version of [Endpoint].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OwnedEndpoint {
    scheme: CompactString,
    address: CompactString,
}

impl OwnedEndpoint {
    pub fn scheme(&self) -> &str {
        &self.scheme
    }

    pub fn address(&self) -> &str {
        &self.address
    }

    pub fn as_ref(&self) -> Endpoint<'_> {
        Endpoint { scheme: self.scheme(), address: self.address() }
    }
}

impl Hash for OwnedEndpoint {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_ref().hash(state);
    }
}

impl PartialEq<Endpoint<'_>> for OwnedEndpoint {
    fn eq(&self, other: &Endpoint<'_>) -> bool {
        self.as_ref().eq(other)
    }
}

impl Equivalent<OwnedEndpoint> for Endpoint<'_> {
    fn equivalent(&self, key: &OwnedEndpoint) -> bool {
        key == self
    }
}

impl Equivalent<OwnedEndpoint> for str {
    fn equivalent(&self, key: &OwnedEndpoint) -> bool {
        key == self
    }
}

impl PartialEq<str> for OwnedEndpoint {
    fn eq(&self, other: &str) -> bool {
        self.as_ref() == other
    }
}

impl PartialEq<&str> for OwnedEndpoint {
    fn eq(&self, other: &&str) -> bool {
        self.as_ref() == *other
    }
}

impl Display for OwnedEndpoint {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        self.as_ref().fmt(f)
    }
}

/// Owned version of [ResourceId].
pub type OwnedResourceId = ResourceId<'static>;

/// Identify an resource in cluster.
#[derive(Debug, Eq)]
pub struct ResourceId<'a> {
    str: Cow<'a, str>,
    scheme: &'a str,
    address: &'a str,
    path: &'a str,
}

impl<'a> ResourceId<'a> {
    pub fn scheme(&self) -> &'a str {
        self.scheme
    }

    pub fn address(&self) -> &'a str {
        self.address
    }

    /// Absolute path to this resource including leading slash.
    pub fn path(&self) -> &'a str {
        self.path
    }

    /// Endpoint of the cluster this resource located in.
    pub fn endpoint(&self) -> Endpoint<'a> {
        Endpoint { scheme: self.scheme, address: self.address }
    }

    pub fn as_str(&self) -> &str {
        &self.str
    }

    pub fn to_owned(&self) -> OwnedResourceId {
        self.clone().into_owned()
    }

    pub fn into_owned(self) -> OwnedResourceId {
        let str = match &self.str {
            // Safety: invariant: `str` is owned only for static endpoint.
            Cow::Owned(_) => return unsafe { std::mem::transmute(self) },
            Cow::Borrowed(str) => str.to_string(),
        };
        let uri = UriParts { scheme: self.scheme, address: self.address, path: self.path, params: &Params::default() };
        // Safety: `str` is heap allocated.
        let uri: UriParts<'static> = unsafe { uri.into_relocated(&str) };
        ResourceId { str: Cow::Owned(str), scheme: uri.scheme, address: uri.address, path: uri.path }
    }

    pub fn parse_named(name: &'_ str, str: impl Into<Cow<'a, str>>) -> Result<ResourceId<'a>> {
        let uri = ServiceUri::parse(str)?;
        if uri.path().len() <= 1 {
            return Err(anyhow!("{name} expect path: {uri}"));
        } else if !uri.params().is_empty() {
            return Err(anyhow!("{name} expect no params: {uri}"));
        }
        Ok(Self { str: uri.str, scheme: uri.scheme, address: uri.address, path: uri.path })
    }

    fn is_valid_path(s: &str) -> bool {
        if s.is_empty() {
            return true;
        } else if !s.starts_with('/') {
            return false;
        }
        for segment in s[1..].split('/') {
            if segment.is_empty() || Segment::try_from(segment).is_err() {
                return false;
            }
        }
        true
    }
}

impl Deref for ResourceId<'_> {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl Clone for ResourceId<'_> {
    fn clone(&self) -> Self {
        match self.str {
            Cow::Borrowed(str) => {
                Self { str: Cow::Borrowed(str), scheme: self.scheme, address: self.address, path: self.path }
            },
            Cow::Owned(ref str) => {
                let str = str.clone();
                let uri = UriParts {
                    scheme: self.scheme,
                    address: self.address,
                    path: self.path,
                    params: &Params::default(),
                };
                // Safety: `str` is heap allocated.
                let uri = unsafe { uri.into_relocated(&str) };
                Self { str: Cow::Owned(str), scheme: uri.scheme, address: uri.address, path: uri.path }
            },
        }
    }
}

impl PartialEq<Self> for ResourceId<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.str.as_ref() == other.str.as_ref()
    }
}

impl PartialEq<str> for ResourceId<'_> {
    fn eq(&self, other: &str) -> bool {
        self.str.as_ref() == other
    }
}

impl PartialEq<&str> for ResourceId<'_> {
    fn eq(&self, other: &&str) -> bool {
        self.str.as_ref() == *other
    }
}

impl Hash for ResourceId<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_str().hash(state)
    }
}

impl Equivalent<ResourceId<'_>> for str {
    fn equivalent(&self, key: &ResourceId<'_>) -> bool {
        key == self
    }
}

impl From<ResourceId<'_>> for String {
    fn from(uri: ResourceId<'_>) -> String {
        match uri.str {
            Cow::Borrowed(str) => str.to_owned(),
            Cow::Owned(str) => str,
        }
    }
}

impl<'a> TryFrom<&'a str> for ResourceId<'a> {
    type Error = Error;

    fn try_from(s: &'a str) -> Result<ResourceId<'a>> {
        Self::parse_named("resource id", s)
    }
}

impl Display for ResourceId<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.write_str(self.as_str())
    }
}

struct UriParts<'a> {
    scheme: &'a str,
    address: &'a str,
    path: &'a str,
    params: &'a Params<'a>,
}

impl<'a> UriParts<'a> {
    unsafe fn relocate(&mut self, str: &'a str) {
        debug_assert_eq!(self.to_string(), str);
        self.scheme = &str[..self.scheme.len()];
        let start = self.scheme.len() + 3;
        self.address = &str[start..start + self.address.len()];
        let start = start + self.address.len();
        self.path = &str[start..start + self.path.len()];
    }

    unsafe fn into_relocated<'b>(self, str: &str) -> UriParts<'b> {
        let mut uri: UriParts<'b> = std::mem::transmute(self);
        let str: &'b str = std::mem::transmute(str);
        uri.relocate(str);
        uri
    }

    fn reshape(&mut self) -> String {
        let str = self.to_string();
        // Safety: str is heap allocated
        unsafe {
            let str = std::mem::transmute(str.as_str());
            self.relocate(str);
        }
        str
    }

    fn into(self) -> (&'a str, &'a str, &'a str) {
        (self.scheme, self.address, self.path)
    }
}

impl Display for UriParts<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.write_str(self.scheme)?;
        f.write_str("://")?;
        f.write_str(self.address)?;
        f.write_str(self.path)?;
        for (i, (k, v)) in self.params.map.iter().enumerate() {
            if i == 0 {
                f.write_char('?')?;
            } else {
                f.write_char('&')?;
            }
            f.write_str(k)?;
            f.write_char('=')?;
            f.write_str(v)?;
        }
        Ok(())
    }
}

/// Owned version of [ServiceUri].
pub type OwnedServiceUri = ServiceUri<'static>;

/// Queryable endpoint for cluster resource.
///
/// It has shape `schema://address[path][?param1=abc&param2=xyz]`.
#[derive(Clone, Debug, Eq)]
pub struct ServiceUri<'a> {
    str: Cow<'a, str>,
    scheme: &'a str,
    address: &'a str,
    path: &'a str,
    params: Params<'a>,
}

/// Query parameters to custom behaviors in connect/open/query to service.
#[derive(Clone, Default, Debug, PartialEq, Eq, Hash)]
pub struct Params<'a> {
    map: LinkedHashMap<CompactString, CompactString>,
    _marker: std::marker::PhantomData<&'a ()>,
}

/// Owned version of [Params].
pub type OwnedParams = Params<'static>;

impl Params<'_> {
    fn new(map: LinkedHashMap<CompactString, CompactString>) -> Self {
        Self { map, _marker: std::marker::PhantomData }
    }

    pub fn query(&self, key: &str) -> Option<&str> {
        self.map.get(key).map(|s| s.as_str())
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn to_owned(&self) -> OwnedParams {
        self.clone().into_owned()
    }

    pub fn into_owned(self) -> OwnedParams {
        // Safety: `Params` has no references.
        unsafe { std::mem::transmute(self) }
    }
}

impl<'a> ServiceUri<'a> {
    pub fn scheme(&self) -> &str {
        self.scheme
    }

    pub fn address(&self) -> &str {
        self.address
    }

    pub fn path(&self) -> &str {
        self.path
    }

    pub fn params(&self) -> &Params {
        &self.params
    }

    pub fn query(&self, key: &str) -> Option<&str> {
        self.params.query(key)
    }

    pub fn endpoint(&self) -> Endpoint<'_> {
        Endpoint { scheme: self.scheme(), address: self.address() }
    }

    pub fn resource_id(&self) -> ResourceId<'_> {
        let len = self.scheme.len() + 3 + self.address.len() + self.path.len();
        ResourceId {
            str: Cow::Borrowed(&self.str.as_ref()[..len]),
            scheme: self.scheme(),
            address: self.address(),
            path: self.path(),
        }
    }

    pub fn parts(&self) -> (ResourceId<'_>, &Params<'_>) {
        (self.resource_id(), self.params())
    }

    pub fn with_path(self, path: &str) -> Result<OwnedServiceUri> {
        if !ResourceId::is_valid_path(path) {
            return Err(anyhow!("invalid path {path} for service uri"));
        }
        let params = self.params.into_owned();
        let mut parts = UriParts { scheme: self.scheme, address: self.address, path, params: &params };
        let uri = parts.reshape();
        // Safety: they are pointing to heap allocated string now.
        let (scheme, address, path) = unsafe { std::mem::transmute(parts.into()) };
        Ok(OwnedServiceUri { str: Cow::Owned(uri), scheme, address, path, params })
    }

    pub fn as_str(&self) -> &str {
        self.str.as_ref()
    }

    pub fn to_owned(&self) -> OwnedServiceUri {
        self.clone().into_owned()
    }

    pub fn into_owned(self) -> OwnedServiceUri {
        let str = match &self.str {
            // Safety: invariant: `str` is owned only for static endpoint.
            Cow::Owned(_) => return unsafe { std::mem::transmute(self) },
            Cow::Borrowed(str) => str.to_string(),
        };
        let params = self.params.into_owned();
        let uri = UriParts { scheme: self.scheme, address: self.address, path: self.path, params: &params };
        // Safety: `str` is heap allocated.
        let uri = unsafe { uri.into_relocated(&str) };
        ServiceUri { str: Cow::Owned(str), scheme: uri.scheme, address: uri.address, path: uri.path, params }
    }

    pub fn parse(s: impl Into<Cow<'a, str>>) -> Result<Self> {
        let s = s.into();
        let Some((scheme, trailing)) = s.split_once("://") else {
            return Err(anyhow!("invalid service uri: {}", s));
        };
        match Scheme::try_from(scheme) {
            Err(SchemeError::Empty) => return Err(anyhow!("no scheme in service uri: {s}")),
            Err(_) => return Err(anyhow!("invalid scheme in service uri: {s}")),
            _ => {},
        };

        let (address, trailing) = match trailing.find(['/', '?']) {
            None => (trailing, Default::default()),
            Some(i) => (&trailing[..i], &trailing[i..]),
        };
        if address.is_empty() {
            return Err(anyhow!("no address in service uri: {}", s));
        }
        for server in address.split(',') {
            let Ok(authority) = Authority::try_from(server) else {
                return Err(anyhow!("invalid address in service uri: {}", s));
            };
            if authority.has_username() {
                return Err(anyhow!("unsupported username in service uri: {s}"));
            }
        }

        let (path, trailing) = match trailing.split_once('?') {
            Some((path, trailing)) => (path, Some(trailing)),
            None => (trailing, None),
        };
        if !ResourceId::is_valid_path(path) {
            return Err(anyhow!("invalid path in service uri: {}", s));
        }

        let params = match trailing {
            Some("") => return Err(anyhow!("empty params in service uri: {s}")),
            Some(trailing) => parse_params(trailing).ok_or_else(|| anyhow!("invalid params in service uri: {s}"))?,
            None => Default::default(),
        };
        // Safety: they are pointing to what cow holds
        let (scheme, address, path) = unsafe { std::mem::transmute((scheme, address, path)) };
        Ok(ServiceUri { str: s, scheme, address, path, params })
    }
}

impl PartialEq<Self> for ServiceUri<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.str.as_ref() == other.str.as_ref()
    }
}

impl PartialEq<str> for ServiceUri<'_> {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl PartialEq<&str> for ServiceUri<'_> {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl Hash for ServiceUri<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_str().hash(state)
    }
}

impl Equivalent<ServiceUri<'_>> for str {
    fn equivalent(&self, key: &ServiceUri<'_>) -> bool {
        key == self
    }
}

impl From<ServiceUri<'_>> for String {
    fn from(uri: ServiceUri<'_>) -> String {
        match uri.str {
            Cow::Borrowed(str) => str.to_owned(),
            Cow::Owned(str) => str,
        }
    }
}

impl TryFrom<String> for OwnedServiceUri {
    type Error = Error;

    fn try_from(s: String) -> Result<Self> {
        ServiceUri::parse(s)
    }
}

impl<'a> TryFrom<&'a str> for ServiceUri<'a> {
    type Error = Error;

    fn try_from(s: &'a str) -> Result<Self> {
        ServiceUri::parse(s)
    }
}

fn split_param(s: &str) -> Option<(&str, &str)> {
    let (key, value) = match s.split_once('=') {
        None | Some(("", _)) | Some((_, "")) => return None,
        Some((key, value)) => (key, value),
    };
    if value.split_once('=').is_some() {
        return None;
    }
    Some((key, value))
}

fn parse_params(s: &str) -> Option<OwnedParams> {
    if Query::try_from(s).is_err() {
        return None;
    }
    let n = s.chars().filter(|c| *c == '&').count();
    let mut params = LinkedHashMap::with_capacity(n);
    let mut trailing = s;
    loop {
        let (param, left) = match trailing.split_once('&') {
            None => (trailing, Default::default()),
            Some((_, "")) => return None,
            Some(pair) => pair,
        };
        let (key, value) = split_param(param)?;
        if params.insert(CompactString::new(key), CompactString::new(value)).is_some() {
            return None;
        }
        if left.is_empty() {
            break;
        }
        trailing = left;
    }
    Some(Params::new(params))
}

impl Display for ServiceUri<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::hash_map::DefaultHasher;

    use hashbrown::HashMap;
    use speculoos::*;
    use test_case::test_case;

    use super::*;

    trait HashCode {
        fn hash_code(&self) -> u64;
    }

    impl<T> HashCode for T
    where
        T: Hash,
    {
        fn hash_code(&self) -> u64 {
            let mut hasher = DefaultHasher::default();
            self.hash(&mut hasher);
            hasher.finish()
        }
    }

    #[test]
    #[should_panic(expected = "endpoint expect no path")]
    fn test_endpoint_no_path() {
        Endpoint::try_from("scheme://address/path?key=value").unwrap();
    }

    #[test]
    #[should_panic(expected = "endpoint expect no params")]
    fn test_endpoint_no_params() {
        Endpoint::try_from("scheme://address?key=value").unwrap();
    }

    #[test]
    fn test_endpoint_ok() {
        Endpoint::try_from("scheme://address,host1:9999,127.0.0.1").unwrap();
    }

    #[test]
    fn test_endpoint_equal() {
        let str = "scheme://address,host1:9999,127.0.0.1";
        let endpoint = Endpoint::try_from(str).unwrap();
        let owned_endpoint = endpoint.to_owned();
        assert_eq!(endpoint, str);
        assert_eq!(endpoint, *str);
        assert_eq!(owned_endpoint, str);
        assert_eq!(owned_endpoint, *str);
        assert_eq!(owned_endpoint, endpoint);
        assert_eq!(owned_endpoint.as_ref(), endpoint);

        assert_eq!(endpoint.hash_code(), str.hash_code());
        assert_eq!(owned_endpoint.hash_code(), str.hash_code());
        assert_eq!(owned_endpoint.as_ref().hash_code(), str.hash_code());
    }

    #[test]
    fn test_endpoint_hashmap() {
        let str = "scheme://address,host1:9999,127.0.0.1";
        let endpoint = Endpoint::try_from(str).unwrap();
        let owned_endpoint = endpoint.to_owned();
        let mut map = HashMap::new();
        map.insert(owned_endpoint, "v1");
        assert_that!(map.get(str).cloned()).is_equal_to(Some("v1"));
        assert_that!(map.get(&endpoint).cloned()).is_equal_to(Some("v1"));
    }

    #[test]
    fn test_endpoint_split() {
        let endpoint = Endpoint::try_from("scheme://address,host1:9999,127.0.0.1").unwrap();
        let servers: Vec<_> = endpoint.split().collect();
        assert_eq!(servers, vec![
            Endpoint { scheme: "scheme", address: "address" },
            Endpoint { scheme: "scheme", address: "host1:9999" },
            Endpoint { scheme: "scheme", address: "127.0.0.1" }
        ]);

        let servers: Vec<_> = endpoint.split_with_scheme("http").collect();
        assert_eq!(servers, vec![
            Endpoint { scheme: "http", address: "address" },
            Endpoint { scheme: "http", address: "host1:9999" },
            Endpoint { scheme: "http", address: "127.0.0.1" }
        ]);

        let (server, remainings) = endpoint.split_once().unwrap();
        assert_eq!(server, Endpoint { scheme: "scheme", address: "address" });
        assert_eq!(remainings, Endpoint { scheme: "scheme", address: "host1:9999,127.0.0.1" });

        assert_eq!(server.split_once(), None);
    }

    #[test]
    #[should_panic(expected = "resource id expect path")]
    fn test_resource_id_path() {
        ResourceId::try_from("scheme://address?key=value").unwrap();
    }

    #[test]
    #[should_panic(expected = "resource id expect no params")]
    fn test_resource_id_no_params() {
        ResourceId::try_from("scheme://address/path?key=value").unwrap();
    }

    #[test]
    fn test_resource_id_ok() {
        ResourceId::try_from("scheme://address/path").unwrap();
    }

    #[test]
    fn test_resource_id_equal() {
        let str = "scheme://address,host1:9999,127.0.0.1/path";
        let resource_id = ResourceId::try_from(str).unwrap();
        let owned_resource_id = resource_id.to_owned();
        assert_eq!(resource_id, str);
        assert_eq!(resource_id, *str);
        assert_eq!(owned_resource_id, str);
        assert_eq!(owned_resource_id, *str);
        assert_eq!(owned_resource_id, resource_id);
        assert_eq!(resource_id.endpoint().to_string(), "scheme://address,host1:9999,127.0.0.1");

        assert_eq!(resource_id.hash_code(), str.hash_code());
        assert_eq!(owned_resource_id.hash_code(), str.hash_code());
    }

    #[test]
    fn test_resource_id_hashmap() {
        let str = "scheme://address/path";
        let uri = ResourceId::try_from(str).unwrap();
        let mut map = HashMap::new();
        map.insert(uri, "v1");
        assert_that!(map.get(str).cloned()).is_equal_to(Some("v1"));
    }

    #[test]
    fn test_service_resource_id() {
        let resource_id = ResourceId::try_from("scheme://address,host1:9999,127.0.0.1/path").unwrap();
        let service_uri: ServiceUri = format!("{}?key1=value1", resource_id).try_into().unwrap();
        assert_eq!(service_uri.resource_id(), resource_id);
    }

    #[test_case("://localhost/path"; "")]
    #[should_panic(expected = "no scheme")]
    fn test_scheme_absent(uri: &str) {
        ServiceUri::parse(uri).unwrap();
    }

    #[test_case("%://localhost/path"; "")]
    #[should_panic(expected = "invalid scheme")]
    fn test_scheme_invalid(uri: &str) {
        ServiceUri::parse(uri).unwrap();
    }

    #[test_case("a://"; "no address")]
    #[test_case("a:///path"; "no address with path")]
    #[test_case("a://?"; "no address with empty params")]
    #[test_case("a://?c=d"; "no address with params")]
    #[should_panic(expected = "no address")]
    fn test_address_absent(uri: &str) {
        ServiceUri::parse(uri).unwrap();
    }

    #[test_case("a://server1%"; "invalid char")]
    #[test_case("a://server1, server2:9090"; "space")]
    #[should_panic(expected = "invalid address")]
    fn test_address_invalid(uri: &str) {
        ServiceUri::parse(uri).unwrap();
    }

    #[test_case("a://host/%"; "")]
    #[test_case("a://host/a/"; "trailing separator")]
    #[test_case("a://host/a//b"; "double separator")]
    #[should_panic(expected = "invalid path")]
    fn test_path_invalid(uri: &str) {
        ServiceUri::parse(uri).unwrap();
    }

    #[test]
    #[should_panic(expected = "empty params")]
    fn test_params_empty() {
        ServiceUri::parse("scheme://host/path?").unwrap();
    }

    #[test_case("scheme://host/path0?=")]
    #[test_case("scheme://host/path1?a=")]
    #[test_case("scheme://host/path2?=b")]
    #[test_case("scheme://host/path3?&")]
    #[test_case("scheme://host/path4?a=&")]
    #[test_case("scheme://host/path5?a=%&")]
    #[test_case("scheme://host/path6?a=b&")]
    #[test_case("scheme://host/path7?a=b&c=")]
    #[should_panic(expected = "invalid params")]
    fn test_params_invalid(uri: &str) {
        ServiceUri::parse(uri).unwrap();
    }

    #[test]
    fn test_service_uri_equal() {
        let str = "scheme://server1,server2:9090/path/xyz?key1=value1&key2=value2";
        let uri = ServiceUri::try_from(str).unwrap();
        let owned_uri = uri.to_owned();
        assert_eq!(uri, str);
        assert_eq!(uri, *str);
        assert_eq!(owned_uri, str);
        assert_eq!(owned_uri, *str);
        assert_eq!(owned_uri, uri);
        assert_eq!(uri.endpoint(), "scheme://server1,server2:9090");
        assert_eq!(uri.resource_id(), "scheme://server1,server2:9090/path/xyz");

        assert_eq!(uri.hash_code(), str.hash_code());
        assert_eq!(owned_uri.hash_code(), str.hash_code());
    }

    #[test]
    fn test_service_uri_hashmap() {
        let str = "scheme://server1,server2:9090/path/xyz?key1=value1&key2=value2";
        let uri = ServiceUri::try_from(str).unwrap();
        let mut map = HashMap::new();
        map.insert(uri, "v1");
        assert_that!(map.get(str).cloned()).is_equal_to(Some("v1"));
    }

    #[test]
    fn test_service_uri_with_path() {
        let str = "scheme://host/path1";
        let uri = ServiceUri::try_from(str).unwrap().with_path("/path2").unwrap();
        assert_eq!(uri, "scheme://host/path2");
    }

    #[test]
    #[should_panic(expected = "invalid path")]
    fn test_service_uri_with_path_invalid() {
        let str = "scheme://host/path1";
        ServiceUri::try_from(str).unwrap().with_path("/path2/%").unwrap();
    }

    #[test]
    fn test_valid_uris() {
        let str = "scheme://server1,server2:9090/path/xyz?key1=value1&key2=value2";
        let uri = ServiceUri::parse(str).unwrap();
        assert_that!(uri).is_equal_to(ServiceUri {
            str: str.into(),
            scheme: "scheme".into(),
            address: "server1,server2:9090".into(),
            path: "/path/xyz".into(),
            params: {
                let mut params = LinkedHashMap::new();
                params.insert("key1".into(), "value1".into());
                params.insert("key2".into(), "value2".into());
                Params::new(params)
            },
        });
        assert_that!(uri.to_string()).is_equal_to(str.to_string());

        let str = "scheme://address/path";
        let uri = ServiceUri::parse(str).unwrap();
        assert_that!(uri).is_equal_to(ServiceUri {
            str: str.into(),
            scheme: "scheme".into(),
            address: "address".into(),
            path: "/path".into(),
            params: Default::default(),
        });
        assert_that!(uri.to_string()).is_equal_to(str.to_string());

        let str = "scheme+a://address";
        let uri = ServiceUri::parse(str).unwrap();
        assert_that!(uri).is_equal_to(ServiceUri {
            str: str.into(),
            scheme: "scheme+a".into(),
            address: "address".into(),
            path: "".into(),
            params: Default::default(),
        });
        assert_that!(uri.to_string()).is_equal_to(str.to_string());

        let str = "scheme-b://address";
        let uri = ServiceUri::parse(str).unwrap();
        assert_that!(uri).is_equal_to(ServiceUri {
            str: str.into(),
            scheme: "scheme-b".into(),
            address: "address".into(),
            path: "".into(),
            params: Default::default(),
        });
        assert_that!(uri.to_string()).is_equal_to(str.to_string());
    }
}
