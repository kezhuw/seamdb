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

use std::convert::TryFrom;
use std::fmt::{Display, Formatter, Write as _};
use std::str::FromStr;

use anyhow::{anyhow, Error, Result};
use compact_str::CompactString;
use hashbrown::Equivalent;
use hashlink::LinkedHashMap;
use uriparse::{Authority, Query, Scheme, SchemeError, Segment};

/// Service endpoint for cluster.
///
/// It has shape `schema://host1[:port1][,host2]`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
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
        let Some((server, remainings)) = address.split_once(',') else {
            return None;
        };
        let scheme = self.scheme;
        Some((Self { scheme, address: server }, Self { scheme, address: remainings }))
    }

    pub fn split_with_scheme(self, scheme: &'a str) -> impl Iterator<Item = Endpoint<'a>> + 'a {
        let address = self.address;
        address.split(',').map(move |s| Self { scheme, address: s })
    }
}

impl<'a> TryFrom<&'a str> for Endpoint<'a> {
    type Error = Error;

    fn try_from(s: &'a str) -> Result<Self> {
        let (resource_id, params) = ServiceUri::parse(s)?;
        if !resource_id.path().is_empty() {
            return Err(anyhow!("endpoint expect no path: {s}"));
        } else if !params.is_empty() {
            return Err(anyhow!("endpoint expect no params: {s}"));
        }
        Ok(resource_id.endpoint())
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
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
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

impl Display for OwnedEndpoint {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        self.as_ref().fmt(f)
    }
}

/// Identify an resource in cluster.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ResourceId<'a> {
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

    pub fn to_owned(&self) -> OwnedResourceId {
        OwnedResourceId { scheme: self.scheme.into(), address: self.address.into(), path: self.path.into() }
    }

    pub fn parse_named(name: &'_ str, s: &'a str) -> Result<ResourceId<'a>> {
        let (resource_id, params) = ServiceUri::parse(s)?;
        if resource_id.path().len() <= 1 {
            return Err(anyhow!("{name} expect path: {s}"));
        } else if !params.is_empty() {
            return Err(anyhow!("{name} expect no params: {s}"));
        }
        Ok(resource_id)
    }

    /// # Safety
    /// The given string must equal to resource uri.
    pub unsafe fn relocate(self, s: &str) -> ResourceId<'_> {
        debug_assert_eq!(self.to_string(), s);
        let scheme = &s[..self.scheme.len()];
        let address = &s[self.scheme.len() + 3..self.scheme.len() + 3 + self.address.len()];
        let path = &s[s.len() - self.path.len()..];
        ResourceId { scheme, address, path }
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

impl<'a> TryFrom<&'a str> for ResourceId<'a> {
    type Error = Error;

    fn try_from(s: &'a str) -> Result<ResourceId<'a>> {
        Self::parse_named("resource id", s)
    }
}

impl Display for ResourceId<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        self.endpoint().fmt(f)?;
        f.write_str(self.path)
    }
}

/// Owned version of [ResourceId].
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct OwnedResourceId {
    scheme: CompactString,
    address: CompactString,
    path: CompactString,
}

impl OwnedResourceId {
    pub fn scheme(&self) -> &str {
        &self.scheme
    }

    pub fn address(&self) -> &str {
        &self.address
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn endpoint(&self) -> Endpoint<'_> {
        Endpoint { scheme: self.scheme(), address: self.address() }
    }

    pub fn as_ref(&self) -> ResourceId<'_> {
        ResourceId { scheme: self.scheme(), address: self.address(), path: self.path() }
    }
}

impl Display for OwnedResourceId {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        self.as_ref().fmt(f)
    }
}

/// Queryable endpoint for cluster resource.
///
/// It has shape `schema://address[path][?param1=abc&param2=xyz]`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ServiceUri {
    scheme: CompactString,
    address: CompactString,
    path: CompactString,
    params: Params,
}

/// Query parameters to custom behaviors in connect/open/query to service.
#[derive(Clone, Default, Debug, PartialEq, Eq, Hash)]
pub struct Params(LinkedHashMap<CompactString, CompactString>);

impl Params {
    pub fn query(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(|v| v.as_str())
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl ServiceUri {
    pub fn scheme(&self) -> &str {
        &self.scheme
    }

    pub fn address(&self) -> &str {
        &self.address
    }

    pub fn path(&self) -> &str {
        &self.path
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
        ResourceId { scheme: self.scheme(), address: self.address(), path: self.path() }
    }

    pub fn with_path(self, path: impl Into<CompactString>) -> Result<Self> {
        let path = path.into();
        if !ResourceId::is_valid_path(&path) {
            return Err(anyhow!("invalid path {path} for service uri"));
        }
        Ok(Self { path, ..self })
    }

    pub fn into(self) -> (OwnedResourceId, Params) {
        (OwnedResourceId { scheme: self.scheme, address: self.address, path: self.path }, self.params)
    }

    pub fn parse(s: &str) -> Result<(ResourceId, Params)> {
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
        let resource_id = ResourceId { scheme, address, path };
        Ok((resource_id, params))
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

fn parse_params(s: &str) -> Option<Params> {
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
        let Some((key, value)) = split_param(param) else {
            return None;
        };
        if params.insert(CompactString::new(key), CompactString::new(value)).is_some() {
            return None;
        }
        if left.is_empty() {
            break;
        }
        trailing = left;
    }
    Some(Params(params))
}

impl FromStr for ServiceUri {
    type Err = Error;

    fn from_str(s: &str) -> Result<ServiceUri> {
        let (resource_id, params) = ServiceUri::parse(s)?;
        Ok(ServiceUri {
            scheme: resource_id.scheme.into(),
            address: resource_id.address.into(),
            path: resource_id.path.into(),
            params,
        })
    }
}

impl Display for ServiceUri {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        self.resource_id().fmt(f)?;
        for (i, (k, v)) in self.params.0.iter().enumerate() {
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

#[cfg(test)]
mod tests {
    use speculoos::*;
    use test_case::test_case;

    use super::*;

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
        let endpoint = Endpoint::try_from("scheme://address,host1:9999,127.0.0.1").unwrap();
        let owned_endpoint = endpoint.to_owned();
        assert_eq!(owned_endpoint, endpoint);
        assert_eq!(owned_endpoint.as_ref(), endpoint);
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
        let resource_id = ResourceId::try_from("scheme://address,host1:9999,127.0.0.1/path").unwrap();
        let owned_resource_id = resource_id.to_owned();
        assert_eq!(owned_resource_id.as_ref(), resource_id);
        assert_eq!(resource_id.endpoint().to_string(), "scheme://address,host1:9999,127.0.0.1")
    }

    #[test]
    fn test_service_resource_id() {
        let resource_id = ResourceId::try_from("scheme://address,host1:9999,127.0.0.1/path").unwrap();
        let service_uri: ServiceUri = format!("{}?key1=value1", resource_id).parse().unwrap();
        assert_eq!(service_uri.resource_id(), resource_id);
    }

    #[test_case("://localhost/path"; "")]
    #[should_panic(expected = "no scheme")]
    fn test_scheme_absent(uri: &str) {
        ServiceUri::from_str(uri).unwrap();
    }

    #[test_case("%://localhost/path"; "")]
    #[should_panic(expected = "invalid scheme")]
    fn test_scheme_invalid(uri: &str) {
        ServiceUri::from_str(uri).unwrap();
    }

    #[test_case("a://"; "no address")]
    #[test_case("a:///path"; "no address with path")]
    #[test_case("a://?"; "no address with empty params")]
    #[test_case("a://?c=d"; "no address with params")]
    #[should_panic(expected = "no address")]
    fn test_address_absent(uri: &str) {
        ServiceUri::from_str(uri).unwrap();
    }

    #[test_case("a://server1%"; "invalid char")]
    #[test_case("a://server1, server2:9090"; "space")]
    #[should_panic(expected = "invalid address")]
    fn test_address_invalid(uri: &str) {
        ServiceUri::from_str(uri).unwrap();
    }

    #[test_case("a://host/%"; "")]
    #[test_case("a://host/a/";)]
    #[test_case("a://host/a//b";)]
    #[should_panic(expected = "invalid path")]
    fn test_path_invalid(uri: &str) {
        ServiceUri::from_str(uri).unwrap();
    }

    #[test]
    #[should_panic(expected = "empty params")]
    fn test_params_empty() {
        ServiceUri::from_str("scheme://host/path?").unwrap();
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
        ServiceUri::from_str(uri).unwrap();
    }

    #[test]
    fn test_valid_uris() {
        let str = "scheme://server1,server2:9090/path/xyz?key1=value1&key2=value2";
        let uri = ServiceUri::from_str(str).unwrap();
        assert_that!(uri).is_equal_to(ServiceUri {
            scheme: "scheme".into(),
            address: "server1,server2:9090".into(),
            path: "/path/xyz".into(),
            params: {
                let mut params = LinkedHashMap::new();
                params.insert("key1".into(), "value1".into());
                params.insert("key2".into(), "value2".into());
                Params(params)
            },
        });
        assert_that!(uri.to_string()).is_equal_to(str.to_string());

        let str = "scheme://address/path";
        let uri = ServiceUri::from_str(str).unwrap();
        assert_that!(uri).is_equal_to(ServiceUri {
            scheme: "scheme".into(),
            address: "address".into(),
            path: "/path".into(),
            params: Default::default(),
        });
        assert_that!(uri.to_string()).is_equal_to(str.to_string());

        let str = "scheme+a://address";
        let uri = ServiceUri::from_str(str).unwrap();
        assert_that!(uri).is_equal_to(ServiceUri {
            scheme: "scheme+a".into(),
            address: "address".into(),
            path: "".into(),
            params: Default::default(),
        });
        assert_that!(uri.to_string()).is_equal_to(str.to_string());

        let str = "scheme-b://address";
        let uri = ServiceUri::from_str(str).unwrap();
        assert_that!(uri).is_equal_to(ServiceUri {
            scheme: "scheme-b".into(),
            address: "address".into(),
            path: "".into(),
            params: Default::default(),
        });
        assert_that!(uri.to_string()).is_equal_to(str.to_string());
    }
}
