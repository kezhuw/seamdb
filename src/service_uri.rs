use std::fmt::Write as _;
use std::str::FromStr;

use anyhow::{anyhow, Error, Result};
use compact_str::CompactString;
use hashlink::LinkedHashMap;
use uriparse::{Authority, Query, Scheme, SchemeError, Segment};

/// schema[+spec]://address[path][?param1=abc&param2=xyz]
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ServiceUri {
    pub scheme: CompactString,
    pub spec: CompactString,
    pub address: CompactString,
    pub path: CompactString,
    pub params: LinkedHashMap<CompactString, CompactString>,
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

fn parse_params(s: &str) -> Option<LinkedHashMap<CompactString, CompactString>> {
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
    Some(params)
}

impl FromStr for ServiceUri {
    type Err = Error;

    fn from_str(s: &str) -> Result<ServiceUri> {
        let Some((leading, trailing)) = s.split_once("://") else {
            return Err(anyhow!("invalid service uri: {}", s));
        };
        let (scheme, spec) = match leading.split_once('+') {
            Some((scheme, spec)) => (scheme, Some(spec)),
            None => (leading, Default::default()),
        };
        match Scheme::try_from(scheme) {
            Err(SchemeError::Empty) => return Err(anyhow!("no scheme in service uri: {s}")),
            Err(_) => return Err(anyhow!("invalid scheme in service uri: {s}")),
            _ => {},
        };

        let spec = match spec {
            Some("") => return Err(anyhow!("empty spec in service uri: {s}")),
            Some(spec) => spec,
            None => "",
        };
        match Scheme::try_from(spec) {
            Err(SchemeError::Empty) => {},
            Err(_) => return Err(anyhow!("invalid spec in service uri: {s}")),
            _ => {},
        }

        let (address, trailing) = match trailing.find(['/', '?']) {
            None => (trailing, Default::default()),
            Some(i) => (&trailing[..i], &trailing[i..]),
        };
        if address.is_empty() {
            return Err(anyhow!("no address in service uri: {}", s));
        }
        let Ok(authority) = Authority::try_from(address) else {
            return Err(anyhow!("invalid address in service uri: {}", s));
        };
        if authority.has_username() {
            return Err(anyhow!("unsupported username in service uri: {s}"));
        }

        let (path, trailing) = match trailing.split_once('?') {
            Some((path, trailing)) => (path, Some(trailing)),
            None => (trailing, None),
        };
        for segment in path.split('/') {
            if Segment::try_from(segment).is_err() {
                return Err(anyhow!("invalid path in service uri: {}", s));
            }
        }

        let params = match trailing {
            Some("") => return Err(anyhow!("empty params in service uri: {s}")),
            Some(trailing) => parse_params(trailing).ok_or_else(|| anyhow!("invalid params in service uri: {s}"))?,
            None => Default::default(),
        };
        Ok(ServiceUri { scheme: scheme.into(), spec: spec.into(), address: address.into(), path: path.into(), params })
    }
}

impl std::fmt::Display for ServiceUri {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.write_str(&self.scheme).unwrap();
        if !self.spec.is_empty() {
            f.write_char('+').unwrap();
            f.write_str(&self.spec).unwrap();
        }
        f.write_str("://").unwrap();
        f.write_str(&self.address).unwrap();
        f.write_str(&self.path).unwrap();
        for (i, (k, v)) in self.params.iter().enumerate() {
            if i == 0 {
                f.write_char('?').unwrap();
            } else {
                f.write_char('&').unwrap();
            }
            f.write_str(k).unwrap();
            f.write_char('=').unwrap();
            f.write_str(v).unwrap();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use speculoos::*;
    use test_case::test_case;

    use super::*;

    #[test_case("://localhost/path"; "no scheme")]
    #[test_case("+://localhost/path"; "no scheme with no spec")]
    #[test_case("+tls://localhost/path"; "no scheme with spec")]
    #[test_case("+%://localhost/path"; "no scheme with invalid spec")]
    #[should_panic(expected = "no scheme")]
    fn test_scheme_absent(uri: &str) {
        ServiceUri::from_str(uri).unwrap();
    }

    #[test_case("%://localhost/path"; "invalid scheme")]
    #[test_case("%+://localhost/path"; "invalid scheme with no spec")]
    #[test_case("%+tls://localhost/path"; "invalid scheme with spec")]
    #[test_case("%+%://localhost/path"; "invalid scheme with invalid spec")]
    #[should_panic(expected = "invalid scheme")]
    fn test_scheme_invalid(uri: &str) {
        ServiceUri::from_str(uri).unwrap();
    }

    #[test_case("b+://localhost/path"; "")]
    #[should_panic(expected = "empty spec")]
    fn test_spec_empty(uri: &str) {
        ServiceUri::from_str(uri).unwrap();
    }

    #[test_case("b+%://localhost/path"; "")]
    #[should_panic(expected = "invalid spec")]
    fn test_spec_invalid(uri: &str) {
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

    #[test_case("a://host/%"; "")]
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
        let str = "scheme+spec://address/path/xyz?key1=value1&key2=value2";
        let uri = ServiceUri::from_str(str).unwrap();
        assert_that!(uri).is_equal_to(ServiceUri {
            scheme: "scheme".into(),
            address: "address".into(),
            spec: "spec".into(),
            path: "/path/xyz".into(),
            params: {
                let mut params = LinkedHashMap::new();
                params.insert("key1".into(), "value1".into());
                params.insert("key2".into(), "value2".into());
                params
            },
        });
        assert_that!(uri.to_string()).is_equal_to(str.to_string());

        let str = "scheme://address/path";
        let uri = ServiceUri::from_str(str).unwrap();
        assert_that!(uri).is_equal_to(ServiceUri {
            scheme: "scheme".into(),
            address: "address".into(),
            spec: Default::default(),
            path: "/path".into(),
            params: Default::default(),
        });
        assert_that!(uri.to_string()).is_equal_to(str.to_string());

        let str = "scheme://address/";
        let uri = ServiceUri::from_str(str).unwrap();
        assert_that!(uri).is_equal_to(ServiceUri {
            scheme: "scheme".into(),
            address: "address".into(),
            spec: Default::default(),
            path: "/".into(),
            params: Default::default(),
        });
        assert_that!(uri.to_string()).is_equal_to(str.to_string());

        let str = "scheme://address";
        let uri = ServiceUri::from_str(str).unwrap();
        assert_that!(uri).is_equal_to(ServiceUri {
            scheme: "scheme".into(),
            address: "address".into(),
            spec: Default::default(),
            path: "".into(),
            params: Default::default(),
        });
        assert_that!(uri.to_string()).is_equal_to(str.to_string());
    }
}
