/*
   Copyright 2021 Ivan Boldyrev

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
use std::convert::{TryFrom, TryInto};
use std::fmt::Display;
use std::str::Utf8Error;

use thiserror::Error;
use uriparse::{Scheme, SchemeError, URIError, URIReference, URIReferenceError, URI};

// https://url.spec.whatwg.org/#path-percent-encode-set
const PATH_PERCENT_ENCODE_SET: &percent_encoding::AsciiSet = &percent_encoding::CONTROLS
    // query percent-encode set
    .add(b' ')
    .add(b'"')
    .add(b'#')
    .add(b'<')
    .add(b'>')
    // path per se
    .add(b'?')
    .add(b'`')
    .add(b'{')
    .add(b'}');

#[derive(Debug, Error)]
pub enum PathError {
    #[error(transparent)]
    BaseError(URIError),
    #[error(transparent)]
    PartError(URIReferenceError),
}

/**
 * Convert HDFS path to an URIReference.  They look similar, but HDFS
 * path is never percent-encoded, and URI/URIReference is always
 * percent-encoded.
 *
 * Moreover, Java's URL/URL does percent-encoding at multi-argument
 * constructs, and uriparse does not.
 *
 * Please note that we do not percent-encode the authority part
 * (username, password, host), as otherwise you will not be able to
 * use arbitrary user and password.  This seems to be incompatible
 * with original HDFS.
 *
 * We also do not detect Windows path (yet).
 *
 * This function follows org/apache/haddop/fs/Path.java from Hadoop.
 */
pub fn hdfs_path_to_uri(path: &str) -> Result<URIReference<'static>, PathError> {
    // I wish split_once was stable.
    let mut scheme_split = path.splitn(2, ':');
    let maybe_scheme = scheme_split.next().unwrap();
    let maybe_rest = scheme_split.next();

    let (scheme, rest) = if maybe_scheme.contains('/') {
        (None, path)
    } else {
        match maybe_rest {
            Some(rest) => (Some(maybe_scheme), rest),
            None => (None, path),
        }
    };

    let (authority, path) = if let Some(after) = rest.strip_prefix("//") {
        let mut authority_split = after.splitn(2, '/');
        let authority = authority_split.next().unwrap();
        let maybe_path = authority_split.next();
        (Some(authority), maybe_path.unwrap_or("/"))
    } else {
        (None, rest)
    };

    let percent_path =
        percent_encoding::utf8_percent_encode(path, PATH_PERCENT_ENCODE_SET).to_string();
    let mut uri_builder = URIReference::builder().with_path(
        percent_path
            .as_str()
            .try_into()
            .map_err(|e: uriparse::PathError| PathError::PartError(e.into()))?,
    );
    if let Some(scheme) = scheme {
        uri_builder = uri_builder.with_scheme(Some(
            scheme
                .try_into()
                .map_err(|e: SchemeError| PathError::PartError(e.into()))?,
        ));
    };
    if let Some(authority) = authority {
        // authority should be escaped in the input.  Otherwise, you
        // will not be able to use user/password that contains any of
        // "@/:".
        uri_builder = uri_builder.with_authority(Some(
            authority
                .try_into()
                .map_err(|e: uriparse::AuthorityError| PathError::PartError(e.into()))?,
        ));
    }

    let mut uriref = uri_builder.build().map_err(PathError::PartError)?;
    uriref.normalize();
    Ok(uriref.into_owned())
}

/**
 * Return percent-decoded part of the URI reference.
 */
pub fn uri_path_to_hdfs_path(uriref: &URIReference<'_>) -> Result<String, Utf8Error> {
    percent_encoding::percent_decode_str(&uriref.path().to_string())
        .decode_utf8()
        .map(Into::into)
}

pub struct UriResolver {
    pub(crate) default_uri: URI<'static>,
}

impl UriResolver {
    pub fn new<'a>(
        default_host: &'a str,
        default_user: &'a str,
        default_password: Option<&'a str>,
        default_prefix: Option<&'a str>,
    ) -> Result<Self, PathError> {
        let mut default_path = uriparse::Path::<'a>::try_from(default_prefix.unwrap_or("/user"))
            .map_err(|e| PathError::BaseError(e.into()))?;
        default_path
            .push(default_user)
            .map_err(|e| PathError::BaseError(e.into()))?;
        let percent_path = percent_encoding::utf8_percent_encode(
            &default_path.to_string(),
            PATH_PERCENT_ENCODE_SET,
        )
        .to_string();

        let mut default_uri = URI::builder()
            .with_scheme(
                Scheme::try_from("hdfs")
                    .map_err(|e: SchemeError| PathError::BaseError(e.into()))?,
            )
            .with_authority(Some(
                uriparse::Authority::from_parts(
                    Some(default_user),
                    default_password,
                    default_host,
                    None,
                )
                .map_err(|e| PathError::BaseError(e.into()))?,
            ))
            .with_path(
                percent_path
                    .as_str()
                    .try_into()
                    .map_err(|e: uriparse::PathError| PathError::BaseError(e.into()))?,
            )
            .build()
            .map_err(PathError::BaseError)?
            .into_owned();
        default_uri.normalize();
        Ok(Self { default_uri })
    }

    pub fn resolve<'a>(&'a self, path: &'a str) -> Result<URI<'a>, PathError> {
        let uri = hdfs_path_to_uri(path)?;
        Ok(if uri.is_relative_path_reference() {
            let mut res = self.default_uri.clone();
            let mut res_path = res.path().to_borrowed();
            for part in uri.path().segments() {
                res_path
                    .push(part.clone())
                    .map_err(|e| PathError::PartError(e.into()))?;
            }
            res_path.normalize(false);
            let res_path = res_path.into_owned();
            // TODO: that's wrong, actually, as this happen because of
            // wrong part.  This function cannot return BaseError at
            // all.
            res.set_path(res_path).map_err(PathError::BaseError)?;
            res.into_owned()
        } else if uri.is_absolute_path_reference() {
            let mut res = self.default_uri.clone();
            res.set_path(uri.into_parts().2)
                .map_err(PathError::BaseError)?;
            res.into_owned()
        } else {
            let mut res = self.default_uri.clone();
            // TODO fragment can present.
            let (mb_scheme, mb_auth, path, _mb_query, _mb_fragment) = uri.into_parts();
            if let Some(scheme) = mb_scheme {
                res.set_scheme(scheme).map_err(PathError::BaseError)?;
            }
            if let Some(mut auth) = mb_auth {
                if auth.username().is_none() {
                    auth.set_username(self.default_uri.username().cloned())
                        .map_err(|e| PathError::PartError(e.into()))?;
                }
                if let uriparse::Host::RegisteredName(rn) = auth.host() {
                    if rn.as_str().is_empty() {
                        // The default URL alwasy has host, so it is
                        // always Some(..).
                        auth.set_host(self.default_uri.host().unwrap().to_owned())
                            .map_err(|e| PathError::PartError(e.into()))?;
                        auth.set_port(self.default_uri.port());
                    }
                }
                res.set_authority(Some(auth))
                    .map_err(PathError::BaseError)?;
            }
            res.set_path(path).map_err(PathError::BaseError)?;
            // TODO query shouldn't present; should we
            // return error if they do present?
            res
        })
    }
}

pub struct Path<'a> {
    path: URIReference<'a>,
}

impl<'a> Path<'a> {
    pub fn new(path: &'a str) -> Result<Self, PathError> {
        // TODO hdfs_path_to_uri should be rewritten with Cows everywhere,
        // as otherwise we always get Path<'static>.
        hdfs_path_to_uri(path).map(|p| Path { path: p })
    }

    pub fn join(&self, more: &'a str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let new_path = self.path.clone();
        if more.is_empty() {
            return Ok(Path { path: new_path });
        }

        // TODO uriparse::RelativeReference.
        let more_uri = hdfs_path_to_uri(more)?;

        let (scheme, authority, mut pre_path, query, fragment) = new_path.into_parts();

        for more_segment in more_uri.path().segments() {
            pre_path.push(more_segment.clone())?;
        }
        pre_path.normalize(true);

        // Remove empty segments to avoid path//like///this.
        //
        // TODO one could remove empty segments for input paths
        // instead: in the hdfs_path_to_uri.
        let path = if pre_path.segments().iter().any(|seg| seg.is_empty()) {
            let mut path = uriparse::Path::try_from("")?; // Well...  I do not expect it to fail.
            path.set_absolute(pre_path.is_absolute());
            for seg in pre_path.segments() {
                if !seg.is_empty() {
                    path.push(seg.clone())?;
                }
            }
            path
        } else {
            pre_path
        };

        Ok(Path {
            path: URIReference::from_parts(scheme, authority, path, query, fragment)?,
        })
    }

    pub fn into_owned(self) -> Path<'static> {
        Path {
            path: self.path.into_owned(),
        }
    }
}

impl<'a> Display for Path<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Well, the unwrap_or_else should never execute.
        f.write_str(&uri_path_to_hdfs_path(&self.path).unwrap_or_else(|_| self.path.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_resolver_new() {
        let res = UriResolver::new("myhost", "myself", None, None).unwrap();
        assert_eq!(
            res.default_uri.to_string(),
            "hdfs://myself@myhost/user/myself"
        );
    }

    #[test]
    fn test_resolver_new_with_password() {
        let res = UriResolver::new("myhost", "myself", Some("mypwd"), None).unwrap();
        assert_eq!(
            res.default_uri.to_string(),
            "hdfs://myself:mypwd@myhost/user/myself"
        );
    }

    #[test]
    fn test_resolver_new_with_prefix() {
        let res = UriResolver::new("myhost", "myself", None, Some("users")).unwrap();
        assert_eq!(
            res.default_uri.to_string(),
            "hdfs://myself@myhost/users/myself"
        );
    }

    #[test]
    fn test_resolver_new_error1() {
        assert!(matches!(
            UriResolver::new("myh ost", "myself", None, None),
            Err(_)
        ));
    }

    #[test]
    fn test_resolver_new_error2() {
        assert!(matches!(
            // TODO: should we urlescape host and user?
            UriResolver::new("myhost", "my self", None, None),
            Err(_)
        ));
    }

    #[test]
    fn test_resolve_relative() {
        let res = UriResolver::new("myhost", "myself", None, None).unwrap();
        assert_eq!(
            res.resolve("test").unwrap().to_string(),
            "hdfs://myself@myhost/user/myself/test"
        );
    }

    #[test]
    fn test_resolve_relative_dot() {
        let res = UriResolver::new("myhost", "myself", None, None).unwrap();
        assert_eq!(
            res.resolve("./test").unwrap().to_string(),
            "hdfs://myself@myhost/user/myself/test"
        );
    }

    #[test]
    fn test_resolve_relative_dotdot() {
        let res = UriResolver::new("myhost", "myself", None, None).unwrap();
        assert_eq!(
            res.resolve("../test").unwrap().to_string(),
            "hdfs://myself@myhost/user/test"
        );
    }

    #[test]
    fn test_resolve_absolute() {
        let res = UriResolver::new("myhost", "myself", None, None).unwrap();
        assert_eq!(
            res.resolve("/test").unwrap().to_string(),
            "hdfs://myself@myhost/test"
        );
    }

    #[test]
    fn test_resolve_absolute2() {
        let res = UriResolver::new("myhost", "myself", None, None).unwrap();
        assert_eq!(
            res.resolve("//test/me").unwrap().to_string(),
            "hdfs://myself@test/me"
        );
    }

    #[test]
    fn test_resolve_absolute3() {
        let res = UriResolver::new("myhost", "myself", None, None).unwrap();
        assert_eq!(
            res.resolve("///test").unwrap().to_string(),
            "hdfs://myself@myhost/test"
        );
    }

    #[test]
    fn test_resolve_host_nouser() {
        let res = UriResolver::new("myhost", "myself", None, None).unwrap();
        assert_eq!(
            res.resolve("//host/test").unwrap().to_string(),
            "hdfs://myself@host/test"
        );
    }

    #[test]
    fn test_resolve_spaces() {
        let res = UriResolver::new("myhost", "myself", None, None).unwrap();
        assert_eq!(
            res.resolve("/te st").unwrap().to_string(),
            "hdfs://myself@myhost/te%20st"
        );
    }

    #[test]
    fn test_resolve_full() {
        let res = UriResolver::new("myhost", "myself", None, None).unwrap();
        assert_eq!(
            res.resolve("hdfs://test:pass@host/test")
                .unwrap()
                .to_string(),
            "hdfs://test:pass@host/test"
        );
    }

    #[test]
    fn test_path_new_absolute() {
        let path = Path::new("/abs/path".into()).unwrap();
        assert_eq!(path.to_string(), "/abs/path");
    }

    #[test]
    fn test_path_new_space() {
        let path = Path::new("/abs/pa th".into()).unwrap();
        assert_eq!(path.to_string(), "/abs/pa th");
    }

    #[test]
    fn test_path_new_rel() {
        let path = Path::new("./path".into()).unwrap();
        assert_eq!(path.to_string(), "path");
    }

    #[test]
    fn test_path_new_dotdot() {
        let path = Path::new("../path".into()).unwrap();
        assert_eq!(path.to_string(), "../path");
    }

    #[test]
    fn test_path_join() {
        let path = Path::new("../path".into()).unwrap();
        assert_eq!(path.join("test/me").unwrap().to_string(), "../path/test/me");
    }

    #[test]
    fn test_path_join_absolute() {
        let path = Path::new("/path".into()).unwrap();
        assert_eq!(path.join("test/me").unwrap().to_string(), "/path/test/me");
    }

    #[test]
    fn test_path_join_slash() {
        let path = Path::new("../path/".into()).unwrap();
        assert_eq!(path.join("test/me").unwrap().to_string(), "../path/test/me");
    }

    #[test]
    fn test_path_join_dot() {
        let path = Path::new("../path".into()).unwrap();
        assert_eq!(
            path.join("./test/me").unwrap().to_string(),
            "../path/test/me"
        );
    }

    #[test]
    fn test_path_join_dot_dot() {
        let path = Path::new("../path".into()).unwrap();
        assert_eq!(
            path.join("././test/me").unwrap().to_string(),
            "../path/test/me"
        );
    }

    #[test]
    fn test_path_join_dotdot() {
        let path = Path::new("../path".into()).unwrap();
        assert_eq!(path.join("../test/me").unwrap().to_string(), "../test/me");
    }

    #[test]
    fn test_path_join_abs() {
        let path = Path::new("../path".into()).unwrap();
        assert_eq!(
            path.join("/test/me").unwrap().to_string(),
            "../path/test/me"
        );
    }

    #[test]
    fn test_path_join_empty() {
        let path = Path::new("../path".into()).unwrap();
        assert_eq!(path.join("").unwrap().to_string(), "../path");
    }
}