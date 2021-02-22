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
use std::str::Utf8Error;

use uriparse::{URIReference, URI};

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

/**
 * Convert HDFS path to an URIReference.  They look similar, but HDFS
 * path is never URL-escaped, and URI/URIReference is always
 * URL-escaped.
 *
 * Moreover, Java's URL/URL does quoting at multi-argument constructs,
 * and uriparse does not.
 *
 * This function follows org/apache/haddop/fs/shell/PathData.java and org/apache/haddop/fs/Path.java from hadoop.
 */
pub fn hdfs_path_to_uri(path: &str) -> Result<URIReference<'static>, Box<dyn std::error::Error>> {
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
    let mut uri_builder = URIReference::builder().with_path(percent_path.as_str().try_into()?);
    if let Some(scheme) = scheme {
        uri_builder = uri_builder.with_scheme(Some(scheme.try_into()?));
    };
    if let Some(authority) = authority {
        // TODO Actually, host should be escaped too.
        uri_builder = uri_builder.with_authority(Some(authority.try_into()?));
    }

    let mut uriref = uri_builder.build()?;
    uriref.normalize();
    Ok(uriref.into_owned())
}

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
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let mut default_path = uriparse::Path::<'a>::try_from(default_prefix.unwrap_or("/user"))?;
        default_path.push(default_user)?;
        let percent_path = percent_encoding::utf8_percent_encode(
            &default_path.to_string(),
            PATH_PERCENT_ENCODE_SET,
        )
        .to_string();

        let mut default_uri = URI::builder()
            .with_scheme(uriparse::Scheme::Unregistered("hdfs".try_into()?))
            .with_authority(Some(uriparse::Authority::from_parts(
                Some(default_user),
                default_password,
                default_host,
                None,
            )?))
            .with_path(percent_path.as_str().try_into()?)
            .build()?
            .into_owned();
        default_uri.normalize();
        Ok(Self { default_uri })
    }

    pub fn resolve(&self, path: &str) -> Result<URI<'static>, Box<dyn std::error::Error>> {
        let uri = hdfs_path_to_uri(path)?;
        Ok(if uri.is_relative_path_reference() {
            let mut res = self.default_uri.clone();
            let mut res_path = res.path().to_borrowed();
            for part in uri.path().segments() {
                res_path.push(part.clone())?;
            }
            res_path.normalize(false);
            let res_path = res_path.into_owned();
            res.set_path(res_path)?;
            res.into_owned()
        } else if uri.is_absolute_path_reference() {
            let mut res = self.default_uri.clone();
            res.set_path(uri.into_parts().2)?;
            res.into_owned()
        } else {
            let mut res = self.default_uri.clone();
            let (mb_scheme, mb_auth, path, _mb_query, _mb_fragment) = uri.into_parts();
            if let Some(scheme) = mb_scheme {
                res.set_scheme(scheme)?;
            }
            if let Some(mut auth) = mb_auth {
                if auth.username().is_none() {
                    auth.set_username(self.default_uri.username().cloned())?;
                }
                if let uriparse::Host::RegisteredName(rn) = auth.host() {
                    if rn.as_str().is_empty() {
                        // The default URL alwasy has host, so it is
                        // always Some(..).
                        auth.set_host(self.default_uri.host().unwrap().to_owned())?;
                        auth.set_port(self.default_uri.port());
                    }
                }
                res.set_authority(Some(auth))?;
            }
            res.set_path(path)?;
            // TODO query and fragment shouldn't present; should we
            // return error if they do present?
            res.into_owned()
        })
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
}
