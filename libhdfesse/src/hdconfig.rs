use std::collections::HashMap;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use thiserror::Error;
use xml::reader::{EventReader, XmlEvent};

/// Try to get path to config from the environment.  It is the
/// "hdfs-site.xml" either from HADOOP_CONF_DIR default variable or
/// "/etc/hadoop/conf" directory.
pub fn get_config_path() -> PathBuf {
    let conf_dir = std::env::var("HADOOP_CONF_DIR");
    let path = match conf_dir {
        Ok(dir) => PathBuf::from(dir),
        Err(std::env::VarError::NotUnicode(raw_string)) => {
            // It hard to expect invalid Unicode these days, perhaps,
            // we should jus tfail, but anyway.
            // TODO warning.
            PathBuf::from(raw_string)
        }
        _ => {
            // TODO warning: using default path.
            PathBuf::from("/etc/hadoop/conf".to_owned())
        }
    };

    path.join("hdfs-site.xml".to_owned())
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to open config {:?}: {:?}", .1, .0)]
    Io(io::Error, PathBuf),
    // TODO: xml Error has ErrorKind::Io(std::io::Error).
    #[error("failed to read config {:?}: {:?}", .1, .0)]
    Xml(xml::reader::Error, PathBuf),
}

/// Load the XML Hadoop/HDFS config and return properties' name/values as dict.
/// It performs only minimal validation.
// TODO: read from string/bytes for tests.
pub fn load_config_as_dict(config_path: &Path) -> Result<HashMap<String, String>, ConfigError> {
    let mut buf = io::BufReader::new(
        std::fs::File::open(config_path).map_err(|e| ConfigError::Io(e, config_path.to_owned()))?,
    );

    read_config_as_dict(&mut buf, config_path)
}

pub fn read_config_as_dict<R: Read>(
    r: R,
    config_path: &Path,
) -> Result<HashMap<String, String>, ConfigError> {
    let parser = EventReader::new(r);

    let mut elt = None;
    let mut key = None;
    let mut val = None;

    let mut res = HashMap::new();

    for e in parser {
        match e.map_err(|e| ConfigError::Xml(e, config_path.to_owned()))? {
            XmlEvent::StartElement { name, .. } => {
                elt = Some(name.to_string());
            }
            XmlEvent::EndElement { name } => {
                if name.to_string() == "property" {
                    if let Some((k, v)) = key.take().zip(val.take()) {
                        res.insert(k, v);
                    }
                }
                elt = None;
            }
            XmlEvent::Characters(text) => {
                if elt.as_deref() == Some("name") {
                    key = Some(text);
                } else if elt.as_deref() == Some("value") {
                    val = Some(text);
                }
            }
            _ => {}
        }
    }

    Ok(res)
}

#[derive(Debug, PartialEq, Eq)]
pub struct NamenodeConfig {
    pub name: String,
    // We do not use materialized socket address because
    // name resolving may change.
    pub rpc_address: String,
    pub servicerpc_address: String,
}

#[derive(Debug, PartialEq, Eq)]
pub struct NameserviceConfig {
    pub name: String,
    pub rpc_nodes: Vec<NamenodeConfig>,
}

fn parse_namenode(
    conf: &HashMap<String, String>,
    namenode: &str,
    nameservice: &str,
) -> Option<NamenodeConfig> {
    let rpc_key = format!("dfs.namenode.rpc-address.{}.{}", nameservice, namenode);
    let servicerpc_key = format!(
        "dfs.namenode.servicerpc-address.{}.{}",
        nameservice, namenode
    );

    let rpc = conf.get(&rpc_key);
    let servicerpc = conf.get(&servicerpc_key);

    rpc.zip(servicerpc).map(|(rpc, servicercp)| NamenodeConfig {
        name: namenode.to_owned(),
        rpc_address: rpc.clone(),
        servicerpc_address: servicercp.clone(),
    })
}

/// Return named NameserviceConfig pairs.  First nameservice config is
/// the default one, isn't it?
pub fn parse_config(conf: &HashMap<String, String>) -> Vec<NameserviceConfig> {
    let mut res = vec![];

    for name in conf
        .get("dfs.nameservices")
        .map(String::as_str)
        .unwrap_or("")
        .split(',')
    {
        let namenodes = conf
            .get(&format!("dfs.ha.namenodes.{}", name))
            .map(String::as_str)
            .unwrap_or("");

        let serv = NameserviceConfig {
            name: name.to_owned(),
            // We simply ignore incorrect addresses.
            rpc_nodes: namenodes
                .split(',')
                .flat_map(|namenode| parse_namenode(conf, namenode, name))
                .collect(),
        };
        res.push(serv);
    }

    res
}

#[cfg(test)]
mod tests {
    use io::Cursor;
    use std::error::Error;

    use super::*;

    #[test]
    fn test_config_read_as_dict() -> Result<(), Box<dyn Error>> {
        let data = b"<?xml version=\"1.0\" encoding=\"UTF-8\"?><configuration><property><name>test</name><value>value0</value></property></configuration>";
        let parsed = read_config_as_dict(&mut Cursor::new(data), Path::new("/test/me"))?;
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed.get("test"), Some(&"value0".to_owned()));
        Ok(())
    }

    #[test]
    fn test_config_read_as_dict_malformed() {
        let data = b"<?xml version=\"1.0\" encoding=\"UTF-8\"?><configuration><property><name>test</name><value>value0</value></configuration>";
        let parsed = read_config_as_dict(&mut Cursor::new(data), Path::new("/test/me"));
        match parsed {
            Err(ConfigError::Xml(_, path)) => assert_eq!(path.to_str(), Some("/test/me")),
            _ => assert!(false, "Expecint XML error"),
        }
    }
}
