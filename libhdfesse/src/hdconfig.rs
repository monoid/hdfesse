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
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::{collections::HashMap, ops::Deref};
use thiserror::Error;
use xml::reader::{EventReader, XmlEvent};

/// Try to get path to config from the environment.  It is the
/// "hdfs-site.xml" either from HADOOP_CONF_DIR default variable or
/// "/etc/hadoop/conf" directory.
pub fn get_config_path() -> PathBuf {
    let conf_dir = std::env::var_os("HADOOP_CONF_DIR").unwrap_or_else(|| "/etc/hadoop/conf".into());
    PathBuf::from(conf_dir).join("hdfs-site.xml")
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to open config {:?}: {:?}", .1, .0)]
    Io(io::Error, PathBuf),
    // TODO: xml Error has ErrorKind::Io(std::io::Error).
    #[error("failed to read config {:?}: {:?}", .1, .0)]
    Xml(xml::reader::Error, PathBuf),
}

#[derive(Clone, Debug)]
pub struct ConfigData {
    value: Box<str>,
    final_: bool,
}

impl ConfigData {
    pub fn new<T: Into<Box<str>>>(value: T, final_: bool) -> Self {
        Self {
            value: value.into(),
            final_,
        }
    }

    pub fn value(&self) -> &str {
        &self.value
    }

    pub fn is_final(&self) -> bool {
        self.final_
    }
}

impl Deref for ConfigData {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

#[derive(Clone, Debug)]
pub struct ConfigMap(HashMap<Box<str>, ConfigData>);

impl ConfigMap {
    pub fn new() -> Self {
        Self(Default::default())
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn get(&self, key: &str) -> Option<&ConfigData> {
        self.0.get(key)
    }

    pub fn insert<T: Into<Box<str>> + AsRef<str>>(&mut self, key: T, val: T, final_: bool) -> bool {
        let old_final = match self.0.get(key.as_ref()) {
            Some(data) => data.is_final(),
            None => false,
        };
        if !old_final {
            self.0.insert(key.into(), ConfigData::new(val, final_));
        }
        old_final
    }

    pub fn merge_config<'c, R: Read>(
        &mut self,
        r: R,
        config_path: &Path,
    ) -> Result<(), ConfigError> {
        let parser = EventReader::new(r);

        let mut elt = None;
        let mut key = None;
        let mut val: Option<String> = None;
        let mut final_: Option<String> = None;

        for e in parser {
            match e.map_err(|e| ConfigError::Xml(e, config_path.to_owned()))? {
                XmlEvent::StartElement { name, .. } => {
                    elt = Some(name.to_string());
                }
                XmlEvent::EndElement { name } => {
                    if name.to_string() == "property" {
                        if let Some((k, v)) = key.take().zip(val.take()) {
                            self.insert(k, v, final_.map(|v| v == "true").unwrap_or(false));
                        }
                        key = None;
                        val = None;
                        final_ = None;
                    }
                    elt = None;
                }
                XmlEvent::Characters(text) => {
                    if elt.as_deref() == Some("name") {
                        key = Some(text.into());
                    } else if elt.as_deref() == Some("value") {
                        val = Some(text.into());
                    } else if elt.as_deref() == Some("final") {
                        final_ = Some(text.into())
                    }
                }
                _ => {}
            }
        }

        Ok(())
    }
}

/**
Load the XML Hadoop/HDFS config and return properties' name/values as
dict.  It performs only minimal validation.
*/
pub fn load_config<'p, Paths: Iterator<Item = &'p Path>>(
    config_paths: Paths,
) -> Result<ConfigMap, ConfigError> {
    let mut config_map = ConfigMap::new();

    for config_path in config_paths {
        let mut buf = io::BufReader::new(
            std::fs::File::open(config_path)
                .map_err(|e| ConfigError::Io(e, config_path.to_owned()))?,
        );

        config_map.merge_config(&mut buf, config_path)?;
    }
    Ok(config_map)
}

#[derive(Debug, PartialEq, Eq)]
pub struct NamenodeConfig {
    pub name: Box<str>,
    // We do not use materialized socket address because
    // name resolving may change.
    pub rpc_address: Box<str>,
    pub servicerpc_address: Box<str>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct NameserviceConfig {
    pub name: Box<str>,
    pub rpc_nodes: Vec<NamenodeConfig>,
}

fn parse_namenode(conf: &ConfigMap, namenode: &str, nameservice: &str) -> Option<NamenodeConfig> {
    let rpc_key = format!("dfs.namenode.rpc-address.{}.{}", nameservice, namenode);
    let servicerpc_key = format!(
        "dfs.namenode.servicerpc-address.{}.{}",
        nameservice, namenode
    );

    let rpc = conf.get(rpc_key.as_str());
    let servicerpc = conf.get(servicerpc_key.as_str());

    rpc.zip(servicerpc).map(|(rpc, servicercp)| NamenodeConfig {
        name: namenode.into(),
        rpc_address: rpc.deref().into(),
        servicerpc_address: servicercp.deref().into(),
    })
}

/// Return named NameserviceConfig pairs.  First nameservice config is
/// the default one, isn't it?
pub fn parse_config(conf: &ConfigMap) -> Vec<NameserviceConfig> {
    let mut res = vec![];

    for name in conf
        .get("dfs.nameservices")
        .map(Deref::deref)
        .unwrap_or("")
        .split(',')
    {
        let namenodes = conf
            .get(format!("dfs.ha.namenodes.{}", name).as_str())
            .map(Deref::deref)
            .unwrap_or("");

        let serv = NameserviceConfig {
            name: name.into(),
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
    fn test_config_merge_config() -> Result<(), Box<dyn Error>> {
        let data = b"<?xml version=\"1.0\" encoding=\"UTF-8\"?><configuration>
<property>
   <name>test0</name>
   <value>value0</value>
   <final>true</final>
</property>
<property>
   <name>test1</name>
   <value>value1</value>
   <final>false</final>
</property>
<property>
   <name>test2</name>
   <value>value2</value>
   <final></final>
</property>
<property>
   <name>test3</name>
   <value>value3</value>
</property>
</configuration>";
        let mut config = ConfigMap::new();
        config.merge_config(&mut Cursor::new(data), Path::new("/test/me"))?;

        assert_eq!(config.len(), 4);

        assert_eq!(config.get("test0").map(Deref::deref), Some("value0"));
        assert_eq!(config.get("test0").map(ConfigData::is_final), Some(true));

        assert_eq!(config.get("test1").map(Deref::deref), Some("value1"));
        assert_eq!(config.get("test1").map(ConfigData::is_final), Some(false));
        assert_eq!(config.get("test2").map(Deref::deref), Some("value2"));
        assert_eq!(config.get("test2").map(ConfigData::is_final), Some(false));
        assert_eq!(config.get("test3").map(Deref::deref), Some("value3"));
        assert_eq!(config.get("test3").map(ConfigData::is_final), Some(false));
        Ok(())
    }

    #[test]
    fn test_merge_config_overwrite() -> Result<(), Box<dyn Error>> {
        let data0 = b"<?xml version=\"1.0\" encoding=\"UTF-8\"?><configuration>
<property>
   <name>test0</name>
   <value>value0</value>
   <final>true</final>
</property>
<property>
   <name>test1</name>
   <value>value1</value>
</property>
</configuration>";
        let data1 = b"<?xml version=\"1.0\" encoding=\"UTF-8\"?><configuration>
<property>
   <name>test0</name>
   <value>othervalue0</value>
</property>
<property>
   <name>test1</name>
   <value>othervalue1</value>
</property>
</configuration>";
        let mut config = ConfigMap::new();
        config.merge_config(&mut Cursor::new(data0), Path::new("/test/me1"))?;
        config.merge_config(&mut Cursor::new(data1), Path::new("/test/me2"))?;

        assert_eq!(config.get("test0").map(Deref::deref), Some("value0"));
        assert_eq!(config.get("test1").map(Deref::deref), Some("othervalue1"));

        Ok(())
    }

    #[test]
    fn test_config_merge_config_malformed() {
        let data = b"<?xml version=\"1.0\" encoding=\"UTF-8\"?><configuration><property><name>test</name><value>value0</value></configuration>";
        let mut config = ConfigMap::new();

        match config.merge_config(&mut Cursor::new(data), Path::new("/test/me")) {
            Err(ConfigError::Xml(_, path)) => assert_eq!(path.to_str(), Some("/test/me")),
            _ => assert!(false, "Expecint XML error"),
        }
    }
}
