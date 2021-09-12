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
use std::{collections::HashMap, fmt::Debug, ops::Deref};
use thiserror::Error;
use tracing::{debug, info, warn};
use xml::reader::{EventReader, XmlEvent};

#[cfg(feature = "serde_support")]
use serde::{Deserialize, Serialize};

/// Try to get path to config from the environment.  It is either from
/// HADOOP_CONF_DIR default variable or "/etc/hadoop/conf" directory.
pub fn get_config_path(path: &str) -> PathBuf {
    let conf_dir = std::env::var_os("HADOOP_CONF_DIR").unwrap_or_else(|| "/etc/hadoop/conf".into());
    PathBuf::from(conf_dir).join(path)
}

#[derive(Debug)]
pub struct ConfigPathGroup {
    paths: Vec<&'static str>,
}

impl ConfigPathGroup {
    fn merge<I: Iterator<Item = &'static str>>(paths: &mut Vec<&'static str>, new_paths: I) {
        for path in new_paths {
            if !paths.contains(&path) {
                paths.push(path);
            }
        }
    }
    pub fn from_paths<I: Iterator<Item = &'static str>>(new_paths: I) -> Self {
        let mut paths_res = vec![];
        Self::merge(&mut paths_res, new_paths);
        Self { paths: paths_res }
    }

    pub fn from_parent_and_paths<I: Iterator<Item = &'static str>>(
        parent: &ConfigPathGroup,
        new_paths: I,
    ) -> Self {
        let mut paths_res = parent.paths.clone();
        Self::merge(&mut paths_res, new_paths);
        Self { paths: paths_res }
    }

    pub fn iter(&self) -> impl Iterator<Item = &str> {
        self.paths.iter().copied()
    }
}

lazy_static::lazy_static! {
    pub static ref HADOOP_CONFIG: ConfigPathGroup =
        ConfigPathGroup::from_paths([
            "core-default.xml", "core-site.xml", "hadoop-site.xml"
        ].iter().cloned());

    pub static ref HDFS_CONFIG: ConfigPathGroup = ConfigPathGroup::from_parent_and_paths(
        &HADOOP_CONFIG,
        [
            "hdfs-default.xml",
            "hdfs-rbf-default.xml",
            "hdfs-site.xml",
            "hdfs-rbf-site.xml",
        ].iter().cloned(),
    );
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

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn get(&self, key: &str) -> Option<&ConfigData> {
        self.0.get(key)
    }

    #[tracing::instrument]
    pub fn insert<T: Into<Box<str>> + AsRef<str> + Debug>(
        &mut self,
        key: T,
        val: T,
        final_: bool,
    ) -> bool {
        let old_final = match self.0.get(key.as_ref()) {
            Some(data) => {
                info!(
                    key = key.as_ref(),
                    val = val.as_ref(),
                    "skipping because it is already exists and final"
                );
                data.is_final()
            }
            None => false,
        };
        if !old_final {
            self.0.insert(key.into(), ConfigData::new(val, final_));
        }
        old_final
    }

    #[tracing::instrument(skip(r))]
    pub fn merge_config<R: Read>(&mut self, r: R, config_path: &Path) -> Result<(), ConfigError> {
        let parser = EventReader::new(r);

        let mut elt = None;
        let mut key = None;
        let mut val: Option<String> = None;
        let mut final_: Option<String> = None;

        for e in parser {
            match e.map_err(|e| ConfigError::Xml(e, config_path.to_owned()))? {
                XmlEvent::StartElement { name, .. } => {
                    let name = name.to_string();
                    if name == "property" {
                        key = None;
                        val = None;
                        final_ = None;
                    }
                    elt = Some(name);
                }
                XmlEvent::EndElement { name } => {
                    if name.to_string() == "property" {
                        if let Some((k, v)) = key.take().zip(val.take()) {
                            self.insert(
                                k,
                                v,
                                final_.as_ref().map(|v| v == "true").unwrap_or(false),
                            );
                        }
                    }
                    elt = None;
                }
                XmlEvent::Characters(text) => {
                    if elt.as_deref() == Some("name") {
                        key = Some(text);
                    } else if elt.as_deref() == Some("value") {
                        val = Some(text);
                    } else if elt.as_deref() == Some("final") {
                        final_ = Some(text)
                    }
                }
                _ => {}
            }
        }

        Ok(())
    }
}

impl Default for ConfigMap {
    fn default() -> Self {
        Self::new()
    }
}

/**
Load the XML Hadoop/HDFS configs from a config groups, and return
ConfigMap.
*/
#[tracing::instrument]
pub fn load_config(config_path_group: &ConfigPathGroup) -> ConfigMap {
    let mut config_map = ConfigMap::new();

    let config_paths = config_path_group.iter().map(get_config_path);

    for config_path in config_paths {
        debug!("merging config file {:?}", config_path);
        let mut buf = io::BufReader::new(
            match std::fs::File::open(&config_path)
                .map_err(|e| ConfigError::Io(e, config_path.to_owned()))
            {
                Ok(f) => f,
                Err(e) => {
                    debug!("failed to open config file: {:?}: {:?}", config_path, e);
                    continue;
                }
            },
        );

        match config_map.merge_config(&mut buf, &config_path) {
            Ok(_) => {
                debug!("config file {:?} successfully loaded", config_path);
            }
            Err(e) => {
                warn!("failed to load config file {:?}: {:?}", config_path, e);
            }
        }
    }
    config_map
}

#[derive(Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde_support", derive(Deserialize, Serialize))]
pub struct NamenodeConfig {
    pub name: Box<str>,
    // We do not use materialized socket address because
    // name resolving may change.
    pub rpc_address: Box<str>,
    pub servicerpc_address: Box<str>,
}

#[derive(Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde_support", derive(Deserialize, Serialize))]
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

    rpc.map(|rpc| {
        let servicerpc = servicerpc.unwrap_or_else(|| rpc);
        NamenodeConfig {
            name: namenode.into(),
            rpc_address: rpc.deref().into(),
            servicerpc_address: servicerpc.deref().into(),
        }
    })
}

#[cfg_attr(feature = "serde_support", derive(Deserialize, Serialize))]
pub struct Config {
    pub default_fs: Option<Box<str>>,
    pub services: Vec<NameserviceConfig>,
}

impl Config {
    pub fn auto() -> Self {
        get_auto_config(&HDFS_CONFIG)
    }
}

/// Get useful data as a config object.
pub fn parse_config(conf: &ConfigMap) -> Config {
    let mut services = vec![];

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
        services.push(serv);
    }

    let default_fs = conf.get("fs.defaultFS").map(|x| x.value.trim().into());

    Config {
        default_fs,
        services,
    }
}

pub fn get_auto_config(config_path_group: &ConfigPathGroup) -> Config {
    parse_config(&load_config(config_path_group))
}

#[cfg(test)]
mod tests {
    use io::Cursor;
    use itertools::Itertools;
    use std::error::Error;

    use super::*;

    #[test]
    fn test_config_path_group() {
        let group = ConfigPathGroup::from_paths(["a", "b", "c", "b", "c"].iter().cloned());
        assert_eq!(group.iter().collect_vec(), vec!["a", "b", "c"]);
    }

    #[test]
    fn test_config_path_group_parent() {
        let parent = ConfigPathGroup::from_paths(["a", "b", "c"].iter().cloned());
        let child =
            ConfigPathGroup::from_parent_and_paths(&parent, ["c", "d", "e", "a"].iter().cloned());
        assert_eq!(child.iter().collect_vec(), vec!["a", "b", "c", "d", "e"]);
    }
    #[test]
    fn test_config_merge_config() -> Result<(), Box<dyn Error>> {
        let data = b"<?xml version=\"1.0\" encoding=\"UTF-8\"?><configuration>
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
<property>
   <name>test0</name>
   <value>value0</value>
   <final>true</final>
</property>
<property>
   <value>value6</value>
</property>
<property>
   <name>test5</name>
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
        assert!(config.get("test6").is_none());
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
