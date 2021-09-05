#[cfg(feature = "integration_test")]
use libhdfesse::{
    hdconfig::{Config, NamenodeConfig, NameserviceConfig},
    path::{Path, UriResolver},
};

#[cfg(feature = "integration_test")]
const HADOOP_HOST: &str = "hadoop";
#[cfg(feature = "integration_test")]
const HADOOP_DEFAULT: &str = "default2";

// standard testing config
#[cfg(feature = "integration_test")]
fn get_config() -> Config {
    Config {
        default_fs: Some(format!("hdfs://{}", HADOOP_DEFAULT).into()),
        services: vec![NameserviceConfig {
            name: HADOOP_DEFAULT.into(),
            rpc_nodes: vec![NamenodeConfig {
                name: HADOOP_HOST.into(),
                rpc_address: format!("{}:9000", HADOOP_HOST).into(),
                servicerpc_address: format!("{}:8040", HADOOP_HOST).into(),
            }],
        }],
    }
}

#[cfg(feature = "integration_test")]
#[test]
fn test_connect() -> Result<(), Box<dyn std::error::Error>> {
    let config = get_config();
    let default_fs = Path::new(
        config
            .default_fs
            .as_ref()
            .expect("config without defaultFs is not supported; perhaps, config is not found"),
    )?;

    let dfs = default_fs
        .host()
        .expect("defaultFS has to have a host, otherwise not supported");

    let ns = match config
        .services
        .iter()
        .find(|s| s.name.as_ref() == dfs.as_str())
    {
        Some(x) => x,
        None => {
            panic!("Service {:?} not found", dfs);
        }
    };
    let client =
        libhdfesse::ha_rpc::HaHdfsConnection::new(ns, libhdfesse::rpc::SimpleConnector {})?;

    let service = libhdfesse::service::ClientNamenodeService::new(client);
    let resolve = UriResolver::new("STUB", service.get_user(), None, None)?;
    let mut hdfs = libhdfesse::fs::Hdfs::new(service, resolve);
    let _ = hdfs.get_status()?;
    Ok(())
}
