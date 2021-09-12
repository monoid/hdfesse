use libhdfesse::{
    hdconfig::{get_auto_config, Config, HDFS_CONFIG},
    path::{Path, UriResolver},
};

const HADOOP_HOST: &str = "hadoop";
const HADOOP_DEFAULT: &str = "default2";

// standard testing config
fn get_config() -> Config {
    get_auto_config(&HDFS_CONFIG)
}

#[test]
fn test_connect() -> Result<(), Box<dyn std::error::Error>> {
    let config = get_config();
    let default_fs = Path::new(
        config
            .default_fs
            .as_ref()
            .expect("config without defaultFS is not supported; perhaps, config is not found"),
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
