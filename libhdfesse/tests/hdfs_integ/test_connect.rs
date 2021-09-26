use libhdfesse::path::{Path, UriResolver};

#[test]
fn test_connect() -> Result<(), Box<dyn std::error::Error>> {
    let config = crate::common::get_default_config();
    let default_fs = Path::new(config.default_fs.as_ref().unwrap())?;

    let dfs = default_fs.host().unwrap();

    let ns = match config.services.iter().find(|s| s.name.as_ref() == dfs) {
        Some(x) => x,
        None => {
            panic!("Service {:?} not found", dfs);
        }
    };
    let client =
        libhdfesse::ha_rpc::HaHdfsConnection::new(ns, libhdfesse::rpc::SimpleConnector {})?;

    let service = libhdfesse::service::ClientNamenodeService::new(client);
    let resolve = UriResolver::new(&dfs, service.get_user(), None, None)?;
    let mut hdfs = libhdfesse::fs::Hdfs::new(service, resolve);
    let _ = hdfs.get_status()?;
    Ok(())
}
