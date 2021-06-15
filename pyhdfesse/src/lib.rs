use pyo3::create_exception;
use pyo3::prelude::*;

create_exception!(pyhdfesse, HdfsError, pyo3::exceptions::PyException);

#[pyclass]
struct Hdfs {
    nested: libhdfesse::fs::Hdfs,
}

impl Hdfs {
    fn create_default() -> Result<Self, Box<dyn std::error::Error>> {
        let config = libhdfesse::hdconfig::get_auto_config(&libhdfesse::hdconfig::HDFS_CONFIG);

        let default_fs = libhdfesse::path::Path::new(
            &config
                .default_fs
                .as_ref()
                .ok_or_else(
                    || HdfsError::new_err("config without defaultFs is not supported; perhaps, Hadoop config is not found"),
                )?
        )?;

        let dfs = default_fs.host().ok_or_else(|| {
            HdfsError::new_err("defaultFS has to have a host, otherwise not supported")
        })?;

        let ns = config
            .services
            .iter()
            .find(|s| s.name.as_ref() == dfs.as_str())
            .ok_or_else(|| HdfsError::new_err(format!("Service {:?} not found", dfs)))?;
        let client =
            libhdfesse::ha_rpc::HaHdfsConnection::new(&ns, libhdfesse::rpc::SimpleConnector {})?;

        let service = libhdfesse::service::ClientNamenodeService::new(client);
        let resolve = libhdfesse::path::UriResolver::new("STUB", service.get_user(), None, None)?;
        Ok(Hdfs {
            nested: libhdfesse::fs::Hdfs::new(service, resolve),
        })
    }
}

#[pymethods]
impl Hdfs {
    #[new]
    fn new() -> PyResult<Self> {
        Self::create_default().map_err(|e| HdfsError::new_err(e.to_string()))
    }
}

#[pymodule]
fn pyhdfesse(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Hdfs>()?;
    Ok(())
}
