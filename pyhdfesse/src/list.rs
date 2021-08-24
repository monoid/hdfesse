use hdfesse_proto::hdfs::HdfsFileStatusProto;
use libhdfesse::fs::HdfsError;
use libhdfesse::{fs::FsError, ha_rpc::HaHdfsConnection, rpc::SimpleConnector};
use pyo3::class::iter::IterNextOutput;
use pyo3::{prelude::*, PyIterProtocol};

#[pyclass]
pub(crate) struct FileInfo {
    name: Vec<u8>,
    size: u64,
}

#[pymethods]
impl FileInfo {
    #[getter(name)]
    fn name(&self) -> PyResult<&[u8]> {
        Ok(&self.name)
    }

    #[getter(name)]
    fn size(&self) -> PyResult<u64> {
        Ok(self.size)
    }
}

impl From<HdfsFileStatusProto> for FileInfo {
    fn from(s: HdfsFileStatusProto) -> Self {
        FileInfo {
            name: Vec::from(s.get_path()),
            size: s.get_length(),
        }
    }
}

#[pyclass]
pub(crate) struct LsIterator {
    it: Box<dyn Iterator<Item=Result<HdfsFileStatusProto, HdfsError>> + Send + Sync + 'static>,
}

#[pyproto]
impl<'p> PyIterProtocol for LsIterator {
    fn __iter__(slf: PyRef<'p, Self>) -> PyRef<'p, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'p, Self>) -> IterNextOutput<FileInfo, ()> {
        match slf.it.next() {
            Some(Ok(v)) => IterNextOutput::Yield(FileInfo::from(v)),
            Some(Err(e)) => {
                let gil = pyo3::Python::acquire_gil();
                let py = gil.python();
                crate::HdfsError::new_err(e.to_string()).restore(py);
                IterNextOutput::Return(())
            }
            None => IterNextOutput::Return(()),
        }
    }
}
