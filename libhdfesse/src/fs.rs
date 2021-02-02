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
use crate::rpc;
use crate::service;
use hdfesse_proto::hdfs::HdfsFileStatusProto;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum FsError {
    #[error("`{0}': No such file or directory")]
    NotFound(String),
    #[error(transparent)]
    Rpc(rpc::RpcError),
}

pub struct HDFS<'a> {
    // TODO make it private after moving here LsGroupiterator.
    pub service: &'a mut service::ClientNamenodeService,
}

impl<'a> HDFS<'a> {
    pub fn new(service: &'a mut service::ClientNamenodeService) -> Self {
        Self { service }
    }

    pub fn get_file_info(&mut self, src: String) -> Result<HdfsFileStatusProto, FsError> {
        self.service
            .getFileInfo(src.clone())
            .map_err(FsError::Rpc)?
            .ok_or(FsError::NotFound(src))
    }
}
