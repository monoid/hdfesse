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
use std::{borrow::Cow, fmt::Display};

pub use crate::fs_ls::LsGroupIterator;
use crate::{
    fs_ls::LsIterator,
    path::{Path, PathError, UriResolver},
    rpc::{self, RpcConnection},
    service,
};
use hdfesse_proto::{
    acl::FsPermissionProto,
    hdfs::{HdfsFileStatusProto, HdfsFileStatusProto_FileType},
    ClientNamenodeProtocol::MkdirsRequestProto,
};
use thiserror::Error;

const DEFAULT_DIR_PERM: u32 = 0o777;

#[derive(Debug, Error)]
pub enum FsError {
    #[error("`{0}': Invalid path name")]
    Path(#[from] PathError),
    #[error("`{0}': No such file or directory")]
    NotFound(String),
    #[error(transparent)]
    Rpc(rpc::RpcError),
    #[error("`{0}': Is not a directory")]
    NotDir(String),
    #[error("`{0}': File exists")]
    FileExists(String),
}

#[derive(Debug)]
pub enum HdfsErrorKind {
    Src,
    Dst,
    Op,
}

impl Display for HdfsErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            HdfsErrorKind::Src => "invalid source",
            HdfsErrorKind::Dst => "invalid destination",
            HdfsErrorKind::Op => "failed operation",
        })
    }
}

#[derive(Debug, Error)]
#[error("{}", .source)]
pub struct HdfsError {
    pub kind: HdfsErrorKind,
    pub source: FsError,
}

impl HdfsError {
    pub fn src<E: Into<FsError>>(source: E) -> Self {
        Self {
            kind: HdfsErrorKind::Src,
            source: source.into(),
        }
    }
    pub fn dst<E: Into<FsError>>(source: E) -> Self {
        Self {
            kind: HdfsErrorKind::Dst,
            source: source.into(),
        }
    }
    pub fn op<E: Into<FsError>>(source: E) -> Self {
        Self {
            kind: HdfsErrorKind::Op,
            source: source.into(),
        }
    }
}

pub fn ensure_dir(
    file_info: &HdfsFileStatusProto,
    path: Cow<'_, str>,
    kind: HdfsErrorKind,
) -> Result<(), HdfsError> {
    if file_info.get_fileType() == HdfsFileStatusProto_FileType::IS_DIR {
        Ok(())
    } else {
        Err(HdfsError {
            kind,
            source: FsError::NotDir(path.into_owned()),
        })
    }
}

pub fn ensure_not_exists(
    file_info_result: Result<HdfsFileStatusProto, FsError>,
    path: Cow<'_, str>,
    kind: HdfsErrorKind,
) -> Result<(), HdfsError> {
    match file_info_result {
        Ok(_) => Err(HdfsError {
            kind,
            source: FsError::FileExists(path.into_owned()),
        }),
        Err(FsError::NotFound(_)) => Ok(()),
        Err(source) => Err(HdfsError { kind, source }),
    }
}

pub struct Hdfs<R: RpcConnection> {
    service: service::ClientNamenodeService<R>,
    resolve: UriResolver,
}

impl<R: RpcConnection> Hdfs<R> {
    pub fn new(service: service::ClientNamenodeService<R>, resolve: UriResolver) -> Self {
        Self { service, resolve }
    }

    pub fn get_user(&self) -> &str {
        self.service.get_user()
    }

    pub fn list_status<'s>(
        &'s mut self,
        src: &Path<'_>,
    ) -> Result<impl Iterator<Item = Result<HdfsFileStatusProto, HdfsError>> + 's, HdfsError> {
        let src = self.resolve.resolve_path(src).map_err(HdfsError::src)?;

        ensure_dir(
            &self.get_file_info(&src).map_err(HdfsError::src)?,
            src.to_string().into(),
            HdfsErrorKind::Src,
        )?;

        Ok(
            LsIterator::new(LsGroupIterator::new(&mut self.service, &src))
                .map(|r| r.map_err(HdfsError::op)),
        )
    }

    pub fn get_file_info(&mut self, src: &Path<'_>) -> Result<HdfsFileStatusProto, FsError> {
        let src = self.resolve.resolve_path(src)?;

        self.service
            .getFileInfo(src.to_path_string())
            .map_err(FsError::Rpc)?
            .ok_or_else(|| FsError::NotFound(src.to_path_string()))
    }

    // TODO a sketch; one should check that dst exists or doesn't
    // exist and srcs do exist, etc.
    pub fn rename(&mut self, src: &Path, dst: &Path<'_>) -> Result<(), HdfsError> {
        let src = self.resolve.resolve_path(src).map_err(HdfsError::src)?;
        let dst = self.resolve.resolve_path(dst).map_err(HdfsError::dst)?;

        self.service
            .rename(src.to_path_string(), dst.to_path_string())
            .map_err(FsError::Rpc)
            .map_err(HdfsError::op)?;
        Ok(())
    }

    // Almost functional implementation, requires some polishing.
    pub fn mkdirs(&mut self, src: &Path, create_parent: bool) -> Result<(), HdfsError> {
        let src_res = self.resolve.resolve_path(src).map_err(HdfsError::src)?;

        if !create_parent {
            // create_parent also assumes that it is ok if path exists
            ensure_not_exists(
                self.get_file_info(&src),
                src.to_string().into(),
                HdfsErrorKind::Src,
            )?;
        }

        let mut args = MkdirsRequestProto::new();
        let mut fs_perm = FsPermissionProto::new();
        fs_perm.set_perm(DEFAULT_DIR_PERM);
        args.set_src(src_res.to_path_string());
        args.set_createParent(create_parent);
        args.set_masked(fs_perm);
        self.service
            .mkdirs(&args)
            .map_err(FsError::Rpc)
            .map_err(HdfsError::op)
            .map(|_| ())
    }

    #[inline]
    pub fn shutdown(self) -> Result<(), HdfsError> {
        self.service
            .shutdown()
            .map_err(FsError::Rpc)
            .map_err(HdfsError::op)
    }
}
