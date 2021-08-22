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
use std::{
    borrow::{BorrowMut, Cow},
    fmt::Display,
};

pub use crate::fs_ls::LsGroupIterator;
use crate::{
    fs_ls::LsIterator,
    path::{Path, PathError, UriResolver},
    rpc::{self, RpcConnection},
    service,
    status::LocatedBlock,
};
pub use hdfesse_proto::hdfs::ErasureCodingPolicyState;
use hdfesse_proto::{
    acl::FsPermissionProto,
    hdfs::{HdfsFileStatusProto, HdfsFileStatusProto_FileType},
    ClientNamenodeProtocol::{
        DeleteRequestProto, GetBlockLocationsRequestProto, GetFsStatusRequestProto,
        MkdirsRequestProto, SetPermissionRequestProto, SetTimesRequestProto,
    },
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
    #[error("`{0}': Is a directory")]
    IsDir(String),
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

pub fn ensure_not_dir(
    file_info: &HdfsFileStatusProto,
    path: Cow<'_, str>,
    kind: HdfsErrorKind,
) -> Result<(), HdfsError> {
    if file_info.get_fileType() != HdfsFileStatusProto_FileType::IS_DIR {
        Ok(())
    } else {
        Err(HdfsError {
            kind,
            source: FsError::IsDir(path.into_owned()),
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

pub struct FsStatus {
    pub capacity: u64,
    pub used: u64,
    pub remaining: u64,
    pub under_replicated: u64,
    pub corrupt_blocks: u64,
    pub missing_blocks: u64,
    pub missing_repl_one_blocks: u64,
    pub blocks_in_future: u64,
    pub pending_deletion_blocks: u64,
}

pub struct Hdfs<
    R = crate::ha_rpc::HaHdfsConnection<crate::rpc::SimpleConnector>,
    SRef = service::ClientNamenodeService<R>,
> where
    R: RpcConnection,
    SRef: BorrowMut<service::ClientNamenodeService<R>>,
{
    service: SRef,
    resolve: UriResolver,
    _phantom: std::marker::PhantomData<R>,
}

impl<R, SRef> Hdfs<R, SRef>
where
    R: RpcConnection,
    SRef: BorrowMut<service::ClientNamenodeService<R>>,
{
    pub fn new(service: SRef, resolve: UriResolver) -> Self {
        Self {
            service,
            resolve,
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn get_user(&self) -> &str {
        self.service.borrow().get_user()
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
            LsIterator::new(LsGroupIterator::new(self.service.borrow_mut(), &src))
                .map(|r| r.map_err(HdfsError::op)),
        )
    }

    pub fn get_file_info(&mut self, src: &Path<'_>) -> Result<HdfsFileStatusProto, FsError> {
        let src = self.resolve.resolve_path(src)?;

        self.service
            .borrow_mut()
            .getFileInfo(src.to_path_string())
            .map_err(FsError::Rpc)?
            .ok_or_else(|| FsError::NotFound(src.to_path_string()))
    }

    // TODO a sketch; one should check that dst exists or doesn't
    // exist and srcs do exist, etc.
    pub fn rename(&mut self, src: &Path<'_>, dst: &Path<'_>) -> Result<(), HdfsError> {
        let src = self.resolve.resolve_path(src).map_err(HdfsError::src)?;
        let dst = self.resolve.resolve_path(dst).map_err(HdfsError::dst)?;

        self.service
            .borrow_mut()
            .rename(src.to_path_string(), dst.to_path_string())
            .map_err(FsError::Rpc)
            .map_err(HdfsError::op)?;
        Ok(())
    }

    // Almost functional implementation, requires some polishing.
    pub fn mkdirs(&mut self, src: &Path<'_>, create_parent: bool) -> Result<bool, HdfsError> {
        let src_res = self.resolve.resolve_path(src).map_err(HdfsError::src)?;

        if !create_parent {
            // create_parent also assumes that it is ok if path exists
            ensure_not_exists(
                self.get_file_info(src),
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
            .borrow_mut()
            .mkdirs(&args)
            .map_err(FsError::Rpc)
            .map_err(HdfsError::op)
            .map(|resp| resp.get_result())
    }

    /// Delete path
    pub fn delete(&mut self, path: &Path<'_>, recursive: bool) -> Result<bool, HdfsError> {
        let path_res = self.resolve.resolve_path(path).map_err(HdfsError::src)?;
        if !recursive {
            ensure_not_dir(
                &self.get_file_info(path).map_err(HdfsError::src)?,
                path.to_string().into(),
                HdfsErrorKind::Src,
            )?;
        }
        let mut args = DeleteRequestProto::default();
        args.set_src(path_res.to_path_string());
        args.borrow_mut().set_recursive(recursive);
        self.service
            .borrow_mut()
            .delete(&args)
            .map_err(FsError::Rpc)
            .map_err(HdfsError::src)
            .map(|resp| resp.get_result())
    }

    pub fn get_status(&mut self) -> Result<FsStatus, HdfsError> {
        let args = GetFsStatusRequestProto::default();
        match self.service.borrow_mut().getFsStats(&args) {
            Ok(stats) => Ok(FsStatus {
                capacity: stats.get_capacity(),
                used: stats.get_used(),
                remaining: stats.get_used(),
                under_replicated: stats.get_under_replicated(),
                corrupt_blocks: stats.get_corrupt_blocks(),
                missing_blocks: stats.get_missing_blocks(),
                missing_repl_one_blocks: stats.get_missing_repl_one_blocks(),
                blocks_in_future: stats.get_blocks_in_future(),
                pending_deletion_blocks: stats.get_pending_deletion_blocks(),
            }),
            Err(e) => Err(HdfsError::op(FsError::Rpc(e))),
        }
    }

    // The method returns protobuf record, and it can be considered as
    // a implementation leak.  One should just allocate new records
    // vector and move data like strings into it.  See hadoop's
    // DFSUtilClient.locatedBlocks2Locations.
    pub fn get_file_block_locations(
        &mut self,
        file_status: &HdfsFileStatusProto,
        length: u64,
        offset: u64,
    ) -> Result<Vec<LocatedBlock>, HdfsError> {
        // TODO check path is not dir
        let path = std::str::from_utf8(file_status.get_path())
            .map_err(|e| HdfsError::src(FsError::Path(PathError::Utf8(e))))?
            .to_string();

        let path1 = Path::new(&path)
            .map_err(FsError::Path)
            .map_err(HdfsError::src)?;

        // TODO is re-resolving really required?
        let path_res = self.resolve.resolve_path(&path1).map_err(HdfsError::src)?;

        let mut args = GetBlockLocationsRequestProto::default();
        args.set_src(path_res.to_path_string());
        args.set_length(length);
        args.set_offset(offset);

        let mut blocks = self
            .service
            .borrow_mut()
            .getBlockLocations(&args)
            .map_err(FsError::Rpc)
            .map_err(HdfsError::op)?;
        Ok(blocks
            .take_locations()
            .take_blocks()
            .into_iter()
            .map(Into::into)
            .collect())
    }

    pub fn chmod(&mut self, path: &Path<'_>, chmod: u32) -> Result<(), HdfsError> {
        let path_res = self.resolve.resolve_path(path).map_err(HdfsError::src)?;

        let mut perm = FsPermissionProto::default();
        perm.set_perm(chmod);

        let mut args = SetPermissionRequestProto::default();
        args.set_src(path_res.to_path_string());
        args.set_permission(perm);

        self.service
            .borrow_mut()
            .setPermission(&args)
            .map_err(FsError::Rpc)
            .map_err(HdfsError::src)?;
        Ok(())
    }

    pub fn set_time(
        &mut self,
        path: &Path<'_>,
        mtime: Option<u64>,
        atime: Option<u64>,
    ) -> Result<(), HdfsError> {
        let path_res = self.resolve.resolve_path(path).map_err(HdfsError::src)?;

        let mut args = SetTimesRequestProto::default();
        args.set_src(path_res.to_path_string());
        if let Some(mtime) = mtime {
            // Convert to milliseconds; see hdfs.c.
            args.set_mtime(mtime * 1000);
        }
        if let Some(atime) = atime {
            args.set_atime(atime * 1000);
        }

        self.service
            .borrow_mut()
            .setTimes(&args)
            .map_err(FsError::Rpc)
            .map_err(HdfsError::src)?;
        Ok(())
    }
}

impl<R: RpcConnection> Hdfs<R, service::ClientNamenodeService<R>> {
    #[inline]
    pub fn shutdown(self) -> Result<(), HdfsError> {
        self.service
            .shutdown()
            .map_err(FsError::Rpc)
            .map_err(HdfsError::op)
    }
}
