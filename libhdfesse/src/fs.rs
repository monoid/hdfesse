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
use std::{borrow::Cow, collections::HashMap, fmt::Display};

pub use crate::fs_ls::LsGroupIterator;
use crate::{
    fs_ls::LsIterator,
    path::{Path, PathError, UriResolver},
    rpc::{self, RpcConnection},
    service,
};
use hdfesse_proto::{ClientNamenodeProtocol::{
        DeleteRequestProto, GetBlockLocationsRequestProto, GetFsStatusRequestProto,
        MkdirsRequestProto, SetPermissionRequestProto, SetTimesRequestProto,
    }, acl::FsPermissionProto, hdfs::{ECSchemaProto, ErasureCodingPolicyProto, HdfsFileStatusProto, HdfsFileStatusProto_FileType, LocatedBlockProto, LocatedBlocksProto}};
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

pub struct FsPermission {
    pub perm: u16,
}

impl From<&FsPermissionProto> for FsPermission {
    fn from(perm: &FsPermissionProto) -> Self {
        Self {
            perm: perm.get_perm() as u16,
        }
    }
}

pub struct ExtendedBlock {
}

pub struct LocatedBlock {
    pub b: ExtendedBlock,
    pub offset: u64,
    pub locs: Vec<()>,
    pub storage_ids: Vec<String>,
    pub storage_types: Vec<()>,
    pub corrupt: bool,
    pub block_token: (),
    pub cached_locs: Vec<()>,
}

impl From<LocatedBlockProto> for LocatedBlock {
    fn from(source: LocatedBlockProto) -> Self {
        Self {
            b: unimplemented!(),
            offset: source.get_offset(),
            locs: unimplemented!(),
            storage_ids: source.take_storageIDs().to_vec(),
            storage_types: unimplemented!(),
            corrupt: source.get_corrupt(),
            block_token: unimplemented!(),
            cached_locs: unimplemented!(),
        }
    }
}


pub struct FileEncryptionInfo {
}

pub struct EcSchema {
    pub codec_name: Box<str>,
    pub data_units: u32,
    pub parity_units: u32,
    pub options: HashMap<Box<str>, Box<str>>
}

impl From<ECSchemaProto> for EcSchema {
    fn from(source: ECSchemaProto) -> Self {
        Self {
            codec_name: source.take_codecName().into(),
            data_units: source.get_dataUnits(),
            parity_units: source.get_parityUnits(),
            options: source.take_options().into_iter().map(
                |o| (o.take_key().into(), o.take_value().into())
            ).collect(),
        }
    }
}

pub struct EcPolicy {
    pub name: Box<str>,
    pub schema: (),
    pub cell_size: u64,
    pub id: u8,
}

pub struct ErasureCodingPolicy {
}

pub enum ErasureCodingPolicyState {
}

pub struct ErasureCodingPolicyInfo {
    pub policy: ErasureCodingPolicy,
    pub state: ErasureCodingPolicyState,
}

impl From<ErasureCodingPolicyProto> for ErasureCodingPolicyInfo {
    fn from(source: ErasureCodingPolicyProto) -> Self {
        Self {
            policy: source.
        }
    }
}


pub struct LocatedBlocks {
    pub length: u64,
    pub under_construction: bool,
    pub block_list: Vec<LocatedBlock>,
    pub last_block: Option<LocatedBlock>,
    pub is_last_block_complete: bool,
    pub file_encription_info: FileEncryptionInfo,
    pub ec_policy: Option<EcPolicy>,
}

impl From<LocatedBlocksProto> for LocatedBlocks {
    fn from(source: LocatedBlocksProto) -> Self {
        Self {
            length: source.get_fileLength(),
            under_construction: source.get_underConstruction(),
            block_list: unimplemented!(),
            last_block: if source.has_lastBlock() {
                Some(source.take_lastBlock().into())
            } else {
                None
            },
            is_last_block_complete: source.get_isLastBlockComplete(),
            file_encription_info: if source.has_fileEncryptionInfo() {
                Some(source.take_fileEncryptionInfo().into())
            } else {
                None
            },
            ec_policy: if source.has_ecPolicy() {
                Some(source.take_ecPolicy().into())
            } else {
                None
            }
        }
    }
}

pub struct HdfsFileStatus {
    pub length: u64,
    pub isdir: bool,
    pub replication: u32,
    pub blocksize: u64,
    pub mtime: u64,
    pub atime: u64,
    pub perm: FsPermission,
    pub flags: (),
    pub owner: Box<str>,
    pub group: Box<str>,
    pub symlink: Option<Box<[u8]>>,
    pub path: Box<[u8]>,
    pub field_id: Option<u64>,
    pub locations: Option<()>,
    pub children: Option<i32>,
    pub fe_info: Option<()>,
    pub storage_policty: Option<i8>,
    pub ec_policty: Option<EcPolicy>,
}

// See PBHelperClient.java
impl From<HdfsFileStatusProto> for HdfsFileStatus {
    fn from(fs: HdfsFileStatusProto) -> Self {
        let flags = if fs.has_flags() {
            fs.get_flags()
        } else {
            fs.get_permission() as u16,
        };
        Self {
            length: fs.get_length(),
            isdir: fs.get_fileType() == HdfsFileStatusProto_FileType::IS_DIR,
            replication: fs.get_block_replication(),
            blocksize: fs.get_blocksize(),
            mtime: fs.get_modification_time(),
            atime: fs.get_access_time(),
            perm: fs.get_permission().into(),
            flags,
            owner: fs.take_owner().into(),
            group: fs.take_group().into(),
            symlink: if fs.get_fileType() == HdfsFileStatusProto_FileType::IS_SYMLINK {
                Some(fs.take_symlink().into())
            } else {
                None
            },
            path: fs.take_path().into(),
            field_id: if fs.has_fileId() {
                Some(fs.get_fileId())
            } else {
                None
            },
            locations: if fs.has_locations() {
                Some(fs.take_locations().into())
            } else {
                None
            },
            children: if fs.has_childrenNum() {
                Some(fs.get_childrenNum())
            } else {
                None
            },
            fe_info: if fs.has_fileEncryptionInfo() {
                Some(fs.get_fileEncryptionInfo().into())
            } else {
                None
            },
            storage_policty: if fs.has_storagePolicy() {
                Some(fs.get_storagePolicy() as i8)
            } else {
                None
            },
            ec_policty: if fs.has_ecPolicy() {
                Some(fs.take_ecPolicy().into())
            } else {
                None
            }
        }
    }
}

pub struct Hdfs<R: RpcConnection = crate::ha_rpc::HaHdfsConnection<crate::rpc::SimpleConnector>> {
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
    pub fn mkdirs(&mut self, src: &Path, create_parent: bool) -> Result<bool, HdfsError> {
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
            .map(|resp| resp.get_result())
    }

    /// Delete path
    pub fn delete(&mut self, path: &Path, recursive: bool) -> Result<bool, HdfsError> {
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
        args.set_recursive(recursive);
        self.service
            .delete(&args)
            .map_err(FsError::Rpc)
            .map_err(HdfsError::src)
            .map(|resp| resp.get_result())
    }

    pub fn get_status(&mut self) -> Result<FsStatus, HdfsError> {
        let args = GetFsStatusRequestProto::default();
        match self.service.getFsStats(&args) {
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
    ) -> Result<Vec<LocatedBlockProto>, HdfsError> {
        // TODO check path is not dir
        let path_res = self.resolve.resolve_path(path).map_err(HdfsError::src)?;

        // TODO should we check that the path exists and is a file?
        let mut args = GetBlockLocationsRequestProto::default();
        args.set_src(file_status.get_path());
        args.set_length(length);
        args.set_offset(offset);

        let mut blocks = self
            .service
            .getBlockLocations(&args)
            .map_err(FsError::Rpc)
            .map_err(HdfsError::op)?;
        Ok(blocks.take_locations().take_blocks().into_vec())
    }

    pub fn chmod(&mut self, path: &Path, chmod: u32) -> Result<(), HdfsError> {
        let path_res = self.resolve.resolve_path(path).map_err(HdfsError::src)?;

        let mut perm = FsPermissionProto::default();
        perm.set_perm(chmod);

        let mut args = SetPermissionRequestProto::default();
        args.set_src(path_res.to_path_string());
        args.set_permission(perm);

        self.service
            .setPermission(&args)
            .map_err(FsError::Rpc)
            .map_err(HdfsError::src)?;
        Ok(())
    }

    pub fn set_time(
        &mut self,
        path: &Path,
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
            .setTimes(&args)
            .map_err(FsError::Rpc)
            .map_err(HdfsError::src)?;
        Ok(())
    }

    #[inline]
    pub fn shutdown(self) -> Result<(), HdfsError> {
        self.service
            .shutdown()
            .map_err(FsError::Rpc)
            .map_err(HdfsError::op)
    }
}
