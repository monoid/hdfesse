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
use std::{borrow::Cow, collections::HashMap, fmt::Display, sync::Arc};

pub use crate::fs_ls::LsGroupIterator;
use crate::{erasure::SystemErasureCodingPolicy, fs_ls::LsIterator, path::{Path, PathError, UriResolver}, rpc::{self, RpcConnection}, service};
pub use hdfesse_proto::hdfs::ErasureCodingPolicyState;
use hdfesse_proto::{
    acl::FsPermissionProto,
    hdfs::{
        CipherSuiteProto, CryptoProtocolVersionProto, DatanodeIDProto, DatanodeInfoProto,
        DatanodeInfoProto_AdminState, ECSchemaProto, ErasureCodingPolicyProto, ExtendedBlockProto,
        FileEncryptionInfoProto, HdfsFileStatusProto, HdfsFileStatusProto_FileType,
        LocatedBlockProto, LocatedBlocksProto, StorageTypeProto,
    },
    ClientNamenodeProtocol::{
        DeleteRequestProto, GetBlockLocationsRequestProto, GetFsStatusRequestProto,
        MkdirsRequestProto, SetPermissionRequestProto, SetTimesRequestProto,
    },
    Security::TokenProto,
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

pub struct DatanodeID {
    pub ip_addr: Box<str>,
    pub host_name: Box<str>,
    pub datanode_uuid: Box<str>,
    pub xfer_port: u32,
    pub info_port: u32,
    pub info_secure_port: Option<u32>,
    pub ipc_port: u32,
}

impl From<DatanodeIDProto> for DatanodeID {
    fn from(mut proto: DatanodeIDProto) -> Self {
        Self {
            ip_addr: proto.take_ipAddr().into(),
            host_name: proto.take_hostName().into(),
            datanode_uuid: proto.take_datanodeUuid().into(),
            xfer_port: proto.get_xferPort(),
            info_port: proto.get_infoPort(),
            info_secure_port: if proto.has_infoSecurePort() {
                Some(proto.get_infoSecurePort())
            } else {
                None
            },
            ipc_port: proto.get_ipcPort(),
        }
    }
}

pub type AdminState = DatanodeInfoProto_AdminState;
pub type StorageType = StorageTypeProto;

pub struct DatanodeInfo {
    pub id: DatanodeID,
    pub network_location: Option<Box<str>>,
    pub upgrade_domain: Option<Box<str>>,
    pub capacity: u64,
    pub dfs_used: u64,
    pub non_dfs_used: u64,
    pub remaining: u64,
    pub block_pool_used: u64,
    pub cache_capacity: u64,
    pub cache_used: u64,
    pub last_update: u64,
    pub last_update_monotonic: u64,
    pub xceiver_count: u32,
    pub admin_state: AdminState,
    pub last_block_report_time: u64,
    pub last_block_report_monotonic: u64,
    pub num_blocks: u32,
}

impl From<DatanodeInfoProto> for DatanodeInfo {
    fn from(mut proto: DatanodeInfoProto) -> Self {
        Self {
            id: proto.take_id().into(),
            network_location: if proto.has_location() {
                Some(proto.get_location().into())
            } else {
                None
            },
            capacity: proto.get_cacheCapacity(),
            dfs_used: proto.get_dfsUsed(),
            non_dfs_used: proto.get_nonDfsUsed(),
            remaining: proto.get_remaining(),
            block_pool_used: proto.get_blockPoolUsed(),
            cache_capacity: proto.get_cacheCapacity(),
            cache_used: proto.get_cacheUsed(),
            last_update: proto.get_lastUpdate(),
            last_update_monotonic: proto.get_lastUpdateMonotonic(),
            xceiver_count: proto.get_xceiverCount(),
            admin_state: proto.get_adminState().into(),
            upgrade_domain: if proto.has_upgradeDomain() {
                Some(proto.get_upgradeDomain().into())
            } else {
                None
            },
            last_block_report_time: proto.get_lastBlockReportTime(),
            last_block_report_monotonic: proto.get_lastBlockReportMonotonic(),
            num_blocks: proto.get_numBlocks(),
        }
    }
}

pub struct Token {
    pub identifier: Vec<u8>,
    pub password: Vec<u8>,
    pub kind: Box<str>,
    pub service: Box<str>,
}

impl From<TokenProto> for Token {
    fn from(mut proto: TokenProto) -> Self {
        Self {
            identifier: proto.take_identifier(),
            password: proto.take_password(),
            kind: proto.take_kind().into(),
            service: proto.take_service().into(),
        }
    }
}

pub struct LocatedBlock {
    pub b: ExtendedBlock,
    pub offset: u64,
    pub locs: Vec<Arc<DatanodeInfo>>,
    pub storage_ids: Vec<String>,
    pub storage_types: Vec<StorageType>,
    pub corrupt: bool,
    pub block_token: Token,
    pub cached_locs: Vec<Arc<DatanodeInfo>>,
}

impl From<LocatedBlockProto> for LocatedBlock {
    fn from(mut proto: LocatedBlockProto) -> Self {
        let locs: Vec<Arc<DatanodeInfo>> = proto
            .take_locs()
            .into_iter()
            .map(Into::into)
            .map(Arc::new)
            .collect();
        let cached_locs: Vec<Arc<DatanodeInfo>> = proto
            .take_isCached()
            .into_iter()
            .zip(locs.iter())
            .filter_map(|(is_cached, loc)| if is_cached { Some(loc.clone()) } else { None })
            .collect();
        Self {
            b: proto.take_b().into(),
            offset: proto.get_offset(),
            locs,
            storage_ids: proto.take_storageIDs().to_vec(),
            storage_types: proto.take_storageTypes(),
            corrupt: proto.get_corrupt(),
            block_token: proto.take_blockToken().into(),
            cached_locs,
        }
    }
}

pub type CipherSuite = CipherSuiteProto;
pub type CryptoProtocolVersion = CryptoProtocolVersionProto;

pub struct FileEncryptionInfo {
    pub suite: CipherSuite,
    pub version: CryptoProtocolVersion,
    pub edek: Box<[u8]>,
    pub iv: Box<[u8]>,
    pub key_name: Box<str>,
    pub ez_key_version_name: Box<str>,
}

impl From<FileEncryptionInfoProto> for FileEncryptionInfo {
    fn from(mut proto: FileEncryptionInfoProto) -> Self {
        Self {
            suite: proto.get_suite().into(),
            version: proto.get_cryptoProtocolVersion().into(),
            edek: proto.take_key().into(),
            iv: proto.take_iv().into(),
            key_name: proto.take_keyName().into(),
            ez_key_version_name: proto.take_ezKeyVersionName().into(),
        }
    }
}

#[derive(Clone)]
pub struct EcSchema {
    pub codec_name: Cow<'static, str>,
    pub data_units: u32,
    pub parity_units: u32,
    pub options: HashMap<Box<str>, Box<str>>,
}

impl From<ECSchemaProto> for EcSchema {
    fn from(mut proto: ECSchemaProto) -> Self {
        Self {
            codec_name: proto.take_codecName().into(),
            data_units: proto.get_dataUnits(),
            parity_units: proto.get_parityUnits(),
            options: proto
                .take_options()
                .into_iter()
                .map(|mut o| (o.take_key().into(), o.take_value().into()))
                .collect(),
        }
    }
}


#[derive(Clone)]
pub struct ErasureCodingPolicy {
    pub name: Cow<'static, str>,
    pub schema: EcSchema,
    pub cell_size: u32,
    pub id: u8,
}

impl From<&ErasureCodingPolicyProto> for ErasureCodingPolicy {
    fn from(proto: &ErasureCodingPolicyProto) -> Self {
        let id = (proto.get_id() & 0xFF) as u8;
        match SystemErasureCodingPolicy::get_by_id(id) {
            Some(policy) => policy.clone(),
            None => {
                // TODO check precondition
                ErasureCodingPolicy {
                    name: proto.get_name().to_owned().into(),
                    schema: proto.get_schema().clone().into(),
                    cell_size: proto.get_cellSize(),
                    id,
                }
            }
        }
    }
}

pub struct ErasureCodingPolicyInfo {
    pub policy: ErasureCodingPolicy,
    pub state: ErasureCodingPolicyState,
}

impl From<ErasureCodingPolicyProto> for ErasureCodingPolicyInfo {
    fn from(source: ErasureCodingPolicyProto) -> Self {
        // TODO: use TryInto, but it will affect many other conversions.
        assert!(source.has_state());
        Self {
            policy: (&source).into(),
            state: source.get_state().into(),
        }
    }
}

pub struct ExtendedBlock {
    pub pool_id: Box<str>,
    pub block_id: u64,
    pub num_bytes: u64,
    pub generation_stamp: u64,
}

impl From<ExtendedBlockProto> for ExtendedBlock {
    fn from(mut proto: ExtendedBlockProto) -> Self {
        Self {
            pool_id: proto.take_poolId().into(),
            block_id: proto.get_blockId(),
            num_bytes: proto.get_numBytes(),
            generation_stamp: proto.get_generationStamp(),
        }
    }
}

pub struct LocatedBlocks {
    pub length: u64,
    pub under_construction: bool,
    pub block_list: Vec<LocatedBlock>,
    pub last_block: Option<LocatedBlock>,
    pub is_last_block_complete: bool,
    pub file_encription_info: Option<FileEncryptionInfo>,
    pub ec_policy: Option<ErasureCodingPolicy>,
}

impl From<LocatedBlocksProto> for LocatedBlocks {
    fn from(mut proto: LocatedBlocksProto) -> Self {
        Self {
            length: proto.get_fileLength(),
            under_construction: proto.get_underConstruction(),
            block_list: proto.take_blocks().into_iter().map(Into::into).collect(),
            last_block: proto.lastBlock.take().map(Into::into),
            is_last_block_complete: proto.get_isLastBlockComplete(),
            file_encription_info: proto.fileEncryptionInfo.into_option().map(Into::into),
            ec_policy: proto.ecPolicy.into_option().as_ref().map(Into::into),
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
    pub flags: u32,
    pub owner: Box<str>,
    pub group: Box<str>,
    pub symlink: Option<Box<[u8]>>,
    pub path: Box<[u8]>,
    pub field_id: Option<u64>,
    pub locations: Option<LocatedBlocks>,
    pub children: Option<i32>,
    pub fe_info: Option<FileEncryptionInfo>,
    pub storage_policy: Option<i8>,
    pub ec_policty: Option<ErasureCodingPolicy>,
}

// See PBHelperClient.java
impl From<HdfsFileStatusProto> for HdfsFileStatus {
    fn from(mut fs: HdfsFileStatusProto) -> Self {
        let flags = if fs.has_flags() {
            fs.get_flags()
        } else {
            fs.get_permission().get_perm()
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
                Some(fs.take_fileEncryptionInfo().into())
            } else {
                None
            },
            storage_policy: if fs.has_storagePolicy() {
                Some(fs.get_storagePolicy() as i8)
            } else {
                None
            },
            ec_policty: if fs.has_ecPolicy() {
                Some(fs.get_ecPolicy().into())
            } else {
                None
            },
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
