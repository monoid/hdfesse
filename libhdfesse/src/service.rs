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
use hdfesse_proto::{
    acl::*, encryption::*, erasurecoding::*, hdfs::HdfsFileStatusProto, xattr::*,
    ClientNamenodeProtocol::*, Security::*,
};

use crate::rpc;

type Result<V> = std::result::Result<V, rpc::RpcError>;

pub struct ClientNamenodeService<C: rpc::RpcConnection> {
    conn: C,
}

impl<C: rpc::RpcConnection> ClientNamenodeService<C> {
    pub fn new(conn: C) -> Self {
        Self { conn }
    }

    pub fn get_user(&self) -> &str {
        self.conn.get_user()
    }

    pub fn into_inner(self) -> C {
        self.conn
    }

    // TODO this op takes args, other take proto struct.
    #[allow(non_snake_case)]
    pub fn getListing(
        &mut self,
        src: String,
        startAfter: Vec<u8>,
        needLocation: bool,
    ) -> Result<GetListingResponseProto> {
        let mut list = GetListingRequestProto::default();
        list.set_src(src);
        list.set_startAfter(startAfter);
        list.set_needLocation(needLocation);

        let data: GetListingResponseProto = self.conn.call("getListing".into(), &list)?;

        Ok(data)
    }

    #[allow(non_snake_case)]
    pub fn getBlockLocations(
        &mut self,
        args: &GetBlockLocationsRequestProto,
    ) -> Result<GetBlockLocationsResponseProto> {
        self.conn.call("getBlockLocations".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn getServerDefaults(
        &mut self,
        args: &GetServerDefaultsRequestProto,
    ) -> Result<GetServerDefaultsResponseProto> {
        self.conn.call("getServerDefaults".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn create(&mut self, args: &CreateRequestProto) -> Result<CreateResponseProto> {
        self.conn.call("create".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn append(&mut self, args: &AppendRequestProto) -> Result<AppendResponseProto> {
        self.conn.call("append".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn setReplication(
        &mut self,
        args: &SetReplicationRequestProto,
    ) -> Result<SetReplicationResponseProto> {
        self.conn.call("setReplication".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn setStoragePolicy(
        &mut self,
        args: &SetStoragePolicyRequestProto,
    ) -> Result<SetStoragePolicyResponseProto> {
        self.conn.call("setStoragePolicy".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn unsetStoragePolicy(
        &mut self,
        args: &UnsetStoragePolicyRequestProto,
    ) -> Result<UnsetStoragePolicyResponseProto> {
        self.conn.call("unsetStoragePolicy".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn getStoragePolicy(
        &mut self,
        args: &GetStoragePolicyRequestProto,
    ) -> Result<GetStoragePolicyResponseProto> {
        self.conn.call("getStoragePolicy".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn getStoragePolicies(
        &mut self,
        args: &GetStoragePoliciesRequestProto,
    ) -> Result<GetStoragePoliciesResponseProto> {
        self.conn.call("getStoragePolicies".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn setPermission(
        &mut self,
        args: &SetPermissionRequestProto,
    ) -> Result<SetPermissionResponseProto> {
        self.conn.call("setPermission".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn setOwner(&mut self, args: &SetOwnerRequestProto) -> Result<SetOwnerResponseProto> {
        self.conn.call("setOwner".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn abandonBlock(
        &mut self,
        args: &AbandonBlockRequestProto,
    ) -> Result<AbandonBlockResponseProto> {
        self.conn.call("abandonBlock".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn addBlock(&mut self, args: &AddBlockRequestProto) -> Result<AddBlockResponseProto> {
        self.conn.call("addBlock".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn getAdditionalDatanode(
        &mut self,
        args: &GetAdditionalDatanodeRequestProto,
    ) -> Result<GetAdditionalDatanodeResponseProto> {
        self.conn.call("getAdditionalDatanode".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn complete(&mut self, args: &CompleteRequestProto) -> Result<CompleteResponseProto> {
        self.conn.call("complete".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn reportBadBlocks(
        &mut self,
        args: &ReportBadBlocksRequestProto,
    ) -> Result<ReportBadBlocksResponseProto> {
        self.conn.call("reportBadBlocks".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn concat(&mut self, args: &ConcatRequestProto) -> Result<ConcatResponseProto> {
        self.conn.call("concat".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn truncate(&mut self, args: &TruncateRequestProto) -> Result<TruncateResponseProto> {
        self.conn.call("truncate".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn rename(&mut self, src: String, dst: String) -> Result<RenameResponseProto> {
        let mut args = RenameRequestProto::default();
        args.set_src(src);
        args.set_dst(dst);

        self.conn.call("rename".into(), &args)
    }

    #[allow(non_snake_case)]
    pub fn rename2(&mut self, args: &Rename2RequestProto) -> Result<Rename2ResponseProto> {
        self.conn.call("rename2".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn delete(&mut self, args: &DeleteRequestProto) -> Result<DeleteResponseProto> {
        self.conn.call("delete".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn mkdirs(&mut self, args: &MkdirsRequestProto) -> Result<MkdirsResponseProto> {
        self.conn.call("mkdirs".into(), args)
    }

    // #[allow(non_snake_case)]
    // pub fn getListing(&mut self, args: &GetListingRequestProto) -> Result<GetListingResponseProto> {
    //     self.conn.call("getListing".into(), args)
    // }

    #[allow(non_snake_case)]
    pub fn getBatchedListing(
        &mut self,
        args: &GetBatchedListingRequestProto,
    ) -> Result<GetBatchedListingResponseProto> {
        self.conn.call("getBatchedListing".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn renewLease(&mut self, args: &RenewLeaseRequestProto) -> Result<RenewLeaseResponseProto> {
        self.conn.call("renewLease".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn recoverLease(
        &mut self,
        args: &RecoverLeaseRequestProto,
    ) -> Result<RecoverLeaseResponseProto> {
        self.conn.call("recoverLease".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn getFsStats(
        &mut self,
        args: &GetFsStatusRequestProto,
    ) -> Result<GetFsStatsResponseProto> {
        self.conn.call("getFsStats".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn getFsReplicatedBlockStats(
        &mut self,
        args: &GetFsReplicatedBlockStatsRequestProto,
    ) -> Result<GetFsReplicatedBlockStatsResponseProto> {
        self.conn.call("getFsReplicatedBlockStats".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn getFsECBlockGroupStats(
        &mut self,
        args: &GetFsECBlockGroupStatsRequestProto,
    ) -> Result<GetFsECBlockGroupStatsResponseProto> {
        self.conn.call("getFsECBlockGroupStats".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn getDatanodeReport(
        &mut self,
        args: &GetDatanodeReportRequestProto,
    ) -> Result<GetDatanodeReportResponseProto> {
        self.conn.call("getDatanodeReport".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn getDatanodeStorageReport(
        &mut self,
        args: &GetDatanodeStorageReportRequestProto,
    ) -> Result<GetDatanodeStorageReportResponseProto> {
        self.conn.call("getDatanodeStorageReport".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn getPreferredBlockSize(
        &mut self,
        args: &GetPreferredBlockSizeRequestProto,
    ) -> Result<GetPreferredBlockSizeResponseProto> {
        self.conn.call("getPreferredBlockSize".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn setSafeMode(
        &mut self,
        args: &SetSafeModeRequestProto,
    ) -> Result<SetSafeModeResponseProto> {
        self.conn.call("setSafeMode".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn saveNamespace(
        &mut self,
        args: &SaveNamespaceRequestProto,
    ) -> Result<SaveNamespaceResponseProto> {
        self.conn.call("saveNamespace".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn rollEdits(&mut self, args: &RollEditsRequestProto) -> Result<RollEditsResponseProto> {
        self.conn.call("rollEdits".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn restoreFailedStorage(
        &mut self,
        args: &RestoreFailedStorageRequestProto,
    ) -> Result<RestoreFailedStorageResponseProto> {
        self.conn.call("restoreFailedStorage".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn refreshNodes(
        &mut self,
        args: &RefreshNodesRequestProto,
    ) -> Result<RefreshNodesResponseProto> {
        self.conn.call("refreshNodes".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn finalizeUpgrade(
        &mut self,
        args: &FinalizeUpgradeRequestProto,
    ) -> Result<FinalizeUpgradeResponseProto> {
        self.conn.call("finalizeUpgrade".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn upgradeStatus(
        &mut self,
        args: &UpgradeStatusRequestProto,
    ) -> Result<UpgradeStatusResponseProto> {
        self.conn.call("upgradeStatus".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn rollingUpgrade(
        &mut self,
        args: &RollingUpgradeRequestProto,
    ) -> Result<RollingUpgradeResponseProto> {
        self.conn.call("rollingUpgrade".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn listCorruptFileBlocks(
        &mut self,
        args: &ListCorruptFileBlocksRequestProto,
    ) -> Result<ListCorruptFileBlocksResponseProto> {
        self.conn.call("listCorruptFileBlocks".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn metaSave(&mut self, args: &MetaSaveRequestProto) -> Result<MetaSaveResponseProto> {
        self.conn.call("metaSave".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn getFileInfo(&mut self, src: String) -> Result<Option<HdfsFileStatusProto>> {
        let mut args = GetFileInfoRequestProto::new();
        args.set_src(src);
        let mut res: GetFileInfoResponseProto = self.conn.call("getFileInfo".into(), &args)?;
        Ok(if res.has_fs() {
            Some(res.take_fs())
        } else {
            None
        })
    }

    #[allow(non_snake_case)]
    pub fn getLocatedFileInfo(
        &mut self,
        args: &GetLocatedFileInfoRequestProto,
    ) -> Result<GetLocatedFileInfoResponseProto> {
        self.conn.call("getLocatedFileInfo".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn addCacheDirective(
        &mut self,
        args: &AddCacheDirectiveRequestProto,
    ) -> Result<AddCacheDirectiveResponseProto> {
        self.conn.call("addCacheDirective".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn modifyCacheDirective(
        &mut self,
        args: &ModifyCacheDirectiveRequestProto,
    ) -> Result<ModifyCacheDirectiveResponseProto> {
        self.conn.call("modifyCacheDirective".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn removeCacheDirective(
        &mut self,
        args: &RemoveCacheDirectiveRequestProto,
    ) -> Result<RemoveCacheDirectiveResponseProto> {
        self.conn.call("removeCacheDirective".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn listCacheDirectives(
        &mut self,
        args: &ListCacheDirectivesRequestProto,
    ) -> Result<ListCacheDirectivesResponseProto> {
        self.conn.call("listCacheDirectives".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn addCachePool(
        &mut self,
        args: &AddCachePoolRequestProto,
    ) -> Result<AddCachePoolResponseProto> {
        self.conn.call("addCachePool".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn modifyCachePool(
        &mut self,
        args: &ModifyCachePoolRequestProto,
    ) -> Result<ModifyCachePoolResponseProto> {
        self.conn.call("modifyCachePool".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn removeCachePool(
        &mut self,
        args: &RemoveCachePoolRequestProto,
    ) -> Result<RemoveCachePoolResponseProto> {
        self.conn.call("removeCachePool".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn listCachePools(
        &mut self,
        args: &ListCachePoolsRequestProto,
    ) -> Result<ListCachePoolsResponseProto> {
        self.conn.call("listCachePools".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn getFileLinkInfo(
        &mut self,
        args: &GetFileLinkInfoRequestProto,
    ) -> Result<GetFileLinkInfoResponseProto> {
        self.conn.call("getFileLinkInfo".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn getContentSummary(
        &mut self,
        args: &GetContentSummaryRequestProto,
    ) -> Result<GetContentSummaryResponseProto> {
        self.conn.call("getContentSummary".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn setQuota(&mut self, args: &SetQuotaRequestProto) -> Result<SetQuotaResponseProto> {
        self.conn.call("setQuota".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn fsync(&mut self, args: &FsyncRequestProto) -> Result<FsyncResponseProto> {
        self.conn.call("fsync".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn setTimes(&mut self, args: &SetTimesRequestProto) -> Result<SetTimesResponseProto> {
        self.conn.call("setTimes".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn createSymlink(
        &mut self,
        args: &CreateSymlinkRequestProto,
    ) -> Result<CreateSymlinkResponseProto> {
        self.conn.call("createSymlink".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn getLinkTarget(
        &mut self,
        args: &GetLinkTargetRequestProto,
    ) -> Result<GetLinkTargetResponseProto> {
        self.conn.call("getLinkTarget".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn updateBlockForPipeline(
        &mut self,
        args: &UpdateBlockForPipelineRequestProto,
    ) -> Result<UpdateBlockForPipelineResponseProto> {
        self.conn.call("updateBlockForPipeline".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn updatePipeline(
        &mut self,
        args: &UpdatePipelineRequestProto,
    ) -> Result<UpdatePipelineResponseProto> {
        self.conn.call("updatePipeline".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn getDelegationToken(
        &mut self,
        args: &GetDelegationTokenRequestProto,
    ) -> Result<GetDelegationTokenResponseProto> {
        self.conn.call("getDelegationToken".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn renewDelegationToken(
        &mut self,
        args: &RenewDelegationTokenRequestProto,
    ) -> Result<RenewDelegationTokenResponseProto> {
        self.conn.call("renewDelegationToken".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn cancelDelegationToken(
        &mut self,
        args: &CancelDelegationTokenRequestProto,
    ) -> Result<CancelDelegationTokenResponseProto> {
        self.conn.call("cancelDelegationToken".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn setBalancerBandwidth(
        &mut self,
        args: &SetBalancerBandwidthRequestProto,
    ) -> Result<SetBalancerBandwidthResponseProto> {
        self.conn.call("setBalancerBandwidth".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn getDataEncryptionKey(
        &mut self,
        args: &GetDataEncryptionKeyRequestProto,
    ) -> Result<GetDataEncryptionKeyResponseProto> {
        self.conn.call("getDataEncryptionKey".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn createSnapshot(
        &mut self,
        args: &CreateSnapshotRequestProto,
    ) -> Result<CreateSnapshotResponseProto> {
        self.conn.call("createSnapshot".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn renameSnapshot(
        &mut self,
        args: &RenameSnapshotRequestProto,
    ) -> Result<RenameSnapshotResponseProto> {
        self.conn.call("renameSnapshot".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn allowSnapshot(
        &mut self,
        args: &AllowSnapshotRequestProto,
    ) -> Result<AllowSnapshotResponseProto> {
        self.conn.call("allowSnapshot".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn disallowSnapshot(
        &mut self,
        args: &DisallowSnapshotRequestProto,
    ) -> Result<DisallowSnapshotResponseProto> {
        self.conn.call("disallowSnapshot".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn getSnapshottableDirListing(
        &mut self,
        args: &GetSnapshottableDirListingRequestProto,
    ) -> Result<GetSnapshottableDirListingResponseProto> {
        self.conn.call("getSnapshottableDirListing".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn getSnapshotListing(
        &mut self,
        args: &GetSnapshotListingRequestProto,
    ) -> Result<GetSnapshotListingResponseProto> {
        self.conn.call("getSnapshotListing".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn deleteSnapshot(
        &mut self,
        args: &DeleteSnapshotRequestProto,
    ) -> Result<DeleteSnapshotResponseProto> {
        self.conn.call("deleteSnapshot".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn getSnapshotDiffReport(
        &mut self,
        args: &GetSnapshotDiffReportRequestProto,
    ) -> Result<GetSnapshotDiffReportResponseProto> {
        self.conn.call("getSnapshotDiffReport".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn getSnapshotDiffReportListing(
        &mut self,
        args: &GetSnapshotDiffReportListingRequestProto,
    ) -> Result<GetSnapshotDiffReportListingResponseProto> {
        self.conn.call("getSnapshotDiffReportListing".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn isFileClosed(
        &mut self,
        args: &IsFileClosedRequestProto,
    ) -> Result<IsFileClosedResponseProto> {
        self.conn.call("isFileClosed".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn modifyAclEntries(
        &mut self,
        args: &ModifyAclEntriesRequestProto,
    ) -> Result<ModifyAclEntriesResponseProto> {
        self.conn.call("modifyAclEntries".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn removeAclEntries(
        &mut self,
        args: &RemoveAclEntriesRequestProto,
    ) -> Result<RemoveAclEntriesResponseProto> {
        self.conn.call("removeAclEntries".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn removeDefaultAcl(
        &mut self,
        args: &RemoveDefaultAclRequestProto,
    ) -> Result<RemoveDefaultAclResponseProto> {
        self.conn.call("removeDefaultAcl".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn removeAcl(&mut self, args: &RemoveAclRequestProto) -> Result<RemoveAclResponseProto> {
        self.conn.call("removeAcl".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn setAcl(&mut self, args: &SetAclRequestProto) -> Result<SetAclResponseProto> {
        self.conn.call("setAcl".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn getAclStatus(
        &mut self,
        args: &GetAclStatusRequestProto,
    ) -> Result<GetAclStatusResponseProto> {
        self.conn.call("getAclStatus".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn setXAttr(&mut self, args: &SetXAttrRequestProto) -> Result<SetXAttrResponseProto> {
        self.conn.call("setXAttr".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn getXAttrs(&mut self, args: &GetXAttrsRequestProto) -> Result<GetXAttrsResponseProto> {
        self.conn.call("getXAttrs".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn listXAttrs(&mut self, args: &ListXAttrsRequestProto) -> Result<ListXAttrsResponseProto> {
        self.conn.call("listXAttrs".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn removeXAttr(
        &mut self,
        args: &RemoveXAttrRequestProto,
    ) -> Result<RemoveXAttrResponseProto> {
        self.conn.call("removeXAttr".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn checkAccess(
        &mut self,
        args: &CheckAccessRequestProto,
    ) -> Result<CheckAccessResponseProto> {
        self.conn.call("checkAccess".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn createEncryptionZone(
        &mut self,
        args: &CreateEncryptionZoneRequestProto,
    ) -> Result<CreateEncryptionZoneResponseProto> {
        self.conn.call("createEncryptionZone".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn listEncryptionZones(
        &mut self,
        args: &ListEncryptionZonesRequestProto,
    ) -> Result<ListEncryptionZonesResponseProto> {
        self.conn.call("listEncryptionZones".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn reencryptEncryptionZone(
        &mut self,
        args: &ReencryptEncryptionZoneRequestProto,
    ) -> Result<ReencryptEncryptionZoneResponseProto> {
        self.conn.call("reencryptEncryptionZone".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn listReencryptionStatus(
        &mut self,
        args: &ListReencryptionStatusRequestProto,
    ) -> Result<ListReencryptionStatusResponseProto> {
        self.conn.call("listReencryptionStatus".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn getEZForPath(
        &mut self,
        args: &GetEZForPathRequestProto,
    ) -> Result<GetEZForPathResponseProto> {
        self.conn.call("getEZForPath".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn setErasureCodingPolicy(
        &mut self,
        args: &SetErasureCodingPolicyRequestProto,
    ) -> Result<SetErasureCodingPolicyResponseProto> {
        self.conn.call("setErasureCodingPolicy".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn unsetErasureCodingPolicy(
        &mut self,
        args: &UnsetErasureCodingPolicyRequestProto,
    ) -> Result<UnsetErasureCodingPolicyResponseProto> {
        self.conn.call("unsetErasureCodingPolicy".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn getECTopologyResultForPolicies(
        &mut self,
        args: &GetECTopologyResultForPoliciesRequestProto,
    ) -> Result<GetECTopologyResultForPoliciesResponseProto> {
        self.conn
            .call("getECTopologyResultForPolicies".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn getCurrentEditLogTxid(
        &mut self,
        args: &GetCurrentEditLogTxidRequestProto,
    ) -> Result<GetCurrentEditLogTxidResponseProto> {
        self.conn.call("getCurrentEditLogTxid".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn getEditsFromTxid(
        &mut self,
        args: &GetEditsFromTxidRequestProto,
    ) -> Result<GetEditsFromTxidResponseProto> {
        self.conn.call("getEditsFromTxid".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn getErasureCodingPolicies(
        &mut self,
        args: &GetErasureCodingPoliciesRequestProto,
    ) -> Result<GetErasureCodingPoliciesResponseProto> {
        self.conn.call("getErasureCodingPolicies".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn addErasureCodingPolicies(
        &mut self,
        args: &AddErasureCodingPoliciesRequestProto,
    ) -> Result<AddErasureCodingPoliciesResponseProto> {
        self.conn.call("addErasureCodingPolicies".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn removeErasureCodingPolicy(
        &mut self,
        args: &RemoveErasureCodingPolicyRequestProto,
    ) -> Result<RemoveErasureCodingPolicyResponseProto> {
        self.conn.call("removeErasureCodingPolicy".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn enableErasureCodingPolicy(
        &mut self,
        args: &EnableErasureCodingPolicyRequestProto,
    ) -> Result<EnableErasureCodingPolicyResponseProto> {
        self.conn.call("enableErasureCodingPolicy".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn disableErasureCodingPolicy(
        &mut self,
        args: &DisableErasureCodingPolicyRequestProto,
    ) -> Result<DisableErasureCodingPolicyResponseProto> {
        self.conn.call("disableErasureCodingPolicy".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn getErasureCodingPolicy(
        &mut self,
        args: &GetErasureCodingPolicyRequestProto,
    ) -> Result<GetErasureCodingPolicyResponseProto> {
        self.conn.call("getErasureCodingPolicy".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn getErasureCodingCodecs(
        &mut self,
        args: &GetErasureCodingCodecsRequestProto,
    ) -> Result<GetErasureCodingCodecsResponseProto> {
        self.conn.call("getErasureCodingCodecs".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn getQuotaUsage(
        &mut self,
        args: &GetQuotaUsageRequestProto,
    ) -> Result<GetQuotaUsageResponseProto> {
        self.conn.call("getQuotaUsage".into(), args)
    }
    #[allow(non_snake_case)]
    pub fn listOpenFiles(
        &mut self,
        args: &ListOpenFilesRequestProto,
    ) -> Result<ListOpenFilesResponseProto> {
        self.conn.call("listOpenFiles".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn msync(&mut self, args: &MsyncRequestProto) -> Result<MsyncResponseProto> {
        self.conn.call("msync".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn satisfyStoragePolicy(
        &mut self,
        args: &SatisfyStoragePolicyRequestProto,
    ) -> Result<SatisfyStoragePolicyResponseProto> {
        self.conn.call("satisfyStoragePolicy".into(), args)
    }

    #[allow(non_snake_case)]
    pub fn getHAServiceState(
        &mut self,
        args: &HAServiceStateRequestProto,
    ) -> Result<HAServiceStateResponseProto> {
        self.conn.call("getServiceState".into(), args)
    }

    #[inline]
    pub fn shutdown(self) -> Result<()> {
        self.conn.shutdown()
    }
}
