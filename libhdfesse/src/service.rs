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
    acl::*, encryption::*, erasurecoding::*, xattr::*, ClientNamenodeProtocol::*, Security::*,
};
use std::borrow::Cow;

use crate::rpc;

type Result<V> = std::result::Result<V, rpc::RpcError>;

pub struct ClientNamenodeService {
    conn: rpc::HdfsConnection,
}

impl ClientNamenodeService {
    pub fn new(conn: rpc::HdfsConnection) -> Self {
        Self { conn }
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

        let data: GetListingResponseProto = self.conn.call(Cow::Borrowed("getListing"), &list)?;

        Ok(data)
    }

    #[allow(non_snake_case)]
    pub fn getBlockLocations(
        &mut self,
        args: &GetBlockLocationsRequestProto,
    ) -> Result<GetBlockLocationsResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("getBlockLocations"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn getServerDefaults(
        &mut self,
        args: &GetServerDefaultsRequestProto,
    ) -> Result<GetServerDefaultsResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("getServerDefaults"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn create(&mut self, args: &CreateRequestProto) -> Result<CreateResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("create"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn append(&mut self, args: &AppendRequestProto) -> Result<AppendResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("append"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn setReplication(
        &mut self,
        args: &SetReplicationRequestProto,
    ) -> Result<SetReplicationResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("setReplication"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn setStoragePolicy(
        &mut self,
        args: &SetStoragePolicyRequestProto,
    ) -> Result<SetStoragePolicyResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("setStoragePolicy"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn unsetStoragePolicy(
        &mut self,
        args: &UnsetStoragePolicyRequestProto,
    ) -> Result<UnsetStoragePolicyResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("unsetStoragePolicy"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn getStoragePolicy(
        &mut self,
        args: &GetStoragePolicyRequestProto,
    ) -> Result<GetStoragePolicyResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("getStoragePolicy"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn getStoragePolicies(
        &mut self,
        args: &GetStoragePoliciesRequestProto,
    ) -> Result<GetStoragePoliciesResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("getStoragePolicies"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn setPermission(
        &mut self,
        args: &SetPermissionRequestProto,
    ) -> Result<SetPermissionResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("setPermission"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn setOwner(&mut self, args: &SetOwnerRequestProto) -> Result<SetOwnerResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("setOwner"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn abandonBlock(
        &mut self,
        args: &AbandonBlockRequestProto,
    ) -> Result<AbandonBlockResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("abandonBlock"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn addBlock(&mut self, args: &AddBlockRequestProto) -> Result<AddBlockResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("addBlock"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn getAdditionalDatanode(
        &mut self,
        args: &GetAdditionalDatanodeRequestProto,
    ) -> Result<GetAdditionalDatanodeResponseProto> {
        Ok(self
            .conn
            .call(Cow::Borrowed("getAdditionalDatanode"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn complete(&mut self, args: &CompleteRequestProto) -> Result<CompleteResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("complete"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn reportBadBlocks(
        &mut self,
        args: &ReportBadBlocksRequestProto,
    ) -> Result<ReportBadBlocksResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("reportBadBlocks"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn concat(&mut self, args: &ConcatRequestProto) -> Result<ConcatResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("concat"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn truncate(&mut self, args: &TruncateRequestProto) -> Result<TruncateResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("truncate"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn rename(&mut self, args: &RenameRequestProto) -> Result<RenameResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("rename"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn rename2(&mut self, args: &Rename2RequestProto) -> Result<Rename2ResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("rename2"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn delete(&mut self, args: &DeleteRequestProto) -> Result<DeleteResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("delete"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn mkdirs(&mut self, args: &MkdirsRequestProto) -> Result<MkdirsResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("mkdirs"), args)?)
    }

    // #[allow(non_snake_case)]
    // pub fn getListing(&mut self, args: &GetListingRequestProto) -> Result<GetListingResponseProto> {
    //     Ok(self.conn.call(Cow::Borrowed("getListing"), args)?)
    // }

    #[allow(non_snake_case)]
    pub fn getBatchedListing(
        &mut self,
        args: &GetBatchedListingRequestProto,
    ) -> Result<GetBatchedListingResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("getBatchedListing"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn renewLease(&mut self, args: &RenewLeaseRequestProto) -> Result<RenewLeaseResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("renewLease"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn recoverLease(
        &mut self,
        args: &RecoverLeaseRequestProto,
    ) -> Result<RecoverLeaseResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("recoverLease"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn getFsStats(
        &mut self,
        args: &GetFsStatusRequestProto,
    ) -> Result<GetFsStatsResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("getFsStats"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn getFsReplicatedBlockStats(
        &mut self,
        args: &GetFsReplicatedBlockStatsRequestProto,
    ) -> Result<GetFsReplicatedBlockStatsResponseProto> {
        Ok(self
            .conn
            .call(Cow::Borrowed("getFsReplicatedBlockStats"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn getFsECBlockGroupStats(
        &mut self,
        args: &GetFsECBlockGroupStatsRequestProto,
    ) -> Result<GetFsECBlockGroupStatsResponseProto> {
        Ok(self
            .conn
            .call(Cow::Borrowed("getFsECBlockGroupStats"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn getDatanodeReport(
        &mut self,
        args: &GetDatanodeReportRequestProto,
    ) -> Result<GetDatanodeReportResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("getDatanodeReport"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn getDatanodeStorageReport(
        &mut self,
        args: &GetDatanodeStorageReportRequestProto,
    ) -> Result<GetDatanodeStorageReportResponseProto> {
        Ok(self
            .conn
            .call(Cow::Borrowed("getDatanodeStorageReport"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn getPreferredBlockSize(
        &mut self,
        args: &GetPreferredBlockSizeRequestProto,
    ) -> Result<GetPreferredBlockSizeResponseProto> {
        Ok(self
            .conn
            .call(Cow::Borrowed("getPreferredBlockSize"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn setSafeMode(
        &mut self,
        args: &SetSafeModeRequestProto,
    ) -> Result<SetSafeModeResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("setSafeMode"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn saveNamespace(
        &mut self,
        args: &SaveNamespaceRequestProto,
    ) -> Result<SaveNamespaceResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("saveNamespace"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn rollEdits(&mut self, args: &RollEditsRequestProto) -> Result<RollEditsResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("rollEdits"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn restoreFailedStorage(
        &mut self,
        args: &RestoreFailedStorageRequestProto,
    ) -> Result<RestoreFailedStorageResponseProto> {
        Ok(self
            .conn
            .call(Cow::Borrowed("restoreFailedStorage"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn refreshNodes(
        &mut self,
        args: &RefreshNodesRequestProto,
    ) -> Result<RefreshNodesResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("refreshNodes"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn finalizeUpgrade(
        &mut self,
        args: &FinalizeUpgradeRequestProto,
    ) -> Result<FinalizeUpgradeResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("finalizeUpgrade"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn upgradeStatus(
        &mut self,
        args: &UpgradeStatusRequestProto,
    ) -> Result<UpgradeStatusResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("upgradeStatus"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn rollingUpgrade(
        &mut self,
        args: &RollingUpgradeRequestProto,
    ) -> Result<RollingUpgradeResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("rollingUpgrade"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn listCorruptFileBlocks(
        &mut self,
        args: &ListCorruptFileBlocksRequestProto,
    ) -> Result<ListCorruptFileBlocksResponseProto> {
        Ok(self
            .conn
            .call(Cow::Borrowed("listCorruptFileBlocks"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn metaSave(&mut self, args: &MetaSaveRequestProto) -> Result<MetaSaveResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("metaSave"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn getFileInfo(
        &mut self,
        args: &GetFileInfoRequestProto,
    ) -> Result<GetFileInfoResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("getFileInfo"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn getLocatedFileInfo(
        &mut self,
        args: &GetLocatedFileInfoRequestProto,
    ) -> Result<GetLocatedFileInfoResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("getLocatedFileInfo"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn addCacheDirective(
        &mut self,
        args: &AddCacheDirectiveRequestProto,
    ) -> Result<AddCacheDirectiveResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("addCacheDirective"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn modifyCacheDirective(
        &mut self,
        args: &ModifyCacheDirectiveRequestProto,
    ) -> Result<ModifyCacheDirectiveResponseProto> {
        Ok(self
            .conn
            .call(Cow::Borrowed("modifyCacheDirective"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn removeCacheDirective(
        &mut self,
        args: &RemoveCacheDirectiveRequestProto,
    ) -> Result<RemoveCacheDirectiveResponseProto> {
        Ok(self
            .conn
            .call(Cow::Borrowed("removeCacheDirective"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn listCacheDirectives(
        &mut self,
        args: &ListCacheDirectivesRequestProto,
    ) -> Result<ListCacheDirectivesResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("listCacheDirectives"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn addCachePool(
        &mut self,
        args: &AddCachePoolRequestProto,
    ) -> Result<AddCachePoolResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("addCachePool"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn modifyCachePool(
        &mut self,
        args: &ModifyCachePoolRequestProto,
    ) -> Result<ModifyCachePoolResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("modifyCachePool"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn removeCachePool(
        &mut self,
        args: &RemoveCachePoolRequestProto,
    ) -> Result<RemoveCachePoolResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("removeCachePool"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn listCachePools(
        &mut self,
        args: &ListCachePoolsRequestProto,
    ) -> Result<ListCachePoolsResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("listCachePools"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn getFileLinkInfo(
        &mut self,
        args: &GetFileLinkInfoRequestProto,
    ) -> Result<GetFileLinkInfoResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("getFileLinkInfo"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn getContentSummary(
        &mut self,
        args: &GetContentSummaryRequestProto,
    ) -> Result<GetContentSummaryResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("getContentSummary"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn setQuota(&mut self, args: &SetQuotaRequestProto) -> Result<SetQuotaResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("setQuota"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn fsync(&mut self, args: &FsyncRequestProto) -> Result<FsyncResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("fsync"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn setTimes(&mut self, args: &SetTimesRequestProto) -> Result<SetTimesResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("setTimes"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn createSymlink(
        &mut self,
        args: &CreateSymlinkRequestProto,
    ) -> Result<CreateSymlinkResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("createSymlink"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn getLinkTarget(
        &mut self,
        args: &GetLinkTargetRequestProto,
    ) -> Result<GetLinkTargetResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("getLinkTarget"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn updateBlockForPipeline(
        &mut self,
        args: &UpdateBlockForPipelineRequestProto,
    ) -> Result<UpdateBlockForPipelineResponseProto> {
        Ok(self
            .conn
            .call(Cow::Borrowed("updateBlockForPipeline"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn updatePipeline(
        &mut self,
        args: &UpdatePipelineRequestProto,
    ) -> Result<UpdatePipelineResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("updatePipeline"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn getDelegationToken(
        &mut self,
        args: &GetDelegationTokenRequestProto,
    ) -> Result<GetDelegationTokenResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("getDelegationToken"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn renewDelegationToken(
        &mut self,
        args: &RenewDelegationTokenRequestProto,
    ) -> Result<RenewDelegationTokenResponseProto> {
        Ok(self
            .conn
            .call(Cow::Borrowed("renewDelegationToken"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn cancelDelegationToken(
        &mut self,
        args: &CancelDelegationTokenRequestProto,
    ) -> Result<CancelDelegationTokenResponseProto> {
        Ok(self
            .conn
            .call(Cow::Borrowed("cancelDelegationToken"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn setBalancerBandwidth(
        &mut self,
        args: &SetBalancerBandwidthRequestProto,
    ) -> Result<SetBalancerBandwidthResponseProto> {
        Ok(self
            .conn
            .call(Cow::Borrowed("setBalancerBandwidth"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn getDataEncryptionKey(
        &mut self,
        args: &GetDataEncryptionKeyRequestProto,
    ) -> Result<GetDataEncryptionKeyResponseProto> {
        Ok(self
            .conn
            .call(Cow::Borrowed("getDataEncryptionKey"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn createSnapshot(
        &mut self,
        args: &CreateSnapshotRequestProto,
    ) -> Result<CreateSnapshotResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("createSnapshot"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn renameSnapshot(
        &mut self,
        args: &RenameSnapshotRequestProto,
    ) -> Result<RenameSnapshotResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("renameSnapshot"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn allowSnapshot(
        &mut self,
        args: &AllowSnapshotRequestProto,
    ) -> Result<AllowSnapshotResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("allowSnapshot"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn disallowSnapshot(
        &mut self,
        args: &DisallowSnapshotRequestProto,
    ) -> Result<DisallowSnapshotResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("disallowSnapshot"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn getSnapshottableDirListing(
        &mut self,
        args: &GetSnapshottableDirListingRequestProto,
    ) -> Result<GetSnapshottableDirListingResponseProto> {
        Ok(self
            .conn
            .call(Cow::Borrowed("getSnapshottableDirListing"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn getSnapshotListing(
        &mut self,
        args: &GetSnapshotListingRequestProto,
    ) -> Result<GetSnapshotListingResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("getSnapshotListing"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn deleteSnapshot(
        &mut self,
        args: &DeleteSnapshotRequestProto,
    ) -> Result<DeleteSnapshotResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("deleteSnapshot"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn getSnapshotDiffReport(
        &mut self,
        args: &GetSnapshotDiffReportRequestProto,
    ) -> Result<GetSnapshotDiffReportResponseProto> {
        Ok(self
            .conn
            .call(Cow::Borrowed("getSnapshotDiffReport"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn getSnapshotDiffReportListing(
        &mut self,
        args: &GetSnapshotDiffReportListingRequestProto,
    ) -> Result<GetSnapshotDiffReportListingResponseProto> {
        Ok(self
            .conn
            .call(Cow::Borrowed("getSnapshotDiffReportListing"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn isFileClosed(
        &mut self,
        args: &IsFileClosedRequestProto,
    ) -> Result<IsFileClosedResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("isFileClosed"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn modifyAclEntries(
        &mut self,
        args: &ModifyAclEntriesRequestProto,
    ) -> Result<ModifyAclEntriesResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("modifyAclEntries"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn removeAclEntries(
        &mut self,
        args: &RemoveAclEntriesRequestProto,
    ) -> Result<RemoveAclEntriesResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("removeAclEntries"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn removeDefaultAcl(
        &mut self,
        args: &RemoveDefaultAclRequestProto,
    ) -> Result<RemoveDefaultAclResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("removeDefaultAcl"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn removeAcl(&mut self, args: &RemoveAclRequestProto) -> Result<RemoveAclResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("removeAcl"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn setAcl(&mut self, args: &SetAclRequestProto) -> Result<SetAclResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("setAcl"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn getAclStatus(
        &mut self,
        args: &GetAclStatusRequestProto,
    ) -> Result<GetAclStatusResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("getAclStatus"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn setXAttr(&mut self, args: &SetXAttrRequestProto) -> Result<SetXAttrResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("setXAttr"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn getXAttrs(&mut self, args: &GetXAttrsRequestProto) -> Result<GetXAttrsResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("getXAttrs"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn listXAttrs(&mut self, args: &ListXAttrsRequestProto) -> Result<ListXAttrsResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("listXAttrs"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn removeXAttr(
        &mut self,
        args: &RemoveXAttrRequestProto,
    ) -> Result<RemoveXAttrResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("removeXAttr"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn checkAccess(
        &mut self,
        args: &CheckAccessRequestProto,
    ) -> Result<CheckAccessResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("checkAccess"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn createEncryptionZone(
        &mut self,
        args: &CreateEncryptionZoneRequestProto,
    ) -> Result<CreateEncryptionZoneResponseProto> {
        Ok(self
            .conn
            .call(Cow::Borrowed("createEncryptionZone"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn listEncryptionZones(
        &mut self,
        args: &ListEncryptionZonesRequestProto,
    ) -> Result<ListEncryptionZonesResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("listEncryptionZones"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn reencryptEncryptionZone(
        &mut self,
        args: &ReencryptEncryptionZoneRequestProto,
    ) -> Result<ReencryptEncryptionZoneResponseProto> {
        Ok(self
            .conn
            .call(Cow::Borrowed("reencryptEncryptionZone"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn listReencryptionStatus(
        &mut self,
        args: &ListReencryptionStatusRequestProto,
    ) -> Result<ListReencryptionStatusResponseProto> {
        Ok(self
            .conn
            .call(Cow::Borrowed("listReencryptionStatus"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn getEZForPath(
        &mut self,
        args: &GetEZForPathRequestProto,
    ) -> Result<GetEZForPathResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("getEZForPath"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn setErasureCodingPolicy(
        &mut self,
        args: &SetErasureCodingPolicyRequestProto,
    ) -> Result<SetErasureCodingPolicyResponseProto> {
        Ok(self
            .conn
            .call(Cow::Borrowed("setErasureCodingPolicy"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn unsetErasureCodingPolicy(
        &mut self,
        args: &UnsetErasureCodingPolicyRequestProto,
    ) -> Result<UnsetErasureCodingPolicyResponseProto> {
        Ok(self
            .conn
            .call(Cow::Borrowed("unsetErasureCodingPolicy"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn getECTopologyResultForPolicies(
        &mut self,
        args: &GetECTopologyResultForPoliciesRequestProto,
    ) -> Result<GetECTopologyResultForPoliciesResponseProto> {
        Ok(self
            .conn
            .call(Cow::Borrowed("getECTopologyResultForPolicies"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn getCurrentEditLogTxid(
        &mut self,
        args: &GetCurrentEditLogTxidRequestProto,
    ) -> Result<GetCurrentEditLogTxidResponseProto> {
        Ok(self
            .conn
            .call(Cow::Borrowed("getCurrentEditLogTxid"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn getEditsFromTxid(
        &mut self,
        args: &GetEditsFromTxidRequestProto,
    ) -> Result<GetEditsFromTxidResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("getEditsFromTxid"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn getErasureCodingPolicies(
        &mut self,
        args: &GetErasureCodingPoliciesRequestProto,
    ) -> Result<GetErasureCodingPoliciesResponseProto> {
        Ok(self
            .conn
            .call(Cow::Borrowed("getErasureCodingPolicies"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn addErasureCodingPolicies(
        &mut self,
        args: &AddErasureCodingPoliciesRequestProto,
    ) -> Result<AddErasureCodingPoliciesResponseProto> {
        Ok(self
            .conn
            .call(Cow::Borrowed("addErasureCodingPolicies"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn removeErasureCodingPolicy(
        &mut self,
        args: &RemoveErasureCodingPolicyRequestProto,
    ) -> Result<RemoveErasureCodingPolicyResponseProto> {
        Ok(self
            .conn
            .call(Cow::Borrowed("removeErasureCodingPolicy"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn enableErasureCodingPolicy(
        &mut self,
        args: &EnableErasureCodingPolicyRequestProto,
    ) -> Result<EnableErasureCodingPolicyResponseProto> {
        Ok(self
            .conn
            .call(Cow::Borrowed("enableErasureCodingPolicy"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn disableErasureCodingPolicy(
        &mut self,
        args: &DisableErasureCodingPolicyRequestProto,
    ) -> Result<DisableErasureCodingPolicyResponseProto> {
        Ok(self
            .conn
            .call(Cow::Borrowed("disableErasureCodingPolicy"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn getErasureCodingPolicy(
        &mut self,
        args: &GetErasureCodingPolicyRequestProto,
    ) -> Result<GetErasureCodingPolicyResponseProto> {
        Ok(self
            .conn
            .call(Cow::Borrowed("getErasureCodingPolicy"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn getErasureCodingCodecs(
        &mut self,
        args: &GetErasureCodingCodecsRequestProto,
    ) -> Result<GetErasureCodingCodecsResponseProto> {
        Ok(self
            .conn
            .call(Cow::Borrowed("getErasureCodingCodecs"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn getQuotaUsage(
        &mut self,
        args: &GetQuotaUsageRequestProto,
    ) -> Result<GetQuotaUsageResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("getQuotaUsage"), args)?)
    }
    #[allow(non_snake_case)]
    pub fn listOpenFiles(
        &mut self,
        args: &ListOpenFilesRequestProto,
    ) -> Result<ListOpenFilesResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("listOpenFiles"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn msync(&mut self, args: &MsyncRequestProto) -> Result<MsyncResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("msync"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn satisfyStoragePolicy(
        &mut self,
        args: &SatisfyStoragePolicyRequestProto,
    ) -> Result<SatisfyStoragePolicyResponseProto> {
        Ok(self
            .conn
            .call(Cow::Borrowed("satisfyStoragePolicy"), args)?)
    }

    #[allow(non_snake_case)]
    pub fn getHAServiceState(
        &mut self,
        args: &HAServiceStateRequestProto,
    ) -> Result<HAServiceStateResponseProto> {
        Ok(self.conn.call(Cow::Borrowed("getHAServiceState"), args)?)
    }
}
