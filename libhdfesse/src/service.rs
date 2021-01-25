use hdfesse_proto::ClientNamenodeProtocol::{GetListingRequestProto, GetListingResponseProto};
use std::borrow::Cow;

use crate::rpc;

pub struct HdfsService {
    conn: rpc::HdfsConnection,
}

impl HdfsService {
    #[allow(non_snake_case)]
    pub fn getListing(
        &mut self,
        src: String,
        startAfter: Vec<u8>,
        needLocation: bool,
    ) -> Result<GetListingResponseProto, rpc::RpcError> {
        let mut list = GetListingRequestProto::default();
        list.set_src(src);
        list.set_startAfter(startAfter);
        list.set_needLocation(needLocation);

        let mut data = GetListingResponseProto::new();
        self.conn
            .call(Cow::Borrowed("getListing"), &list, &mut data)?;

        Ok(data)
    }
}
