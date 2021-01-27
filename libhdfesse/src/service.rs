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
use hdfesse_proto::ClientNamenodeProtocol::{GetListingRequestProto, GetListingResponseProto};
use std::borrow::Cow;

use crate::rpc;

pub struct HdfsService {
    conn: rpc::HdfsConnection,
}

impl HdfsService {
    pub fn new(conn: rpc::HdfsConnection) -> Self {
        HdfsService { conn }
    }

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

        let data: GetListingResponseProto = self.conn.call(Cow::Borrowed("getListing"), &list)?;

        Ok(data)
    }
}
