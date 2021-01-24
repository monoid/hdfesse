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
use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::net::ToSocketAddrs;
use std::{borrow::Cow, fmt::Debug};

use thiserror::Error;

use crate::proto::IpcConnectionContext::*;
use crate::proto::ProtobufRpcEngine::RequestHeaderProto;
use crate::proto::RpcHeader::*;
use protobuf::{CodedInputStream, CodedOutputStream, Message};

const RPC_HEADER: &[u8; 4] = b"hrpc";
const RPC_VERSION: u8 = 9;
const RPC_HDFS_PROTOCOL: &str = "org.apache.hadoop.hdfs.protocol.ClientProtocol";

/**
 * Creating a TCP connection.  This trait may implement different strategies
 * for connecting, including pooling (that recquires some kind of initialization
 * code to implement), exponentional retries, etc.
 */
pub trait Connector {
    type Error: std::error::Error + Debug;
    // async
    fn get_connection<T: ToSocketAddrs>(&self, addr: T) -> Result<TcpStream, Self::Error>;
}

/**
 * Simpliest implementation of connector without any retry.
 */
pub struct SimpleConnector {}

impl Connector for SimpleConnector {
    type Error = io::Error;
    // async
    fn get_connection<T: ToSocketAddrs>(&self, addr: T) -> Result<TcpStream, Self::Error> {
        TcpStream::connect(addr)
    }
}

struct InfiniteSeq {
    val: i32,
}

impl InfiniteSeq {
    fn new() -> Self {
        Self {
            val: -1, // Sequence starts with 0.
        }
    }

    fn next(&mut self) -> i32 {
        // when used for call_id, negative numbers are rejected by the
        // HDFS.  So far we do not care.
        self.val += 1;
        self.val
    }
}

impl Default for InfiniteSeq {
    fn default() -> Self {
        Self::new()
    }
}

/**
 * HDFS connection, i.e. connection to HDFS master NameNode.
 */
pub struct HdfsConnection {
    stream: TcpStream,
    call_id: InfiniteSeq,
    client_id: Vec<u8>,
}

#[derive(Debug, Error)]
pub enum RpcConnectError<CE: std::error::Error + Debug + 'static> {
    #[error(transparent)]
    Connector(CE),
    #[error(transparent)]
    Rpc(RpcError),
}

#[derive(Debug, Error)]
pub enum RpcError {
    #[error(transparent)]
    IO(#[from] io::Error),
    #[error(transparent)]
    Protobuf(#[from] protobuf::ProtobufError),
    #[error("{:?}:{}: {}", .0.get_status(), .0.get_exceptionClassName(), .0.get_errorMsg())]
    Response(Box<RpcResponseHeaderProto>),
}

impl HdfsConnection {
    /** Connect to HDFS master NameNode, creating a new HdfsConnection.
     */
    pub fn new<C: Connector, A: ToSocketAddrs>(
        addr: A,
        connector: &C,
    ) -> Result<Self, RpcConnectError<C::Error>> {
        let stream = connector
            .get_connection(addr)
            .map_err(RpcConnectError::Connector)?;
        Self {
            stream,
            call_id: Default::default(),
            client_id: vec![], // TODO gen random
        }
        .init_connection()
        .map_err(RpcConnectError::Rpc)
    }

    fn init_connection(mut self) -> Result<Self, RpcError> {
        self.stream.set_nodelay(true)?;
        {
            let mut cos = CodedOutputStream::new(&mut self.stream);

            cos.write_all(&RPC_HEADER[..])?;
            cos.write_all(&[
                RPC_VERSION,
                80, // TODO no magic
                0,  // TODO no magic
            ])?;

            let mut hh = RpcRequestHeaderProto::default();
            hh.set_rpcKind(RpcKindProto::RPC_PROTOCOL_BUFFER);
            hh.set_rpcOp(RpcRequestHeaderProto_OperationProto::RPC_FINAL_PACKET);
            hh.set_callId(-3); // Use out-of order call_id for the header.
            hh.set_retryCount(-1);
            hh.set_clientId(self.client_id.clone());

            let mut cc = IpcConnectionContextProto::default();
            cc.mut_userInfo().set_effectiveUser("ib".to_owned());
            cc.set_protocol(RPC_HDFS_PROTOCOL.to_owned());

            Self::send_message_group(&mut cos, &[&hh, &cc])?;
            cos.flush()?;
        }
        Ok(self)
    }

    fn send_message_group(
        cos: &mut CodedOutputStream,
        messages: &[&dyn Message],
    ) -> Result<(), RpcError> {
        let header_len: u32 = messages
            .iter()
            .map(|msg| msg.compute_size())
            .map(|len| len + ::protobuf::rt::compute_raw_varint32_size(len))
            .sum();

        // TODO byteorder
        cos.write_all(&header_len.to_be_bytes())?;
        for msg in messages {
            msg.write_length_delimited_to(cos)?;
        }
        Ok(cos.flush()?)
    }

    pub fn call(
        &mut self,
        method_name: Cow<'_, str>,
        input: &dyn Message,
        output: &mut dyn Message,
    ) -> Result<(), RpcError> {
        // TODO smallvec buffer for async IO? also, const generic can be used
        // for expected header size.  But it makes no lot sense for async, as it
        // does not use stack, but creates structs all the time.
        let mut hh = RpcRequestHeaderProto::default();
        hh.set_rpcKind(RpcKindProto::RPC_PROTOCOL_BUFFER);
        hh.set_rpcOp(RpcRequestHeaderProto_OperationProto::RPC_FINAL_PACKET);
        hh.set_callId(self.call_id.next());
        hh.set_retryCount(-1);
        hh.set_clientId(self.client_id.clone());

        let mut rh = RequestHeaderProto::default();
        rh.set_declaringClassProtocolName(RPC_HDFS_PROTOCOL.to_owned());
        rh.set_clientProtocolVersion(1);
        rh.set_methodName(method_name.into_owned());

        let mut pbs = CodedOutputStream::new(&mut self.stream);

        Self::send_message_group(&mut pbs, &[&hh, &rh, input])?;

        // TODO: byteorder
        let mut data = [0u8; 4];
        self.stream.read_exact(&mut data)?;
        let resp_len = u32::from_be_bytes(data);

        let mut frame = (&mut self.stream).take(resp_len as u64);
        let mut pis = CodedInputStream::new(&mut frame);

        // Delimited message
        let resp_header: RpcResponseHeaderProto = pis.read_message()?;

        if resp_header.get_status() != RpcResponseHeaderProto_RpcStatusProto::SUCCESS {
            return Err(RpcError::Response(Box::new(resp_header)));
        }

        output.merge_from(&mut pis)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_infinite_seq1() {
        let mut is = InfiniteSeq::new();
        assert_eq!(is.next(), 0);
        assert_eq!(is.next(), 1);
        assert_eq!(is.next(), 2);
    }
}
