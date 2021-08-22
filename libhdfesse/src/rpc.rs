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
use std::{borrow::Cow, fmt::Debug, ops::Deref};

use thiserror::Error;
use tracing::{instrument, trace};

use crate::hdconfig;
use crate::path::Path;
use crate::util;
use hdfesse_proto::IpcConnectionContext::*;
use hdfesse_proto::ProtobufRpcEngine::RequestHeaderProto;
use hdfesse_proto::RpcHeader::*;
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
    // async
    fn get_connection<T: ToSocketAddrs>(&self, addr: T) -> Result<TcpStream, io::Error>;
}

/**
 * Simpliest implementation of connector without any retry.
 */
#[derive(Debug)]
pub struct SimpleConnector {}

impl Connector for SimpleConnector {
    // async
    fn get_connection<T: ToSocketAddrs>(&self, addr: T) -> Result<TcpStream, io::Error> {
        TcpStream::connect(addr)
    }
}

#[derive(Debug)]
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

pub type RpcStatus = RpcResponseHeaderProto_RpcStatusProto;
pub type RpcErrorCode = RpcResponseHeaderProto_RpcErrorCodeProto;

#[derive(Debug, Clone, Copy)]
pub enum RpcErrorKind {
    Snapshot,
}

#[derive(Debug, Error)]
pub enum RpcError {
    #[error(transparent)]
    Connector(io::Error),
    #[error(transparent)]
    NoUser(Box<dyn std::error::Error + Send + Sync>),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Protobuf(#[from] protobuf::ProtobufError),
    /// Most operation fail silently if you provide incorrect
    /// arguments; for example, if you list nonexistent directory, you
    /// will just get an empty list.  But some operations do fail,
    /// like snapshot-related ones.  This variant is for such errors.
    #[error("{:?}: {}", .kind, .error_msg)]
    KnownError {
        status: RpcStatus,
        kind: RpcErrorKind,
        error_msg: String,
        error_detail: RpcErrorCode,
        exception: String,
        method: String,
    },
    /// Non-fatal error: you may retry this operation or issue an
    /// another one.
    #[error("error: {:?}: {}", .status, .error_msg)]
    ErrorResponse {
        status: RpcStatus,
        error_msg: String,
        error_detail: RpcErrorCode,
        exception: String,
        method: String,
    },
    /// Fatal error: you have to close connection and open a new one.
    #[error("fatal error: {:?}: {}", .status, .error_msg)]
    FatalResponse {
        status: RpcStatus,
        error_msg: String,
        error_detail: RpcErrorCode,
        exception: String,
        method: String,
    },
    #[error("incomplete protobuf record")]
    IncompleteResponse,
}

impl RpcError {
    #[inline]
    pub fn get_class_name(&self) -> Option<&str> {
        match self {
            RpcError::KnownError { exception, .. } => Some(exception),
            RpcError::ErrorResponse { exception, .. } => Some(exception),
            RpcError::FatalResponse { exception, .. } => Some(exception),
            _ => None,
        }
    }
}

pub static ERROR_CLASS_MAP: ::phf::Map<&'static str, RpcErrorKind> = ::phf::phf_map! {
    "org/apache/hadoop/hdfs/protocol/SnapshotException" => RpcErrorKind::Snapshot,
};

/**
 * HDFS connecion, e.g. simple or HA.
 */
pub trait RpcConnection {
    /// Get user name.
    fn get_user(&self) -> &str;

    /// Perform an rpc call over the connection.
    // TODO I failed to read to &dyn Message easily with current
    // protobuf, thus this method has Output type argument.
    fn call<Output: Message>(
        &mut self,
        method_name: Cow<'_, str>,
        input: &dyn Message,
    ) -> Result<Output, RpcError>;

    /// Shoutdown the connection.
    fn shutdown(self) -> Result<(), RpcError>;
}

/**
 * HDFS connection, i.e. connection to HDFS master NameNode.
 */
#[derive(Debug)]
pub struct HdfsConnection {
    stream: TcpStream,
    user: Box<str>,
    call_id: InfiniteSeq,
    client_id: [u8; 16],
}

impl HdfsConnection {
    pub fn new_from_path<C: Connector, Cfg: Deref<Target = hdconfig::Config>>(
        config: Cfg,
        path: Path<'_>,
        connector: &C,
    ) -> Result<Self, RpcError> {
        let host = path.host().expect("TODO: expected host");
        for serv in &config.services {
            if serv.name.as_ref() == host {
                let addr = serv.rpc_nodes[0].rpc_address.as_ref();
                let user = path.user().map(Into::into);
                return Self::new_with_user(user, addr, connector);
            }
        }
        unimplemented!("TODO: handle service undefined in the config")
    }

    pub fn new_with_user<C: Connector, A: ToSocketAddrs>(
        user: Option<Cow<'_, str>>,
        addr: A,
        connector: &C,
    ) -> Result<Self, RpcError> {
        let user = user.map(Ok).unwrap_or_else(|| {
            util::get_username()
                .map(Into::into)
                .map_err(RpcError::NoUser)
        })?;
        Self::new(user, addr, connector)
    }

    pub fn new_without_user<C: Connector, A: ToSocketAddrs>(
        addr: A,
        connector: &C,
    ) -> Result<Self, RpcError> {
        Self::new_with_user(None, addr, connector)
    }

    /** Connect to HDFS master NameNode, creating a new HdfsConnection.
     */
    pub fn new<C: Connector, A: ToSocketAddrs>(
        user: Cow<'_, str>,
        addr: A,
        connector: &C,
    ) -> Result<Self, RpcError> {
        let stream = connector
            .get_connection(addr)
            .map_err(RpcError::Connector)?;
        Self {
            stream,
            user: user.into(),
            call_id: Default::default(),
            // "ClientId must be a UUID - that is 16 octets"
            // (hadoop/../RetryCache.java).
            client_id: *uuid::Uuid::new_v4().as_bytes(),
        }
        .init_connection()
    }

    #[instrument]
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
            hh.set_clientId(Vec::from(&self.client_id[..]));

            let mut cc = IpcConnectionContextProto::default();
            cc.mut_userInfo().set_effectiveUser(self.user.to_string());
            cc.set_protocol(RPC_HDFS_PROTOCOL.to_owned());

            Self::send_message_group(&mut cos, &[&hh, &cc])?;
            cos.flush()?;
        }
        Ok(self)
    }

    #[instrument(skip(cos))]
    fn send_message_group(
        cos: &mut CodedOutputStream<'_>,
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
}

impl RpcConnection for HdfsConnection {
    fn get_user(&self) -> &str {
        &self.user
    }

    #[instrument]
    fn call<Output: Message>(
        &mut self,
        method_name: Cow<'_, str>,
        input: &dyn Message,
    ) -> Result<Output, RpcError> {
        // TODO smallvec buffer for async IO? also, const generic can be used
        // for expected header size.  But it makes no lot sense for async, as it
        // does not use stack, but creates structs all the time.
        let mut hh = RpcRequestHeaderProto::default();
        hh.set_rpcKind(RpcKindProto::RPC_PROTOCOL_BUFFER);
        hh.set_rpcOp(RpcRequestHeaderProto_OperationProto::RPC_FINAL_PACKET);
        hh.set_callId(self.call_id.next());
        hh.set_retryCount(-1);
        hh.set_clientId(Vec::from(&self.client_id[..]));

        let mut rh = RequestHeaderProto::default();
        rh.set_declaringClassProtocolName(RPC_HDFS_PROTOCOL.to_owned());
        rh.set_clientProtocolVersion(1);
        rh.set_methodName(method_name.to_string());

        let mut pbs = CodedOutputStream::new(&mut self.stream);

        Self::send_message_group(&mut pbs, &[&hh, &rh, input])?;

        // TODO: byteorder
        let mut data = [0u8; 4];
        self.stream.read_exact(&mut data)?;
        let resp_len = u32::from_be_bytes(data);

        let mut frame = (&mut self.stream).take(resp_len as u64);
        let mut pis = CodedInputStream::new(&mut frame);

        // Delimited message
        let mut resp_header: RpcResponseHeaderProto = pis.read_message()?;

        let res = match resp_header.get_status() {
            // Delimited message
            RpcStatus::SUCCESS => Ok(pis.read_message()?),
            RpcStatus::ERROR => {
                if let Some(kind) = ERROR_CLASS_MAP
                    .get(resp_header.get_exceptionClassName())
                    .copied()
                {
                    Err(RpcError::KnownError {
                        status: resp_header.get_status(),
                        kind,
                        error_msg: resp_header.take_errorMsg(),
                        error_detail: resp_header.get_errorDetail(),
                        exception: resp_header.take_exceptionClassName(),
                        method: method_name.to_string(),
                    })
                } else {
                    Err(RpcError::ErrorResponse {
                        status: resp_header.get_status(),
                        error_msg: resp_header.take_errorMsg(),
                        error_detail: resp_header.get_errorDetail(),
                        exception: resp_header.take_exceptionClassName(),
                        method: method_name.to_string(),
                    })
                }
            }
            RpcStatus::FATAL => Err(RpcError::FatalResponse {
                status: resp_header.get_status(),
                error_msg: resp_header.take_errorMsg(),
                error_detail: resp_header.get_errorDetail(),
                exception: resp_header.take_exceptionClassName(),
                method: method_name.to_string(),
            }),
        };

        trace!(
            target = "call",
            "call complete: {}, res: {:?}",
            method_name,
            res
        );

        res
    }

    /// Send a closing packet to the server.  It should be just
    /// Drop::drop, but it wouldn't work for the anticipated async
    /// version.
    #[instrument]
    fn shutdown(mut self) -> Result<(), RpcError> {
        let mut hh = RpcRequestHeaderProto::default();
        hh.set_rpcKind(RpcKindProto::RPC_PROTOCOL_BUFFER);
        hh.set_rpcOp(RpcRequestHeaderProto_OperationProto::RPC_CLOSE_CONNECTION);
        hh.set_callId(self.call_id.next());
        hh.set_retryCount(-1);
        hh.set_clientId(Vec::from(&self.client_id[..]));

        {
            let mut pbs = CodedOutputStream::new(&mut self.stream);
            Self::send_message_group(&mut pbs, &[&hh])?;
            pbs.flush()?;
        }
        // the stream will be closed by drop.
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
