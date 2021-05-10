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
/*!
 * High-availability RPC connection. It tries to resend RPC query
 * if current fails and the query is retriable.
 */

use std::iter::Cycle;
use tracing::{instrument, trace};

use crate::{
    hdconfig,
    rpc::{Connector, HdfsConnection, RpcConnection, RpcError, RpcErrorCode},
    util,
};

/**
* High-availability RPC connection.
*/
#[derive(Debug)]
pub struct HaHdfsConnection<C: Connector + std::fmt::Debug> {
    user: Box<str>,
    current: Option<HdfsConnection>,
    connector: C,
    connection_num: usize,
    // TODO: it allocates String on each connection attempt, but it seems
    // to be minor problem.
    connections: Cycle<std::vec::IntoIter<String>>,
}

impl<C: Connector + std::fmt::Debug> HaHdfsConnection<C> {
    pub fn new(namenode: &hdconfig::NameserviceConfig, connector: C) -> Result<Self, RpcError> {
        let connection_num = namenode.rpc_nodes.len();

        Ok(Self {
            user: util::get_username().unwrap().into(),
            current: None,
            connector,
            connection_num,
            connections: namenode
                .rpc_nodes
                .iter()
                .map(|node| node.rpc_address.to_string())
                .collect::<Vec<String>>()
                .into_iter()
                .cycle(),
        })
    }

    fn ensure_connection(
        &mut self,
        attempts_left: &mut usize,
    ) -> Result<&mut HdfsConnection, RpcError> {
        if let Some(ref mut conn) = self.current {
            Ok(conn)
        } else {
            self.try_connect(attempts_left)
        }
    }

    #[instrument]
    fn try_connect(&mut self, attempts_left: &mut usize) -> Result<&mut HdfsConnection, RpcError> {
        let mut last_err = None;

        for addr in self.connections.by_ref().take(*attempts_left) {
            // TODO: retry on network error?
            trace!(
                target = "connect",
                "trying {:?}, attempts left {}",
                addr,
                attempts_left
            );
            *attempts_left -= 1;
            match HdfsConnection::new_with_user(
                Some(self.user.as_ref().into()),
                &addr,
                &self.connector,
            ) {
                Ok(conn) => {
                    self.current = Some(conn);
                    return Ok(self.current.as_mut().unwrap());
                }
                Err(e) => {
                    last_err = Some(e);
                    // TODO: pause
                }
            }
        }
        // TODO fails if all nodes are standby/observer/intiailized
        Err(last_err.unwrap())
    }

    fn fail(&mut self) {
        self.current.take().map(|c| c.shutdown());
    }
}

impl<C: Connector + std::fmt::Debug> RpcConnection for HaHdfsConnection<C> {
    fn get_user(&self) -> &str {
        &self.user
    }

    #[instrument]
    fn call<Output: protobuf::Message>(
        &mut self,
        method_name: std::borrow::Cow<'_, str>,
        input: &dyn protobuf::Message,
    ) -> Result<Output, RpcError> {
        // It has to share count with HaHdfsconnection::connect loop.

        let mut attempts_left = self.connection_num;

        loop {
            let conn = self.ensure_connection(&mut attempts_left)?;
            let res = conn.call(method_name.clone(), input);
            if let Err(RpcError::ErrorResponse {
                error_detail: RpcErrorCode::ERROR_APPLICATION,
                exception: ref ex,
                ..
            }) = &res
            {
                if ex == "org.apache.hadoop.ipc.StandbyException" {
                    trace!(
                        taget = "call",
                        "Use next service because of StandbyException: {:?}",
                        res
                    );
                    self.fail();
                    continue;
                }
            }
            // else
            return res;
        }
    }

    fn shutdown(self) -> Result<(), RpcError> {
        match self.current {
            Some(conn) => conn.shutdown(),
            None => Ok(()),
        }
    }
}
