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
    borrow::BorrowMut,
    collections::VecDeque,
    io::Read,
    sync::{Arc, Mutex},
    time::Instant,
};

use crate::{fs, path::Path, rpc::RpcConnection, service, status::LocatedBlocks};

struct HdfsLatestBlockLocations {
    last_update: Instant,
    state: LocatedBlocks,
}

struct HdfsFileReadingState {
    path: Path<'static>,
    locations: Option<HdfsLatestBlockLocations>,
    failed_hosts: VecDeque<()>, // Do we really keep the order?
                                // What if number of hosts is huge?
}

impl HdfsFileReadingState {
    fn ensure_locations(&mut self) -> &LocatedBlocks {
        todo!()
    }
}

type ReadBuffer = VecDeque<u8>;

pub(crate) struct HdfsReader<'a, R: RpcConnection, S: BorrowMut<service::ClientNamenodeService<R>>>
{
    // TODO It certainly shouldn't be a reference.
    hdfs: &'a fs::Hdfs<R, S>,
    state: Arc<Mutex<HdfsFileReadingState>>,
    length: u64,     // from HdfsFileStatus
    block_size: u64, // from HdfsFileStatus
    buf: ReadBuffer,
    next_start: usize,
}

impl<'a, R: RpcConnection, S: BorrowMut<service::ClientNamenodeService<R>>> HdfsReader<'a, R, S> {
    fn update_buf(&mut self) -> std::io::Result<()> {
        assert!(self.buf.is_empty());
        let mut state_guard = self.state.lock().expect("Poisoned HdfsReader state RwLock");
        // find range
        let block = state_guard
            .ensure_locations()
            .block_list
            .iter()
            .find(|loc| {
                (loc.offset <= self.next_start as _)
                    & ((self.next_start as u64) < loc.offset + self.block_size)
            });
        // try hosts to update buffer
        todo!("Force loading");
    }
}

impl<'a, R: RpcConnection, S: BorrowMut<service::ClientNamenodeService<R>>> Read
    for HdfsReader<'a, R, S>
{
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.buf.is_empty() {
            self.update_buf()?;

            if self.buf.is_empty() {
                return Ok(0);
            }
        }

        let mut pos = 0;
        let ready_len = std::cmp::min(buf.len(), self.buf.len());
        // TODO: It is likely to be somewhat slow, but does it even matter?
        for b in self.buf.drain(0..ready_len) {
            buf[pos] = b;
            pos += 1;
        }
        Ok(pos)
    }
}
