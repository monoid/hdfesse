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
use std::{borrow::BorrowMut, collections::VecDeque, io::Read, sync::{Arc, Mutex}, time::{Duration, Instant}};

use hdfesse_proto::hdfs::HdfsFileStatusProto;
use rand::{seq::SliceRandom as _, thread_rng};

use crate::{fs, path::Path, rpc::RpcConnection, service, status::{DatanodeInfo, HdfsFileStatus, LocatedBlock}};

struct Cached<T> {
    value: Option<(T, Instant)>,
}

// I think the API suxx, but it is Rusty.
impl<T> Cached<T> {
    fn new() -> Self {
        Self { value: None }
    }

    fn get(&mut self) -> Option<&T> {
        self.value.as_ref().and_then(|(t, inst)| {
            if inst <= &Instant::now() {
                Some(t)
            } else {
                None
            }
        })
    }

    fn set(&mut self, val: T, expire_in: Duration) -> &mut T {
        &mut self.value.insert((val, Instant::now() + expire_in)).0
    }

    fn check_expiration(&mut self) {
        if self.value.as_ref().map(|(_, deadline)| *deadline > Instant::now()).unwrap_or(false) {
            return;
        }
        self.value = None;
    }

    // It seems this Rusty API doesn't suxx
    fn get_fn<'a, E, F>(&'a mut self, expire_in: Duration, update_fn: &mut F) -> Result<&'a mut T, E>
    where F: FnMut() -> Result<T, E> {
        self.check_expiration();

        if let Some((ref mut val, _)) = self.value {
            return Ok(val);
        }

        let new_val = update_fn()?;
        Ok(
            &mut self.value.insert((new_val, Instant::now() + expire_in)).0
        )
    }
}

impl<T> Default for Cached<T> {
    fn default() -> Self {
        Self { value: None }
    }
}

// It seems this objec is not really needed.
struct HdfsFileReadingState {
    path: Path<'static>,
    file_status: HdfsFileStatusProto,
    length: u64,
    offset: u64,
    expire: Duration,
    locations: Cached<Vec<LocatedBlock>>,
    failed_hosts: VecDeque<()>, // Do we really keep the order?
                                // What if number of hosts is huge?
}

impl HdfsFileReadingState {
    fn new(path: Path<'static>, file_status: HdfsFileStatusProto, expire: Duration) -> Self {
        let length = file_status.get_length();
        Self {
            path,
            expire,
            file_status,
            offset: 0,
            length,
            locations: Default::default(),
            failed_hosts: Default::default(),
        }
    }

    /// Fill the `location` field with a valid value.
    fn ensure_locations<R: RpcConnection, S: BorrowMut<service::ClientNamenodeService<R>>>(
        &mut self, hdfs: &mut fs::Hdfs<R, S>,
    ) -> Result<&mut Vec<LocatedBlock>, &'static str> {
        self.locations.get_fn(self.expire, &mut || {
            hdfs.get_file_block_locations(&self.file_status, self.length, self.offset)
                .map_err(|_| "bad")
        })
    }
}

type ReadBuffer = VecDeque<u8>;

pub(crate) struct HdfsReader<'a, R: RpcConnection, S: BorrowMut<service::ClientNamenodeService<R>>>
{
    // TODO It certainly shouldn't be a reference.
    hdfs: &'a mut fs::Hdfs<R, S>,
    state: Arc<Mutex<HdfsFileReadingState>>,
    length: u64,     // from HdfsFileStatus
    block_size: u64, // from HdfsFileStatus
    buf: ReadBuffer,
    next_start: usize,
}

impl<'a, R: RpcConnection, S: BorrowMut<service::ClientNamenodeService<R>>> HdfsReader<'a, R, S> {
    fn update_buf(&mut self) -> Result<(), &'static str> {
        assert!(self.buf.is_empty());
        // One could simply ignore poisoning, as guard is still
        // created and returned somewhere within Err.
        let mut state_guard = self.state.lock().expect("Poisoned HdfsReader state RwLock");
        // find block for the position.
        let mut block = state_guard
            .ensure_locations(self.hdfs)?
            .into_iter()
            .find(|loc| {
                (loc.offset <= self.next_start as _)
                    & ((self.next_start as u64) < loc.offset + self.block_size)
            });
        if let Some(the_block) = block.as_mut() {
            if the_block.corrupt {
                return Err("the block is corrupt");
            }
            self.update_buf_from_hosts(&mut the_block.cached_locs)
        } else {
            return Err("no host is found");
        }
    }

    fn update_buf_from_hosts(&self, hosts: &mut [Arc<DatanodeInfo>]) -> Result<(), &'static str> {
        // try hosts to update buffer
        // TODO For now, select random order; further, remember failed hosts
        // and reduce their priority.
        hosts.shuffle(&mut thread_rng());
        let mut last_error = None;
        for host in hosts {
            // Connect to host
            // Query data
            // On success:
            return Ok(());
            // else:
            last_error = Some("the error");
        }

        Err(last_error.unwrap_or("no hosts for this chunk"))
    }
}

impl<'a, R: RpcConnection, S: BorrowMut<service::ClientNamenodeService<R>>> Read
    for HdfsReader<'a, R, S>
{
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.buf.is_empty() {
            self.update_buf().map_err(
                // TODO: update_buf should return std::io::Error too.
                |s| std::io::Error::new(std::io::ErrorKind::Other, s),
            )?;

            if self.buf.is_empty() {
                return Ok(0);
            }
        }

        let mut pos = 0;
        let ready_len = std::cmp::min(buf.len(), self.buf.len());
        // TODO: It is likely to be somewhat "slow", but does it even matter?
        // Well, it does, that's why no-copy methods do exist.
        for b in self.buf.drain(0..ready_len) {
            buf[pos] = b;
            pos += 1;
        }
        Ok(pos)
    }
}
