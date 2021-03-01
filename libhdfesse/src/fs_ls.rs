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
use std::iter::{FlatMap, Peekable};

use crate::{fs::FsError, path::Path, rpc::RpcError, service::ClientNamenodeService};
use protobuf::RepeatedField;

use hdfesse_proto::hdfs::HdfsFileStatusProto;

pub struct LsGroupIterator<'a> {
    path_string: String,
    prev_name: Option<Vec<u8>>,
    len: Option<usize>,
    count: usize,

    service: &'a mut ClientNamenodeService,
}

impl<'a> LsGroupIterator<'a> {
    pub fn new(service: &'a mut ClientNamenodeService, path: &Path<'_>) -> Self {
        Self {
            path_string: path.to_path_string(),
            prev_name: Default::default(),
            len: None,
            count: 0,
            service,
        }
    }

    fn next_group(&mut self) -> Result<(usize, RepeatedField<HdfsFileStatusProto>), RpcError> {
        let list_from = self.prev_name.take().unwrap_or_default();
        let mut listing = self
            .service
            .getListing(self.path_string.clone(), list_from, false)?;
        let partial_list = listing.mut_dirList().take_partialListing();

        self.count += partial_list.len();
        let remaining_len = listing.get_dirList().get_remainingEntries() as usize;
        self.len = Some(self.count + remaining_len);

        // Search further from the last value
        // It is very unlikely that partial_list is empty and
        // prev_name is None while remainingEntries is not zero.
        // Perhaps, it should be reported as a server's invalid
        // data.
        self.prev_name = partial_list.last().map(|entry| entry.get_path().to_vec());

        // The remaining_len returns number of items after the last
        // element of the partial_list.  We return here remaining
        // items including the partial_list.
        Ok((remaining_len + partial_list.len(), partial_list))
    }
}

impl<'a> Iterator for LsGroupIterator<'a> {
    type Item = Result<(usize, RepeatedField<HdfsFileStatusProto>), FsError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.len.map(|len| self.count >= len).unwrap_or(false) {
            None
        } else {
            Some(self.next_group().map_err(FsError::Rpc))
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, self.len)
    }
}

type KludgeIterator = itertools::Either<
    std::iter::Map<
        std::vec::IntoIter<HdfsFileStatusProto>,
        fn(HdfsFileStatusProto) -> std::result::Result<HdfsFileStatusProto, FsError>,
    >,
    std::vec::IntoIter<Result<HdfsFileStatusProto, FsError>>,
>;

pub struct LsIterator<'a, F>
where
    F: FnMut(Result<(usize, RepeatedField<HdfsFileStatusProto>), FsError>) -> KludgeIterator,
{
    nested: FlatMap<Peekable<LsGroupIterator<'a>>, KludgeIterator, F>,
}

impl<'a, F> LsIterator<'a, F>
where
    F: FnMut(Result<(usize, RepeatedField<HdfsFileStatusProto>), FsError>) -> KludgeIterator,
{
    pub fn new(f: F, service: &'a mut ClientNamenodeService, path: &Path<'_>) -> Self {
        Self {
            // TODO rewrite manually, as FlatMap doesn't seem to allow
            // to peek.
            nested: LsGroupIterator::new(service, path).peekable().flat_map(f),
        }
    }
}

pub(crate) fn ls_iter<'a>(
    service: &'a mut ClientNamenodeService,
    path: &Path<'_>,
) -> impl Iterator<Item = Result<HdfsFileStatusProto, FsError>> + 'a {
    LsIterator::new(
        |x| {
            match x {
                Ok((_, data)) => itertools::Either::Left(data.into_iter().map(Ok)),
                Err(e) => {
                    // vec![A; 1] calls internal from_elem func that
                    // requires Clone. :(
                    let mut errs = Vec::with_capacity(1);
                    errs.push(Result::Err(e));
                    itertools::Either::Right(errs.into_iter())
                }
            }
        },
        service,
        path,
    )
}

impl<'a, F> Iterator for LsIterator<'a, F>
where
    F: FnMut(Result<(usize, RepeatedField<HdfsFileStatusProto>), FsError>) -> KludgeIterator,
{
    type Item = Result<HdfsFileStatusProto, FsError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.nested.next()
    }
}
