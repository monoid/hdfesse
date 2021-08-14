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
    iter::{ExactSizeIterator, FusedIterator},
};

use crate::{
    fs::FsError,
    path::Path,
    rpc::{RpcConnection, RpcError},
    service::ClientNamenodeService,
};
use protobuf::RepeatedField;

use hdfesse_proto::hdfs::HdfsFileStatusProto;

pub struct LsGroupIterator<R: RpcConnection, SRef: BorrowMut<ClientNamenodeService<R>>> {
    path_string: String,
    prev_name: Option<Vec<u8>>,
    len: Option<usize>,
    count: usize,

    service: SRef,
    _phantom: std::marker::PhantomData<R>,
}

impl<R: RpcConnection, SRef: BorrowMut<ClientNamenodeService<R>>> LsGroupIterator<R, SRef> {
    pub fn new(service: SRef, path: &Path<'_>) -> Self {
        Self {
            path_string: path.to_path_string(),
            prev_name: Default::default(),
            len: None,
            count: 0,
            service,
            _phantom: std::marker::PhantomData,
        }
    }

    fn next_group(&mut self) -> Result<(usize, RepeatedField<HdfsFileStatusProto>), RpcError> {
        let list_from = self.prev_name.take().unwrap_or_default();
        let mut listing =
            self.service
                .borrow_mut()
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

impl<R: RpcConnection, SRef: BorrowMut<ClientNamenodeService<R>>> Iterator
    for LsGroupIterator<R, SRef>
{
    type Item = Result<(usize, RepeatedField<HdfsFileStatusProto>), FsError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.len.map(|len| self.count >= len).unwrap_or(false) {
            None
        } else {
            Some(self.next_group().map_err(FsError::Rpc))
        }
    }
}

pub(crate) struct LsIterator<CI, I, E> {
    gi: Option<CI>,
    current: Result<std::vec::IntoIter<I>, E>,
    expected: usize,
}

impl<CI, I, E, V> LsIterator<CI, I, E>
where
    CI: Iterator<Item = Result<(usize, V), E>>,
    V: IntoIterator<IntoIter = std::vec::IntoIter<I>>,
{
    /// Fetches new chunk from the group iterator, if current chunk is empty.
    fn ensure_new_data(&mut self) {
        if let Some(ref mut gi) = self.gi {
            // it is false for Err(_) because we never read after the error.
            let should_fetch = self
                .current
                .as_ref()
                .map(|it| it.len() == 0)
                .unwrap_or(false);
            if should_fetch {
                if self.expected == 0 {
                    // We have reached the end of the last chunk.
                    // Nothing is remained.
                    self.gi = None;
                } else {
                    match gi.next() {
                        None => {
                            self.gi = None;
                        }
                        Some(Ok((expected, new_data))) => {
                            self.expected = expected;
                            self.current = Ok(new_data.into_iter());
                        }
                        Some(Err(e)) => {
                            self.expected = 1;
                            self.current = Err(e);
                        }
                    }
                }
            }
        }
    }

    pub(crate) fn new(ci: CI) -> Self {
        let mut it = LsIterator {
            gi: Some(ci),
            current: Ok(vec![].into_iter()),
            expected: 1, // Fake value to fetch something
        };
        it.ensure_new_data();
        it
    }
}

impl<CI, I, E, V> Iterator for LsIterator<CI, I, E>
where
    CI: Iterator<Item = Result<(usize, V), E>>,
    V: IntoIterator<IntoIter = std::vec::IntoIter<I>>,
{
    fn size_hint(&self) -> (usize, Option<usize>) {
        let remain_len = match self.gi {
            Some(_) => self.expected + self.current.as_ref().map(|it| it.len()).unwrap_or(0),
            None => 0,
        };
        // Vec adds one, so we sub. :D
        (remain_len.saturating_sub(1), Some(remain_len))
    }

    type Item = Result<I, E>;

    fn next(&mut self) -> Option<Self::Item> {
        self.ensure_new_data();
        match self.gi {
            None => None,
            Some(_) => {
                match &mut self.current {
                    Ok(it) => it.next().map(Ok),
                    Err(_) => {
                        let mut val = Ok(vec![].into_iter()); // Should not allocate!
                        std::mem::swap(&mut self.current, &mut val);
                        self.gi.take();
                        Some(val.map(|_| unreachable!()))
                    }
                }
            }
        }
    }
}

impl<I, E, CI, V> FusedIterator for LsIterator<CI, I, E>
where
    CI: Iterator<Item = Result<(usize, V), E>>,
    V: IntoIterator<IntoIter = std::vec::IntoIter<I>>,
{
}

// It is very enticing to implement ExactSizeIterator for the
// LsIterator, however, we cannot guarantee that we will return exact
// number of elments, as 1. error may happen, 2. new files may be
// created in process.

#[cfg(test)]
mod test {
    use super::*;

    #[derive(Eq, PartialEq, Debug)]
    struct Error {}

    #[test]
    fn test_empty() {
        let gi: Vec<Result<(usize, Vec<i32>), Error>> = vec![Ok((0, vec![]))];
        assert_eq!(LsIterator::new(gi.into_iter()).collect::<Vec<_>>(), vec![]);
    }

    #[test]
    fn test_empty_size_hint() {
        let gi: Vec<Result<(usize, Vec<i32>), Error>> = vec![Ok((0, vec![]))];
        assert_eq!(LsIterator::new(gi.into_iter()).size_hint(), (0, Some(0)));
    }

    #[test]
    fn test_really_empty() {
        let gi: Vec<Result<(usize, Vec<i32>), Error>> = vec![];
        assert_eq!(LsIterator::new(gi.into_iter()).collect::<Vec<_>>(), vec![]);
    }

    #[test]
    fn test_really_empty_size_hint() {
        let gi: Vec<Result<(usize, Vec<i32>), Error>> = vec![];
        assert_eq!(LsIterator::new(gi.into_iter()).size_hint(), (0, Some(0)));
    }

    #[test]
    fn test_single() {
        let gi: Vec<Result<(usize, Vec<i32>), Error>> = vec![Ok((0, vec![1, 2]))];
        assert_eq!(
            LsIterator::new(gi.into_iter()).collect::<Vec<_>>(),
            vec![Ok(1), Ok(2)]
        );
    }

    #[test]
    fn test_single_size_hint() {
        let gi: Vec<Result<(usize, Vec<i32>), Error>> = vec![Ok((0, vec![1, 2]))];
        assert_eq!(LsIterator::new(gi.into_iter()).size_hint(), (1, Some(2)));
    }

    #[test]
    fn test_chunks() {
        let gi: Vec<Result<(usize, Vec<i32>), Error>> =
            vec![Ok((2, vec![1, 2])), Ok((0, vec![3, 4]))];
        assert_eq!(
            LsIterator::new(gi.into_iter()).collect::<Vec<_>>(),
            vec![Ok(1), Ok(2), Ok(3), Ok(4)]
        );
    }

    #[test]
    fn test_chunks_size_hint() {
        let gi: Vec<Result<(usize, Vec<i32>), Error>> =
            vec![Ok((2, vec![1, 2])), Ok((0, vec![3, 4]))];
        let mut it = LsIterator::new(gi.into_iter());
        assert_eq!(it.size_hint(), (3, Some(4)));
        it.next();
        assert_eq!(it.size_hint(), (2, Some(3)));
        it.next();
        assert_eq!(it.size_hint(), (1, Some(2)));
        it.next();
        assert_eq!(it.size_hint(), (0, Some(1)));
        it.next();
        assert_eq!(it.size_hint(), (0, Some(0)));
    }

    #[test]
    fn test_chunk_error() {
        let gi: Vec<Result<(usize, Vec<i32>), Error>> =
            vec![Ok((2, vec![1, 2])), Err(Error {}), Ok((0, vec![3, 4]))];
        assert_eq!(
            LsIterator::new(gi.into_iter()).collect::<Vec<_>>(),
            vec![Ok(1), Ok(2), Err(Error {})]
        );
    }

    #[test]
    fn test_chunk_error_size_hint() {
        let gi: Vec<Result<(usize, Vec<i32>), Error>> =
            vec![Ok((2, vec![1, 2])), Err(Error {}), Ok((0, vec![3, 4]))];
        let mut it = LsIterator::new(gi.into_iter());
        assert_eq!(it.size_hint(), (3, Some(4)));
        it.next().unwrap().unwrap();
        assert_eq!(it.size_hint(), (2, Some(3)));
        it.next().unwrap().unwrap();
        assert_eq!(it.size_hint(), (1, Some(2)));
        it.next().unwrap().unwrap_err();
        assert_eq!(it.size_hint(), (0, Some(0)));
        it.next();
        assert_eq!(it.size_hint(), (0, Some(0)));
    }

    #[test]
    fn test_first_error() {
        let gi: Vec<Result<(usize, Vec<i32>), Error>> =
            vec![Err(Error {}), Ok((2, vec![1, 2])), Ok((0, vec![3, 4]))];
        assert_eq!(
            LsIterator::new(gi.into_iter()).collect::<Vec<_>>(),
            vec![Err(Error {})]
        );
    }

    #[test]
    fn test_first_error_size_hint() {
        let gi: Vec<Result<(usize, Vec<i32>), Error>> =
            vec![Err(Error {}), Ok((2, vec![1, 2])), Ok((0, vec![3, 4]))];
        let mut it = LsIterator::new(gi.into_iter());
        assert_eq!(it.size_hint(), (0, Some(1)));
        it.next();
        assert_eq!(it.size_hint(), (0, Some(0)));
    }
}
