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
use std::{cmp::Reverse, io::Write};

use super::Command;
use crate::cli::ls_output::{LineFormat, Record};
use hdfesse_proto::hdfs::HdfsFileStatusProto;
use libhdfesse::fs::{FsError, Hdfs};
use libhdfesse::path::{Path, PathError};
use libhdfesse::rpc::RpcError;
use libhdfesse::service::ClientNamenodeService;
use protobuf::RepeatedField;
use structopt::StructOpt;
use thiserror::Error;

/*
 * See
 * hadoop/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/shell/Ls.java
 */
/// ls options are factored out to separate struct for convenience.
#[derive(Debug, StructOpt)]
pub struct LsOpts {
    // TODO unimplemented
    #[structopt(
        short,
        name = "directory",
        help = "Directories are listed as plain files"
    )]
    directory: bool,
    #[structopt(
        short = "t",
        name = "sort_mtime",
        conflicts_with = "stream",
        help = "Sort output by modification time (most recent first)"
    )]
    sort_mtime: bool,
    #[structopt(
        short = "u",
        help = "Use access time rather than modification time for display and sorting"
    )]
    atime: bool,
    #[structopt(short = "C", help = "Display the paths of files and directories only")]
    path_only: bool,
    #[structopt(short = "r", help = "Reverse the sort order")]
    sort_reversed: bool,
    #[structopt(
        short = "R",
        conflicts_with = "stream",
        conflicts_with = "directory",
        help = "Recursively list subdirectories encountered"
    )]
    // TODO unimplemented
    recursive: bool,
    #[structopt(
        short = "S",
        conflicts_with = "sort_mtime",
        conflicts_with = "stream",
        help = "Sort output by file size"
    )]
    sort_size: bool,
    // TODO: all it does is it replaces certain Unicode char types by
    // '?' in whole output string (not only filename, but owner and
    // group too).
    #[structopt(short = "q", help = "Print ? instead of non-printable characters")]
    quote: bool,
    #[structopt(
        short = "h",
        help = "Formats the sizes of files in a human-readable fashion"
    )]
    human: bool,
    // TODO unimplemented
    #[structopt(long = "--stream", help = "Streaiming mode")]
    stream: bool,
    // TODO ...
}

#[derive(Debug, StructOpt)]
pub struct LsArgs {
    #[structopt(flatten)]
    opts: LsOpts,
    #[structopt(name = "path")]
    paths: Vec<String>,
}

#[derive(Debug, Error)]
pub enum LsError {
    #[error(transparent)]
    Uri(PathError),
    #[error("ls: {0}")]
    Fs(#[from] FsError),
    #[error(transparent)]
    LocalIo(std::io::Error),
}

// TODO it has to be moved to libhdfesse::fs and made public.
struct LsGroupIterator<'a> {
    path: &'a str,
    prev_name: Option<Vec<u8>>,
    len: Option<usize>,
    count: usize,

    service: &'a mut ClientNamenodeService,
}

impl<'a> LsGroupIterator<'a> {
    fn new(service: &'a mut ClientNamenodeService, path: &'a str) -> Self {
        Self {
            path,
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
            .getListing(self.path.to_owned(), list_from, false)?;
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
    type Item = Result<(usize, RepeatedField<HdfsFileStatusProto>), RpcError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.len.map(|len| self.count >= len).unwrap_or(false) {
            None
        } else {
            Some(self.next_group())
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, self.len)
    }
}

pub struct Ls<'a> {
    hdfs: &'a mut Hdfs,
}

impl<'a> Ls<'a> {
    pub fn new(hdfs: &'a mut Hdfs) -> Self {
        Self { hdfs }
    }

    fn list_dir(&mut self, path: &str, args: &LsOpts) -> Result<(), LsError> {
        // TODO resolving
        let path = Path::new(path).map_err(LsError::Uri)?;
        let path_str = path.to_path_string();
        // Ensure file exists.
        self.hdfs
            .get_file_info(path_str.as_str().into())
            .map_err(LsError::Fs)?;

        let mut is_first = true;
        let mut data = Vec::new();

        let info = match self
            .hdfs
            .service
            // TODO use hdfs method
            .getFileInfo(path_str.clone())
            .map_err(FsError::Rpc)?
        {
            Some(info) => info,
            None => return Err(FsError::NotFound(path_str).into()),
        };

        let stdout_obj = std::io::stdout();
        let mut stdout = std::io::LineWriter::new(stdout_obj.lock());

        if args.directory {
            data.push(Record::from_hdfs_file_status(info, args.atime));
        } else {
            for group in LsGroupIterator::new(&mut self.hdfs.service, &path_str) {
                let (remaining_len, group) = group.map_err(FsError::Rpc)?;

                // Noop for all iterations except the first, unless new file
                // will appear in process of listing.
                data.reserve(remaining_len);

                if !args.recursive & is_first {
                    // For first item, remaining_len is the total length.
                    println!("Found {} items", remaining_len);
                    is_first = false;
                }

                data.extend(group.into_iter().map(|entry: HdfsFileStatusProto| {
                    Record::from_hdfs_file_status(entry, args.atime)
                }));
            }
        }

        if args.sort_mtime {
            if args.sort_reversed {
                data.sort_unstable_by_key(|a| a.timestamp);
            } else {
                // Please note that by default `hdfs dfs -ls` sorts
                // by timestamp from older to newer.
                data.sort_unstable_by_key(|a| Reverse(a.timestamp));
            }
        } else if args.sort_size {
            if args.sort_reversed {
                data.sort_unstable_by_key(|a| a.size);
            } else {
                // Please note that by default `hdfs dfs -ls` sorts
                // by file size from largest to smallerst.
                data.sort_unstable_by_key(|a| Reverse(a.size));
            }
        } else {
            // Default sort is sort by name; can be just reversed if
            // needed.
            if args.sort_reversed {
                data.reverse();
            }
        }

        let mut format = if args.path_only {
            LineFormat::compact(path)
        } else {
            LineFormat::full(path, args.human)
        };
        // Using streaming approach is crucial for huge directories where
        // data does not fit into memory.  For sorted data, one has to
        // collect everything in memory; but in case of problem, you can
        // at least get default list and sort it with some external tool.
        for entry in data.iter() {
            for fmt in &mut format.formatters {
                fmt.update_len(entry);
            }
        }
        for entry in data.iter() {
            for fmt in &format.formatters {
                fmt.print(&mut stdout, entry).map_err(LsError::LocalIo)?;
            }
            writeln!(&mut stdout).map_err(LsError::LocalIo)?;
        }
        Ok(())
    }
}

impl<'a> Command for Ls<'a> {
    type Args = LsArgs;
    type Error = LsError;

    fn run(&mut self, args: Self::Args) -> Result<i32, Self::Error> {
        let mut has_err = false;
        for path in args.paths {
            if let Err(e) = self.list_dir(&path, &args.opts) {
                if let LsError::LocalIo(ioe) = &e {
                    if ioe.kind() == std::io::ErrorKind::BrokenPipe {
                        // Exit early because of EPIPE
                        break;
                    }
                }
                has_err = true;
                eprintln!("{}", e);
            }
        }
        Ok(if has_err { 1 } else { 0 })
    }
}
