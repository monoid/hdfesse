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
use libhdfesse::fs::{Hdfs, HdfsError};
use libhdfesse::path::{Path, PathError};
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
    Fs(#[from] HdfsError),
    #[error(transparent)]
    LocalIo(std::io::Error),
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

        let stdout_obj = std::io::stdout();
        let mut stdout = std::io::LineWriter::new(stdout_obj.lock());

        let mut data = if args.directory {
            vec![Record::from_hdfs_file_status(
                self.hdfs.get_file_info(&path).map_err(LsError::Fs)?,
                args.atime,
            )]
        } else {
            self.hdfs
                .list_status(&path)?
                .map(|res| res.map(|ent| Record::from_hdfs_file_status(ent, args.atime)))
                .collect::<Result<Vec<_>, HdfsError>>()?
        };

        if !args.recursive {
            println!("Found {} items", data.len());
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
