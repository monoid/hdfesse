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
use super::Command;
use anyhow::Result;
use hdfesse_proto::hdfs::HdfsFileStatusProto_FileType;
use libhdfesse::service::HdfsService;
use structopt::StructOpt;

fn format_flag_group(group: u32) -> &'static str {
    match group {
        0 => "---",
        1 => "--x",
        2 => "-w-",
        3 => "-wx",
        4 => "r--",
        5 => "r-x",
        6 => "rw-",
        7 => "rwx",
        _ => unreachable!(),
    }
}

fn format_flags(flags: u32, type_: HdfsFileStatusProto_FileType) -> String {
    let mut res = String::with_capacity(10);
    res.push(match type_ {
        HdfsFileStatusProto_FileType::IS_DIR => 'd',
        HdfsFileStatusProto_FileType::IS_FILE => '-',
        HdfsFileStatusProto_FileType::IS_SYMLINK => 's',
    });
    for offset in [6u32, 3, 0].iter() {
        res.extend(format_flag_group((flags >> offset) & 0x7).chars());
    }
    res
}

/*
 * See
 * hadoop/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/shell/Ls.java
 */
#[derive(Debug, StructOpt)]
pub struct LsArgs {
    #[structopt(short)]
    directory: bool,
    // TODO mtime and atime are exclusive
    #[structopt(short)]
    mtime: bool,
    #[structopt(short)]
    atime: bool,
    #[structopt(short = "C")]
    path_only: bool,
    #[structopt(short = "r")]
    reversed: bool,
    #[structopt(short = "R")]
    recursive: bool,
    #[structopt(name = "path")]
    paths: Vec<String>,
    // TODO ...
}

pub struct Ls<'a> {
    service: &'a mut HdfsService,
}

impl<'a> Ls<'a> {
    pub fn new(service: &'a mut HdfsService) -> Self {
        Ls { service }
    }

    fn list_dir(&mut self, path: String, args: &LsArgs) -> Result<()> {
        let mut state: Option<Vec<u8>> = None;

        loop {
            let is_first = &state.is_none();
            let list_from = state.unwrap_or(vec![]);
            let listing = self.service.getListing(path.clone(), list_from, false)?;
            let partial_list = listing.get_dirList().get_partialListing();
            let list_len = partial_list.len();

            if !args.recursive & is_first {
                println!(
                    "Found {} items",
                    partial_list.len() + listing.get_dirList().get_remainingEntries() as usize
                );
            }

            // Using streaming approach is crucial for huge directories where
            // data does not fit into memory.  For sorted data, one has to
            // collect everything in memory; but in case of problem, you can
            // at least get default list and sort it with some external tool.
            for entry in partial_list.iter() {
                println!(
                    "{} {} {} {} {} {} {}",
                    format_flags(entry.get_permission().get_perm(), entry.get_fileType()),
                    entry.get_block_replication(),
                    entry.get_owner(),
                    entry.get_group(),
                    entry.get_length(),
                    // TODO format date and time
                    entry.get_modification_time(),
                    // TODO original implementation uses different lossy char
                    String::from_utf8_lossy(entry.get_path()),
                );
            }
            if listing.get_dirList().get_remainingEntries() == 0 {
                break;
            }
            if list_len > 0 {
                // Search further from the last value
                state = partial_list
                    .last()
                    .map(|entry| entry.get_path().iter().cloned().collect());
            } else {
                state = Some(vec![]);
            }
        }
        Ok(())
    }
}

impl<'a> Command for Ls<'a> {
    type Args = LsArgs;

    fn run(&mut self, mut args: Self::Args) -> Result<()> {
        // TODO sort and other keys
        let mut paths = vec![];
        std::mem::swap(&mut paths, &mut args.paths);
        for path in paths {
            self.list_dir(path, &args)?;
        }
        Ok(())
    }
}
