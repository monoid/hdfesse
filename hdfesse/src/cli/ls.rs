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
use hdfesse_proto::hdfs::{
    HdfsFileStatusProto, HdfsFileStatusProto_FileType, HdfsFileStatusProto_Flags,
};
use libhdfesse::service::ClientNamenodeService;
use protobuf::RepeatedField;
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

fn format_type(type_: HdfsFileStatusProto_FileType) -> char {
    match type_ {
        HdfsFileStatusProto_FileType::IS_DIR => 'd',
        HdfsFileStatusProto_FileType::IS_FILE => '-',
        // It seems that original hdfs doesn't care about this
        // case.
        HdfsFileStatusProto_FileType::IS_SYMLINK => 's',
    }
}

// TODO It can be optimized to write, not to create a string.  But
// does it worth it?
fn format_flags(flags: u32) -> String {
    let mut res = String::with_capacity(9);
    for offset in [6u32, 3, 0].iter() {
        res.push_str(format_flag_group((flags >> offset) & 0x7));
    }
    res
}

/*
 * See
 * hadoop/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/shell/Ls.java
 */
/// ls options are factored out to separate struct for convenience.
#[derive(Debug, StructOpt)]
pub struct LsOpts {
    #[structopt(short)]
    directory: bool,
    #[structopt(short = "t")]
    mtime: bool,
    #[structopt(short = "u")]
    atime: bool,
    #[structopt(short = "C")]
    path_only: bool,
    #[structopt(short = "r")]
    reversed: bool,
    #[structopt(short = "R")]
    recursive: bool,
    // TODO ...
}

#[derive(Debug, StructOpt)]
pub struct LsArgs {
    #[structopt(flatten)]
    opts: LsOpts,
    #[structopt(name = "path")]
    paths: Vec<String>,
}

// TODO it has to be moved to libhdfesse and made public.
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

    fn next_group(&mut self) -> Result<(usize, RepeatedField<HdfsFileStatusProto>)> {
        let list_from = self.prev_name.take().unwrap_or_default();
        let mut listing = self
            .service
            .getListing(self.path.to_owned(), list_from, false)?;
        let partial_list = listing.mut_dirList().take_partialListing();

        self.count += partial_list.len();
        let len = self.count + listing.get_dirList().get_remainingEntries() as usize;
        self.len = Some(len);

        // Search further from the last value
        // It is very unlikely that partial_list is empty and
        // prev_name is None while remainingEntries is not zero.
        // Perhaps, it should be reported as a server's invalid
        // data.
        self.prev_name = partial_list.last().map(|entry| entry.get_path().to_vec());

        Ok((len, partial_list))
    }
}

impl<'a> Iterator for LsGroupIterator<'a> {
    type Item = Result<(usize, RepeatedField<HdfsFileStatusProto>)>;

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
    service: &'a mut ClientNamenodeService,
}

impl<'a> Ls<'a> {
    pub fn new(service: &'a mut ClientNamenodeService) -> Self {
        Ls { service }
    }

    fn list_dir(&mut self, path: String, args: &LsOpts) -> Result<()> {
        // TODO handle sorting and other keys
        let mut is_first = true;

        for group in LsGroupIterator::new(self.service, &path) {
            let (total_len, group) = group?;

            if !args.recursive & is_first {
                println!("Found {} items", total_len,);
                is_first = false;
            }

            // Using streaming approach is crucial for huge directories where
            // data does not fit into memory.  For sorted data, one has to
            // collect everything in memory; but in case of problem, you can
            // at least get default list and sort it with some external tool.
            for entry in group.iter() {
                print!(
                    "{}{}{} ",
                    format_type(entry.get_fileType()),
                    format_flags(entry.get_permission().get_perm()),
                    if entry.get_flags() & (HdfsFileStatusProto_Flags::HAS_ACL as u32) != 0 {
                        '+'
                    } else {
                        '-'
                    },
                );
                if entry.get_fileType() == HdfsFileStatusProto_FileType::IS_DIR {
                    print!("-");
                } else {
                    print!("{}", entry.get_block_replication());
                }
                let time = chrono::NaiveDateTime::from_timestamp(
                    if args.atime {
                        entry.get_access_time()
                    } else {
                        entry.get_modification_time()
                    } as i64
                        / 1000, // millisec to secs
                    0,
                );
                println!(
                    " {} {} {} {} {}",
                    entry.get_owner(),
                    entry.get_group(),
                    entry.get_length(),
                    time.format("%Y-%m-%d %H:%M"),
                    // TODO original implementation uses different lossy char
                    String::from_utf8_lossy(entry.get_path()),
                );
            }
        }
        Ok(())
    }
}

impl<'a> Command for Ls<'a> {
    type Args = LsArgs;

    fn run(&mut self, args: Self::Args) -> Result<()> {
        for path in args.paths {
            self.list_dir(path, &args.opts)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_flags() {
        assert_eq!(format_flags(0o000), "---------");
        assert_eq!(format_flags(0o007), "------rwx");
        assert_eq!(format_flags(0o077), "---rwxrwx");
        assert_eq!(format_flags(0o777), "rwxrwxrwx");
        assert_eq!(format_flags(0o707), "rwx---rwx");
        assert_eq!(format_flags(0o123), "--x-w--wx");
        assert_eq!(format_flags(0o456), "r--r-xrw-");

        assert_eq!(format_flags(1), "--------x");
        assert_eq!(format_flags(2), "-------w-");
        assert_eq!(format_flags(3), "-------wx");
        assert_eq!(format_flags(4), "------r--");
        assert_eq!(format_flags(5), "------r-x");
        assert_eq!(format_flags(6), "------rw-");
        assert_eq!(format_flags(7), "------rwx");
        assert_eq!(format_flags(42), "---r-x-w-");
    }
}
