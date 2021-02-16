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
use hdfesse_proto::hdfs::HdfsFileStatusProto_FileType;
use number_prefix::NumberPrefix;
use std::borrow::Cow;
use std::cmp::max;

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

pub(crate) struct Record {
    pub(crate) file_type: HdfsFileStatusProto_FileType,
    pub(crate) perm: u32,
    #[allow(unused)]
    pub(crate) has_acl: bool,
    pub(crate) replication: u32,
    pub(crate) owner: Box<str>,
    pub(crate) group: Box<str>,
    pub(crate) size: u64,
    pub(crate) timestamp: u64,
    pub(crate) path: Box<str>,
}

pub(crate) trait FieldFormatter {
    fn update_len(&mut self, rec: &Record);
    // TODO both print and print_streaming should get a StdoutLock,
    // use write! to return io::Error for EPIPE to be handled in the
    // main.
    fn print(&self, rec: &Record) -> std::io::Result<()>;
    fn print_streaming(&self, rec: &Record) -> std::io::Result<()>;
}

struct PermFormatter {}

impl Default for PermFormatter {
    fn default() -> Self {
        Self {}
    }
}

impl FieldFormatter for PermFormatter {
    fn update_len(&mut self, _rec: &Record) {
        // Fixed-size rec
    }

    fn print(&self, entry: &Record) -> std::io::Result<()> {
        print!(
            "{}{}",
            format_type(entry.file_type),
            format_flags(entry.perm),
            // TODO this field is print when -e is passed, which is not
            // supported by us.
            // if entry.has_acl { '+' } else { '-' },
        );
        Ok(())
    }

    fn print_streaming(&self, rec: &Record) -> std::io::Result<()> {
        self.print(rec)
    }
}

struct ReplicationFormatter {
    max_len: usize,
}

impl ReplicationFormatter {
    fn format(entry: &Record) -> Cow<'static, str> {
        if entry.file_type == HdfsFileStatusProto_FileType::IS_DIR {
            Cow::from("-")
        } else {
            Cow::from(format!("{}", entry.replication))
        }
    }
}

impl Default for ReplicationFormatter {
    fn default() -> Self {
        Self { max_len: 3 }
    }
}

impl FieldFormatter for ReplicationFormatter {
    fn update_len(&mut self, entry: &Record) {
        self.max_len = max(self.max_len, Self::format(entry).chars().count());
    }

    fn print(&self, entry: &Record) -> std::io::Result<()> {
        print!(" {0:>1$}", Self::format(entry), self.max_len);
        Ok(())
    }

    fn print_streaming(&self, entry: &Record) -> std::io::Result<()> {
        print!("{}", Self::format(entry));
        Ok(())
    }
}

struct SimpleSizeFormatter {}

impl Default for SimpleSizeFormatter {
    fn default() -> Self {
        Self {}
    }
}

impl FieldFormatter for SimpleSizeFormatter {
    fn update_len(&mut self, _entry: &Record) {}

    fn print(&self, entry: &Record) -> std::io::Result<()> {
        print!("{0:>10}", entry.size);
        Ok(())
    }

    fn print_streaming(&self, entry: &Record) -> std::io::Result<()> {
        print!("{}", entry.size);
        Ok(())
    }
}

struct HumanSizeFormatter {
    max_len: usize,
}

impl HumanSizeFormatter {
    fn format(val: u64) -> String {
        match NumberPrefix::binary(val as f64) {
            NumberPrefix::Standalone(bytes) => format!("{:.0}", bytes),
            NumberPrefix::Prefixed(pref, n) => format!("{:.1} {}", n, &pref.symbol()[0..1]),
        }
    }
}

impl Default for HumanSizeFormatter {
    fn default() -> Self {
        Self { max_len: 10 }
    }
}

impl FieldFormatter for HumanSizeFormatter {
    // TODO implement real units formatter
    fn update_len(&mut self, entry: &Record) {
        self.max_len = max(self.max_len, Self::format(entry.size as _).chars().count());
    }

    fn print(&self, entry: &Record) -> std::io::Result<()> {
        print!("{0:>1$}", Self::format(entry.size), self.max_len + 1);
        Ok(())
    }

    fn print_streaming(&self, entry: &Record) -> std::io::Result<()> {
        print!("{}", Self::format(entry.size));
        Ok(())
    }
}

struct DateFormatter {
    max_len: usize,
    tz_offset: chrono::FixedOffset,
}

impl DateFormatter {
    fn format_datetime(&self, entry: &Record) -> String {
        let time = chrono::NaiveDateTime::from_timestamp(
            entry.timestamp as i64 / 1000, // millisec to secs
            // We don't need the millisecond part
            0,
        );
        let time_tz = chrono::DateTime::<chrono::Local>::from_utc(time, self.tz_offset.clone());

        time_tz.format("%Y-%m-%d %H:%M").to_string()
    }
}

impl Default for DateFormatter {
    fn default() -> Self {
        Self {
            max_len: 0,
            // Haha, our installation uses old Java with old timezone
            // data; but the hdfesse uses local timezone data which is
            // updated with system updates.  And for Europe/Moscow it
            // does matter.
            tz_offset: *chrono::Local::now().offset(),
        }
    }
}

impl FieldFormatter for DateFormatter {
    fn update_len(&mut self, entry: &Record) {
        self.max_len = max(self.max_len, self.format_datetime(entry).chars().count());
    }

    fn print(&self, entry: &Record) -> std::io::Result<()> {
        print!("{0:>1$}", self.format_datetime(entry), self.max_len + 1);
        Ok(())
    }

    fn print_streaming(&self, entry: &Record) -> std::io::Result<()> {
        print!("{}", self.format_datetime(entry));
        Ok(())
    }
}

struct OwnerFormatter {
    max_len: usize,
}

impl Default for OwnerFormatter {
    fn default() -> Self {
        Self { max_len: 0 }
    }
}

impl FieldFormatter for OwnerFormatter {
    fn update_len(&mut self, entry: &Record) {
        self.max_len = max(self.max_len, entry.owner.chars().count());
    }

    fn print(&self, entry: &Record) -> std::io::Result<()> {
        print!(" {0:1$}", entry.owner, self.max_len);
        Ok(())
    }

    fn print_streaming(&self, entry: &Record) -> std::io::Result<()> {
        print!("{}", entry.owner);
        Ok(())
    }
}

struct GroupFormatter {
    max_len: usize,
}

impl Default for GroupFormatter {
    fn default() -> Self {
        Self { max_len: 0 }
    }
}

impl FieldFormatter for GroupFormatter {
    fn update_len(&mut self, entry: &Record) {
        self.max_len = max(self.max_len, entry.group.chars().count());
    }

    fn print(&self, entry: &Record) -> std::io::Result<()> {
        print!(" {0:1$}", entry.group, self.max_len);
        Ok(())
    }

    fn print_streaming(&self, entry: &Record) -> std::io::Result<()> {
        print!("{}", entry.group);
        Ok(())
    }
}

struct NameFormatter {}

impl Default for NameFormatter {
    fn default() -> Self {
        Self {}
    }
}

impl FieldFormatter for NameFormatter {
    fn update_len(&mut self, _entry: &Record) {}

    fn print(&self, entry: &Record) -> std::io::Result<()> {
        print!(" {}", entry.path);
        Ok(())
    }

    fn print_streaming(&self, entry: &Record) -> std::io::Result<()> {
        print!("{}", entry.path);
        Ok(())
    }
}

pub(crate) struct LineFormat {
    pub(crate) formatters: Vec<Box<dyn FieldFormatter>>,
}

impl LineFormat {
    /// Path-only output
    pub(crate) fn compact() -> Self {
        Self {
            formatters: vec![Box::new(NameFormatter::default())],
        }
    }

    /// Full output; human is the flag that enables human-readable
    /// file size output.
    pub(crate) fn full(human: bool) -> Self {
        Self {
            formatters: vec![
                Box::new(PermFormatter::default()),
                Box::new(ReplicationFormatter::default()),
                Box::new(OwnerFormatter::default()),
                Box::new(GroupFormatter::default()),
                if human {
                    Box::new(HumanSizeFormatter::default())
                } else {
                    Box::new(SimpleSizeFormatter::default())
                },
                Box::new(DateFormatter::default()),
                Box::new(NameFormatter::default()),
            ],
        }
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