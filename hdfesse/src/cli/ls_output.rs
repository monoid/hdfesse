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
use hdfesse_proto::hdfs::{
    HdfsFileStatusProto, HdfsFileStatusProto_FileType, HdfsFileStatusProto_Flags,
};
use libhdfesse::path;
use number_prefix::NumberPrefix;
use std::borrow::Cow;
use std::cmp::max;
use std::io::Write;

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

// Repeats org.apache.hadoop.fs.shell.PrintableString functionality.
fn to_printable(v: &str) -> Cow<'_, str> {
    lazy_static::lazy_static! {
        // Surrogate codepoints are considered out of range in Rust
        // String, and are not included in the regex crate.  Thus, if
        // pathname contains it, it will fail on converting &[u8] to
        // string.
        static ref RE: regex::Regex = regex::Regex::new(r#"[\p{Control}\p{Format}\p{PrivateUse}\p{Unassigned}]"#).unwrap();
    }

    RE.replace_all(v, "?")
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
    // datetime string is quite expensive to calculate, thus we
    // precompute it.
    pub(crate) timestmap_str: String,
    pub(crate) path: Box<str>,
}

impl Record {
    pub(crate) fn from_hdfs_file_status(
        mut entry: HdfsFileStatusProto,
        atime: bool,
        tz_offset: chrono::FixedOffset,
    ) -> Self {
        let timestamp = if atime {
            entry.get_access_time()
        } else {
            entry.get_modification_time()
        };
        Record {
            file_type: entry.get_fileType(),
            perm: entry.get_permission().get_perm(),
            has_acl: entry.get_flags() & (HdfsFileStatusProto_Flags::HAS_ACL as u32) != 0,
            replication: entry.get_block_replication(),
            owner: entry.take_owner().into(),
            group: entry.take_group().into(),
            size: entry.get_length(),
            timestamp: if atime {
                entry.get_access_time()
            } else {
                entry.get_modification_time()
            },
            timestmap_str: DateFormatter::format_datetime(timestamp, tz_offset),
            // TODO: move formatting option to formatter.
            // Record should hold a Vec.
            path: String::from_utf8_lossy(entry.get_path()).into(),
        }
    }
}

pub(crate) trait FieldFormatter<W: Write> {
    fn update_len(&mut self, rec: &Record);
    fn print(&self, out: &mut W, rec: &Record) -> std::io::Result<()>;
    fn print_streaming(&self, out: &mut W, rec: &Record) -> std::io::Result<()>;
}

#[derive(Default)]
struct PermFormatter {}

impl<W: Write> FieldFormatter<W> for PermFormatter {
    fn update_len(&mut self, _rec: &Record) {
        // Fixed-size rec
    }

    fn print(&self, out: &mut W, entry: &Record) -> std::io::Result<()> {
        write!(
            out,
            "{}{}",
            format_type(entry.file_type),
            format_flags(entry.perm),
            // TODO this field is print when -e is passed, which is not
            // supported by us.
            // if entry.has_acl { '+' } else { '-' },
        )
    }

    fn print_streaming(&self, out: &mut W, rec: &Record) -> std::io::Result<()> {
        self.print(out, rec)
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

impl<W: Write> FieldFormatter<W> for ReplicationFormatter {
    fn update_len(&mut self, entry: &Record) {
        self.max_len = max(self.max_len, Self::format(entry).chars().count());
    }

    fn print(&self, out: &mut W, entry: &Record) -> std::io::Result<()> {
        write!(out, " {0:>1$}", Self::format(entry), self.max_len)
    }

    fn print_streaming(&self, out: &mut W, entry: &Record) -> std::io::Result<()> {
        write!(out, "{}", Self::format(entry))
    }
}

#[derive(Default)]
struct SimpleSizeFormatter {}

impl<W: Write> FieldFormatter<W> for SimpleSizeFormatter {
    fn update_len(&mut self, _entry: &Record) {}

    fn print(&self, out: &mut W, entry: &Record) -> std::io::Result<()> {
        write!(out, " {0:>10}", entry.size)
    }

    fn print_streaming(&self, out: &mut W, entry: &Record) -> std::io::Result<()> {
        write!(out, "{}", entry.size)
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
impl<W: Write> FieldFormatter<W> for HumanSizeFormatter {
    // TODO implement real units formatter
    fn update_len(&mut self, entry: &Record) {
        self.max_len = max(self.max_len, Self::format(entry.size as _).chars().count());
    }

    fn print(&self, out: &mut W, entry: &Record) -> std::io::Result<()> {
        write!(out, "{0:>1$}", Self::format(entry.size), self.max_len + 1)
    }

    fn print_streaming(&self, out: &mut W, entry: &Record) -> std::io::Result<()> {
        write!(out, "{}", Self::format(entry.size))
    }
}

#[derive(Default)]
struct DateFormatter {
    max_len: usize,
}

impl DateFormatter {
    fn format_datetime(timestamp: u64, tz_offset: chrono::FixedOffset) -> String {
        let time = chrono::NaiveDateTime::from_timestamp(
            timestamp as i64 / 1000, // millisec to secs
            // We don't need the millisecond part
            0,
        );
        let time_tz = chrono::DateTime::<chrono::Local>::from_utc(time, tz_offset);

        time_tz.format("%Y-%m-%d %H:%M").to_string()
    }
}

impl<W: Write> FieldFormatter<W> for DateFormatter {
    fn update_len(&mut self, entry: &Record) {
        self.max_len = max(self.max_len, entry.timestmap_str.chars().count());
    }

    fn print(&self, out: &mut W, entry: &Record) -> std::io::Result<()> {
        write!(out, "{0:>1$}", entry.timestmap_str, self.max_len + 1)
    }

    fn print_streaming(&self, out: &mut W, entry: &Record) -> std::io::Result<()> {
        write!(out, "{}", entry.timestmap_str)
    }
}

#[derive(Default)]
struct OwnerFormatter {
    max_len: usize,
}

impl<W: Write> FieldFormatter<W> for OwnerFormatter {
    fn update_len(&mut self, entry: &Record) {
        self.max_len = max(self.max_len, entry.owner.chars().count());
    }

    fn print(&self, out: &mut W, entry: &Record) -> std::io::Result<()> {
        write!(out, " {0:1$}", entry.owner, self.max_len)
    }

    fn print_streaming(&self, out: &mut W, entry: &Record) -> std::io::Result<()> {
        write!(out, "{}", entry.owner)
    }
}

#[derive(Default)]
struct GroupFormatter {
    max_len: usize,
}

impl<W: Write> FieldFormatter<W> for GroupFormatter {
    fn update_len(&mut self, entry: &Record) {
        self.max_len = max(self.max_len, entry.group.chars().count());
    }

    fn print(&self, out: &mut W, entry: &Record) -> std::io::Result<()> {
        write!(out, " {0:1$}", entry.group, self.max_len)
    }

    fn print_streaming(&self, out: &mut W, entry: &Record) -> std::io::Result<()> {
        write!(out, "{}", entry.group)
    }
}

struct NameFormatter {
    base: path::Path<'static>,
    quote: bool,
}

impl NameFormatter {
    fn new(base: path::Path<'_>, quote: bool) -> Self {
        Self {
            base: base.into_owned(),
            quote,
        }
    }
}

impl<W: Write> FieldFormatter<W> for NameFormatter {
    fn update_len(&mut self, _entry: &Record) {}

    fn print(&self, out: &mut W, entry: &Record) -> std::io::Result<()> {
        let joined = self.base.join(&entry.path).unwrap().to_string(); // TODO
        write!(
            out,
            " {}",
            if self.quote {
                to_printable(&joined)
            } else {
                joined.into()
            }
        )
    }

    fn print_streaming(&self, out: &mut W, entry: &Record) -> std::io::Result<()> {
        let joined = self.base.join(&entry.path).unwrap().to_string(); // TODO
        write!(
            out,
            "{}",
            if self.quote {
                to_printable(&joined)
            } else {
                joined.into()
            }
        )
    }
}

pub(crate) struct LineFormat<W: Write> {
    formatters: Vec<Box<dyn FieldFormatter<W>>>,
}

impl<W: Write> LineFormat<W> {
    /// Path-only output
    pub(crate) fn compact(base: path::Path<'_>, quote: bool) -> Self {
        Self {
            formatters: vec![Box::new(NameFormatter::new(base, quote))],
        }
    }

    /// Full output; human is the flag that enables human-readable
    /// file size output.
    pub(crate) fn full(base: path::Path<'_>, human: bool, quote: bool) -> Self {
        Self {
            formatters: vec![
                Box::<PermFormatter>::default(),
                Box::<ReplicationFormatter>::default(),
                Box::<OwnerFormatter>::default(),
                Box::<GroupFormatter>::default(),
                if human {
                    Box::<HumanSizeFormatter>::default()
                } else {
                    Box::<SimpleSizeFormatter>::default()
                },
                Box::<DateFormatter>::default(),
                Box::new(NameFormatter::new(base, quote)),
            ],
        }
    }

    pub(crate) fn update_len(&mut self, entry: &Record) {
        for fmt in &mut self.formatters {
            fmt.update_len(entry);
        }
    }

    pub(crate) fn print(&self, out: &mut W, entry: &Record) -> std::io::Result<()> {
        for fmt in &self.formatters {
            fmt.print(out, entry)?;
        }
        writeln!(out)
    }

    pub(crate) fn print_streaming(&self, out: &mut W, entry: &Record) -> std::io::Result<()> {
        for (idx, fmt) in self.formatters.iter().enumerate() {
            if idx != 0 {
                write!(out, "\t")?;
            }
            fmt.print_streaming(out, entry)?;
        }
        writeln!(out)
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

    #[test]
    fn test_printable_ascii() {
        assert_eq!(to_printable("abcdef347"), "abcdef347");
        assert_eq!(to_printable(" !\"|}~"), " !\"|}~");
    }

    #[test]
    fn test_printable_unicode_bmp() {
        assert_eq!(to_printable("\u{1050}\u{2533}--"), "\u{1050}\u{2533}--");

        // This test is commented out, as Rust compiler doesn't accept
        // surrotages at all. :(
        //
        // assert_eq!(
        //     to_printable("\u{D800}\u{DC00}'''\u{D802}\u{DD00}"),
        //     "\u{D800}\u{DC00}'''\u{D802}\u{DD00}"
        // );
    }

    #[test]
    fn test_printable_non_printable() {
        assert_eq!(to_printable("abc\rdef"), "abc?def");
        assert_eq!(to_printable("\x08abc\tdef"), "?abc?def");
        assert_eq!(to_printable("\x0c\x0c\x08\n"), "????");
        assert_eq!(to_printable("\x17ab\0"), "?ab?");
    }

    #[test]
    fn test_printable_non_printable_unicode() {
        // Formatting Unicode
        assert_eq!(to_printable("-\u{FEFF}--"), "-?--");
        assert_eq!(to_printable("\u{2063}\t"), "??");

        // Private use Unicode
        assert_eq!(to_printable("\u{E000}"), "?");
        assert_eq!(to_printable("\u{E123}abc\u{F432}"), "?abc?");
        // Excluded some surrogates.
        assert_eq!(to_printable("z\u{1050}"), "z\u{1050}");

        // The original testsuite also had some surrogate tests that
        // are not appropriate for Rust.
    }
}
