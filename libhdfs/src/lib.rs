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
#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
mod errors;

use crate::errors::LibError;
use hdfesse_proto::hdfs::{
    HdfsFileStatusProto, HdfsFileStatusProto_FileType, HdfsFileStatusProto_Flags,
};
use libhdfesse::{fs, path::Path, path::PathError};

use std::ffi::{c_void, CStr, CString};
use std::os::raw::{c_char, c_int, c_short};
use std::ptr::{null, null_mut};
use std::{collections::HashMap, convert::TryFrom};

macro_rules! expect_mut {
    ($var:ident) => {
        ($var)
            .as_mut()
            .expect(concat!("Expecting non-null pointer in ", stringify!($var)))
    };
}

macro_rules! expect_ref {
    ($var:ident) => {
        ($var)
            .as_ref()
            .expect(concat!("Expecting non-null pointer in ", stringify!($var)))
    };
}

/**

Drop-in replacement of libhdfs.

Based on the
https://github.com/apache/hadoop/blob/a89ca56a1b0eb949f56e7c6c5c25fdf87914a02f/hadoop-hdfs-project/hadoop-hdfs-native-client/src/main/native/libhdfs/include/hdfs/hdfs.h

*/

pub type tPort = u16;
pub type tSize = i32;
pub type tOffset = i64;
pub type tTime = i64; // TODO libc time_t

// TODO: what is C's enum size?
#[repr(C)]
pub enum tObjectKind {
    kObjectKindFile = b'F' as isize,
    kObjectKindDirectory = b'D' as isize,
}

#[allow(clippy::upper_case_acronyms)]
pub type hdfsFS = *mut libhdfesse::fs::Hdfs;
// TODO make these types distinct
pub type hdfsStreamBuilder = c_void;
pub type hadoopRzOptions = c_void;
pub type hadoopRzBuffer = c_void;

#[repr(C)]
pub struct hdfsBuilder {
    force_new_instance: bool,
    nn: *const c_char,
    port: tPort,
    kerb_ticket_cache_path: *const c_char,
    user_name: *const c_char,
    // TODO: Original library uses list, and thus values may repeat;
    // is it OK?
    opts: HashMap<&'static CStr, &'static CStr>,
}

impl hdfsBuilder {
    fn new() -> Self {
        Self {
            force_new_instance: false,
            nn: null(),
            port: 0,
            kerb_ticket_cache_path: null(),
            user_name: null(),
            opts: Default::default(),
        }
    }
}
#[repr(C)]
pub struct hdfs_internal {}

#[repr(C)]
pub struct hdfsFile_internal {}

#[repr(C)]
pub struct hdfsReadStatistics {
    totalBytesRead: u64,
    totalLocalBytesRead: u64,
    totalShortCircuitBytesRead: u64,
    totalZeroCopyBytesRead: u64,
}

#[repr(C)]
pub struct hdfsHedgedReadMetrics {
    hedgedReadOps: u64,
    hedgedReadOpsWin: u64,
    hedgedReadOpsInCurThread: u64,
}

pub type hdfsFile = *mut hdfsFile_internal;

#[no_mangle]
pub extern "C" fn hdfsFileIsOpenForRead(_file: hdfsFile) -> c_int {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn hdfsFileIsOpenForWrite(_file: hdfsFile) -> c_int {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn hdfsFileGetReadStatistics(
    _file: hdfsFile,
    _stats: *mut *mut hdfsReadStatistics,
) -> c_int {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn hdfsReadStatisticsGetRemoteBytesRead(_stats: *mut hdfsReadStatistics) -> i64 {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn hdfsFileClearReadStatistics(_file: hdfsFile) -> c_int {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn hdfsFileFreeReadStatistics(_stats: *mut hdfsReadStatistics) {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn hdfsFileGetHedgedReadMetrics(
    _file: hdfsFile,
    _stats: *mut *mut hdfsHedgedReadMetrics,
) -> c_int {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn hdfsFileFreeHedgedReadMetrics(_stats: *mut hdfsHedgedReadMetrics) {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn hdfsConnectAsUser(
    _nn: *const c_char,
    _port: tPort,
    _user: *const c_char,
) -> hdfsFS {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn hdfsFileConnect(_nn: *const c_char, _port: tPort) -> hdfsFS {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn hdfsConnectAsUserNewInstance(
    _nn: *const c_char,
    _port: tPort,
    _user: *const c_char,
) -> hdfsFS {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn hdfsConnectNewInstance(_nn: *const c_char, _port: tPort) -> hdfsFS {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn hdfsBuilderConnect(_bld: *mut hdfsBuilder) -> hdfsFS {
    unimplemented!()
}

/**
Creates a new hdfsBuilder.  You have to free the result with
hdfsFreeBuilder().
*/
#[no_mangle]
pub extern "C" fn hdfsBuilder() -> *mut hdfsBuilder {
    Box::into_raw(Box::new(hdfsBuilder::new()))
}

/**

Sets the force_new_instance_flag.  If it is set, new connection to the
name node is created instead of cached shared one.

# Safety
bld is a non-null pointer returned from hdfsBuilder() function.
*/
#[no_mangle]
pub unsafe extern "C" fn hdfsBuilderSetForceNewInstance(bld: *mut hdfsBuilder) {
    expect_mut!(bld).force_new_instance = true;
}

/**
Sets the namenode.  If never set or set to null, default namenode is used.

# Safety

bld is a non-null pointer returned from hdfsBuilder() function.  nn is
a nul-terminated C string owned by the application.
*/
#[no_mangle]
pub unsafe extern "C" fn hdfsBuilderSetNameNode(bld: *mut hdfsBuilder, nn: *const c_char) {
    expect_mut!(bld).nn = nn;
}

/**
Sets the namenode port.  If never set, default port is used.

# Safety
bld is a non-null pointer returned from hdfsBuilder() function.
*/
#[no_mangle]
pub unsafe extern "C" fn hdfsBuilderSetNameNodePort(bld: *mut hdfsBuilder, port: tPort) {
    expect_mut!(bld).port = port;
}

/**
Sets the username.  If never set or set to null, the username is
derived from the environment.

# Safety

bld is a non-null pointer returned from hdfsBuilder() function.  userName is
a nul-terminated C string owned by the application.
*/
#[no_mangle]
pub unsafe extern "C" fn hdfsBuilderSetUserName(bld: *mut hdfsBuilder, userName: *const c_char) {
    expect_mut!(bld).user_name = userName;
}

/**
Sets the Kerberos ticket chache path.

# Safety

bld is a non-null pointer returned from hdfsBuilder() function.
kerbTicketCachePath is a nul-terminated C string owned by the application.
*/
#[no_mangle]
pub unsafe extern "C" fn hdfsSetKerbTicketCachePath(
    bld: *mut hdfsBuilder,
    kerbTicketCachePath: *const c_char,
) {
    expect_mut!(bld).kerb_ticket_cache_path = kerbTicketCachePath;
}

/**
Free builder created with hdfsBuilder function.

# Safety

bld is a valid pointer returned from hdfsBuilder() function.

*/
#[no_mangle]
pub unsafe extern "C" fn hdfsFreeBuilder(bld: *mut hdfsBuilder) {
    Box::from_raw(bld);
}

/**
Set builder's configuration variable.  The caller manages key and val
lifetimes.

# Safety

bld is a valid builder returned by hdfsBuilder function; key and val
are nul-terminated C strings with lifetime larger than lifetime of
bld.
*/
#[no_mangle]
pub unsafe extern "C" fn hdfsBuilderConfSetStr(
    bld: *mut hdfsBuilder,
    key: *const c_char,
    val: *const c_char,
) -> c_int {
    expect_mut!(bld)
        .opts
        .insert(CStr::from_ptr(key), CStr::from_ptr(val));
    0
}

#[no_mangle]
pub extern "C" fn hdfsConfGetStr(_key: *const c_char, _val: *mut *mut c_char) -> c_int {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn hdfsConfGetInt(_key: *const c_char, _val: *mut *mut i32) -> c_int {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn hdfsConfStrFree(_val: *mut c_char) {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn hdfsDisconnect(_fs: hdfsFS) -> c_int {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn hdfsOpenFile(
    _fs: hdfsFS,
    _path: *const c_char,
    _flags: c_int,
    _bufferSize: c_int,
    _replication: c_short,
    _blocksize: tSize,
) -> hdfsFile {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn hdfsStreamBuilderAlloc(
    _fs: hdfsFS,
    _path: *const c_char,
    _flags: c_int,
) -> *mut hdfsStreamBuilder {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn hdfsStreamBuilderFree(_bld: *mut hdfsStreamBuilder) {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn hdfsStreamBuilderSetBufferSize(
    _bld: *mut hdfsStreamBuilder,
    _bufferSize: i32,
) -> c_int {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn hdfsStreamBuilderSetReplication(
    _bld: *mut hdfsStreamBuilder,
    _replication: i16,
) -> c_int {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn hdfsStreamBuilderSetDefaultBlockSize(
    _bld: *mut hdfsStreamBuilder,
    _defaultBlockSize: i64,
) -> c_int {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn hdfsStreamBuilderBuild(_bld: *mut hdfsStreamBuilder) -> hdfsFile {
    // TODO free _bld
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn hdfsTruncateFile(
    _fs: hdfsFS,
    _path: *const c_char,
    _newLength: tOffset,
) -> c_int {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn hdfsUnbufferFile(_file: hdfsFile) -> c_int {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn hdfsCloseFile(_fs: hdfsFS, _file: hdfsFile) -> c_int {
    // TODO free _file
    unimplemented!()
}

/**

Return 1 if path exists, 0 if not, negative value on error.

# Safety

fs value should be a value constructed with hdfs*Connect* family of
functions, and path is a null-terminated C string.

*/
#[no_mangle]
pub unsafe extern "C" fn hdfsExists(fs: hdfsFS, path: *const c_char) -> c_int {
    let path = CStr::from_ptr(path).to_str();
    let fs = expect_mut!(fs);

    let path = path.map_err(PathError::Utf8).and_then(Path::new);

    match path {
        Ok(path) => match fs.get_file_info(&path) {
            Ok(_) => 1,
            Err(e) => match e {
                // set_errno_with_hadoop_error handles it too, but
                // for this function it is a normal situation.
                fs::FsError::NotFound(_) => 0,
                _ => {
                    errors::set_errno_with_hadoop_error(fs::HdfsError::src(e));
                    -1
                }
            },
        },
        _ => {
            // TODO seems to be the only option.
            errno::set_errno(errno::Errno(errors::EINTERNAL));
            -1
        }
    }
}

#[no_mangle]
pub extern "C" fn hdfsSeek(_fs: hdfsFS, _file: hdfsFile, _disiredPos: tOffset) -> c_int {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn hdfsTell(_fs: hdfsFS, _file: hdfsFile) -> tOffset {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn hdfsRead(
    _fs: hdfsFS,
    _file: hdfsFile,
    _buffer: *mut c_void,
    _length: tSize,
) -> tSize {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn hdfsPread(
    _fs: hdfsFS,
    _file: hdfsFile,
    _position: tOffset,
    _buffer: *mut c_void,
    _length: tSize,
) -> tSize {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn hdfsPreadFully(
    _fs: hdfsFS,
    _file: hdfsFile,
    _position: tOffset,
    _buffer: *mut c_void,
    _length: tSize,
) -> tSize {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn hdfsWrite(
    _fs: hdfsFS,
    _file: hdfsFile,
    _buffer: *const c_void,
    _length: tSize,
) -> tSize {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn hdfsFlush(_fs: hdfsFS, _file: hdfsFile) -> c_int {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn hdfsHFlush(_fs: hdfsFS, _file: hdfsFile) -> c_int {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn hdfsHSync(_fs: hdfsFS, _file: hdfsFile) -> c_int {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn hdfsAvailable(_fs: hdfsFS, _file: hdfsFile) -> c_int {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn hdfsCopy(
    _srcFs: hdfsFS,
    _src: *const c_char,
    _dstFs: hdfsFS,
    _dst: *const char,
) -> c_int {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn hdfsMove(
    _srcFs: hdfsFS,
    _src: *const c_char,
    _dstFs: hdfsFS,
    _dst: *const char,
) -> c_int {
    unimplemented!()
}

/**

Delete a file.  Directories can be removed recursively only.  Return 1
if deletion is successful, 0 if not, negative value on error (like
network error or non-recursive directory deletion).

# Safety

fs value should be a value constructed with hdfs*Connect* family of
functions, and path is a null-terminated C string.

*/
#[no_mangle]
pub unsafe extern "C" fn hdfsDelete(fs: hdfsFS, path: *const c_char, recursive: c_int) -> c_int {
    let path = CStr::from_ptr(path).to_str();
    let fs = expect_mut!(fs);

    let path = path.map_err(PathError::Utf8).and_then(Path::new);

    match path {
        Ok(path) => match fs.delete(&path, recursive != 0) {
            Ok(n) => n as _,
            Err(e) => {
                errors::set_errno_with_hadoop_error(e);
                -1
            }
        },
        _ => {
            // TODO seems to be the only option.
            errno::set_errno(errno::Errno(errors::EINTERNAL));
            -1
        }
    }
}

#[no_mangle]
pub extern "C" fn hdfsRename(_fs: hdfsFS, _oldPath: *const c_char, _newPath: *const char) -> c_int {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn hdfsGetWorkingDirectory(
    _fs: hdfsFS,
    _buffer: *mut c_char,
    _bufferSize: usize,
) -> *mut c_char {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn hdfsSetWorkingDirectory(_fs: hdfsFS, _path: *const c_char) -> c_int {
    unimplemented!()
}

/**

Create directories recursively.  Returns 0 in case of success (if
directory already exists, it is success), or -1 in case of errors.

# Safety

fs value should be a value constructed with hdfs*Connect* family of
functions, and path is a null-terminated C string.

*/
#[no_mangle]
pub unsafe extern "C" fn hdfsCreateDirectory(fs: hdfsFS, path: *const c_char) -> c_int {
    let fs = expect_mut!(fs);
    let path = CStr::from_ptr(path).to_str();

    let path = match path.map_err(PathError::Utf8).and_then(Path::new) {
        Ok(path) => path,
        Err(_) => {
            errno::set_errno(errno::Errno(libc::EINVAL));
            return -1;
        }
    };

    match fs.mkdirs(&path, true) {
        Ok(true) => 0,
        Ok(false) => {
            // Actually, mkdirs's success value is *always* true.  We
            // repeat hdfs.c's code that handles this case anyway.
            errno::set_errno(errno::Errno(libc::EIO));
            -1
        }
        Err(e) => {
            errors::set_errno_with_hadoop_error(e);
            -1
        }
    }
}

#[no_mangle]
pub extern "C" fn hdfsSetReplication(
    _fs: hdfsFS,
    _path: *const c_char,
    _replication: i16,
) -> c_int {
    unimplemented!()
}

#[repr(C)]
pub struct hdfsFileInfo {
    mKind: tObjectKind,
    mName: *mut c_char,
    mLastMod: tTime,
    mSize: tOffset,
    mReplication: c_short,
    mBlockSize: tOffset,
    mOwner: *mut c_char,
    mGroup: *mut c_char,
    mPermissions: c_short,
    mLastAccess: tTime,
}

const HDFS_EXTENDED_FILE_INFO_ENCRYPTED: c_int = 0x1;

/**
 * Extended file information.
 */
#[repr(C)]
struct hdfsExtendedFileInfo {
    // TODO: so far one byte is enough, we just follow the original
    // version.
    flags: c_int,
}

fn align_to_file_info(len: usize) -> usize {
    // Compiles to (x & !7)
    (len + 7) / 8 * 8
}

impl TryFrom<&HdfsFileStatusProto> for hdfsFileInfo {
    type Error = LibError;

    fn try_from(fstat: &HdfsFileStatusProto) -> Result<Self, Self::Error> {
        let mKind = if fstat.get_fileType() == HdfsFileStatusProto_FileType::IS_DIR {
            tObjectKind::kObjectKindDirectory
        } else {
            tObjectKind::kObjectKindFile
        };
        let mName = CString::new(fstat.get_path())?;

        // The original libhdfs has an ugly hack: it places
        // another struct (extInfo) just behind the mOwner allocated string.
        // And extInfo.flags is updated with isEncrypted() flag value.
        //
        // TODO consider to store all strings and the
        // hdfsExtendedfileinfo in a signle memory allocation.
        let owner_file_info_offset = align_to_file_info(fstat.get_owner().len());
        let owner_file_info_size =
            owner_file_info_offset + std::mem::size_of::<hdfsExtendedFileInfo>();
        // Safe because we just allocate memory
        let owner_buffer = unsafe { libc::malloc(owner_file_info_size) } as *mut u8;
        if owner_buffer.is_null() {
            return Err(LibError::Oom);
        }

        let owner = fstat.get_owner().as_bytes();
        // Safe because we copy to the allocated data, and size is correct.
        unsafe {
            libc::memcpy(owner_buffer as _, owner.as_ptr() as _, owner.len());
            owner_buffer.add(owner.len() + 1).write(0); // Terminating byte

            let encrypted = (fstat.get_flags() & HdfsFileStatusProto_Flags::HAS_CRYPT as u32) != 0;
            (owner_buffer.add(owner_file_info_offset) as *mut hdfsExtendedFileInfo).write(
                hdfsExtendedFileInfo {
                    flags: if encrypted {
                        HDFS_EXTENDED_FILE_INFO_ENCRYPTED
                    } else {
                        0
                    },
                },
            );
        }

        let mGroup = CString::new(fstat.get_group())?;

        Ok(hdfsFileInfo {
            mKind,
            mName: mName.into_raw(),
            mLastMod: (fstat.get_modification_time() / 1000) as _,
            mSize: fstat.get_length() as _,
            mReplication: fstat.get_block_replication() as _,
            mBlockSize: fstat.get_blocksize() as _,
            mOwner: owner_buffer as _,
            mGroup: mGroup.into_raw(),
            mPermissions: fstat.get_permission().get_perm() as _,
            mLastAccess: (fstat.get_access_time() / 1000) as _,
        })
    }
}

impl hdfsFileInfo {
    // We cannot implement Drop for a repr(C) struct; use a manual one.
    // Technically, it doesn't need to be &mut, but it is.
    unsafe fn free(&mut self) {
        CString::from_raw(self.mName);
        // mOwner is different as it contains some kludgy extra data
        // and allocated with malloc.
        libc::free(self.mOwner as _);
        CString::from_raw(self.mGroup);
    }
}

/**

Return allocated single struct hdfsFileInfo with path info.

# Safety

fs value should be a value constructed with hdfs*Connect* family of
functions, and path is a null-terminated C string. numEntry points to
memory where length is written in case of success.
 */
#[no_mangle]
pub unsafe extern "C" fn hdfsListDirectory(
    fs: hdfsFS,
    path: *const c_char,
    numEntries: *mut c_int,
) -> *mut hdfsFileInfo {
    match hdfs_list_directory_impl(fs, path) {
        Ok(data) => {
            let mut sl = data.into_boxed_slice();
            let ptr = sl.as_mut_ptr();
            numEntries.write(sl.len() as _);
            std::mem::forget(sl);
            ptr
        }
        Err(e) => {
            errors::set_errno_with_hadoop_error(e);
            null_mut()
        }
    }
}

unsafe fn hdfs_list_directory_impl(
    fs: hdfsFS,
    path: *const c_char,
) -> Result<Vec<hdfsFileInfo>, LibError> {
    let path = CStr::from_ptr(path).to_str();
    let path = path
        .map_err(PathError::Utf8)
        .and_then(Path::new)
        .map_err(fs::HdfsError::src)?;

    let fs = expect_mut!(fs);

    let stat_iter = fs.list_status(&path)?;

    stat_iter
        .map(|r| {
            r.map_err(LibError::Hdfs)
                .and_then(|entry| hdfsFileInfo::try_from(&entry))
        })
        .collect()
}

/**

Return allocated single struct hdfsFileInfo with path info.

# Safety

fs value should be a value constructed with hdfs*Connect* family of
functions, and path is a null-terminated C string.

*/
#[no_mangle]
pub unsafe extern "C" fn hdfsGetPathInfo(fs: hdfsFS, path: *const c_char) -> *mut hdfsFileInfo {
    // We have common interface for freeing, thus result of
    // hdfsListdirectory and hdfsGetPathinfo are to be freed
    // uniformly.  Thus we allocate a Vec.

    let path = CStr::from_ptr(path).to_str();
    let path = path.map_err(PathError::Utf8).and_then(Path::new);

    let fs = expect_mut!(fs);

    match path {
        Ok(path) => match fs
            .get_file_info(&path)
            .map_err(fs::HdfsError::src)
            .map_err(LibError::Hdfs)
        {
            Ok(fstat) => {
                // TODO as we deallocate as Box<[T]>, one can create
                // it from Box<T> instead of Vec.
                // TODO check instead of unwrap
                let cont = vec![hdfsFileInfo::try_from(&fstat).unwrap()];

                let mut sl = cont.into_boxed_slice();
                let ptr = sl.as_mut_ptr();
                std::mem::forget(sl);
                ptr
            }
            Err(e) => {
                errors::set_errno_with_hadoop_error(e);
                null_mut()
            }
        },
        _ => {
            // it seems this is the most sane value for non-UTF8 strings.
            errno::set_errno(errno::Errno(libc::EINVAL));
            null_mut()
        }
    }
}

/**

Deallocates hdfsFileInfo instance.

# Safety

hdfsFileInfo have to be a value returned from hdfsGetPathInfo or
hdfsListDirectory functions.  For former, numEntries is 1, for latter,
it is a value put into numEntries pointer.

*/
#[no_mangle]
pub unsafe extern "C" fn hdfsFreeFileInfo(hdfsFileInfo: *mut hdfsFileInfo, numEntries: c_int) {
    let mut data = Box::from_raw(std::slice::from_raw_parts_mut(
        hdfsFileInfo,
        numEntries as _,
    ));
    for elt in data.iter_mut() {
        elt.free()
    }
}

/**
Return true value if file is encrypted.

# Safety

hdfsFileInfo have to be a pointer to value returned from hdfsGetPathInfo or
hdfsListDirectory functions.
**/
#[no_mangle]
pub unsafe extern "C" fn hdfsFileIsEncrypted(hdfsFileInfo: *const hdfsFileInfo) -> c_int {
    let owner_ptr = expect_ref!(hdfsFileInfo).mOwner;
    let owner = CStr::from_ptr(owner_ptr);
    let offset = align_to_file_info(owner.to_bytes().len());
    let flag = (owner_ptr.add(offset) as *const hdfsExtendedFileInfo)
        .as_ref()
        .unwrap()
        .flags
        & HDFS_EXTENDED_FILE_INFO_ENCRYPTED;
    (flag != 0) as _
}

/**
 hdfsGetHosts - Get hostnames where a particular block (determined by
 pos & blocksize) of a file is stored. The last element in the array
 is NULL. Due to replication, a single block could be present on
 multiple hosts.
 @param fs The configured filesystem handle.
 @param path The path of the file.
 @param start The start of the block.
 @param length The length of the block.
 @return Returns a dynamically-allocated 2-d array of blocks-hosts;
 NULL on error.

# Safety

fs value should be a value constructed with hdfs*Connect* family of
functions.  path should be a null-terminated string.
*/
#[no_mangle]
pub unsafe extern "C" fn hdfsGetHosts(
    fs: hdfsFS,
    path: *const c_char,
    start: tOffset,
    length: tOffset,
) -> *const *const *const c_char {
    let fs = expect_mut!(fs);
    let path = CStr::from_ptr(path).to_str();
    let path = path.map_err(PathError::Utf8).and_then(Path::new);
    let path = match path {
        Ok(path) => path,
        Err(_) => {
            errno::set_errno(errno::Errno(libc::EINVAL));
            return null();
        }
    };

    let info = match fs.get_file_info(&path) {
        Ok(info) => info,
        Err(e) => {
            errors::set_errno_with_hadoop_error(fs::HdfsError::op(e));
            return null();
        }
    }; // TODO check path

    let block_info = match fs.get_file_block_locations(&path, start as _, length as _) {
        Ok(block_info) => block_info,
        Err(e) => {
            errors::set_errno_with_hadoop_error(e);
            return null();
        }
    };

    // TODO: consider using malloc as one doesn't need to recalculate
    // length on deallocation.
    let mut main: Box<[*const *const c_char]> = vec![null(); block_info.len() + 1].into();

    // main is longer by 1 element than block_info, and the last element remains null()
    for (block_output, block) in main.iter_mut().zip(block_info.iter()) {
        let mut block_data: Box<[*const c_char]> = vec![null(); block.locs.len() + 1].into();
        for (host_output, loc) in block_data.iter_mut().zip(block.locs.iter()) {
            let host_name = loc.get_id().get_hostName();
            let host_name_c = CString::new(host_name).unwrap(/* TODO */);
            *host_output = host_name_c.into_raw();
        }
        *block_output = block_data.as_ptr();
        // Will be freed by hdfsFreeHosts.
        std::mem::forget(block_data);
    }

    let res = main.as_ptr();
    // Will be freed by hdfsFreeHosts.
    std::mem::forget(main);

    res
}

#[no_mangle]
pub extern "C" fn hdfsFreeHosts(_blockHosts: *mut *mut *mut c_char) {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn hdfsGetDefaultBlockSize(_fs: hdfsFS) -> tOffset {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn hdfsGetDefaultBlockSizeAtPath(_fs: hdfsFS, _path: *const c_char) -> tOffset {
    unimplemented!()
}

/**

hdfsGetCapacity - Return the raw capacity of the filesystem.
@param fs The configured filesystem handle.
@return Returns the raw-capacity; -1 on error.

# Safety

fs value should be a value constructed with hdfs*Connect* family of
functions.

 */
#[no_mangle]
pub unsafe extern "C" fn hdfsGetCapacity(fs: hdfsFS) -> tOffset {
    let fs = expect_mut!(fs);
    match fs.get_status() {
        Ok(stats) => stats.capacity as _,
        Err(e) => {
            errors::set_errno_with_hadoop_error(e);
            -1
        }
    }
}

/**

hdfsGetUsed - Return the total raw size of all files in the filesystem.
@param fs The configured filesystem handle.
@return Returns the total-size; -1 on error.

# Safety

fs value should be a value constructed with hdfs*Connect* family of
functions.

 */
#[no_mangle]
pub unsafe extern "C" fn hdfsGetUsed(fs: hdfsFS) -> tOffset {
    let fs = expect_mut!(fs);
    match fs.get_status() {
        Ok(stats) => stats.used as _,
        Err(e) => {
            errors::set_errno_with_hadoop_error(e);
            -1
        }
    }
}

#[no_mangle]
pub extern "C" fn hdfsChown(
    _fs: hdfsFS,
    _path: *const c_char,
    _owner: *const c_char,
    _group: *const c_char,
) -> c_int {
    unimplemented!()
}

/**

hdfsChmod
@param fs The configured filesystem handle.
@param path the path to the file or directory
@param mode the bitmask to set it to
@return 0 on success else -1

# Safety

fs value should be a value constructed with hdfs*Connect* family of
functions, and path is a null-terminated C string.

 */
#[no_mangle]
pub unsafe extern "C" fn hdfsChmod(fs: hdfsFS, path: *const c_char, mode: c_short) -> c_int {
    let fs = expect_mut!(fs);
    let path = CStr::from_ptr(path).to_str();

    let path = match path.map_err(PathError::Utf8).and_then(Path::new) {
        Ok(path) => path,
        Err(_) => {
            errno::set_errno(errno::Errno(libc::EINVAL));
            return -1;
        }
    };

    match fs.chmod(&path, mode as _) {
        Ok(()) => 0,
        Err(e) => {
            errors::set_errno_with_hadoop_error(e);
            -1
        }
    }
}

const NO_TIME: i64 = -1;

fn time_to_option(time: i64) -> Option<u64> {
    if time == NO_TIME {
        None
    } else {
        Some(time as _)
    }
}

/**

hdfsUtime
@param fs The configured filesystem handle.
@param path the path to the file or directory
@param mtime new modification time or -1 for no change
@param atime new access time or -1 for no change
@return 0 on success else -1

# Safety

fs value should be a value constructed with hdfs*Connect* family of
functions, and path is a null-terminated C string.

*/
#[no_mangle]
pub unsafe extern "C" fn hdfsUtime(
    fs: hdfsFS,
    path: *const c_char,
    mtime: tTime,
    atime: tTime,
) -> c_int {
    let fs = expect_mut!(fs);
    let path = CStr::from_ptr(path).to_str();

    let path = match path.map_err(PathError::Utf8).and_then(Path::new) {
        Ok(path) => path,
        Err(_) => {
            errno::set_errno(errno::Errno(libc::EINVAL));
            return -1;
        }
    };

    match fs.set_time(&path, time_to_option(mtime), time_to_option(atime)) {
        Ok(()) => 0,
        Err(e) => {
            errors::set_errno_with_hadoop_error(e);
            -1
        }
    }
}

#[no_mangle]
pub extern "C" fn hdfsRzOptionsAlloc() -> *mut hadoopRzOptions {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn hadoopRzOptionsSetSkipChecksum(
    _opts: *mut hadoopRzOptions,
    _skip: c_int,
) -> c_int {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn hadoopRzOptionsSetByteBufferPool(
    _opts: *mut hadoopRzOptions,
    _className: *const c_char,
) -> c_int {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn hadoopRzOptionsFree(_opts: *mut hadoopRzOptions) {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn hadoopReadZero(
    _file: hdfsFile,
    _opts: *mut hadoopRzOptions,
    _maxLength: i32,
) -> *const hadoopRzBuffer {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn hadoopRzBufferLength(_buffer: *const hadoopRzBuffer) -> i32 {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn hadoopRzBufferGet(_buffer: *const hadoopRzBuffer) -> *const c_void {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn hadoopRzBufferFree(_file: hdfsFile, _buffer: *const hadoopRzBuffer) {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn hdfsGetLastExceptionRootCause() -> *const c_char {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn hdfsGetLastExceptionStackTrace() -> *const c_char {
    unimplemented!()
}
