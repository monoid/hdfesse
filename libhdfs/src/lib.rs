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

use hdfesse_proto::hdfs::{HdfsFileStatusProto, HdfsFileStatusProto_FileType};
use libhdfesse::fs;

use std::convert::TryFrom;
use std::ffi::CString;
use std::os::raw::{c_char, c_int, c_short};
use std::ptr::null_mut;
/**

Drop-in replacement of libhdfs.

Based on the
https://github.com/apache/hadoop/blob/a89ca56a1b0eb949f56e7c6c5c25fdf87914a02f/hadoop-hdfs-project/hadoop-hdfs-native-client/src/main/native/libhdfs/include/hdfs/hdfs.h

*/
use std::{
    borrow::Cow,
    ffi::{c_void, CStr},
};

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

// TODO make these types distinct
pub type hdfsFS = *mut libhdfesse::fs::HDFS;
pub type hdfsBuilder = c_void;
pub type hdfsStreamBuilder = c_void;
pub type hadoopRzOptions = c_void;
pub type hadoopRzBuffer = c_void;

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

#[no_mangle]
pub extern "C" fn hdfsBuilder() -> *mut hdfsBuilder {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn hdfsBuilderSetForceNewInstance(_bld: *mut hdfsBuilder) {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn hdfsBuilderSetNameNodePort(_bld: *mut hdfsBuilder, _port: tPort) {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn hdfsBuilderSetUserName(_bld: *mut hdfsBuilder, _userName: *const c_char) {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn hdfsSetKerbTicketCachePath(
    _bld: *mut hdfsBuilder,
    _kerbTicketCachePath: *const c_char,
) {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn hdfsFreeBuilder(_bld: *mut hdfsBuilder) {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn hdfsBuilderConfSetStr(
    _bld: *mut hdfsBuilder,
    _key: *const c_char,
    _val: *const c_char,
) -> c_int {
    unimplemented!()
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
    let fs = fs.as_mut(); // TODO unwrap?  Fail if it is null.

    match (fs, path) {
        (Some(fs), Ok(path)) => match fs.get_file_info(Cow::Borrowed(path)) {
            Ok(_) => 1,
            Err(e) => match e {
                // set_errno_with_hadoop_error handles it too, but
                // for this function it is a normal situation.
                fs::FsError::NotFound(_) => 0,
                _ => {
                    errors::set_errno_with_hadoop_error(e);
                    -1
                }
            },
        },
        _ => {
            // TODO seems to be the only option.
            libc::__errno_location().write(errors::EINTERNAL);
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

#[no_mangle]
pub extern "C" fn hdfsDelete(_fs: hdfsFS, _pat: *const c_char, _recursive: c_int) -> c_int {
    unimplemented!()
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

#[no_mangle]
pub extern "C" fn hdfsCreateDirectory(_fs: hdfsFS, _path: *const c_char) -> c_int {
    unimplemented!()
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

impl TryFrom<&HdfsFileStatusProto> for hdfsFileInfo {
    type Error = std::ffi::NulError;

    fn try_from(fstat: &HdfsFileStatusProto) -> Result<Self, Self::Error> {
        let mKind = if fstat.get_fileType() == HdfsFileStatusProto_FileType::IS_DIR {
            tObjectKind::kObjectKindDirectory
        } else {
            tObjectKind::kObjectKindFile
        };
        let mName = CString::new(fstat.get_path())?;
        let mOwner = CString::new(fstat.get_owner())?;
        let mGroup = CString::new(fstat.get_group())?;

        Ok(hdfsFileInfo {
            mKind,
            mName: mName.into_raw(),
            mLastMod: (fstat.get_modification_time() / 1000) as _,
            mSize: fstat.get_length() as _,
            mReplication: fstat.get_block_replication() as _,
            mBlockSize: fstat.get_blocksize() as _,
            // TODO the original libhdfs has an ugly hack: it places
            // another struct (extInfo just behind the mOwner allocated string.
            // And extInfo.flags is updated with isEncrypted() flag.
            mOwner: mOwner.into_raw(),
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
        CString::from_raw(self.mOwner);
        CString::from_raw(self.mGroup);
    }
}

#[no_mangle]
pub extern "C" fn hdfsListDirectory(
    _fs: hdfsFS,
    _path: c_char,
    _numEntries: *mut c_int,
) -> *mut hdfsFileInfo {
    unimplemented!()
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
    let fs = fs.as_mut();

    match (fs, path) {
        (Some(fs), Ok(path)) => match fs.get_file_info(Cow::Borrowed(path)) {
            Ok(fstat) => {
                // TODO as we deallocate as Box<[T]>, one can create
                // it from Box<T> instead of Vec.
                let mut cont = Vec::with_capacity(1);
                // TODO check instead of unwrap
                cont.push(hdfsFileInfo::try_from(&fstat).unwrap());

                let mut sl = cont.into_boxed_slice();
                let ptr = sl.as_mut_ptr();
                std::mem::forget(sl);
                ptr
            }
            Err(e) => {
                errors::set_errno_with_hadoop_error(e);
                null_mut()
            }
        }
        _ => {
            // it seems this is the most sane value for non-UTF8 strings.
            libc::__errno_location().write(libc::EINVAL);
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

#[no_mangle]
pub extern "C" fn hdfsFileIsEncrypted(_hdfsFileInfo: *mut hdfsFileInfo) -> c_int {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn hdfsGetHosts(
    _path: *const c_char,
    _start: tOffset,
    _length: tOffset,
) -> *mut *mut *mut c_char {
    unimplemented!()
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

#[no_mangle]
pub extern "C" fn hdfsGetCapacity(_fs: hdfsFS) -> tOffset {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn hdfsGetUsed(_fs: hdfsFS) -> tOffset {
    unimplemented!()
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

#[no_mangle]
pub extern "C" fn hdfsChmod(_fs: hdfsFS, _path: *const c_char, _mode: c_short) -> c_int {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn hdfsUtime(
    _fs: hdfsFS,
    _path: *const c_char,
    _mtime: tTime,
    _atime: tTime,
) -> c_int {
    unimplemented!()
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
