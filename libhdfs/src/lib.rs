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
/**

Drop-in replacement of libhdfs.

Based on the
https://github.com/apache/hadoop/blob/a89ca56a1b0eb949f56e7c6c5c25fdf87914a02f/hadoop-hdfs-project/hadoop-hdfs-native-client/src/main/native/libhdfs/include/hdfs/hdfs.h

*/
use std::ffi::c_void;
use std::os::raw::{c_char, c_int, c_short};

pub type tPort = u16;
pub type tSize = i32;
pub type tOffset = i64;
pub type tTime = i64;  // TODO libc time_t

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
pub struct hdfs_internal {
}

#[repr(C)]
pub struct hdfsFile_internal {
}

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
pub extern "C"
fn hdfsFileIsOpenForRead(_file: hdfsFile) -> c_int {
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hdfsFileIsOpenForWrite(_file: hdfsFile) -> c_int {
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hdfsFileGetReadStatistics(_file: hdfsFile,
                             _stats: *mut *mut hdfsReadStatistics) -> c_int {
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hdfsReadStatisticsGetRemoteBytesRead(_stats: *mut hdfsReadStatistics) -> i64 {
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hdfsFileClearReadStatistics(_file: hdfsFile) -> c_int {
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hdfsFileFreeReadStatistics(_stats: *mut hdfsReadStatistics) {
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hdfsFileGetHedgedReadMetrics(_file: hdfsFile,
                             _stats: *mut *mut hdfsHedgedReadMetrics) -> c_int {
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hdfsFileFreeHedgedReadMetrics(_stats: *mut hdfsHedgedReadMetrics) {
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hdfsConnectAsUser(_nn: *const c_char, _port: tPort, _user: *const c_char) -> hdfsFS {
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hdfsFileConnect(_nn: *const c_char, _port: tPort) -> hdfsFS {
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hdfsConnectAsUserNewInstance(_nn: *const c_char, _port: tPort, _user: *const c_char) -> hdfsFS {
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hdfsConnectNewInstance(_nn: *const c_char, _port: tPort) -> hdfsFS {
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hdfsBuilderConnect(_bld: *mut hdfsBuilder) -> hdfsFS {
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hdfsBuilder() -> *mut hdfsBuilder {
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hdfsBuilderSetForceNewInstance(_bld: *mut hdfsBuilder) {
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hdfsBuilderSetNameNodePort(_bld: *mut hdfsBuilder, _port: tPort) {
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hdfsBuilderSetUserName(_bld: *mut hdfsBuilder, _userName: *const c_char) {
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hdfsSetKerbTicketCachePath(_bld: *mut hdfsBuilder, _kerbTicketCachePath: *const c_char) {
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hdfsFreeBuilder(_bld: *mut hdfsBuilder) {
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hdfsBuilderConfSetStr(_bld: *mut hdfsBuilder, _key: *const c_char, _val: *const c_char) -> c_int {
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hdfsConfGetStr(_key: *const c_char, _val: *mut *mut c_char) -> c_int {
    unimplemented!()
}


#[no_mangle]
pub extern "C"
fn hdfsConfGetInt(_key: *const c_char, _val: *mut *mut i32) -> c_int {
    unimplemented!()
}


#[no_mangle]
pub extern "C"
fn hdfsConfStrFree(_val: *mut c_char ) {
    unimplemented!()    
}

#[no_mangle]
pub extern "C"
fn hdfsDisconnect(_fs: hdfsFS) -> c_int {
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hdfsOpenFile(
    _fs: hdfsFS, _path: *const c_char, _flags: c_int,
    _bufferSize: c_int, _replication: c_short, _blocksize: tSize) -> hdfsFile {
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hdfsStreamBuilderAlloc(_fs: hdfsFS, _path: *const c_char, _flags: c_int)
-> *mut hdfsStreamBuilder {
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hdfsStreamBuilderFree(_bld: *mut hdfsStreamBuilder) {
    unimplemented!()    
}

#[no_mangle]
pub extern "C"
fn hdfsStreamBuilderSetBufferSize(_bld: *mut hdfsStreamBuilder, _bufferSize: i32) -> c_int {
    unimplemented!()
}


#[no_mangle]
pub extern "C"
fn hdfsStreamBuilderSetReplication(_bld: *mut hdfsStreamBuilder, _replication: i16) -> c_int {
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hdfsStreamBuilderSetDefaultBlockSize(_bld: *mut hdfsStreamBuilder, _defaultBlockSize: i64) -> c_int {
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hdfsStreamBuilderBuild(_bld: *mut hdfsStreamBuilder) -> hdfsFile {
    // TODO free _bld
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hdfsTruncateFile(_fs: hdfsFS, _path: *const c_char, _newLength: tOffset) -> c_int {
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hdfsUnbufferFile(_file: hdfsFile) -> c_int {
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hdfsCloseFile(_fs: hdfsFS, _file: hdfsFile) -> c_int {
    // TODO free _file
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hdfsExists(_fs: hdfsFS, _path: *const c_char) -> c_int {
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hdfsSeek(_fs: hdfsFS, _file: hdfsFile, _disiredPos: tOffset) -> c_int {
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hdfsTell(_fs: hdfsFS, _file: hdfsFile) -> tOffset {
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hdfsRead(_fs: hdfsFS, _file: hdfsFile, _buffer: *mut c_void, _length: tSize) -> tSize {
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hdfsPread(
    _fs: hdfsFS, _file: hdfsFile, _position: tOffset,
    _buffer: *mut c_void, _length: tSize) -> tSize {
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hdfsPreadFully(
    _fs: hdfsFS, _file: hdfsFile, _position: tOffset,
    _buffer: *mut c_void, _length: tSize) -> tSize {
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hdfsWrite(_fs: hdfsFS, _file: hdfsFile, _buffer: *const c_void, _length: tSize) -> tSize {
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hdfsFlush(_fs: hdfsFS, _file: hdfsFile) -> c_int {
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hdfsHFlush(_fs: hdfsFS, _file: hdfsFile) -> c_int {
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hdfsHSync(_fs: hdfsFS, _file: hdfsFile) -> c_int {
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hdfsAvailable(_fs: hdfsFS, _file: hdfsFile) -> c_int {
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hdfsCopy(
    _srcFs: hdfsFS, _src: *const c_char,
    _dstFs: hdfsFS, _dst: *const char) -> c_int {
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hdfsMove(
    _srcFs: hdfsFS, _src: *const c_char,
    _dstFs: hdfsFS, _dst: *const char) -> c_int {
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hdfsDelete(
    _fs: hdfsFS, _pat: *const c_char, _recursive: c_int) -> c_int {
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hdfsRename(
    _fs: hdfsFS, _oldPath: *const c_char,
    _newPath: *const char) -> c_int {
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hdfsGetWorkingDirectory(_fs: hdfsFS, _buffer: *mut c_char, _bufferSize: usize) -> *mut c_char {
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hdfsSetWorkingDirectory(_fs: hdfsFS, _path: *const c_char) -> c_int {
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hdfsCreateDirectory(_fs: hdfsFS, _path: *const c_char) -> c_int {
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hdfsSetReplication(_fs: hdfsFS, _path: *const c_char, _replication: i16) -> c_int {
    unimplemented!()
}

#[repr(C)]
pub struct hdfsFileInfo {
    mKind: tObjectKind,
    mName: *mut c_char,
    mLatMod: tTime,
    mSize: tOffset,
    mReplication: c_short,
    mBlockSize: tOffset,
    mOwner: *mut c_char,
    mGroup: *mut c_char,
    mPermissions: c_short,
    mLastAccess: tTime,
}

#[no_mangle]
pub extern "C"
fn hdfsListDirectory(_fs: hdfsFS, _path: c_char, _numEntries: *mut c_int) -> *mut hdfsFileInfo {
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hdfsGetPathInfo(_fs: hdfsFS, _path: c_char) -> *mut hdfsFileInfo {
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hdfsFreeFileInfo(_hdfsFileInfo: *mut hdfsFileInfo, _numEntries: c_int) {
    unimplemented!()    
}

#[no_mangle]
pub extern "C"
fn hdfsFileIsEncrypted(_hdfsFileInfo: *mut hdfsFileInfo) -> c_int {
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hdfsGetHosts(_path: *const c_char, _start: tOffset, _length: tOffset) -> *mut *mut *mut c_char {
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hdfsFreeHosts(_blockHosts: *mut *mut *mut c_char) {    
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hdfsGetDefaultBlockSize(_fs: hdfsFS) -> tOffset {
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hdfsGetDefaultBlockSizeAtPath(_fs: hdfsFS, _path: *const c_char) -> tOffset {
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hdfsGetCapacity(_fs: hdfsFS) -> tOffset {
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hdfsGetUsed(_fs: hdfsFS) -> tOffset {
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hdfsChown(_fs: hdfsFS, _path: *const c_char, _owner: *const c_char, _group: *const c_char) -> c_int {
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hdfsChmod(_fs: hdfsFS, _path: *const c_char, _mode: c_short) -> c_int {
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hdfsUtime(_fs: hdfsFS, _path: *const c_char, _mtime: tTime, _atime: tTime) -> c_int {
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hdfsRzOptionsAlloc() -> *mut hadoopRzOptions {
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hadoopRzOptionsSetSkipChecksum(_opts: *mut hadoopRzOptions, _skip: c_int) -> c_int {
    unimplemented!()
}


#[no_mangle]
pub extern "C"
fn hadoopRzOptionsSetByteBufferPool(_opts: *mut hadoopRzOptions, _className: *const c_char) -> c_int {
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hadoopRzOptionsFree(_opts: *mut hadoopRzOptions) {
    unimplemented!()    
}

#[no_mangle]
pub extern "C"
fn hadoopReadZero(_file: hdfsFile, _opts: *mut hadoopRzOptions, _maxLength: i32) -> *const hadoopRzBuffer {
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hadoopRzBufferLength(_buffer: *const hadoopRzBuffer) -> i32 {
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hadoopRzBufferGet(_buffer: *const hadoopRzBuffer) -> *const c_void {
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hadoopRzBufferFree(_file: hdfsFile, _buffer: *const hadoopRzBuffer) {
    unimplemented!()    
}


#[no_mangle]
pub extern "C"
fn hdfsGetLastExceptionRootCause() -> *const c_char {
    unimplemented!()
}

#[no_mangle]
pub extern "C"
fn hdfsGetLastExceptionStackTrace() -> *const c_char {
    unimplemented!()
}
