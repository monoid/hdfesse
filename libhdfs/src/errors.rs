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
use libhdfesse::{fs, rpc};
use std::os::raw::c_int;
use thiserror::Error;

/**
This type incapsulates both HDFS-related errors and C-specific errors.
*/
#[derive(Debug, Error)]
pub enum LibError {
    #[error(transparent)]
    Hdfs(#[from] fs::HdfsError),
    #[error(transparent)]
    NulString(#[from] std::ffi::NulError),
    #[error("OOM allocating")]
    Oom,
}

static EXCEPTION_INFO: ::phf::Map<&'_ str, c_int> = ::phf::phf_map! {
    // TODO: actually, there are the exception generated by local JVM;
    // however, we handle it for remote exceptions, what is wrong.
    // We should further extend the FsError enum (as we handle the
    // NotFound), using this map as a base.
    "java.io.FileNotFoundException" => libc::ENOENT,
    "org.apache.hadoop.security.AccessControlException" => libc::EACCES,
    "org.apache.hadoop.fs.UnresolvedLinkException" => libc::ENOLINK,
    "org.apache.hadoop.fs.ParentNotDirectoryException" => libc::ENOTDIR,
    "java.lang.IllegalArgumentException" => libc::EINVAL,
    "java.lang.OutOfMemoryError" => libc::ENOMEM,
    "org.apache.hadoop.hdfs.server.namenode.SafeModeException" => libc::EROFS,
    "org.apache.hadoop.fs.FileAlreadyExistsException" => libc::EEXIST,
    "org.apache.hadoop.hdfs.protocol.QuotaExceededException" => libc::EDQUOT,
    "java.lang.UnsupportedOperationException" => libc::ENOTSUP,
};

// TODO: hdfs.h detects if EINTERNAL defined or not.  It seems we have
// no such possibility, thus we define it with hdfs.h default value.
// It breaks binary compatibility in this area.
pub(crate) const EINTERNAL: c_int = 255;

pub(crate) fn get_error_code(class_name: &str) -> libc::c_int {
    EXCEPTION_INFO.get(class_name).cloned().unwrap_or(EINTERNAL)
}

pub(crate) unsafe fn set_errno_with_hadoop_error<E: Into<LibError>>(e: E) {
    let the_errno = match e.into() {
        LibError::Hdfs(he) => match he.source {
            fs::FsError::NotFound(_) => libc::ENOENT,
            fs::FsError::FileExists(_) => libc::EEXIST,
            fs::FsError::IsDir(_) => libc::EISDIR,
            fs::FsError::NotDir(_) => libc::ENOTDIR,
            fs::FsError::Rpc(r) => match r {
                rpc::RpcError::Io(e) => e.raw_os_error().unwrap_or(EINTERNAL),
                _ => r.get_class_name().map(get_error_code).unwrap_or(EINTERNAL),
            },
            fs::FsError::Path(_) => libc::EINVAL,
        },
        LibError::NulString(_) => libc::EINVAL,
        LibError::Oom => libc::ENOMEM,
    };
    errno::set_errno(errno::Errno(the_errno));
}
