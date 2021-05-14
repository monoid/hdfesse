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
use libhdfesse::{
    fs::{Hdfs, HdfsError},
    path::{Path, PathError},
};
use structopt::StructOpt;
use thiserror::Error;

#[derive(Debug, StructOpt)]
#[structopt(name = "mkdir", about = "Create a directory in specified location")]
pub struct MkdirArgs {
    #[structopt(name = "src", required = true)]
    srcs: Vec<String>,
    #[structopt(short = "p", help = "Do not fail if the directory already exists")]
    parents: bool,
}

#[derive(Debug, Error)]
pub enum MkdirError {
    #[error(transparent)]
    Uri(PathError),
    #[error("ls: {0}")]
    Fs(#[from] HdfsError),
}

pub struct Mkdir<'a> {
    hdfs: &'a mut Hdfs,
}

impl<'a> Mkdir<'a> {
    pub fn new(hdfs: &'a mut Hdfs) -> Self {
        Self { hdfs }
    }

    fn mkdir(&mut self, path_str: &str, parents: bool) -> Result<bool> {
        // TODO resolving
        let path = Path::new(path_str).map_err(MkdirError::Uri)?;

        Ok(self.hdfs.mkdirs(&path, parents)?)
    }
}

impl<'a> Command for Mkdir<'a> {
    type Args = MkdirArgs;
    type Error = anyhow::Error;

    fn run(&mut self, args: Self::Args) -> Result<i32> {
        let mut has_error = false;

        for path_str in args.srcs {
            if let Err(e) = self.mkdir(&path_str, args.parents) {
                has_error = true;
                eprintln!("{}", e);
            }
        }

        Ok(if has_error { 1 } else { 0 })
    }
}
