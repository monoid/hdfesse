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
use libhdfesse::{fs::Hdfs, path::Path};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
pub struct RmArgs {
    #[structopt(short = "r", help = "Remove directories recursively")]
    recursive: bool,
    #[structopt(name = "src", required = true)]
    srcs: Vec<String>,
}

pub struct Rm<'a> {
    hdfs: &'a mut Hdfs,
}

impl<'a> Rm<'a> {
    pub fn new(hdfs: &'a mut Hdfs) -> Self {
        Self { hdfs }
    }
}

impl<'a> Command for Rm<'a> {
    type Args = RmArgs;
    type Error = anyhow::Error;

    fn run(&mut self, args: Self::Args) -> Result<i32> {
        let mut has_error = false;

        for src in args.srcs {
            if let Err(e) = self.hdfs.delete(&Path::new(&src)?, args.recursive) {
                has_error = true;
                eprintln!("{}", e);
            }
        }

        Ok(has_error as _)
    }
}
