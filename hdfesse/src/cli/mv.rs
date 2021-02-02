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
use libhdfesse::service::ClientNamenodeService;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
pub struct MvArgs {
    #[structopt(name = "src", required = true)]
    srcs: Vec<String>,
    #[structopt(required = true)]
    dst: String,
}

pub struct Mv<'a> {
    service: &'a mut ClientNamenodeService,
}

impl<'a> Mv<'a> {
    pub fn new(service: &'a mut ClientNamenodeService) -> Self {
        Self { service }
    }
}

impl<'a> Command for Mv<'a> {
    type Args = MvArgs;

    fn run(&mut self, args: Self::Args) -> Result<i32> {
        // TODO It seems that we should prevent overwrites and skip
        // non-existing files istead of failing after the first one.
        if args.srcs.len() > 1 {
            // TODO validate that dst exists and is a dir.
        }
        for src in args.srcs {
            self.service.rename(src, args.dst.clone())?;
        }
        Ok(0)
    }
}
