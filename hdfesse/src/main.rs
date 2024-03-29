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
#![forbid(unsafe_code)]
#![warn(rust_2018_idioms)]
mod cli;
use anyhow::Result;
use cli::Command;
use libhdfesse::hdconfig::Config;
use libhdfesse::path::{Path, UriResolver};
use structopt::StructOpt;
use tracing_subscriber::layer::SubscriberExt;

#[derive(StructOpt)]
struct HdfessseApp {
    #[structopt(subcommand)]
    subcmd: TopSubcmd,
}

// The name is not visible in the command line.
#[derive(StructOpt)]
enum TopSubcmd {
    Dfs(Dfs),
}

#[derive(StructOpt)]
enum Dfs {
    #[structopt(name = "-ls")]
    Ls(cli::ls::LsArgs),
    #[structopt(name = "-mv")]
    Mv(cli::mv::MvArgs),
    #[structopt(name = "-mkdir")]
    Mkdir(cli::mkdir::MkdirArgs),
    #[structopt(name = "-rm")]
    Rm(cli::rm::RmArgs),
}

fn main() -> Result<()> {
    tracing::subscriber::set_global_default(
        tracing_subscriber::Registry::default().with(tracing_tree::HierarchicalLayer::new(2)),
    )
    .unwrap();

    let opt = HdfessseApp::from_args();

    let config = Config::auto();

    let default_fs = Path::new(
        config
            .default_fs
            .as_ref()
            .expect("config without defaultFS is not supported; perhaps, config is not found"),
    )?;

    let dfs = default_fs
        .host()
        .expect("defaultFS has to have a host, otherwise not supported");

    let ns = match config
        .services
        .iter()
        .find(|s| s.name.as_ref() == dfs.as_str())
    {
        Some(x) => x,
        None => {
            panic!("Service {:?} not found", dfs);
        }
    };
    let client =
        libhdfesse::ha_rpc::HaHdfsConnection::new(ns, libhdfesse::rpc::SimpleConnector {})?;

    let service = libhdfesse::service::ClientNamenodeService::new(client);
    let resolve = UriResolver::new("STUB", service.get_user(), None, None)?;
    let mut hdfs = libhdfesse::fs::Hdfs::new(service, resolve);

    let retcode = match opt.subcmd {
        TopSubcmd::Dfs(dfs) => match dfs {
            Dfs::Ls(ls_args) => cli::ls::Ls::new(&mut hdfs).run(ls_args)?,
            Dfs::Mv(mv_args) => cli::mv::Mv::new(&mut hdfs).run(mv_args)?,
            Dfs::Mkdir(mkdir_args) => cli::mkdir::Mkdir::new(&mut hdfs).run(mkdir_args)?,
            Dfs::Rm(rm_args) => cli::rm::Rm::new(&mut hdfs).run(rm_args)?,
        },
    };
    hdfs.shutdown()?;
    std::process::exit(retcode);
}
