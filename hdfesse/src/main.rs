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
mod cli;
use anyhow::Result;
use cli::Command;
use libhdfesse::path::{Path, UriResolver};
use libhdfesse::hdconfig::{HDFS_CONFIG, get_auto_config};
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
}

fn main() -> Result<()> {
    tracing::subscriber::set_global_default(
        tracing_subscriber::Registry::default().with(tracing_tree::HierarchicalLayer::new(2)),
    )
    .unwrap();

    let opt = HdfessseApp::from_args();

    let config = get_auto_config(&HDFS_CONFIG);

    let default_fs = Path::new(&config.default_fs.as_ref().expect("config without defaultFs is not supported; perhaps, config is not found"))?;

    let client = libhdfesse::rpc::HdfsConnection::new_from_path(
        &config,
        default_fs,
        &libhdfesse::rpc::SimpleConnector {},
    )?;

    let service = libhdfesse::service::ClientNamenodeService::new(client);
    let resolve = UriResolver::new("STUB", service.get_user(), None, None)?;
    let mut hdfs = libhdfesse::fs::Hdfs::new(service, resolve);

    let retcode = match opt.subcmd {
        TopSubcmd::Dfs(dfs) => match dfs {
            Dfs::Ls(ls_args) => cli::ls::Ls::new(&mut hdfs).run(ls_args)?,
            Dfs::Mv(mv_args) => cli::mv::Mv::new(&mut hdfs).run(mv_args)?,
            Dfs::Mkdir(mkdir_args) => cli::mkdir::Mkdir::new(&mut hdfs).run(mkdir_args)?,
        },
    };
    hdfs.shutdown()?;
    std::process::exit(retcode);
}
