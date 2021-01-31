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
use structopt::StructOpt;

#[derive(StructOpt)]
struct HdfessseApp {
    #[structopt(long)]
    namenode: String,
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
}

fn main() -> Result<()> {
    let opt = HdfessseApp::from_args();

    let client =
        libhdfesse::rpc::HdfsConnection::new(opt.namenode, &libhdfesse::rpc::SimpleConnector {})?;

    let mut service = libhdfesse::service::ClientNamenodeService::new(client);

    match opt.subcmd {
        TopSubcmd::Dfs(dfs) => match dfs {
            Dfs::Ls(ls_args) => {
                cli::ls::Ls::new(&mut service).run(ls_args)?;
            }
        },
    }
    Ok(())
}
