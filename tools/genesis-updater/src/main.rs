use clap::Parser;
use std::path::PathBuf;

#[derive(Parser)]
struct Cli {
    #[clap(long)]
    genesis_file_in: PathBuf,
    #[clap(long)]
    genesis_file_out: PathBuf,
    #[clap(long)]
    records_file_in: PathBuf,
    #[clap(long)]
    records_file_out: PathBuf,
    #[clap(long)]
    chain_id: Option<String>,
}

fn main() -> anyhow::Result<()> {
    let args = Cli::parse();

    genesis_updater::create_genesis(
        &args.genesis_file_in,
        &args.genesis_file_out,
        &args.records_file_in,
        &args.records_file_out,
        &["foo0", "foo1", "foo2"],
        true,
        args.chain_id.as_deref(),
    )
}
