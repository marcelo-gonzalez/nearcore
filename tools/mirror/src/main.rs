use clap::Parser;
use std::cell::Cell;
use std::path::PathBuf;

use mirror::TxMirror;

#[derive(Parser)]
struct Cli {
    #[clap(subcommand)]
    subcmd: SubCommand,
}

#[derive(Parser)]
enum SubCommand {
    Prepare(PrepareCmd),
    Run(RunCmd),
    Init(InitCmd),
}

#[derive(Parser)]
struct RunCmd {
    #[clap(long)]
    source_home: PathBuf,
    #[clap(long)]
    target_home: PathBuf,
}

impl RunCmd {
    fn run(self) {
        openssl_probe::init_ssl_cert_env_vars();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let _subscriber = runtime.block_on(async {
            near_o11y::default_subscriber(
                near_o11y::EnvFilterBuilder::from_env().finish().unwrap(),
                &Default::default(),
            )
            .await
            .global()
        });

        let system = new_actix_system(runtime);
        system.block_on(async move {
            let m = TxMirror::new(&self.source_home, &self.target_home);
            actix::spawn(m.run());
        });
        system.run().unwrap();
    }
}

#[derive(Parser)]
struct PrepareCmd {
    #[clap(long)]
    records_file_in: PathBuf,
    #[clap(long)]
    records_file_out: PathBuf,
    #[clap(long)]
    mapping_file: PathBuf,
}

impl PrepareCmd {
    fn run(self) {
        mirror::generate_new_keys(
            &self.records_file_in,
            &self.records_file_out,
            &self.mapping_file,
        )
        .unwrap();
    }
}

#[derive(Parser)]
struct InitCmd {
    #[clap(long)]
    target_home: PathBuf,
    #[clap(long)]
    mapping_file: PathBuf,
}

impl InitCmd {
    fn run(self) {
        mirror::init_db(&self.target_home, &self.mapping_file).unwrap();
    }
}

// copied from neard/src/cli.rs
fn new_actix_system(runtime: tokio::runtime::Runtime) -> actix::SystemRunner {
    // `with_tokio_rt()` accepts an `Fn()->Runtime`, however we know that this function is called exactly once.
    // This makes it safe to move out of the captured variable `runtime`, which is done by a trick
    // using a `swap` of `Cell<Option<Runtime>>`s.
    let runtime_cell = Cell::new(Some(runtime));
    actix::System::with_tokio_rt(|| {
        let r = Cell::new(None);
        runtime_cell.swap(&r);
        r.into_inner().unwrap()
    })
}

fn main() {
    let args = Cli::parse();

    match args.subcmd {
        SubCommand::Prepare(r) => r.run(),
        SubCommand::Init(r) => r.run(),
        SubCommand::Run(r) => r.run(),
    }
}
