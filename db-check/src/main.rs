use anyhow::{anyhow, Result};
use clap::Parser;
use reth_chainspec::ChainSpecBuilder;
use reth_db::{
    mdbx::{DatabaseArguments, MaxReadTransactionDuration},
    DatabaseEnv,
};
use reth_db_api::tables::{Bytecodes, PlainAccountState, PlainStorageState};
use reth_node_ethereum::EthereumNode;
use reth_node_types::NodeTypesWithDBAdapter;
use reth_db::transaction::DbTx;
use reth_provider::{
    providers::{RocksDBProvider, StaticFileProvider},
    ProviderFactory, StageCheckpointReader,
};
use reth_stages::StageId;
use std::{path::Path, sync::Arc};

#[derive(Parser)]
#[command(name = "db-check")]
struct Cli {
    #[arg(short = 'd', long = "datadir", help = "Reth datadir path")]
    datadir: String,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    let db_path = Path::new(&cli.datadir).join("db");
    let db = reth_db::open_db_read_only(
        db_path.as_path(),
        DatabaseArguments::default()
            .with_max_read_transaction_duration(Some(MaxReadTransactionDuration::Unbounded)),
    )
    .map_err(|err| anyhow!(err))?;

    println!("Opened DB at {}", db_path.display());

    let tokio_rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let runtime = reth_tasks::Runtime::with_existing_handle(tokio_rt.handle().clone())
        .map_err(|err| anyhow!("{err}"))?;

    let rocksdb_provider = RocksDBProvider::builder(Path::new(&cli.datadir).join("rocksdb"))
        .build()
        .map_err(|err| anyhow!("{err}"))?;

    let spec = ChainSpecBuilder::mainnet().build();
    let factory = ProviderFactory::<NodeTypesWithDBAdapter<EthereumNode, Arc<DatabaseEnv>>>::new(
        db.into(),
        spec.into(),
        StaticFileProvider::read_only(db_path.join("static_files"), true)?,
        rocksdb_provider,
        runtime,
    )
    .map_err(|err| anyhow!("{err}"))?;

    let provider = factory.provider()?;

    let latest_block = provider
        .get_stage_checkpoint(StageId::Finish)?
        .map(|ch| ch.block_number)
        .ok_or(anyhow!("No finish checkpoint found — is the node synced?"))?;

    let tx = provider.into_tx();
    let n_accounts = tx.entries::<PlainAccountState>()?;
    let n_slots = tx.entries::<PlainStorageState>()?;
    let n_bytecodes = tx.entries::<Bytecodes>()?;

    println!("Latest block:    {}", latest_block);
    println!("Accounts:        {}", n_accounts);
    println!("Storage slots:   {}", n_slots);
    println!("Bytecodes:       {}", n_bytecodes);

    Ok(())
}
