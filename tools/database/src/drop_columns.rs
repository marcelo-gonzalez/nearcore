use anyhow::Context;
use clap::Parser;
use near_store::DBCol;
use std::path::PathBuf;
use strum::IntoEnumIterator;

#[derive(Parser)]
pub(crate) struct DropColumnsCommand {}

impl DropColumnsCommand {
    pub(crate) fn run(&self, home: &PathBuf) -> anyhow::Result<()> {
        let config =
            nearcore::load_config(home, near_chain_configs::GenesisValidationMode::UnsafeFast)?;
        let mut unwanted_cols = Vec::new();
        for col in DBCol::iter() {
            match col {
                DBCol::DbVersion
                | DBCol::Misc
                | DBCol::State
                | DBCol::FlatState
                | DBCol::EpochInfo
                | DBCol::FlatStorageStatus
                | DBCol::ChunkExtra => {}
                _ => unwanted_cols.push(col),
            }
        }
        near_store::clear_columns(
            home,
            config.client_config.archive,
            &config.config.store,
            config.config.cold_store.as_ref(),
            &unwanted_cols,
        )
        .context("failed deleting unwanted columns")?;
        Ok(())
    }
}
