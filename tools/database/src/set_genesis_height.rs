use near_chain_configs::GenesisValidationMode;
use near_epoch_manager::{EpochManager, EpochManagerAdapter};
use near_primitives::types::EpochId;
use near_store::NodeStorage;

use std::path::Path;

#[derive(clap::Args)]
pub(crate) struct SetGenesisHeightCommand {}

impl SetGenesisHeightCommand {
    pub(crate) fn run(
        &self,
        home_dir: &Path,
        genesis_validation: GenesisValidationMode,
    ) -> anyhow::Result<()> {
        let near_config = nearcore::config::load_config(&home_dir, genesis_validation)?;
        let opener = NodeStorage::opener(
            home_dir,
            &near_config.config.store,
            near_config.config.archival_config(),
        );

        let storage = opener.open()?;
        let store = storage.get_hot_store();

        let epoch_manager = EpochManager::new_arc_handle(
            store.clone(),
            &near_config.genesis.config,
            Some(home_dir),
        );

        let genesis_epoch_config = epoch_manager.get_epoch_config(&EpochId::default())?;
        near_store::genesis::initialize_sharded_genesis_state(
            store,
            &near_config.genesis,
            &genesis_epoch_config,
            Some(home_dir),
        );
        Ok(())
    }
}
