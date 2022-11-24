use crate::{ChainError, SourceBlock};
use anyhow::Context;
use async_trait::async_trait;
use near_chain::{ChainStore, ChainStoreAccess, RuntimeAdapter};
use near_chain_configs::GenesisValidationMode;
use near_crypto::PublicKey;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::block::BlockHeader;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::Receipt;
use near_primitives::types::{AccountId, BlockHeight, TransactionOrReceiptId};
use near_primitives::views::{
    AccessKeyPermissionView, ExecutionOutcomeWithIdView, QueryRequest, QueryResponseKind,
    ReceiptView,
};
use near_primitives_core::types::ShardId;
use nearcore::NightshadeRuntime;
use std::path::Path;

// this implements the same logic as in Chain::get_execution_outcome(). We will rewrite
// that here because it makes more sense for us to have just the ChainStore and not the chain,
// since we're just reading data, not doing any protocol related stuff
fn is_on_current_chain(
    chain: &ChainStore,
    header: &BlockHeader,
) -> Result<bool, near_chain_primitives::Error> {
    let chain_header = chain.get_block_header_by_height(header.height())?;
    Ok(chain_header.hash() == header.hash())
}

pub(crate) struct ChainAccess {
    chain: ChainStore,
    runtime: NightshadeRuntime,
}

impl ChainAccess {
    pub(crate) fn new<P: AsRef<Path>>(home: P) -> anyhow::Result<Self> {
        let config =
            nearcore::config::load_config(home.as_ref(), GenesisValidationMode::UnsafeFast)
                .with_context(|| format!("Error loading config from {:?}", home.as_ref()))?;
        // leave it ReadWrite since otherwise there are problems with the compiled contract cache
        let store_opener =
            near_store::NodeStorage::opener(home.as_ref(), &config.config.store, None);
        let store = store_opener
            .open()
            .with_context(|| format!("Error opening store in {:?}", home.as_ref()))?
            .get_store(near_store::Temperature::Hot);
        let chain = ChainStore::new(
            store.clone(),
            config.genesis.config.genesis_height,
            !config.client_config.archive,
        );
        let runtime = NightshadeRuntime::from_config(home.as_ref(), store, &config);
        Ok(Self { chain, runtime })
    }
}

#[async_trait(?Send)]
impl crate::ChainAccess for ChainAccess {
    async fn head_height(&self) -> Result<BlockHeight, ChainError> {
        Ok(self.chain.head().context("Could not fetch chain head")?.height)
    }

    async fn get_txs(
        &self,
        height: BlockHeight,
        shards: &[ShardId],
    ) -> Result<Vec<SourceBlock>, ChainError> {
        let block_hash = self.chain.get_block_hash_by_height(height)?;
        let block = self
            .chain
            .get_block(&block_hash)
            .with_context(|| format!("Can't get block {} at height {}", &block_hash, height))?;

        // of course simpler/faster to just have an array of bools but this is a one liner and who cares :)
        let shards = shards.iter().collect::<std::collections::HashSet<_>>();

        let mut chunks = Vec::new();
        for chunk in block.chunks().iter() {
            if !shards.contains(&chunk.shard_id()) {
                continue;
            }
            let chunk = match self.chain.get_chunk(&chunk.chunk_hash()) {
                Ok(c) => c,
                Err(e) => {
                    tracing::error!(
                        "Can't fetch source chain shard {} chunk at height {}. Are we tracking all shards?: {:?}",
                        chunk.shard_id(), height, e
                    );
                    continue;
                }
            };
            chunks.push(SourceBlock {
                shard_id: chunk.shard_id(),
                transactions: chunk.transactions().iter().map(|t| t.clone().into()).collect(),
                receipts: chunk.receipts().iter().map(|t| t.clone().into()).collect(),
            })
        }
        Ok(chunks)
    }

    async fn get_outcome(
        &self,
        id: TransactionOrReceiptId,
    ) -> Result<ExecutionOutcomeWithIdView, ChainError> {
        let id = match id {
            TransactionOrReceiptId::Receipt { receipt_id, .. } => receipt_id,
            TransactionOrReceiptId::Transaction { transaction_hash, .. } => transaction_hash,
        };
        let outcomes = self.chain.get_outcomes_by_id(&id)?;
        outcomes
            .into_iter()
            .find(|outcome| match self.chain.get_block_header(&outcome.block_hash) {
                Ok(header) => is_on_current_chain(&self.chain, &header).unwrap_or(false),
                Err(_) => false,
            })
            .map(Into::into)
            .ok_or(ChainError::Unknown)
    }

    async fn get_receipt(&self, id: &CryptoHash) -> Result<ReceiptView, ChainError> {
        self.chain.get_receipt(id)?.map(|r| Receipt::clone(&r).into()).ok_or(ChainError::Unknown)
    }

    async fn get_full_access_keys(
        &self,
        account_id: &AccountId,
        block_hash: &CryptoHash,
    ) -> Result<Vec<PublicKey>, ChainError> {
        let mut ret = Vec::new();
        let header = self.chain.get_block_header(block_hash)?;
        let shard_id = self.runtime.account_id_to_shard_id(account_id, header.epoch_id())?;
        let shard_uid = self.runtime.shard_id_to_uid(shard_id, header.epoch_id())?;
        let chunk_extra = self.chain.get_chunk_extra(header.hash(), &shard_uid)?;
        match self
            .runtime
            .query(
                shard_uid,
                chunk_extra.state_root(),
                header.height(),
                header.raw_timestamp(),
                header.prev_hash(),
                header.hash(),
                header.epoch_id(),
                &QueryRequest::ViewAccessKeyList { account_id: account_id.clone() },
            )?
            .kind
        {
            QueryResponseKind::AccessKeyList(l) => {
                for k in l.keys {
                    if k.access_key.permission == AccessKeyPermissionView::FullAccess {
                        ret.push(k.public_key);
                    }
                }
            }
            _ => unreachable!(),
        }
        Ok(ret)
    }
}
