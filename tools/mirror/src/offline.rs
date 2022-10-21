use crate::{ChainError, ChunkTxs};
use anyhow::Context;
use async_trait::async_trait;
use near_chain::types::EpochManagerAdapter;
use near_chain::{BlockHeader, ChainStore, ChainStoreAccess, RuntimeAdapter};
use near_chain_configs::GenesisValidationMode;
use near_chain_primitives::error::QueryError;
use near_crypto::PublicKey;
use near_primitives::hash::CryptoHash;
use near_primitives::transaction::{ExecutionOutcomeWithIdAndProof, ExecutionStatus};
use near_primitives::types::{AccountId, BlockHeight, BlockId};
use near_primitives::views::{BlockHeaderView, QueryRequest, QueryResponse, QueryResponseKind};
use near_primitives_core::types::{Nonce, ShardId};
use nearcore::NightshadeRuntime;
use std::path::Path;

use crate::TxOutcome;

pub(crate) struct ChainAccess {
    chain: ChainStore,
    runtime: NightshadeRuntime,
    head_height: BlockHeight,
}

impl ChainAccess {
    pub(crate) fn new<P: AsRef<Path>>(home: P) -> anyhow::Result<Self> {
        let config =
            nearcore::config::load_config(home.as_ref(), GenesisValidationMode::UnsafeFast)
                .with_context(|| format!("Error loading config from {:?}", home.as_ref()))?;
        // leave it ReadWrite since otherwise there are problems with the compiled contract cache
        let store_opener = near_store::NodeStorage::opener(home.as_ref(), &config.config.store);
        let store = store_opener
            .open()
            .with_context(|| format!("Error opening store in {:?}", home.as_ref()))?
            .get_store(near_store::Temperature::Hot);
        let runtime = NightshadeRuntime::from_config(home.as_ref(), store.clone(), &config);
        let chain = ChainStore::new(
            store,
            config.genesis.config.genesis_height,
            !config.client_config.archive,
        );
        let head_height = chain.head().context("Could not fetch chain head")?.height;
        Ok(Self { chain, runtime, head_height })
    }
}

// basically copied from Chain::is_on_current_chain()
fn is_on_current_chain(
    chain: &ChainStore,
    header: &BlockHeader,
) -> Result<bool, near_chain_primitives::Error> {
    let chain_header = chain.get_block_header_by_height(header.height())?;
    Ok(chain_header.hash() == header.hash())
}

// basically copied from Chain::get_execution_outcome(). We could just use a Chain instead of a ChainStore,
// but it makes more semantic sense to just have the store, since we're not implementing any protocol logic,
// but just reading stuff from storage, so we will just reimplement this logic here
fn get_execution_outcome(
    chain: &ChainStore,
    id: &CryptoHash,
) -> Result<ExecutionOutcomeWithIdAndProof, near_chain_primitives::Error> {
    let outcomes = chain.get_outcomes_by_id(id)?;
    outcomes
        .into_iter()
        .find(|outcome| match chain.get_block_header(&outcome.block_hash) {
            Ok(header) => is_on_current_chain(chain, &header).unwrap_or(false),
            Err(_) => false,
        })
        .ok_or_else(|| {
            near_chain_primitives::Error::DBNotFoundErr(format!("EXECUTION OUTCOME: {}", id))
        })
}

fn make_query(
    chain: &ChainStore,
    runtime: &NightshadeRuntime,
    block_hash: &CryptoHash,
    account_id: &AccountId,
    request: &QueryRequest,
) -> anyhow::Result<Result<QueryResponse, QueryError>> {
    let header = chain
        .get_block_header(block_hash)
        .with_context(|| format!("Can't fetch header with hash {}", block_hash))?;
    let shard_id = runtime
        .account_id_to_shard_id(account_id, header.epoch_id())
        .with_context(|| format!("Can't fetch shard id for {} @ {}", account_id, block_hash))?;
    let shard_uid = runtime
        .shard_id_to_uid(shard_id, header.epoch_id())
        .with_context(|| format!("Can't fetch shard uid for {}", shard_id))?;
    let chunk_extra = chain.get_chunk_extra(block_hash, &shard_uid).with_context(|| {
        format!("Can't fetch chunk extra for {} shard {}", block_hash, shard_id)
    })?;
    Ok(runtime.query(
        shard_uid,
        chunk_extra.state_root(),
        header.height(),
        header.raw_timestamp(),
        header.prev_hash(),
        header.hash(),
        header.epoch_id(),
        request,
    ))
}

#[async_trait(?Send)]
impl crate::ChainAccess for ChainAccess {
    async fn head_height(&self) -> anyhow::Result<BlockHeight> {
        Ok(self.head_height)
    }

    async fn get_block_header(&self, id: BlockId) -> Result<BlockHeaderView, ChainError> {
        match id {
            BlockId::Hash(hash) => {
                self.chain.get_block_header(&hash).map(Into::into).map_err(Into::into)
            }
            BlockId::Height(height) => {
                let hash = self.chain.get_block_hash_by_height(height)?;
                self.chain.get_block_header(&hash).map(Into::into).map_err(Into::into)
            }
        }
    }

    async fn get_txs(
        &self,
        height: BlockHeight,
        shards: &[ShardId],
    ) -> Result<Vec<ChunkTxs>, ChainError> {
        let block_hash = self
            .chain
            .get_block_hash_by_height(height)
            .with_context(|| format!("Can't get block hash at height {}", height))?;
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
            chunks.push(ChunkTxs {
                shard_id: chunk.shard_id(),
                transactions: chunk.transactions().iter().map(|t| t.clone().into()).collect(),
            })
        }
        Ok(chunks)
    }

    async fn account_exists(
        &self,
        account_id: &AccountId,
        block_hash: &CryptoHash,
    ) -> anyhow::Result<bool> {
        match make_query(
            &self.chain,
            &self.runtime,
            block_hash,
            account_id,
            &QueryRequest::ViewAccount { account_id: account_id.clone() },
        )? {
            Ok(r) => match r.kind {
                QueryResponseKind::ViewAccount(_) => Ok(true),
                _ => unreachable!(),
            },
            Err(e) => match &e {
                QueryError::UnknownAccount { .. } => Ok(false),
                _ => Err(e).with_context(|| format!("query for {} failed", account_id)),
            },
        }
    }

    async fn fetch_access_key_nonce(
        &self,
        account_id: &AccountId,
        public_key: &PublicKey,
        block_hash: &CryptoHash,
    ) -> anyhow::Result<Option<Nonce>> {
        match make_query(
            &self.chain,
            &self.runtime,
            block_hash,
            account_id,
            &QueryRequest::ViewAccessKey {
                account_id: account_id.clone(),
                public_key: public_key.clone(),
            },
        )? {
            Ok(r) => match r.kind {
                QueryResponseKind::AccessKey(access_key) => Ok(Some(access_key.nonce)),
                _ => unreachable!(),
            },
            Err(e) => match &e {
                QueryError::UnknownAccount { .. } | QueryError::UnknownAccessKey { .. } => Ok(None),
                _ => Err(e)
                    .with_context(|| format!("query for {} {} failed", account_id, public_key)),
            },
        }
    }

    async fn fetch_tx_outcome(
        &self,
        transaction_hash: CryptoHash,
        _signer_id: &AccountId,
        _receiver_id: &AccountId,
    ) -> anyhow::Result<TxOutcome> {
        let outcome = get_execution_outcome(&self.chain, &transaction_hash).with_context(|| {
            format!("Could not find execution outcome for tx {}", transaction_hash)
        })?;
        let receipt_id = match outcome.outcome_with_id.outcome.status {
            ExecutionStatus::SuccessReceiptId(id) => id,
            ExecutionStatus::SuccessValue(_) => unreachable!(),
            ExecutionStatus::Failure(_) | ExecutionStatus::Unknown => {
                return Ok(TxOutcome::Failure)
            }
        };
        match get_execution_outcome(&self.chain, &receipt_id) {
            Ok(outcome) => match outcome.outcome_with_id.outcome.status {
                ExecutionStatus::SuccessReceiptId(_) | ExecutionStatus::SuccessValue(_) => {
                    Ok(TxOutcome::Success(outcome.block_hash))
                }
                ExecutionStatus::Failure(_) | ExecutionStatus::Unknown => Ok(TxOutcome::Failure),
            },
            Err(near_chain::Error::DBNotFoundErr(_)) => Ok(TxOutcome::Pending),
            Err(e) => Err(e)
                .with_context(|| format!("failed fetching outcome for receipt {}", &receipt_id)),
        }
    }
}
