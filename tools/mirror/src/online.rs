use crate::{ChainError, ChunkTxs, TxOutcome};
use actix::Addr;
use anyhow::Context;
use async_trait::async_trait;
use near_chain_configs::GenesisValidationMode;
use near_client::{ClientActor, ViewClientActor};
use near_client_primitives::types::{
    GetBlock, GetChunk, GetChunkError, GetExecutionOutcome, GetExecutionOutcomeError,
    GetExecutionOutcomeResponse, Query, QueryError,
};
use near_crypto::PublicKey;
use near_o11y::WithSpanContextExt;
use near_primitives::hash::CryptoHash;
use near_primitives::types::{
    AccountId, BlockHeight, BlockId, BlockReference, TransactionOrReceiptId,
};
use near_primitives::views::{
    BlockHeaderView, ExecutionStatusView, QueryRequest, QueryResponseKind,
};
use near_primitives_core::types::{Nonce, ShardId};
use std::path::Path;
use std::time::Duration;

pub(crate) struct ChainAccess {
    view_client: Addr<ViewClientActor>,
    client: Addr<ClientActor>,
}

impl ChainAccess {
    pub(crate) fn new<P: AsRef<Path>>(home: P) -> anyhow::Result<Self> {
        let config =
            nearcore::config::load_config(home.as_ref(), GenesisValidationMode::UnsafeFast)
                .with_context(|| format!("Error loading config from {:?}", home.as_ref()))?;

        let node = nearcore::start_with_config(home.as_ref(), config.clone())
            .context("failed to start NEAR node")?;
        Ok(Self { view_client: node.view_client, client: node.client })
    }
}

#[async_trait(?Send)]
impl crate::ChainAccess for ChainAccess {
    // wait until HEAD moves. We don't really need it to be fully synced.
    async fn init(&self) -> anyhow::Result<()> {
        let mut first_height = None;
        loop {
            let head = self.head_height().await?;
            match first_height {
                Some(h) => {
                    if h != head {
                        return Ok(());
                    }
                }
                None => {
                    first_height = Some(head);
                }
            }

            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    async fn head_height(&self) -> anyhow::Result<BlockHeight> {
        self.client
            .send(
                near_client::Status { is_health_check: false, detailed: false }.with_span_context(),
            )
            .await
            .unwrap()
            .map(|s| s.sync_info.latest_block_height)
            .map_err(Into::into)
    }

    async fn get_txs(
        &self,
        height: BlockHeight,
        shards: &[ShardId],
    ) -> Result<Vec<ChunkTxs>, ChainError> {
        let mut chunks = Vec::new();
        for shard_id in shards.iter() {
            let chunk = match self
                .view_client
                .send(GetChunk::Height(height, *shard_id).with_span_context())
                .await
                .context("unexpected actix error")?
            {
                Ok(c) => c,
                Err(e) => match e {
                    GetChunkError::UnknownChunk { .. } => {
                        tracing::error!(
                            "Can't fetch source chain shard {} chunk at height {}. Are we tracking all shards?",
                            shard_id, height
                        );
                        continue;
                    }
                    _ => return Err(e.into()),
                },
            };
            if chunk.header.height_included == height {
                chunks.push(ChunkTxs {
                    shard_id: *shard_id,
                    transactions: chunk.transactions.clone(),
                })
            }
        }

        Ok(chunks)
    }

    async fn fetch_access_key_nonce(
        &self,
        account_id: &AccountId,
        public_key: &PublicKey,
        block_hash: &CryptoHash,
    ) -> anyhow::Result<Option<Nonce>> {
        crate::fetch_access_key_nonce(&self.view_client, account_id, public_key, Some(block_hash))
            .await
    }

    async fn account_exists(
        &self,
        account_id: &AccountId,
        block_hash: &CryptoHash,
    ) -> anyhow::Result<bool> {
        match self
            .view_client
            .send(
                Query::new(
                    BlockReference::BlockId(BlockId::Hash(block_hash.clone())),
                    QueryRequest::ViewAccount { account_id: account_id.clone() },
                )
                .with_span_context(),
            )
            .await?
        {
            Ok(res) => match res.kind {
                QueryResponseKind::ViewAccount(_) => Ok(true),
                other => {
                    panic!("Received unexpected QueryResponse after Querying Account: {:?}", other);
                }
            },
            Err(e) => match &e {
                QueryError::UnknownAccount { .. } => Ok(false),
                _ => Err(e.into()),
            },
        }
    }

    async fn fetch_tx_outcome(
        &self,
        transaction_hash: CryptoHash,
        signer_id: &AccountId,
        receiver_id: &AccountId,
    ) -> anyhow::Result<TxOutcome> {
        let receipt_id = match self
            .view_client
            .send(
                GetExecutionOutcome {
                    id: TransactionOrReceiptId::Transaction {
                        transaction_hash,
                        sender_id: signer_id.clone(),
                    },
                }
                .with_span_context(),
            )
            .await
            .unwrap()
        {
            Ok(GetExecutionOutcomeResponse { outcome_proof, .. }) => {
                match outcome_proof.outcome.status {
                    ExecutionStatusView::SuccessReceiptId(id) => id,
                    ExecutionStatusView::SuccessValue(_) => unreachable!(),
                    ExecutionStatusView::Failure(_) | ExecutionStatusView::Unknown => {
                        return Ok(TxOutcome::Failure)
                    }
                }
            }
            Err(
                GetExecutionOutcomeError::NotConfirmed { .. }
                | GetExecutionOutcomeError::UnknownBlock { .. },
            ) => return Ok(TxOutcome::Pending),
            Err(e) => {
                return Err(e).with_context(|| {
                    format!("failed fetching outcome for tx {}", transaction_hash)
                })
            }
        };
        match self
            .view_client
            .send(
                GetExecutionOutcome {
                    id: TransactionOrReceiptId::Receipt {
                        receipt_id,
                        receiver_id: receiver_id.clone(),
                    },
                }
                .with_span_context(),
            )
            .await
            .unwrap()
        {
            Ok(GetExecutionOutcomeResponse { outcome_proof, .. }) => {
                match outcome_proof.outcome.status {
                    ExecutionStatusView::SuccessReceiptId(_)
                    | ExecutionStatusView::SuccessValue(_) => {
                        // the view client code actually modifies the outcome's block_hash field to be the
                        // next block with a new chunk in the relevant shard, so go backwards one block,
                        // since that's what we'll want to give in the query for AccessKeys
                        let header =
                            self.get_block_header(BlockId::Hash(outcome_proof.block_hash)).await?;
                        Ok(TxOutcome::Success(header.prev_hash))
                    }
                    ExecutionStatusView::Failure(_) | ExecutionStatusView::Unknown => {
                        Ok(TxOutcome::Failure)
                    }
                }
            }
            Err(
                GetExecutionOutcomeError::NotConfirmed { .. }
                | GetExecutionOutcomeError::UnknownBlock { .. }
                | GetExecutionOutcomeError::UnknownTransactionOrReceipt { .. },
            ) => Ok(TxOutcome::Pending),
            Err(e) => Err(e)
                .with_context(|| format!("failed fetching outcome for receipt {}", &receipt_id)),
        }
    }

    async fn get_block_header(&self, id: BlockId) -> Result<BlockHeaderView, ChainError> {
        self.view_client
            .send(GetBlock(BlockReference::BlockId(id)).with_span_context())
            .await
            .unwrap()
            .map(|b| b.header)
            .map_err(Into::into)
    }
}
