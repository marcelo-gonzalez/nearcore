use near_chain::types::RuntimeAdapter;
use near_crypto::PublicKey;
use near_primitives::account::{AccessKey, Account};
use near_primitives::borsh;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::Receipt;
use near_primitives::shard_layout::{ShardLayout, ShardUId};
use near_primitives::trie_key::TrieKey;
use near_primitives::types::{AccountId, BlockHeight, ShardIndex, StateRoot, StoreKey, StoreValue};
use near_store::adapter::StoreUpdateAdapter;
use near_store::{flat::FlatStateChanges, DBCol, ShardTries};
use nearcore::NightshadeRuntime;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

pub(crate) struct ShardUpdateState {
    pub(crate) state_root: StateRoot,
    pub(crate) fake_block_height: BlockHeight,
}

struct ShardUpdates {
    update_state: Arc<Mutex<ShardUpdateState>>,
    delayed_receipt_index: Arc<AtomicU64>,
    updates: Vec<(Vec<u8>, Option<Vec<u8>>)>,
}

/// Object that updates the existing state. Combines all changes, commits them
/// and returns new state roots.
pub(crate) struct SingleShardStorageMutator {
    updates: Vec<ShardUpdates>,
    shard_tries: ShardTries,
}

impl SingleShardStorageMutator {
    pub(crate) fn new(
        runtime: &NightshadeRuntime,
        update_state: Vec<Arc<Mutex<ShardUpdateState>>>,
        delayed_receipt_indices: Vec<Arc<AtomicU64>>,
    ) -> anyhow::Result<Self> {
        assert!(update_state.len() == delayed_receipt_indices.len());
        let updates = update_state
            .into_iter()
            .zip(delayed_receipt_indices)
            .map(|(update_state, delayed_receipt_index)| ShardUpdates {
                update_state,
                delayed_receipt_index,
                updates: Vec::new(),
            })
            .collect();
        Ok(Self { updates, shard_tries: runtime.get_tries() })
    }

    fn set(&mut self, shard_idx: ShardIndex, key: TrieKey, value: Vec<u8>) -> anyhow::Result<()> {
        self.updates[shard_idx].updates.push((key.to_vec(), Some(value)));
        Ok(())
    }

    fn remove(&mut self, shard_idx: ShardIndex, key: TrieKey) -> anyhow::Result<()> {
        self.updates[shard_idx].updates.push((key.to_vec(), None));
        Ok(())
    }

    pub(crate) fn set_account(
        &mut self,
        shard_idx: ShardIndex,
        account_id: AccountId,
        value: Account,
    ) -> anyhow::Result<()> {
        self.set(shard_idx, TrieKey::Account { account_id }, borsh::to_vec(&value)?)
    }

    pub(crate) fn delete_account(
        &mut self,
        shard_idx: ShardIndex,
        account_id: AccountId,
    ) -> anyhow::Result<()> {
        self.remove(shard_idx, TrieKey::Account { account_id })
    }

    pub(crate) fn set_access_key(
        &mut self,
        shard_idx: ShardIndex,
        account_id: AccountId,
        public_key: PublicKey,
        access_key: AccessKey,
    ) -> anyhow::Result<()> {
        self.set(
            shard_idx,
            TrieKey::AccessKey { account_id, public_key },
            borsh::to_vec(&access_key)?,
        )
    }

    pub(crate) fn delete_access_key(
        &mut self,
        shard_idx: ShardIndex,
        account_id: AccountId,
        public_key: PublicKey,
    ) -> anyhow::Result<()> {
        self.remove(shard_idx, TrieKey::AccessKey { account_id, public_key })
    }

    pub(crate) fn set_data(
        &mut self,
        shard_idx: ShardIndex,
        account_id: AccountId,
        data_key: &StoreKey,
        value: StoreValue,
    ) -> anyhow::Result<()> {
        self.set(
            shard_idx,
            TrieKey::ContractData { account_id, key: data_key.to_vec() },
            borsh::to_vec(&value)?,
        )
    }

    pub(crate) fn delete_data(
        &mut self,
        shard_idx: ShardIndex,
        account_id: AccountId,
        data_key: &StoreKey,
    ) -> anyhow::Result<()> {
        self.remove(shard_idx, TrieKey::ContractData { account_id, key: data_key.to_vec() })
    }

    pub(crate) fn set_code(
        &mut self,
        shard_idx: ShardIndex,
        account_id: AccountId,
        value: Vec<u8>,
    ) -> anyhow::Result<()> {
        self.set(shard_idx, TrieKey::ContractCode { account_id }, value)
    }

    pub(crate) fn delete_code(
        &mut self,
        shard_idx: ShardIndex,
        account_id: AccountId,
    ) -> anyhow::Result<()> {
        self.remove(shard_idx, TrieKey::ContractCode { account_id })
    }

    pub(crate) fn set_postponed_receipt(
        &mut self,
        shard_idx: ShardIndex,
        receipt: &Receipt,
    ) -> anyhow::Result<()> {
        self.set(
            shard_idx,
            TrieKey::PostponedReceipt {
                receiver_id: receipt.receiver_id().clone(),
                receipt_id: *receipt.receipt_id(),
            },
            borsh::to_vec(&receipt)?,
        )
    }

    pub(crate) fn delete_postponed_receipt(
        &mut self,
        shard_idx: ShardIndex,
        receipt: &Receipt,
    ) -> anyhow::Result<()> {
        self.remove(
            shard_idx,
            TrieKey::PostponedReceipt {
                receiver_id: receipt.receiver_id().clone(),
                receipt_id: *receipt.receipt_id(),
            },
        )
    }

    pub(crate) fn set_received_data(
        &mut self,
        shard_idx: ShardIndex,
        account_id: AccountId,
        data_id: CryptoHash,
        data: &Option<Vec<u8>>,
    ) -> anyhow::Result<()> {
        self.set(
            shard_idx,
            TrieKey::ReceivedData { receiver_id: account_id, data_id },
            borsh::to_vec(data)?,
        )
    }

    pub(crate) fn delete_received_data(
        &mut self,
        shard_idx: ShardIndex,
        account_id: AccountId,
        data_id: CryptoHash,
    ) -> anyhow::Result<()> {
        self.remove(shard_idx, TrieKey::ReceivedData { receiver_id: account_id, data_id })
    }

    pub(crate) fn set_delayed_receipt(
        &mut self,
        shard_idx: ShardIndex,
        receipt: &Receipt,
    ) -> anyhow::Result<()> {
        let index = self.updates[shard_idx].delayed_receipt_index.fetch_add(1, Ordering::Relaxed);
        self.set(shard_idx, TrieKey::DelayedReceipt { index }, borsh::to_vec(receipt)?)
    }

    pub(crate) fn delete_delayed_receipt(
        &mut self,
        shard_idx: ShardIndex,
        index: u64,
    ) -> anyhow::Result<()> {
        self.remove(shard_idx, TrieKey::DelayedReceipt { index })
    }

    pub(crate) fn should_commit(&self, batch_size: u64) -> bool {
        self.updates.len() >= batch_size as usize
    }

    /// The fake block height is used to allow memtries to garbage collect.
    /// Otherwise it would take significantly more memory holding old nodes.
    fn commit_shard(
        shard_uid: ShardUId,
        shard_tries: &ShardTries,
        updates: ShardUpdates,
    ) -> anyhow::Result<()> {
        let mut update_state = updates.update_state.lock().unwrap();
        let num_updates = updates.updates.len();
        tracing::info!(?shard_uid, num_updates, "commit");
        let flat_state_changes = FlatStateChanges::from_raw_key_value(&updates.updates);
        let mut update = shard_tries.store_update();
        flat_state_changes.apply_to_flat_state(&mut update.flat_store_update(), shard_uid);

        let trie_changes = shard_tries
            .get_trie_for_shard(shard_uid, update_state.state_root)
            .update(updates.updates)?;
        tracing::info!(
            ?shard_uid,
            num_trie_node_insertions = trie_changes.insertions().len(),
            num_trie_node_deletions = trie_changes.deletions().len()
        );
        let state_root = shard_tries.apply_all(&trie_changes, shard_uid, &mut update);
        shard_tries.apply_memtrie_changes(&trie_changes, shard_uid, update_state.fake_block_height);
        // We may not have loaded memtries (some commands don't need to), so check.
        if let Some(memtries) = shard_tries.get_memtries(shard_uid) {
            memtries.write().unwrap().delete_until_height(update_state.fake_block_height - 1);
        }
        update_state.fake_block_height += 1;
        update_state.state_root = state_root;

        tracing::info!(?shard_uid, num_updates, "committing");
        update.store_update().set_ser(
            DBCol::Misc,
            format!("FORK_TOOL_SHARD_ID:{}", shard_uid.shard_id).as_bytes(),
            &state_root,
        )?;

        update.commit()?;
        tracing::info!(?shard_uid, ?state_root, "Commit is done");
        Ok(())
    }

    pub(crate) fn commit(self, shard_layout: &ShardLayout) -> anyhow::Result<()> {
        let Self { updates, shard_tries } = self;

        for (shard_index, update) in updates.into_iter().enumerate() {
            let shard_id = shard_layout.get_shard_id(shard_index).unwrap();
            let shard_uid = ShardUId::from_shard_id_and_layout(shard_id, &shard_layout);
            Self::commit_shard(shard_uid, &shard_tries, update)?;
        }
        Ok(())
    }
}
