use actix::Addr;
use anyhow::{anyhow, Context};
use borsh::{BorshDeserialize, BorshSerialize};
use near_chain_configs::GenesisValidationMode;
use near_client::{ClientActor, ViewClientActor};
use near_client_primitives::types::GetChunk;
use near_crypto::{KeyType, PublicKey, SecretKey};
use near_indexer::{Indexer, StreamerMessage};
use near_network::types::{NetworkClientMessages, NetworkClientResponses};
use near_primitives::hash::CryptoHash;
use near_primitives::state_record::StateRecord;
use near_primitives::transaction::{Action, AddKeyAction, SignedTransaction, Transaction};
use near_primitives::types::BlockHeight;
use near_primitives::views::ActionView;
use near_primitives_core::types::ShardId;
use nearcore::config::NearConfig;
use rocksdb::DB;
use serde::de::{SeqAccess, Visitor};
use serde::ser::{SerializeSeq, Serializer};
use serde::{Deserialize, Deserializer, Serialize};
use std::fmt;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read};
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use strum::IntoEnumIterator;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

#[derive(strum::EnumIter)]
enum DBCol {
    KeyMapping,
    Misc,
}

impl DBCol {
    fn name(&self) -> &'static str {
        match self {
            Self::KeyMapping => "key mapping",
            Self::Misc => "miscellaneous",
        }
    }
}

pub struct TxMirror {
    source_stream: Option<mpsc::Receiver<StreamerMessage>>,
    target_stream: mpsc::Receiver<StreamerMessage>,
    source_view_client: Addr<ViewClientActor>,
    target_view_client: Addr<ViewClientActor>,
    target_client: Addr<ClientActor>,
    db: Arc<DB>,
    target_genesis_height: BlockHeight,
    tracked_shards: Vec<ShardId>,
}

#[derive(Deserialize, Serialize, Debug)]
struct KeyMapping {
    original: PublicKey,
    replacement: SecretKey,
}

pub fn generate_new_keys<P: AsRef<Path>>(
    records_file_in: &P,
    records_file_out: &P,
    mapping_file: &P,
) -> anyhow::Result<()> {
    let reader = BufReader::new(File::open(records_file_in)?);
    let records_out = BufWriter::new(File::create(records_file_out)?);
    let mut records_ser = serde_json::Serializer::new(records_out);
    let mut records_seq = records_ser.serialize_seq(None).unwrap();
    let mapping = BufWriter::new(File::create(mapping_file)?);
    let mut mapping_ser = serde_json::Serializer::new(mapping);
    let mut mapping_seq = mapping_ser.serialize_seq(None).unwrap();

    near_chain_configs::stream_records_from_file(reader, |r| {
        match r {
            StateRecord::AccessKey { account_id, public_key, access_key } => {
                let mapping = KeyMapping {
                    original: public_key,
                    replacement: SecretKey::from_random(KeyType::ED25519),
                };
                let new_record = StateRecord::AccessKey {
                    account_id,
                    public_key: mapping.replacement.public_key(),
                    access_key,
                };

                mapping_seq.serialize_element(&mapping).unwrap();
                records_seq.serialize_element(&new_record).unwrap(); // TODO clean unwraps
            }
            _ => {
                records_seq.serialize_element(&r).unwrap() // TODO clean unwraps
            }
        };
    })?;
    records_seq.end()?;
    mapping_seq.end()?;
    Ok(())
}

struct RecordsProcessor<F> {
    sink: F,
}

impl<'de, F: FnMut(KeyMapping)> Visitor<'de> for RecordsProcessor<&'_ mut F> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("an array of KeyMappings")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        while let Some(record) = seq.next_element::<KeyMapping>()? {
            (self.sink)(record)
        }
        Ok(())
    }
}

fn for_each_key_mapping<R: Read, F: FnMut(KeyMapping)>(
    reader: R,
    mut callback: F,
) -> anyhow::Result<()> {
    let mut deserializer = serde_json::Deserializer::from_reader(reader);
    let records_processor = RecordsProcessor { sink: &mut callback };
    Ok(deserializer.deserialize_any(records_processor)?)
}

pub fn init_db<P: AsRef<Path>>(home: &P, mapping_file: &P) -> anyhow::Result<()> {
    let mapping = BufReader::new(File::open(mapping_file)?);

    let config = nearcore::config::load_config(home.as_ref(), GenesisValidationMode::UnsafeFast)
        .context("Error loading config")?;
    let db = open_db(home, &config, true)?;

    for_each_key_mapping(mapping, |m| {
        db.put_cf(
            db.cf_handle(DBCol::KeyMapping.name()).unwrap(),
            &m.original.try_to_vec().unwrap(),
            serde_json::to_string(&m.replacement).unwrap(),
        )
        .unwrap() // TODO clean unwrap
    })?;
    Ok(())
}

fn open_db<P: AsRef<Path>>(home: &P, config: &NearConfig, create: bool) -> anyhow::Result<DB> {
    let db_path =
        near_store::Store::opener(home.as_ref(), &config.config.store).get_path().join("mirror");
    let mut options = rocksdb::Options::default();
    options.create_missing_column_families(create);
    options.create_if_missing(create);
    let cf_descriptors = DBCol::iter()
        .map(|col| rocksdb::ColumnFamilyDescriptor::new(col.name(), options.clone()))
        .collect::<Vec<_>>();
    Ok(DB::open_cf_descriptors(&options, db_path, cf_descriptors)?)
}

struct TxPreparer {
    db: Arc<DB>,
    source_stream: mpsc::Receiver<StreamerMessage>,
    source_view_client: Addr<ViewClientActor>,
    sender: mpsc::Sender<Vec<SignedTransaction>>,
    height_done: BlockHeight,
    ref_block: Arc<RwLock<CryptoHash>>, // TODO atomiccell
    tracked_shards: Vec<ShardId>,
}

impl TxPreparer {
    fn new(
        db: Arc<DB>,
        source_stream: mpsc::Receiver<StreamerMessage>,
        source_view_client: Addr<ViewClientActor>,
        height_done: BlockHeight,
        ref_block: Arc<RwLock<CryptoHash>>,
        tracked_shards: Vec<ShardId>,
    ) -> (Self, mpsc::Receiver<Vec<SignedTransaction>>) {
        let (tx, rx) = mpsc::channel(100);
        (
            Self {
                db,
                source_stream,
                source_view_client,
                sender: tx,
                height_done,
                ref_block,
                tracked_shards,
            },
            rx,
        )
    }

    fn map_action(&self, a: ActionView) -> anyhow::Result<Action> {
        // this try_from() won't fail since the ActionView was constructed from the Action
        let action = Action::try_from(a).unwrap();

        Ok(match &action {
            Action::AddKey(add_key) => {
                let replacement = SecretKey::from_random(KeyType::ED25519);
                self.db.put_cf(
                    self.db.cf_handle(DBCol::KeyMapping.name()).unwrap(),
                    &add_key.public_key.try_to_vec().unwrap(),
                    serde_json::to_string(&replacement).unwrap(),
                )?;

                Action::AddKey(AddKeyAction {
                    public_key: replacement.public_key(),
                    access_key: add_key.access_key.clone(),
                })
            }
            _ => action,
        })
    }

    async fn prepare_txs(&mut self, height: BlockHeight) -> anyhow::Result<Vec<SignedTransaction>> {
        let ref_hash = *self.ref_block.read().unwrap();
        let mut txs = Vec::new();
        for shard_id in self.tracked_shards.iter() {
            // TODO shard
            let chunk =
                match self.source_view_client.send(GetChunk::Height(height, *shard_id)).await {
                    Ok(c) => match c {
                        Ok(c) => c,
                        Err(_) => continue,
                    },
                    Err(e) => return Err(e.into()),
                };
            if chunk.header.height_included != height {
                continue;
            }

            for t in chunk.transactions {
                let mapped_key = match self.db.get_cf(
                    self.db.cf_handle(DBCol::KeyMapping.name()).unwrap(),
                    &t.public_key.try_to_vec().unwrap(),
                )? {
                    Some(k) => serde_json::from_slice::<SecretKey>(&k)
                        .context("Key deserialization failure")?,
                    None => {
                        tracing::warn!(
                            "Don't have a mapping for key {} appearing in source chain tx {}",
                            &t.public_key,
                            t.hash
                        );
                        continue;
                    }
                };
                let mut tx = Transaction::new(
                    t.signer_id.clone(),
                    mapped_key.public_key(),
                    t.receiver_id.clone(),
                    t.nonce,
                    ref_hash.clone(),
                );
                for action in t.actions {
                    tx.actions.push(self.map_action(action)?);
                }
                let tx =
                    SignedTransaction::new(mapped_key.sign(&tx.get_hash_and_size().0.as_ref()), tx);
                txs.push(tx);
            }
        }
        tracing::debug!(target: "mirror", "prepared {} transacations using reference block hash {}", txs.len(), &ref_hash);
        Ok(txs)
    }

    fn set_height_done(&mut self, height: BlockHeight) -> anyhow::Result<()> {
        self.height_done = height;
        self.db.put_cf(
            self.db.cf_handle(DBCol::Misc.name()).unwrap(),
            "height_done",
            height.try_to_vec().unwrap(),
        )?;
        Ok(())
    }

    async fn run(mut self) {
        loop {
            let msg = match self.source_stream.recv().await {
                Some(msg) => msg,
                None => {
                    tracing::error!("source Indexer streamer channel closed");
                    return;
                }
            };
            tracing::debug!(target: "mirror", "source HEAD @ {}", msg.block.header.height);

            let start = self.height_done;
            for height in start + 1..=msg.block.header.height {
                tracing::debug!(target: "mirror", "do the thing @ {}", height);
                match self.prepare_txs(height).await {
                    Ok(txs) => {
                        if txs.len() > 0 {
                            if let Err(e) = self.sender.send(txs).await {
                                tracing::error!("mirror transaction channel closed: {:?}", e);
                                return;
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!("Error preparing transcations: {:?}", e);
                        return;
                    }
                }
                if let Err(e) = self.set_height_done(height) {
                    tracing::error!("Error setting height done: {:?}", e);
                    return;
                }
            }
        }
    }
}

impl TxMirror {
    pub fn new<S: AsRef<Path>, T: AsRef<Path>>(source_home: &S, target_home: &T) -> Self {
        let config =
            nearcore::config::load_config(target_home.as_ref(), GenesisValidationMode::UnsafeFast)
                .context("Error loading config")
                .unwrap();
        let db = Arc::new(open_db(target_home, &config, false).unwrap());
        let source_indexer = Indexer::new(near_indexer::IndexerConfig {
            home_dir: source_home.as_ref().to_path_buf(),
            sync_mode: near_indexer::SyncModeEnum::LatestSynced,
            await_for_node_synced: near_indexer::AwaitForNodeSyncedEnum::WaitForFullSync,
        })
        .unwrap();

        let target_indexer = Indexer::new(near_indexer::IndexerConfig {
            home_dir: target_home.as_ref().to_path_buf(),
            sync_mode: near_indexer::SyncModeEnum::LatestSynced,
            await_for_node_synced: near_indexer::AwaitForNodeSyncedEnum::WaitForFullSync,
        })
        .unwrap();
        let (source_view_client, _) = source_indexer.client_actors();
        let (target_view_client, target_client) = target_indexer.client_actors();
        let source_stream = Some(source_indexer.streamer());
        let target_stream = target_indexer.streamer();

        Self {
            source_view_client,
            target_view_client,
            target_client,
            source_stream,
            target_stream,
            db,
            target_genesis_height: config.genesis.config.genesis_height,
            tracked_shards: config.config.tracked_shards.clone(),
        }
    }

    async fn get_start_height(&mut self) -> anyhow::Result<BlockHeight> {
        let height =
            self.db.get_cf(self.db.cf_handle(DBCol::Misc.name()).unwrap(), "height_done")?;
        match height {
            Some(h) => Ok(BlockHeight::try_from_slice(&h).unwrap()),
            None => Ok(self.target_genesis_height),
        }
    }

    async fn start(
        &mut self,
    ) -> anyhow::Result<(
        JoinHandle<()>,
        mpsc::Receiver<Vec<SignedTransaction>>,
        Arc<RwLock<CryptoHash>>,
    )> {
        let start_height = self.get_start_height().await?;

        // wait for the target node to be synced
        let target_msg = match self.target_stream.recv().await {
            Some(msg) => msg,
            None => {
                return Err(anyhow!("target Indexer streamer channel closed"));
            }
        };

        let ref_block = Arc::new(RwLock::new(target_msg.block.header.hash));
        let (p, rx) = TxPreparer::new(
            self.db.clone(),
            self.source_stream.take().unwrap(),
            self.source_view_client.clone(),
            start_height,
            ref_block.clone(),
            self.tracked_shards.clone(),
        );
        Ok((actix::spawn(async move { p.run().await }), rx, ref_block))
    }

    async fn sleep_until_next_send(&self, last_sent: Option<Instant>) {
        // TODO: collect stats on how long it's taking our transactions to make it into the chain
        // and adjust this as needed
        match last_sent {
            Some(last_sent) => {
                let since_last = Instant::now() - last_sent;
                // TODO: don't hardcode 1 second
                if since_last < Duration::from_secs(1) {
                    tokio::time::sleep(Duration::from_secs(1) - since_last).await;
                }
            }
            None => {}
        }
    }

    async fn send_transactions(&self, transactions: &[SignedTransaction]) -> anyhow::Result<()> {
        for tx in transactions.iter() {
            let res = match self
                .target_client
                .send(NetworkClientMessages::Transaction {
                    transaction: tx.clone(),
                    is_forwarded: false,
                    check_only: false,
                })
                .await
            {
                Ok(r) => r,
                Err(e) => return Err(e.into()),
            };
            match &res {
                NetworkClientResponses::RequestRouted => {}
                _ => {
                    // TODO: here if we're getting an error because the tx was already included,
                    // we should detect that probably some other instance of this code ran and made progress already.
                    tracing::error!(target: "mirror", "Error sending tx: {:?}", &res);
                }
            }
        }
        tracing::info!(target: "mirror", "sent {} txs", transactions.len());
        Ok(())
    }

    async fn main_loop(
        &mut self,
        rx: &mut mpsc::Receiver<Vec<SignedTransaction>>,
        ref_block: Arc<RwLock<CryptoHash>>,
    ) -> anyhow::Result<()> {
        let mut last_sent = None;
        loop {
            tokio::select! {
                transactions = rx.recv() => {
                    let transactions = match transactions {
                        Some(txs) => txs,
                        None => return Err(anyhow!("TxPreparer channel closed")),
                    };
                    self.sleep_until_next_send(last_sent).await;
                    if transactions.len() > 0 {
                        self.send_transactions(&transactions).await?;
                        last_sent = Some(Instant::now());
                    }
                }
                new_target_block = self.target_stream.recv() => {
                    let msg = match new_target_block {
                        Some(msg) => msg,
                        None => {
                            return Err(anyhow!("target Indexer streamer channel closed"));
                        }
                    };
                    *ref_block.write().unwrap() = msg.block.header.hash;
                }
            };
        }
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        let (_tx_preparer, mut rx, ref_block) = self.start().await?;

        self.main_loop(&mut rx, ref_block).await
    }
}
