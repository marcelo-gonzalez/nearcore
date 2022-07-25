use anyhow::Context;
use near_account_id::AccountId;
use near_chain_configs::{Genesis, GenesisValidationMode};
use near_crypto::PublicKey;
use near_primitives::hash::CryptoHash;
use near_primitives::state_record::StateRecord;
use near_primitives_core::account::{AccessKey, Account};
use near_primitives_core::types::{NumSeats, Balance};
use serde::ser::{SerializeSeq, Serializer};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::Path;
use std::str::FromStr;
use near_primitives::types::AccountInfo;

struct AccountData {
    account_id: &'static str,
    amount: Balance,
}

// TODO: rest of them
const WANTED_ACCOUNTS: &'static [AccountData] = &[
    AccountData { account_id: "test.near", amount: 10_000_000 * nearcore::config::NEAR_BASE },
    AccountData { account_id: "near", amount: 10_000_000 * nearcore::config::NEAR_BASE },
    AccountData { account_id: "skyward.near", amount: 10_000_000 * nearcore::config::NEAR_BASE },
    AccountData { account_id: "token1.near", amount: 1_000_000 * nearcore::config::NEAR_BASE },
];

struct KeyData {
    account_id: &'static str,
    public_key: &'static str,
}

const WANTED_KEYS: &'static [KeyData] = &[
    KeyData {
        account_id: "test.near",
        public_key: "ed25519:76NVkDErhbP1LGrSAf5Db6BsFJ6LBw6YVA4BsfTBohmN",
    },
    KeyData {
        account_id: "near",
        public_key: "ed25519:76NVkDErhbP1LGrSAf5Db6BsFJ6LBw6YVA4BsfTBohmN",
    },
    KeyData {
        account_id: "skyward.near",
        public_key: "ed25519:76NVkDErhbP1LGrSAf5Db6BsFJ6LBw6YVA4BsfTBohmN",
    },
    KeyData {
        account_id: "token1.near",
        public_key: "ed25519:76NVkDErhbP1LGrSAf5Db6BsFJ6LBw6YVA4BsfTBohmN",
    },
];

fn mocknet_validators(validator_node_names: &[&str]) -> anyhow::Result<Vec<(AccountId, Account, PublicKey)>> {
    let mut validators = Vec::new();
    for validator in validator_node_names.iter() {
        // TODO: split(-)
        let name = validator.to_string() + "-load-test.near";
        let account_id =
            AccountId::from_str(&name).with_context(|| format!("Parsing account ID {}", name))?;
        let account =
            Account::new(100 * nearcore::config::NEAR_BASE, 64_000, CryptoHash::default(), 0);
        validators.push((account_id, account, PublicKey::from_str("ed25519:76NVkDErhbP1LGrSAf5Db6BsFJ6LBw6YVA4BsfTBohmN").unwrap()));
    }
    Ok(validators)
}

fn wanted_records(
    validators: &[(AccountId, Account, PublicKey)],
) -> (HashMap<AccountId, Account>, HashMap<AccountId, Vec<(PublicKey, AccessKey)>>) {
    let mut accounts_wanted = HashMap::new();
    let mut keys_wanted = HashMap::<AccountId, Vec<(PublicKey, AccessKey)>>::new();

    for a in WANTED_ACCOUNTS.iter() {
        let account_id = AccountId::from_str(a.account_id).unwrap();
        // TODO: storage_usage
        let account = Account::new(a.amount, 0, CryptoHash::default(), 0);
        accounts_wanted.insert(account_id, account);
    }
    for k in WANTED_KEYS.iter() {
        let account_id = AccountId::from_str(k.account_id).unwrap();
        let public_key = PublicKey::from_str(k.public_key).unwrap();
        keys_wanted
            .entry(account_id.clone())
            .or_default()
            .push((public_key, AccessKey::full_access()));
    }
    for (account_id, account, _public_key) in validators.iter() {
        if accounts_wanted.insert(account_id.clone(), account.clone()).is_some() {
            tracing::warn!("Account {:?} added to the wanted accounts set twice", account_id);
        }
        let public_key =
            PublicKey::from_str("ed25519:76NVkDErhbP1LGrSAf5Db6BsFJ6LBw6YVA4BsfTBohmN").unwrap();
        keys_wanted
            .entry(account_id.clone())
            .or_default()
            .push((public_key, AccessKey::full_access()));
    }
    (accounts_wanted, keys_wanted)
}

fn set_genesis_validators(genesis: &mut Genesis, validators: &[(AccountId, Account, PublicKey)]) {
    genesis.config.validators.clear();
    for (account_id, account, public_key) in validators.iter() {
        genesis.config.validators.push(AccountInfo {
            account_id: account_id.clone(),
            public_key: public_key.clone(),
            amount: account.locked(),
        })
    }
}

// TODO:
// rpc_node_names
// epoch_length=None,
// node_pks=None,
// increasing_stakes
pub fn create_genesis<P: AsRef<Path>>(
    genesis_file_in: P,
    genesis_file_out: P,
    records_file_in: P,
    records_file_out: P,
    validator_node_names: &[&str],
    append: bool,
    chain_id: Option<&str>,
) -> anyhow::Result<()> {
    if !append {
        panic!("dont know what do");
    }
    let mut genesis = Genesis::from_file(genesis_file_in, GenesisValidationMode::UnsafeFast);
    if let Some(chain_id) = chain_id {
        genesis.config.chain_id = chain_id.to_string();
    }
    let reader = BufReader::new(File::open(records_file_in)?);
    let records_out = BufWriter::new(File::create(records_file_out)?);
    let mut records_ser = serde_json::Serializer::new(records_out);
    let mut records_seq = records_ser.serialize_seq(None).unwrap();

    let validators = mocknet_validators(validator_node_names)?;
    let (mut accounts_wanted, mut keys_wanted) = wanted_records(&validators);

    let mut total_supply = 0;
    near_chain_configs::stream_records_from_file(reader, |mut r| {
        match &mut r {
            StateRecord::AccessKey { account_id, public_key, access_key } => {
                if let Some(keys) = keys_wanted.get_mut(account_id) {
                    let mut to_remove = Vec::new();
                    for (i, (key, _access)) in keys.iter().enumerate() {
                        if public_key == key {
                            to_remove.push(i);
                        }
                    }
                    for idx in to_remove.iter().rev() {
                        let (_pub_key, access) = keys.swap_remove(*idx);
                        *access_key = access;
                    }
                    if keys.len() == 0 {
                        keys_wanted.remove(account_id);
                    }
                }
            }
            StateRecord::Account { account_id, account } => {
                if let Some(acc) = accounts_wanted.get(account_id) {
                    *account = acc.clone();
                    accounts_wanted.remove(account_id);
                } else {
                    if account.locked() != 0 {
                        account.set_amount(account.amount() + account.locked());
                        account.set_locked(0);
                    }
                }
                total_supply += account.amount() + account.locked();
            }
            _ => {}
        };
        records_seq.serialize_element(&r).unwrap();
    })?;
    for (account_id, account) in accounts_wanted {
        total_supply += account.amount() + account.locked();
        records_seq.serialize_element(&StateRecord::Account { account_id, account }).unwrap();
    }
    for (account_id, keys) in keys_wanted {
        for (public_key, access_key) in keys {
            records_seq
                .serialize_element(&StateRecord::AccessKey {
                    account_id: account_id.clone(),
                    public_key,
                    access_key,
                })
                .unwrap();
        }
    }
    set_genesis_validators(&mut genesis, &validators);
    genesis.config.total_supply = total_supply;
    // TODO: option
    genesis.config.num_block_producer_seats = validators.len() as NumSeats;
    genesis.config.protocol_reward_rate = num_rational::Rational32::new(1, 10);
    genesis.config.block_producer_kickout_threshold = 10;
    genesis.to_file(genesis_file_out);
    records_seq.end()?;
    Ok(())
}
