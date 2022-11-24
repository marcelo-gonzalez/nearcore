//! Contract that adds keys

use near_sdk::borsh::{self, BorshDeserialize, BorshSerialize};
use near_sdk::{env, near_bindgen, Promise, PublicKey};
use std::str::FromStr;

#[near_bindgen]
#[derive(Default, BorshDeserialize, BorshSerialize)]
pub struct KeyAdder {}

#[near_bindgen]
impl KeyAdder {
    pub fn add_key(&mut self, public_key: String) -> Promise {
        let signer_id = env::signer_account_id();
        if signer_id == env::current_account_id() {
            let public_key = PublicKey::from_str(&public_key).unwrap();
            Promise::new(signer_id).add_full_access_key(public_key)
        } else {
            Self::ext(signer_id).add_key(public_key)
        }
    }
}
