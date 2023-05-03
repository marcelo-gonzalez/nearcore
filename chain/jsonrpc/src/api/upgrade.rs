use serde_json::Value;

use near_client_primitives::types::DoProtocolUpgradeError;
use near_jsonrpc_primitives::errors::{RpcError, RpcParseError};

use super::{Params, RpcFrom, RpcRequest};

#[derive(serde::Serialize, serde::Deserialize)]
pub struct DoUpgradeRequest {
    pub upgrade: bool,
}

impl RpcRequest for DoUpgradeRequest {
    fn parse(value: Value) -> Result<Self, RpcParseError> {
        Params::parse(value)
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct DoUpgradeError(String);

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct DoUpgradeResponse(pub ());

impl From<DoUpgradeError> for RpcError {
    fn from(error: DoUpgradeError) -> Self {
        Self::new_internal_error(
            Some(serde_json::to_value(&error.0).unwrap()),
            String::from("error setting upgrade variable"),
        )
    }
}

impl RpcFrom<DoProtocolUpgradeError> for DoUpgradeError {
    fn rpc_from(error: DoProtocolUpgradeError) -> Self {
        Self(error.0.to_string())
    }
}

impl RpcFrom<actix::MailboxError> for DoUpgradeError {
    fn rpc_from(error: actix::MailboxError) -> Self {
        Self(error.to_string())
    }
}
