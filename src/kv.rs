use serde::{Deserialize, Serialize};
#[macro_use]
use thiserror::Error;
use std::fs::File;
use serde::export::TryFrom;


type Result<T> = std::result::Result<T, KvError>;


#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Clone)]
pub struct KVPair {
    pub key: String,
    pub value: String,
}


impl KVPair {
    pub fn persist_to_file(self, file: &mut File) -> Result<()> {
        serde_json::to_writer(file, &self)?;
        Ok(())
    }
}


impl TryFrom<String> for KVPair {
    type Error = KvError;

    fn try_from(value: String) -> std::result::Result<Self, Self::Error> {
        let kv_pair = serde_json::from_str::<KVPair>(&value)?;
        Ok(kv_pair)
    }
}


#[derive(Error, Debug)]
pub enum KvError {
    #[error(transparent)]
    WriteError(#[from] serde_json::error::Error),

}
