use std::fs::File;
use crate::kv::KVPair;
use std::io::{BufReader, BufRead, Write};
use serde_json;
use std::convert::TryFrom;

#[macro_use]
use thiserror::Error;
use crate::kv;


pub struct Wal {
    file: File
}

type Result<T> = std::result::Result<T, WalError>;


#[derive(Error, Debug)]
pub enum WalError {
    #[error(transparent)]
    WriteError(#[from] kv::KvError)
}

impl Wal {
    pub fn new(f: File) -> Self {
        return Wal {
            file: f
        };
    }
    pub fn write(&mut self, kv: KVPair) -> Result<()> {
        kv.persist_to_file(&mut self.file)?;
        Ok(())
    }

    pub fn readall(&self) -> impl Iterator<Item=std::result::Result<KVPair, kv::KvError>> + '_ {
        let reader = BufReader::new(&self.file);
        return reader.lines().map(|string| {
            return KVPair::try_from(
                string.expect("the segment file should not be tampered with"),
            );
        });
    }
}