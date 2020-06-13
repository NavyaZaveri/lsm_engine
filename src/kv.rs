use serde::{Deserialize, Serialize};
#[macro_use]
use thiserror::Error;
use std::fs::File;
use serde::export::TryFrom;
use std::cell::RefCell;
use std::io::{SeekFrom, Seek, BufReader, BufRead, Write};


pub(crate) type Result<T> = std::result::Result<T, KvError>;


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
    JsonError(#[from] serde_json::error::Error),

    #[error(transparent)]
    FileIOError(#[from] std::io::Error),

}

pub trait KVFileIterator {
    fn file(&self) -> &mut File;
    fn seek(&self, pos: u64) -> Result<()> {
        RefCell::new(self.file())
            .borrow_mut()
            .seek(SeekFrom::Start(pos))?;
        Ok(())
    }

    fn tell(&self) -> Result<u64> {
        let offset = RefCell::new(self.file())
            .borrow_mut()
            .seek(SeekFrom::Current(0))?;
        return Ok(offset);
    }
}

pub trait KVFileReader: KVFileIterator {
    fn read(&self) -> Box<dyn Iterator<Item=Result<KVPair>> + '_> {
        let reader = BufReader::new(self.file());
        return Box::new(reader.lines().map(|string| {
            KVPair::try_from(string.expect("the segment file should not be tampered with"))
        }));
    }

    fn read_from_start(&self) -> Result<Box<dyn Iterator<Item=Result<KVPair>> + '_>> {
        self.seek(0)?;
        let reader = BufReader::new(self.file());
        return Ok(Box::new(reader.lines().map(|string| {
            KVPair::try_from(string?)
        })));
    }
}

pub trait KVFileWriter: KVFileIterator {
    fn persist(&mut self, kv: KVPair) -> Result<u64> {
        //check if the previously written key is bigger than the current key
        let current_offset = self.tell()?;
        serde_json::to_writer(self.file(), &kv)?;
        self.file().write(b"\n")?;
        return Ok(current_offset);
    }
}
