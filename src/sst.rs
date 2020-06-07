use std::fs::{File, read};

use std::io::{Read, Write, BufReader, BufRead, SeekFrom, Error, Seek};
use serde::{Serialize, Deserialize};
use std::cell::RefCell;
use std::fs::OpenOptions;


pub struct Segment {
    fd: File,
    size: usize,
}


#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct KVPair {
    key: String,
    value: String,
}


impl Segment {
    pub fn new(path: &'static str) -> Segment {
        return Segment {
            fd: OpenOptions::new().read(true).write(true).create(true).open(path).unwrap(),
            size: 0,
        };
    }

    pub fn with_file(f: File) -> Segment {
        return Segment {
            fd: f,
            size: 0,
        };
    }


    pub fn write(&mut self, key: String, value: String) -> Result<u64, Box<dyn std::error::Error>> {
        let kv = KVPair {
            key,
            value,
        };
        let current_offset = self.tell()?;
        serde_json::to_writer(&self.fd, &kv)?;
        self.fd.write(b"\n")?;
        self.size += 1;
        return Ok(current_offset);
    }


    pub fn seek(&self, pos: u64) -> Result<(), std::io::Error> {
        RefCell::new(&self.fd).borrow_mut().seek(SeekFrom::Start(pos))?;
        Ok(())
    }

    pub fn peek(&self) -> Result<Option<KVPair>, Box<dyn std::error::Error>> {
        let current = self.tell()?;
        let maybe_entry = self.read().take(1).last();

        //reset back to current offset
        self.seek(current)?;
        Ok(maybe_entry)
    }


    pub fn search(&self, key: &str) -> Result<Option<String>, std::io::Error> {
        let current_pos = self.tell()?;
        let result = self.
            read_from_start().
            map(|mut iterator| iterator.find(|kv| &kv.key == key)).
            map(|kv| kv.map(|found| found.value));

        self.seek(current_pos)?;
        return result;
    }


    pub fn tell(&self) -> Result<u64, std::io::Error> {
        let offset = RefCell::new(&self.fd).borrow_mut().seek(SeekFrom::Current(0))?;
        Ok(offset)
    }


    fn reset(&self) -> Result<(), std::io::Error> {
        self.seek(0)?;
        Ok(())
    }


    pub fn read(&self) -> impl Iterator<Item=KVPair> + '_ {
        let reader = BufReader::new(&self.fd);
        return reader.lines().
            map(|string| serde_json::from_str::<KVPair>(&string.unwrap()).unwrap());
    }


    pub fn read_from_start(&self) -> Result<impl Iterator<Item=KVPair> + '_, std::io::Error> {
        self.reset()?;
        return Ok(self.read());
    }
}


#[cfg(test)]
mod tests {
    use std::io::{Write, Seek, Read};
    use crate::sst::Segment;

    extern crate tempfile;


    #[test]
    fn test_search() -> Result<(), Box<dyn std::error::Error>> {
        let mut sst = Segment::with_file(tempfile::tempfile()?);
        sst.write("foo".to_owned(), "bar".to_owned())?;
        sst.write("hello".to_owned(), "world".to_owned())?;
        assert_eq!(Some("world".to_owned()), sst.search("hello")?);
        Ok(())
    }

    #[test]
    fn test_peek() -> Result<(), Box<dyn std::error::Error>> {
        let mut sst = Segment::with_file(tempfile::tempfile()?);
        sst.write("k1".to_owned(), "v1".to_owned())?;
        sst.reset()?;
        let x = sst.peek()?;
        let y = sst.peek()?;
        assert_eq!(x, y);
        Ok(())
    }


    #[test]
    fn test_seek() -> Result<(), Box<dyn std::error::Error>> {
        let mut sst = Segment::with_file(tempfile::tempfile()?);
        let first_offset = sst.write("k1".to_owned(), "v1".to_owned())?;
        let second_offset = sst.write("k2".to_owned(), "v2".to_owned())?;
        sst.write("k3".to_owned(), "v3".to_owned())?;

        sst.seek(first_offset)?;
        let first = sst.read().take(1).last();
        assert_eq!(Some("v1".to_owned()), first.map(|x| x.value));

        sst.seek(second_offset)?;
        let first = sst.read().take(1).last();
        assert_eq!(Some("v2".to_owned()), first.map(|x| x.value));
        Ok(())
    }

    #[test]
    fn test_read() -> Result<(), Box<dyn std::error::Error>> {
        let mut sst = Segment::with_file(tempfile::tempfile()?);
        sst.write("k1".to_owned(), "v1".to_owned())?;
        sst.write("k2".to_owned(), "v2".to_owned())?;
        let iterator = &mut sst.read_from_start()?;

        let first = iterator.next();
        assert_eq!(Some("v1".to_owned()), first.map(|kv| kv.value));

        let second = iterator.next();
        assert_eq!(Some("v2".to_owned()), second.map(|kv| kv.value));

        Ok(())
    }
}
