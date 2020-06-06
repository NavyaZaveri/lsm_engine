use std::fs::File;
use std::path::Path;
use std::io::{Read, Write, BufReader, BufRead, SeekFrom, Error, Seek};
use serde::{Serialize, Deserialize};
use std::cell::RefCell;
use std::fs::OpenOptions;
use std::io;


struct Segment {
    fd: File,
    size: usize,
}


#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
struct KVPair {
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


    fn write(&mut self, key: String, value: String) -> Result<(), Box<dyn std::error::Error>> {
        let kv = KVPair {
            key,
            value,
        };
        serde_json::to_writer(&self.fd, &kv)?;
        self.fd.write(b"\n")?;
        self.size += 1;
        Ok(())
    }


    fn seek(&self, pos: u64) -> Result<(), std::io::Error> {
        RefCell::new(&self.fd).borrow_mut().seek(SeekFrom::Start(pos))?;
        Ok(())
    }


    fn peek(&self) -> Result<KVPair, Box<dyn std::error::Error>> {
        let mut reader = BufReader::new(&self.fd);
        let current = self.tell()?;
        let mut s = String::new();
        reader.read_line(&mut s)?;
        let kv_pair = serde_json::from_str::<KVPair>(&s)?;

        //reset back to current offset
        self.seek(current)?;
        return Ok(kv_pair);
    }


    fn search(&self, key: &str) -> Result<Option<String>, std::io::Error> {
        let current_pos = self.tell()?;
        let result = self.
            read_all().
            map(|mut stream| stream.find(|kv| &kv.key == key)).
            map(|kv| kv.map(|found| found.value));
        self.seek(current_pos)?;
        return result;
    }


    fn tell(&self) -> Result<u64, std::io::Error> {
        let offset = RefCell::new(&self.fd).borrow_mut().seek(SeekFrom::Current(0))?;
        Ok(offset)
    }


    fn reset(&self) -> Result<(), std::io::Error> {
        self.seek(0)?;
        Ok(())
    }


    fn read_all(&self) -> Result<impl Iterator<Item=KVPair> + '_, std::io::Error> {
        self.reset()?;
        let reader = BufReader::new(&self.fd);
        return Ok(reader.lines().
            map(|x| serde_json::from_str::<KVPair>(&x.unwrap()).unwrap()));
    }
}


#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::{Write, Seek, Read, SeekFrom};
    use crate::sst::{KVPair, Segment};
    use std::fs;


    #[test]
    fn test_search() {
        let mut sst = Segment::new("foobar.txt");
        assert_eq!(true, sst.write("foo".to_owned(), "bar".to_owned()).is_ok());
        assert_eq!(true, sst.write("hello".to_owned(), "world".to_owned()).is_ok());
        assert_eq!(Some("world".to_owned()), sst.search("hello").unwrap());
        fs::remove_file("foobar.txt").unwrap();
    }

    #[test]
    fn test_peak() {
        let mut sst = Segment::new("foobar.txt");
        sst.write("k1".to_owned(), "v1".to_owned());
        sst.reset();
        let x = sst.peek();
        let y = sst.peek();
        assert_eq!(x.is_ok(), true);
        assert_eq!(y.is_ok(), true);
        assert_eq!(x.unwrap(), y.unwrap());
    }
}
