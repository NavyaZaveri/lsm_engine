use std::collections::BTreeMap;
use std::collections::btree_map::IntoIter;
use std::hash::Hash;
use std::borrow::Borrow;

pub struct Memtable<K: PartialOrd + Hash + Ord, T> {
    kv_table: BTreeMap<K, T>,
    capacity: usize,
}

impl<K: PartialOrd + Hash + Ord, T> Memtable<K, T> {
    pub fn new(capacity: usize) -> Self {
        Memtable {
            kv_table: BTreeMap::new(),
            capacity: capacity,
        }
    }


    pub fn insert(&mut self, key: K, value: T) {
        self.kv_table.insert(key, value);
    }

    pub fn contains<Q: ?Sized>(&self, key: &Q) -> bool where K: Borrow<Q>, Q: Ord, {
        return self.kv_table.contains_key(key);
    }


    pub fn get<Q: ?Sized>(&self, key: &Q) -> Option<&T> where K: Borrow<Q>, Q: Ord, {
        self.kv_table.get(key)
    }


    pub fn clear(&mut self) {
        self.kv_table.clear();
    }


    pub fn drain(&mut self) -> IntoIter<K, T> {
        std::mem::replace(&mut self.kv_table, BTreeMap::new()).into_iter()
    }

    pub fn at_capacity(&self) -> bool {
        self.kv_table.len() == self.capacity
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let mut memtable = Memtable::new(5);
        memtable.insert("k1", "v1");
    }
}



