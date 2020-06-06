use std::collections::BTreeMap;
use std::collections::btree_map::IntoIter;
use crate::memtable::ValueStatus::{Present, Tombstone};
use std::hash::Hash;
use std::borrow::Borrow;

pub struct Memtable<K: PartialOrd + Hash + Ord, T> {
    kv_table: BTreeMap<K, ValueStatus<T>>,
    capacity: usize,
}


#[derive(Eq, PartialEq, Debug)]
pub enum ValueStatus<T> {
    Present(T),
    Tombstone,
}


impl<K: PartialOrd + Hash + Ord, T> Memtable<K, T> {
    pub fn new(capacity: usize) -> Self {
        Memtable {
            kv_table: BTreeMap::new(),
            capacity: capacity,
        }
    }


    pub fn insert(&mut self, key: K, value: T) {
        self.kv_table.insert(key, Present(value));
    }


    pub fn get<Q: ?Sized>(&self, key: &Q) -> Option<&ValueStatus<T>> where K: Borrow<Q>, Q: Ord, {
        self.kv_table.get(key)
    }


    pub fn clear(&mut self) {
        self.kv_table.clear();
    }

    pub fn delete(&mut self, key: K) {
        self.kv_table.insert(key, Tombstone);
    }


    pub fn drain(&mut self) -> IntoIter<K, ValueStatus<T>> {
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
        memtable.delete("k1");
        assert_eq!(memtable.get("k1"), Some(&Tombstone));
    }
}



