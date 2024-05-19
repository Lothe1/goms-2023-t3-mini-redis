use tokio::sync::{broadcast, Notify};
use tokio::time::{self, Duration, Instant};

use bytes::Bytes;
use std::collections::{BTreeSet, HashMap, LinkedList};
use std::sync::{Arc, Mutex};
use tracing::debug;
const NUM_DBS: usize = 16;

#[derive(PartialEq, Debug)]
pub enum DataTypes {
    Bytes(Bytes),
    List(LinkedList<Bytes>),
}

pub type Db = Arc<Mutex<HashMap<String, DataTypes>>>;

#[derive(Debug, Clone)]
pub struct AllDbs {
    db0: Db,
    db1: Db,
    db2: Db,
    db3: Db,
    db4: Db,
    db5: Db,
    db6: Db,
    db7: Db,
    db8: Db,
    db9: Db,
    db10: Db,
    db11: Db,
    db12: Db,
    db13: Db,
    db14: Db,
    db15: Db,
}


impl AllDbs {
    pub fn new() -> AllDbs {
        AllDbs {
            db0: Db::new(Mutex::new(Default::default())),
            db1: Db::new(Mutex::new(Default::default())),
            db2: Db::new(Mutex::new(Default::default())),
            db3: Db::new(Mutex::new(Default::default())),
            db4: Db::new(Mutex::new(Default::default())),
            db5: Db::new(Mutex::new(Default::default())),
            db6: Db::new(Mutex::new(Default::default())),
            db7: Db::new(Mutex::new(Default::default())),
            db8: Db::new(Mutex::new(Default::default())),
            db9: Db::new(Mutex::new(Default::default())),
            db10: Db::new(Mutex::new(Default::default())),
            db11: Db::new(Mutex::new(Default::default())),
            db12: Db::new(Mutex::new(Default::default())),
            db13: Db::new(Mutex::new(Default::default())),
            db14: Db::new(Mutex::new(Default::default())),
            db15: Db::new(Mutex::new(Default::default())),
        }
    }

    pub fn get_instance(&self, index: usize) -> Option<Db> {
        match index {
            0 => Some(Arc::clone(&self.db0)),
            1 => Some(Arc::clone(&self.db1)),
            2 => Some(Arc::clone(&self.db2)),
            3 => Some(Arc::clone(&self.db3)),
            4 => Some(Arc::clone(&self.db4)),
            5 => Some(Arc::clone(&self.db5)),
            6 => Some(Arc::clone(&self.db6)),
            7 => Some(Arc::clone(&self.db7)),
            8 => Some(Arc::clone(&self.db8)),
            9 => Some(Arc::clone(&self.db9)),
            10 => Some(Arc::clone(&self.db10)),
            11 => Some(Arc::clone(&self.db11)),
            12 => Some(Arc::clone(&self.db12)),
            13 => Some(Arc::clone(&self.db13)),
            14 => Some(Arc::clone(&self.db14)),
            15 => Some(Arc::clone(&self.db15)),
            _ => None,
        }
    }



}



