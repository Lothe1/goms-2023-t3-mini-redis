use std::sync::Arc;
use crate::cmd::{Parse, ParseError};
use crate::{Connection, Db, Frame};
use bytes::Bytes;
use std::time::Duration;
use tracing::{debug, instrument};
use crate::db::AllDbs;

#[derive(Debug)]
pub struct Select {
    db_index: usize,
}

pub struct Client {
    connection: Connection,
    all_dbs: Arc<AllDbs>,
    index: usize,
}

impl Select {

    pub fn new(db_index: usize) -> Select {
        Select { db_index }
    }

    /// Get the db index
    pub fn db_index(&self) -> &usize {
        &self.db_index
    }



    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Select> {
        // Read the database index.
        let db_index = parse.next_string()?
            .parse::<usize>()
            .map_err(|_| "Invalid database index")?;

        if !(0..=12).contains(&db_index) {
            return Err("Database index must be between 0 and 12".into());
        }

        // Attempt to parse another string.
        match parse.next_string() {
            Ok(_) => return Err("currently `Select` only support Select {DB}".into()),
            Err(_) => {} // No further data to parse, which is expected.
        }

        Ok(Select { db_index: db_index.into() })
    }



}