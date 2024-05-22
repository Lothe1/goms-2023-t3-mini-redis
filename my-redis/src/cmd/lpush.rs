use std::collections::VecDeque;
use crate::{Connection, Db, Frame, Parse};

use bytes::Bytes;

#[derive(Debug,Clone)]
pub struct Lpush {
    key: String,
    list: VecDeque<String>
}

impl Lpush {
    pub fn new(key: impl ToString) -> Lpush {
        Lpush {
            key: key.to_string(),
            list: VecDeque::new(),
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn get_lists(&self) -> &VecDeque<String> {
        &self.list
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Lpush> {
        let mut values = VecDeque::new();
        let key = parse.next_string()?;
        while let Ok(value) = parse.next_string() {
            values.push_back(value);
        }
        Ok(Lpush { key, list: values })
    }


}
