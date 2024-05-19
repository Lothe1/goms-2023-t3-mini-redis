use crate::{Connection, Db, Frame, Parse};

use bytes::Bytes;

#[derive(Debug)]
pub struct Lpush {
    key: String,
    list: Vec<String>,
}

impl Lpush {
    pub fn new(key: impl ToString) -> Lpush {
        Lpush {
            key: key.to_string(),
            list: vec![],
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn get_lists(&self) -> &Vec<String> {
        &self.list
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Lpush> {
        let mut values = Vec::new();
        let key = parse.next_string()?;
        while let Ok(value) = parse.next_string() {
            values.push(value);
        }
        Ok(Lpush { key, list: values })
    }


}
