use crate::{Connection, Db, Frame, Parse};

use bytes::Bytes;

#[derive(Debug,Clone)]
pub struct Rpush {
    key: String,
    list: Vec<String>,
}

impl Rpush {
    pub fn new(key: impl ToString) -> Rpush {
        Rpush {
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

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Rpush> {
        let mut values = Vec::new();
        let key = parse.next_string()?;
        while let Ok(value) = parse.next_string() {
            values.push(value);
        }
        Ok(Rpush { key, list: values })
    }


}
