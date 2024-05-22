use crate::{Connection, Db, Frame, Parse};

use bytes::Bytes;
use crate::cmd::rpush::Rpush;

#[derive(Debug,Clone)]
pub struct Brpop {
    list: Vec<String>,
    timeout: f64,
}

impl Brpop {
    pub fn new(key: impl ToString, timeout: f64) -> Brpop {
        Brpop {
            list: vec![],
            timeout,
        }
    }

    pub fn get_lists(&self) -> &Vec<String> {
        &self.list
    }

    pub fn get_timeout(&self) -> &f64 {
        &self.timeout
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Brpop> {
        let mut values = Vec::new();
        while let Ok(value) = parse.next_string() {
            values.push(value);
        }

        let time_out:f64 = values.pop().unwrap().parse().unwrap();
        Ok(Brpop { list: values, timeout:time_out })
    }
}

