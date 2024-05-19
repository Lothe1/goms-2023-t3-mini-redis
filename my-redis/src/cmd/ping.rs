use crate::{Connection, Db, Frame, Parse};
use bytes::Bytes;

#[derive(Debug)]
pub struct Ping {
    key: Bytes,
}

impl Ping {
    pub fn new(key: impl ToString) -> Ping {
        Ping {
            key: Bytes::from(key.to_string()),
        }
    }

    pub fn key(&self) -> &Bytes {
        &self.key
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Ping> {
        let key = parse.next_string();
        let key = key.unwrap_or_else(|_| "Pong".to_string());
        Ok(Ping { key: Bytes::from(key)})
    }

    pub(crate) fn into_frame(self) -> Frame {
          let mut frame = Frame::array();
            frame.push_bulk(Bytes::from("ping".as_bytes()));
            frame.push_bulk(Bytes::from(self.key));
            frame
    }
}