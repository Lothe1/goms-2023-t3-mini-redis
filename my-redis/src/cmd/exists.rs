use bytes::Bytes;
use crate::Frame;
use crate::parse::Parse;

#[derive(Debug,Clone)]
pub struct Exists {
    list: Vec<String>,
}

impl Exists {
    pub fn new(key: impl ToString) -> Exists {
        Exists {
            list: vec![],
        }
    }

    pub fn get_lists(&self) -> &Vec<String> {
        &self.list
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Exists> {
        let mut keys = Vec::new();
        while let Ok(key) = parse.next_string() {
            keys.push(key);
        }
        // If the loop exits because of an error other than EndOfStream, return that error
        if !keys.is_empty() {
            Ok(Exists { list: keys })
        } else {
            Err("No keys found".into())
        }
    }
}