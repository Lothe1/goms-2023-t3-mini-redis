use crate::cmd::{Parse, ParseError};
use crate::{Connection, Db, Frame};

use bytes::Bytes;
use std::time::Duration;
use tracing::{debug, instrument};

#[derive(Debug)]
pub struct Select {
    db_index: u8,
}

impl Select {

    pub fn new(db_index: u8) -> Select {
        Select { db_index }
    }

    /// Get the db index
    pub fn db_index(&self) -> u8 {
        self.db_index
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Select> {
        // Read the database index.
        let db_index = parse.next_string()?
            .parse::<u8>()
            .map_err(|_| "Invalid database index")?;

        // Attempt to parse another string.
        match parse.next_string() {
            Ok(_) => return Err("currently `Select` only support Select {DB}".into()),
            Err(_) => {} // No further data to parse, which is expected.
        }

        Ok(Select { db_index })
    }


    /// Apply the `Set` command to the specified `Db` instance.
    ///
    /// The response is written to `dst`. This is called by the server in order
    /// to execute a received command.
    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {

        // Create a success response and write it to `dst`.
        let response = Frame::Simple("OK".to_string());
        debug!(?response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    /// Converts the command into an equivalent `Frame`.
    ///
    /// This is called by the client when encoding a `Set` command to send to
    /// the server.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("select".as_bytes()));
        frame.push_bulk(Bytes::from(self.db_index.into_bytes()));
        frame
    }
}