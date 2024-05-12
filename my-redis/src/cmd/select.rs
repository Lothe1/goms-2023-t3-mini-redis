use crate::cmd::{Parse, ParseError};
use crate::{Connection, Db, Frame};

use bytes::Bytes;
use std::time::Duration;
use tracing::{debug, instrument};

#[derive(Debug)]
pub struct Select {
    db: String,
}

impl Select {
    /// Create a new `Set` command which sets `key` to `value`.
    ///
    /// If `expire` is `Some`, the value should expire after the specified
    /// duration.
    pub fn new(key: impl ToString) -> Select {
        Select {
            db: key.to_string(),
        }
    }

    /// Get the db
    pub fn db(&self) -> &str {
        &self.db
    }


    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Select> {
        use ParseError::EndOfStream;

        // Read the key to set. This is a required field
        let db = parse.next_string()?;


        // Attempt to parse another string.
        match parse.next_string() {
            Ok(_) => return Err("currently `Select` only support Select {DB}".into()),
            // The `EndOfStream` error indicates there is no further data to
            // parse. In this case, it is a normal run time situation and
            // indicates there are no specified `SET` options.
            Err(EndOfStream) => {}
            // All other errors are bubbled up, resulting in the connection
            // being terminated.
            Err(err) => return Err(err.into()),
        }

        Ok(Select { db })
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
        frame.push_bulk(Bytes::from(self.db.into_bytes()));
        frame
    }
}