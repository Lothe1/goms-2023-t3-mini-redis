//! Minimal Redis server implementation
//!
//! Provides an async `run` function that listens for inbound connections,
//! spawning a task per connection.

use crate::{Command, Connection, Db};
use tokio::sync::mpsc::{Receiver, Sender};
use std::future::Future;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, Semaphore};
use tokio::time::{self, Duration};
use tracing::{debug, error, info, instrument};
use crate::db::AllDbs;
use crate::request::Request;

#[derive(Debug)]
struct Listener {
    db_holder: AllDbs,
    listener: TcpListener,
    limit_connections: Arc<Semaphore>,
}

#[derive(Debug)]
struct Handler {
    connection: Connection,
    databases: Arc<AllDbs>,
}

const MAX_CONNECTIONS: usize = 250;


pub async fn run(mut receiver: Receiver<Request>, index: usize, all_dbs: Arc<AllDbs>) {

}

impl Handler {
}