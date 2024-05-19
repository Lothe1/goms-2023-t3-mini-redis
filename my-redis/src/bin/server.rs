//! mini-redis server.
//!
//! This file is the entry point for the server implemented in the library. It
//! performs command line parsing and passes the arguments on to
//! `mini_redis::server`.
//!
//! The `clap` crate is used for parsing arguments.


use std::collections::LinkedList;
use std::sync::Arc;
use bytes::Bytes;
use my_redis::{server, DEFAULT_PORT, Connection, Frame};
use clap::Parser;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;use my_redis::db::{AllDbs, DataTypes};
use my_redis::request::Request;
use tokio::sync::mpsc::{Receiver, Sender};
pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;

enum ValueType {
    String(String),
    List(Vec<String>),
    Unknown, // In case the data does not match expected formats
}

#[cfg(feature = "otel")]
// To be able to set the XrayPropagator
use opentelemetry::global;
#[cfg(feature = "otel")]
// To configure certain options such as sampling rate
use opentelemetry::sdk::trace as sdktrace;
#[cfg(feature = "otel")]
// For passing along the same XrayId across services
use opentelemetry_aws::trace::XrayPropagator;
#[cfg(feature = "otel")]
// The `Ext` traits are to allow the Registry to accept the
// OpenTelemetry-specific types (such as `OpenTelemetryLayer`)
use tracing_subscriber::{
    fmt, layer::SubscriberExt, util::SubscriberInitExt, util::TryInitError, EnvFilter,
};
use my_redis::Command::{Exists, Get, Lpush,Rpush ,Ping, Select, Set, Unknown};
use my_redis::db::NUM_DBS;
#[tokio::main]
pub async fn main() -> my_redis::Result<()> {

    let port = DEFAULT_PORT;
    // Bind a TCP listener
    let listener = TcpListener::bind(&format!("127.0.0.1:{}", port)).await?;
    let databases = Arc::new(AllDbs::new());
    let mut senders: Vec<Sender<Request>> = vec![];
    for database_index in 0..NUM_DBS {
        let (tx, rx) = mpsc::channel(32);
        let all_dbs_clone = Arc::clone(&databases);
        tokio::spawn(async move {
            run(rx, database_index as usize, all_dbs_clone).await;
        });
        senders.push(tx);
    }
    let sender_arc: Arc<Vec<Sender<Request>>> = Arc::new(senders);
    loop {
        let (socket, _) = listener.accept().await?;
        println!("Accepted");
        let all_dbs_clone = databases.clone();
        let sender_arc_clone = sender_arc.clone();
        tokio::spawn(async move{
            if let Err(e) = process(socket, all_dbs_clone, sender_arc_clone).await {
                eprintln!("An error occurred: {}", e);
            }
        });
    }
}
struct Client {
    connection: Connection,
    all_dbs: Arc<AllDbs>,
    index: usize,
}
async fn process(socket: TcpStream, all_dbs: Arc<AllDbs>, channels: Arc<Vec<Sender<Request>>>) -> Result<()>{
    let mut client = Client {
        connection: Connection::new(socket),
        all_dbs,
        index: 0,
    };
    while let Some(frame) = client.connection.read_frame().await? {
        dbg!(&frame);
        let cmd = my_redis::Command::from_frame(frame);
        match cmd {
            Ok(cmd) => {
                let (request, receiver) = Request::new(cmd);
                match request.cmd {
                    Select(cmd) => {
                        let db_index = *cmd.db_index();
                        client.index = db_index;
                        client.connection.write_frame(&Frame::Simple("OK".to_string())).await?;
                    }
                    _ => {
                        channels.get(client.index).expect("REASON").send(request).await?;
                        let frame = receiver.await?;
                        client.connection.write_frame(&frame).await?;
                    }
                }
            }
            Err(e) => {
                client.connection.write_frame(&Frame::Error(e.to_string())).await?;
            }
        }

    }
    Ok(())
}

async fn run(mut receiver: Receiver<Request>, index: usize, all_dbs: Arc<AllDbs>) {
    while let Some(request) = receiver.recv().await {
        dbg!(&request);
        let response = match request.cmd {
            Set(cmd) => {
                all_dbs.get_instance(index).unwrap().lock().unwrap().insert(cmd.key().to_string(), DataTypes::Bytes(cmd.value().clone()));
                Frame::Simple("OK".to_string())
            }
            Get(cmd) => {
                if let Some(value) = all_dbs.get_instance(index).unwrap().lock().unwrap().get(cmd.key()) {
                    dbg!(value.clone());
                    let value = match value {
                        DataTypes::Bytes(value) => value.clone(),
                        _ => panic!("Unexpected data type"),
                    };
                    Frame::Bulk(value.clone())
                } else {
                    Frame::Null
                }
            }
            Ping(cmd) => {
                let bytes = cmd.key();
                Frame::Bulk(bytes.clone())
            }
            Exists(cmd) => {
                let a = cmd.get_lists();
                let mut exists = 0;
                for key in a {
                    if all_dbs.get_instance(index).unwrap().lock().unwrap().contains_key(key) {
                        exists += 1;
                    }
                }
                Frame::Integer(exists as u64)
            }
            Lpush(cmd) => {
                let key = cmd.key().to_string();
                let values = cmd.get_lists().clone();
                let db_instance = all_dbs.get_instance(index).unwrap();
                let mut db_lock = db_instance.lock().unwrap();
                let list = db_lock.entry(key).or_insert_with(|| DataTypes::List(LinkedList::new()));

                if let DataTypes::List(ref mut data) = list {
                    for value in values {
                        data.push_front(Bytes::from(value));
                    }
                    Frame::Integer(data.len() as u64)
                } else {
                    Frame::Simple("Unexpected data type".parse().unwrap())
                }
            }

            Rpush(cmd) => {
                let key = cmd.key().to_string();
                let values = cmd.get_lists().clone();
                let db_instance = all_dbs.get_instance(index).unwrap();
                let mut db_lock = db_instance.lock().unwrap();
                let list = db_lock.entry(key).or_insert_with(|| DataTypes::List(LinkedList::new()));
                if let DataTypes::List(ref mut data) = list {
                    for value in values {
                        data.push_back(Bytes::from(value));
                    }
                    Frame::Integer(data.len() as u64)
                } else {
                    Frame::Simple("Unexpected data type".parse().unwrap())
                }
            }

            Unknown(cmd) => Frame::Simple(format!("{:?}", cmd)),

            cmd => panic!("unimplemented {:?}", cmd),
        };
        request.sender.send(response).unwrap();
    }
}




