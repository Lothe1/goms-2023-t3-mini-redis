//! mini-redis server.
//!
//! This file is the entry point for the server implemented in the library. It
//! performs command line parsing and passes the arguments on to
//! `m()ini_redis::server`.
//!
//! The `clap` crate is used for parsing arguments.



use my_redis::cmd::Blpop;
use bytes::Bytes;
use std::collections::LinkedList;
use std::future::Future;
use std::io::ErrorKind;
use std::sync::Arc;use tokio::time::Timeout;
use my_redis::{server, DEFAULT_PORT, Connection, Frame};
use clap::Parser;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;use my_redis::db::{AllDbs, DataTypes};
use my_redis::request::Request;
use tokio::sync::mpsc::{Receiver, Sender};
use my_redis::db::SpecialSender;
use log::debug;
use log::Level::Debug;
use tracing::field::debug;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;
use my_redis::db::KeyAndValue;


use my_redis::Command::*;
use my_redis::db::DataTypes::BytesInDb;
use my_redis::db::NUM_DBS;
use my_redis::db::SenderType::{fromBlpop, fromBrpop};

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
            initialize_server(rx, database_index as usize, all_dbs_clone).await;
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
            if let Err(e) = process_incoming_frame(socket, all_dbs_clone, sender_arc_clone).await {
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
async fn process_incoming_frame(socket: TcpStream, all_dbs: Arc<AllDbs>, channels: Arc<Vec<Sender<Request>>>) -> Result<()>{
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

async fn process_commands_for_index_namespace(request: Request, index: usize, all_dbs: Arc<AllDbs>){
    let response = match request.cmd {
        Set(cmd) => {
            all_dbs.get_instance(index).unwrap().lock().unwrap().insert(cmd.key().to_string(), DataTypes::BytesInDb(cmd.value().clone()));
            Frame::Simple("OK".to_string())
        }
        Get(cmd) => {
            if let Some(DataTypes::BytesInDb(value)) = all_dbs.get_instance(index).unwrap().lock().unwrap().get(cmd.key()) {
                Frame::Bulk(value.clone())
            } else {
                Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())
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
            let mut values = cmd.get_lists().clone();
            let mut return_number = values.len();
            //Make copy of the list to avoid deadlock while holding the lock then match to handle commands
            let mut list_copy = {
                let mut db_instance = all_dbs.get_instance(index).unwrap();
                let mut db_lock = db_instance.lock().unwrap();
                let list = db_lock.entry(key.clone()).or_insert_with(|| DataTypes::List(LinkedList::new()));
                (*list).clone()
            };
            match list_copy {
                DataTypes::List(ref mut data) => {
                    let mut db_instance = all_dbs.get_instance(index).unwrap();
                    let mut db_lock = db_instance.lock().unwrap();
                    match db_lock.get_mut(&key).unwrap() {
                        DataTypes::List(data) => {
                            for value in values {
                                data.push_front(Bytes::from(value));
                            }
                            Frame::Integer(data.len() as u64)
                        }
                        _ => Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                    }
                },
                DataTypes::SenderList(senders) => {
                    let mut senders = senders;
                    while !senders.is_empty() && !values.is_empty(){
                        match senders.pop_front()  {
                            None => continue,
                            Some(special_sender) => {
                                let sender = special_sender.sender;
                                if sender.is_closed() {
                                    continue;
                                }
                                let package;
                                match special_sender.type_sender{
                                    fromBlpop =>{
                                        package = KeyAndValue {
                                            key: key.clone(),
                                            value: Bytes::from(values.pop_front().unwrap()),
                                        };
                                    }
                                    fromBrpop =>{
                                        package = KeyAndValue {
                                            key: key.clone(),
                                            value: Bytes::from(values.pop_back().unwrap()),
                                        };
                                    }
                                }
                                tokio::spawn(async move {
                                    sender.send(package.clone()).await.unwrap();
                                });
                            }
                        }
                    }
                    if values.is_empty() && senders.is_empty(){
                        // If values got nothing left then empty then remove  key
                        {
                            let db_instance = all_dbs.get_instance(index).unwrap();
                            let mut db_lock = db_instance.lock().unwrap();
                            let list = db_lock.remove(&key).unwrap();
                        }
                    }else if values.is_empty() && !senders.is_empty(){
                        // out of value but still have senders dont touch the keys
                    } else{
                        // If values got something left then add it to the list
                        {
                            let mut db_instance = all_dbs.get_instance(index).unwrap();
                            let mut db_lock = db_instance.lock().unwrap();
                            db_lock.remove(&key).unwrap();
                            let list = db_lock.entry(key.clone()).or_insert_with(|| DataTypes::List(LinkedList::new()));
                            if let DataTypes::List(ref mut data) = list {
                                for value in values {
                                    data.push_front(Bytes::from(value));
                                }
                            }
                        }
                    }
                    Frame::Integer(return_number as u64)
                },
                _ => Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            }
        }
        Rpush(cmd) => {
            let key = cmd.key().to_string();
            let mut values = cmd.get_lists().clone();
            let mut return_number = values.len();
            //Make copy of the list to avoid deadlock while holding the lock then match to handle commands
            let mut list_copy = {
                let mut db_instance = all_dbs.get_instance(index).unwrap();
                let mut db_lock = db_instance.lock().unwrap();
                let list = db_lock.entry(key.clone()).or_insert_with(|| DataTypes::List(LinkedList::new()));
                (*list).clone()
            };
            match list_copy {
                DataTypes::List(ref mut data) => {
                    let mut db_instance = all_dbs.get_instance(index).unwrap();
                    let mut db_lock = db_instance.lock().unwrap();
                    match db_lock.get_mut(&key).unwrap() {
                        DataTypes::List(data) => {
                            for value in values {
                                data.push_back(Bytes::from(value));
                            }
                            Frame::Integer(data.len() as u64)
                        }
                        _ => Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                    }
                },
                DataTypes::SenderList(senders) => {
                    let mut senders = senders;
                    while !senders.is_empty() && !values.is_empty(){
                        match senders.pop_front()  {
                            None => continue,
                            Some(special_sender) => {
                                let sender = special_sender.sender;
                                if sender.is_closed() {
                                    continue;
                                }
                                let package;
                                match special_sender.type_sender{
                                    fromBlpop =>{
                                        package = KeyAndValue {
                                            key: key.clone(),
                                            value: Bytes::from(values.pop_front().unwrap()),
                                        };
                                    }
                                    fromBrpop =>{
                                        package = KeyAndValue {
                                            key: key.clone(),
                                            value: Bytes::from(values.pop_back().unwrap()),
                                        };
                                    }
                                }
                                tokio::spawn(async move {
                                    sender.send(package.clone()).await.unwrap();
                                });
                            }
                        }
                    }
                    if values.is_empty() && senders.is_empty(){
                        // If values got nothing left then empty then remove  key
                        {
                            let db_instance = all_dbs.get_instance(index).unwrap();
                            let mut db_lock = db_instance.lock().unwrap();
                            let list = db_lock.remove(&key).unwrap();
                        }
                    }else if values.is_empty() && !senders.is_empty(){
                        // out of value but still have senders dont touch the keys
                    } else{
                        // If values got something left then add it to the list
                        {
                            let mut db_instance = all_dbs.get_instance(index).unwrap();
                            let mut db_lock = db_instance.lock().unwrap();
                            db_lock.remove(&key).unwrap();
                            let list = db_lock.entry(key.clone()).or_insert_with(|| DataTypes::List(LinkedList::new()));
                            if let DataTypes::List(ref mut data) = list {
                                for value in values {
                                    data.push_back(Bytes::from(value));
                                }
                            }
                        }
                    }
                    Frame::Integer(return_number as u64)
                },
                _ => Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            }
        }
        Unknown(cmd) => Frame::Simple(format!("{:?}", cmd)),
        Blpop(cmd) => {
            let keys = cmd.get_lists();
            let timeout = cmd.get_timeout();
            let mut found = false;
            let mut found_key= String::new();
            let mut res = Default::default();
            {
                let db_instance = all_dbs.get_instance(index).unwrap();
                let mut db_lock = db_instance.lock().unwrap();
                for key in keys {
                    match db_lock.get_mut(key){
                        None => {
                            //do nothing
                        }
                        Some(DataTypes::SenderList(senders))=>{
                            //do nothing
                        }
                        Some(DataTypes::List(ref mut data))=> {
                            if let Some(value) = data.pop_front() {
                                res = value;
                                found_key = key.clone();
                                found = true;
                                break;
                            }
                        },
                        _ => {
                            Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string());
                        }
                    }
                }
            }
            if found {
                Frame::Array(vec![Frame::Bulk(Bytes::from(found_key)), Frame::Bulk(res)])
            } else {
                let (tx2, mut rx2) = mpsc::channel(20);
                {
                    let mut db_instance = all_dbs.get_instance(index).unwrap();
                    let mut db_lock = db_instance.lock().unwrap();
                    for key in keys{
                        if let Some(data_type) = db_lock.get_mut(key) {
                            match data_type {
                                DataTypes::SenderList(senders) => {
                                    let special_sender = SpecialSender {
                                        sender: tx2.clone(),
                                        type_sender: fromBlpop,
                                    };
                                    senders.push_back(special_sender);
                                },
                                DataTypes::List(data) => {
                                    let special_sender = SpecialSender {
                                        sender: tx2.clone(),
                                        type_sender: fromBlpop,
                                    };
                                    db_lock.insert(key.clone(), DataTypes::SenderList(LinkedList::from([special_sender])));
                                },
                                _ => panic!("Unexpected data type"),
                            }
                        } else {
                            let mut senders = LinkedList::new();
                            let special_sender = SpecialSender {
                                sender: tx2.clone(),
                                type_sender: fromBlpop,
                            };
                            senders.push_back(special_sender);
                            db_lock.insert(key.clone(), DataTypes::SenderList(senders));
                        }
                    }
                }
                if *timeout == 0.0{
                    let res = rx2.recv().await.unwrap();
                    Frame::Array(vec![Frame::Bulk(Bytes::from(res.key)), Frame::Bulk(res.value)])
                }else{
                    let res =
                        tokio::select! {
                        res = rx2.recv() => {
                            Frame::Bulk(res.unwrap().value)
                        },
                        _ = tokio::time::sleep(std::time::Duration::from_secs_f64(*timeout))=> {
                            Frame::Null
                        }
                   };
                    res
                }
            }
        }
        Brpop(cmd) =>{
            let keys = cmd.get_lists();
            let timeout = cmd.get_timeout();
            let mut found = false;
            let mut found_key= String::new();
            let mut res = Default::default();
            {
                let db_instance = all_dbs.get_instance(index).unwrap();
                let mut db_lock = db_instance.lock().unwrap();
                for key in keys {
                    match db_lock.get_mut(key){
                        None => {
                            //do nothing
                        }
                        Some(DataTypes::SenderList(senders))=>{
                            //do nothing
                        }
                        Some(DataTypes::List(ref mut data))=> {
                            if let Some(value) = data.pop_back() {
                                res = value;
                                found_key = key.clone();
                                found = true;
                                break;
                            }
                        },
                        _ => {
                            Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string());
                        }
                    }
                }
            }
            if found {
                Frame::Array(vec![Frame::Bulk(Bytes::from(found_key)), Frame::Bulk(res)])
            } else {
                let (tx2, mut rx2) = mpsc::channel(20);
                {
                    let mut db_instance = all_dbs.get_instance(index).unwrap();
                    let mut db_lock = db_instance.lock().unwrap();
                    for key in keys{
                        if let Some(data_type) = db_lock.get_mut(key) {
                            match data_type {
                                DataTypes::SenderList(senders) => {
                                    let special_sender = SpecialSender {
                                        sender: tx2.clone(),
                                        type_sender: fromBrpop,
                                    };
                                    senders.push_back(special_sender);
                                },
                                DataTypes::List(data) => {
                                    let special_sender = SpecialSender {
                                        sender: tx2.clone(),
                                        type_sender: fromBrpop,
                                    };
                                    db_lock.insert(key.clone(), DataTypes::SenderList(LinkedList::from([special_sender])));
                                },
                                _ => panic!("Unexpected data type"),
                            }
                        } else {
                            let mut senders = LinkedList::new();
                            let special_sender = SpecialSender {
                                sender: tx2.clone(),
                                type_sender: fromBrpop,
                            };
                            senders.push_back(special_sender);
                            db_lock.insert(key.clone(), DataTypes::SenderList(senders));
                        }
                    }
                }
                if *timeout == 0.0{
                    let res = rx2.recv().await.unwrap();
                    Frame::Array(vec![Frame::Bulk(Bytes::from(res.key)), Frame::Bulk(res.value)])
                }else{
                    let res =
                        tokio::select! {
                        res = rx2.recv() => {
                            Frame::Bulk(res.unwrap().value)
                        },
                        _ = tokio::time::sleep(std::time::Duration::from_secs_f64(*timeout))=> {
                            Frame::Null
                        }
                   };
                    res
                }
            }
        }
        cmd => panic!("unimplemented {:?}", cmd),
    };
    request.sender.send(response).unwrap();
}

async fn initialize_server(mut receiver: Receiver<Request>, index: usize, all_dbs: Arc<AllDbs>) {
    while let Some(request) = receiver.recv().await{
        // dbg!(&request);
        let adbs = all_dbs.clone();
        tokio::task::spawn(async move {
            process_commands_for_index_namespace(request, index, adbs).await;
        });
    }

    // vec![arc::new(server::new()); 16]
}




