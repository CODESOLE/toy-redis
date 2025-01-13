use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Ok};
use tokio::io::*;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

#[derive(Debug, PartialEq, Eq)]
enum Value<'a> {
    BulkStr(&'a str),   // $
    SimpleStr(&'a str), // +
    Int(i64),           // :
}

async fn parse_resp(mut str: &str) -> Option<Vec<Value>> {
    let mut arr = Vec::new();
    while let Some((s1, mut s2)) = str.split_once("\r\n") {
        match s1.as_bytes().iter().next().unwrap() {
            b'*' => {
                let num: usize = s1[1..].parse().unwrap();
                for _ in 0..num {
                    match s2.as_bytes().iter().next().unwrap() {
                        b'$' => {
                            let idx = s2.find("\r").unwrap();
                            let bulkstr_size: usize = (&s2[1..idx]).parse().unwrap();
                            if let Some((_, x2)) = s2.split_once("\r\n") {
                                arr.push(Value::BulkStr(&x2[..bulkstr_size]));
                                s2 = x2.split_once("\r\n").unwrap().1;
                            }
                        }
                        _ => panic!("Unknown RESP code!"),
                    }
                }
            }
            _ => panic!("Unknown RESP code!"),
        }
        if s2.is_empty() {
            return Some(arr);
        }
        str = s2;
    }
    None
}

async fn handle_conn(
    mut tcpstream: TcpStream,
    db: Arc<Mutex<HashMap<String, String>>>,
) -> anyhow::Result<()> {
    let mut incoming_msg = vec![0u8; 4096];
    loop {
        if tcpstream.write("".as_bytes()).await.is_err() {
            return Ok(());
        }
        let n = tcpstream
            .read(&mut incoming_msg)
            .await
            .context("stream.read")?;
        let incmsg = String::from_utf8_lossy(&incoming_msg[..n]).to_owned();
        // dbg!(&incmsg);
        if let Some(res) = parse_resp(&incmsg).await {
            match res[0] {
                Value::BulkStr("ping") => {
                    tcpstream
                        .write(b"+PONG\r\n")
                        .await
                        .context("stream.write_all")?;
                }
                Value::BulkStr("echo") => {
                    if let Value::BulkStr(s) = res[1] {
                        let reply = format!("${}\r\n{}\r\n", s.len(), s);
                        tcpstream
                            .write(reply.as_bytes())
                            .await
                            .context("stream.write_all")?;
                    }
                }
                Value::BulkStr("get") => {
                    match res[1] {
                        Value::BulkStr(req_key) => {
                            let database = db.lock().await;
                            if let Some(v) = database.get(req_key) {
                                let reply = format!("${}\r\n{}\r\n", v.len(), v);
                                tcpstream
                                    .write(reply.as_bytes())
                                    .await
                                    .context("stream.write_all")?;
                            } else {
                                tcpstream
                                    .write(b"$-1\r\n")
                                    .await
                                    .context("stream.write_all")?;
                            }
                        }
                        _ => panic!("GET Value not implemented!"),
                    }
                }
                Value::BulkStr("set") => {
                    match res[1] {
                        Value::BulkStr(k) => {
                            match res[2] {
                                Value::BulkStr(v) => {
                                    if let Some(_) = res.iter().find(|&x| *x == Value::BulkStr("px")) {
                                    } else {
                                        let mut database = db.lock().await;
                                        database.insert(k.to_owned(), v.to_owned());
                                        tcpstream
                                            .write(b"+OK\r\n")
                                            .await
                                            .context("stream.write_all")?;
                                    }
                                }
                                _ => panic!("SET Value not implemented!"),
                            }
                        }
                        _ => panic!("SET Value not implemented!"),
                    }
                }
                _ => panic!("Unknown command sent from client!"),
            }
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db = Arc::new(Mutex::new(HashMap::<String, String>::new()));
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    loop {
        let (tcpstream, _) = listener.accept().await?;
        let d = Arc::clone(&db);
        tokio::spawn(async move { handle_conn(tcpstream, d).await });
    }
}
