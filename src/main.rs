#![allow(unused_imports)]

use core::num;
use std::{
    collections::HashMap,
    pin::Pin,
    sync::Arc,
    time::{self, Duration, SystemTime, UNIX_EPOCH},
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
    time::sleep,
};

use memchr::memchr;

type Db = Arc<Mutex<HashMap<String, Value>>>;

struct Value {
    val: String,
}

enum RedisType {
    Array(Vec<RedisType>),
    String(String),
}

enum RedisFormat {
    Number,
    Simple,
    Bulk,
    Error,
}

#[derive(PartialEq)]
enum Status {
    SYNC,
    TRANS,
}
type Transaction = Vec<RedisType>;
struct ThreadStatus {
    status: Status,
    transactions: Vec<Transaction>,
}

const NULL_BULK: &[u8] = b"$-1\r\n";
const OK_SIMPLE: &[u8] = b"+OK\r\n";
const QUEUED_SIMPLE: &[u8] = b"+QUEUED\r\n";

#[tokio::main]
async fn main() {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    let db = Arc::new(Mutex::new(HashMap::<String, Value>::new()));

    loop {
        let stream = listener.accept().await;
        match stream {
            Ok((stream, _)) => {
                println!("accepted new connection");
                let db_t = db.clone();
                let t_stat = ThreadStatus {
                    status: Status::SYNC,
                    transactions: vec![],
                };
                tokio::spawn(async move { handle_connection(stream, db_t, t_stat).await });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

async fn handle_connection(mut stream: TcpStream, db: Db, mut t_stat: ThreadStatus) {
    let mut buf = [0; 512];
    loop {
        let bytes_read = stream.read(&mut buf).await.unwrap();
        if bytes_read == 0 {
            break;
        }

        let (message, _) = parse_redis(&buf, 0).unwrap();
        match handle_answer(message, &db, &mut t_stat).await {
            Ok(vec) => stream.write_all(&vec).await.unwrap(),
            Err(_) => stream
                .write_all("Could not answer".as_bytes())
                .await
                .unwrap(),
        }
    }
}

async fn handle_answer(msg: RedisType, db: &Db, t_stat: &mut ThreadStatus) -> Result<Vec<u8>, ()> {
    match msg {
        RedisType::Array(arr) => match &arr[0] {
            RedisType::String(str) => {
                if t_stat.status == Status::SYNC {
                    handle_answer_by_type(&arr, str, db, t_stat).await
                } else if str.to_ascii_lowercase() == "exec" {
                    let txs = std::mem::take(&mut t_stat.transactions);
                    let mut ans = vec![];
                    for t in txs {
                        if let RedisType::String(str) = &t[0] {
                            match handle_answer_by_type(&t, str, db, t_stat).await {
                                Ok(vec) => ans.push(vec),
                                Err(_) => return Err(()),
                            }
                        } else {
                            return Err(());
                        }
                    }
                    t_stat.status = Status::SYNC;
                    Ok(encode_array(ans))
                } else {
                    t_stat.transactions.push(arr);
                    Ok(QUEUED_SIMPLE.to_vec())
                }
            }

            _ => Err(()),
        },
        _ => Err(()),
    }
}

async fn handle_answer_by_type(
    arr: &Vec<RedisType>,
    str: &String,
    db: &Db,
    t_stat: &mut ThreadStatus,
) -> Result<Vec<u8>, ()> {
    match str.to_ascii_lowercase().as_str() {
        "echo" => match &arr[1] {
            RedisType::String(str) => Ok(encode_bulk(str.clone())),
            RedisType::Array(_) => Err(()),
        },
        "ping" => Ok(b"+PONG\r\n".to_vec()),
        "set" => match (&arr[1], &arr[2]) {
            (RedisType::String(key), RedisType::String(val)) => {
                let ttl = match (arr.get(3), arr.get(4)) {
                    (Some(RedisType::String(unit)), Some(RedisType::String(time))) => {
                        match unit.to_ascii_lowercase().as_str() {
                            "px" => Some(time.parse::<u64>().unwrap()),
                            "ex" => Some(time.parse::<u64>().unwrap() * 1000),
                            _ => return Err(()),
                        }
                    }
                    _ => None,
                };
                db.lock()
                    .await
                    .insert(key.clone(), Value { val: val.clone() });

                if let Some(v) = ttl {
                    let db = db.clone();
                    let key = key.clone();
                    tokio::spawn(async move {
                        sleep(Duration::from_millis(v)).await;
                        db.lock().await.remove(&key);
                    });
                }
                Ok(OK_SIMPLE.to_vec())
            }
            _ => Err(()),
        },
        "get" => match &arr[1] {
            RedisType::String(key) => {
                let guard = db.lock().await;
                if let Some(Value { val }) = guard.get(key) {
                    return Ok(encode_bulk(val.clone()));
                }

                Ok(NULL_BULK.to_vec())
            }
            _ => Err(()),
        },
        "incr" => match &arr[1] {
            RedisType::String(key) => {
                let mut db = db.lock().await;
                let entry = db.entry(key.clone()).or_insert(Value {
                    val: "0".to_string(),
                });

                let mut count = match entry.val.parse::<i64>() {
                    Ok(n) => n,

                    Err(_) => {
                        return Ok(encode_error(
                            "value is not an integer or out of range".to_string(),
                        ));
                    }
                };

                count += 1;

                entry.val = count.to_string();

                Ok(encode_int(count.to_string()))
            }
            _ => Err(()),
        },
        "multi" => {
            t_stat.status = Status::TRANS;
            Ok(OK_SIMPLE.to_vec())
        }
        "exec" => {
            if t_stat.status == Status::TRANS {
                Ok(encode_array(vec![]))
            } else {
                Ok(encode_error("EXEC without MULTI".to_string()))
            }
        }
        _ => Err(()),
    }
}

fn parse_redis(buf: &[u8], pos: usize) -> Result<(RedisType, usize), ()> {
    match buf[pos] {
        b'+' => simple_string(buf, pos + 1),
        b'$' => bulk_string(buf, pos + 1),
        b'*' => array(buf, pos + 1),
        _ => Err(()),
    }
}

fn word(buf: &[u8], pos: usize) -> Result<(String, usize), ()> {
    if buf.len() <= pos {
        return Err(());
    }
    // Find the position of the b'\r'
    memchr(b'\r', &buf[pos..])
        .and_then(|end| {
            if end + 1 < buf.len() {
                // pos + end == first index of b'\r' after `pos`
                // pos + end + 2 skip to after CLRF
                Some(Ok((
                    String::from_utf8(buf[pos..end + pos].to_vec()).unwrap(),
                    pos + end + 2,
                )))
            } else {
                Some(Err(()))
            }
        })
        .expect("Present value")
}

fn simple_string(buf: &[u8], pos: usize) -> Result<(RedisType, usize), ()> {
    match word(buf, pos) {
        Ok((str, pos)) => Ok((RedisType::String(str), pos)),
        Err(_) => Err(()),
    }
}

fn bulk_string(buf: &[u8], pos: usize) -> Result<(RedisType, usize), ()> {
    match word(buf, pos) {
        Ok((len_str, new_pos)) => {
            let len: i32 = len_str.parse().unwrap();
            if len < 0 {
                Err(())
            } else if len == 0 {
                Ok((RedisType::String("".to_string()), new_pos as usize))
            } else {
                Ok((
                    RedisType::String(
                        String::from_utf8(buf[new_pos..new_pos + len as usize].to_vec()).unwrap(),
                    ),
                    new_pos + 2 + len as usize,
                ))
            }
        }
        Err(_) => Err(()),
    }
}

fn array(buf: &[u8], pos: usize) -> Result<(RedisType, usize), ()> {
    match word(buf, pos) {
        Ok((num_str, new_pos)) => {
            let num: i32 = num_str.parse().unwrap();
            if num < 0 {
                Err(())
            } else if num == 0 {
                Ok((RedisType::Array(vec![]), pos))
            } else {
                let mut values = Vec::with_capacity(num as usize);
                let mut curr_pos = new_pos;
                for _ in 0..num {
                    match parse_redis(buf, curr_pos) {
                        Ok((value, new_pos)) => {
                            curr_pos = new_pos;
                            values.push(value);
                        }
                        Err(_) => return Err(()),
                    }
                }
                Ok((RedisType::Array(values), new_pos))
            }
        }
        Err(_) => Err(()),
    }
}

fn encode_bulk(str: String) -> Vec<u8> {
    format!("${}\r\n{}\r\n", str.len(), str).into_bytes()
}

fn encode_simple(str: String) -> Vec<u8> {
    format!("+{}\r\n", str).into_bytes()
}

fn encode_int(val: String) -> Vec<u8> {
    format!(":{}\r\n", val).into_bytes()
}

fn encode_error(str: String) -> Vec<u8> {
    format!("-ERR {}\r\n", str).into_bytes()
}

fn encode_array(vec: Vec<Vec<u8>>) -> Vec<u8> {
    let mut res = format!("*{}\r\n", vec.len());

    for s in vec {
        res.push_str(std::str::from_utf8(&s).unwrap());
    }

    res.into_bytes()
}
