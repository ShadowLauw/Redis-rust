#![allow(unused_imports)]

use core::num;

use anyhow::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use memchr::memchr;

enum RedisType {
    Array(Vec<RedisType>),
    String(String),
}

#[tokio::main]
async fn main() {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        let stream = listener.accept().await;
        match stream {
            Ok((stream, _)) => {
                println!("accepted new connection");
                tokio::spawn(async move { answer(stream).await });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

async fn answer(mut stream: TcpStream) {
    let mut buf = [0; 512];
    loop {
        let bytes_read = stream.read(&mut buf).await.unwrap();
        if bytes_read == 0 {
            break;
        }
        match parse_redis(&buf, 0) {
            Ok((RedisType::Array(arr), _)) => match &arr[0] {
                RedisType::Array(_) => break,
                RedisType::String(str) => match str.to_ascii_lowercase().as_str() {
                    "echo" => match &arr[1] {
                        RedisType::Array(_) => break,
                        RedisType::String(str) => {
                            stream.write_all(&encode_bulk(&str)).await.unwrap()
                        }
                    },
                    "ping" => stream.write_all(b"+PONG\r\n").await.unwrap(),
                    _ => break,
                },
            },
            _ => break,
        }
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

fn encode_bulk(str: &String) -> Vec<u8> {
    format!("${}\r\n{}\r\n", str.len(), str).into_bytes()
}
