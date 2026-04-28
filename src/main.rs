#![allow(unused_imports)]
use std::{
    io::Write,
    net::{TcpListener, TcpStream},
};

fn main() {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("accepted new connection");
                answer(stream);
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn answer(mut stream: TcpStream) {
    let _ = stream.write_all(b"+PONG\r\n");
}
