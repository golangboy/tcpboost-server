use std::io::Cursor;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt};
use byteorder::{BigEndian, ReadBytesExt};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

struct MsgBlock {
    magic: u32, //0x11223344
    client_id: u32,
    data_size: u32,
    server_seq: u32,
    client_seq: u32,
    data: Vec<u8>,
}

type SocketMap = Arc<Mutex<HashMap<u32, Vec<Arc<Mutex<TcpStream>>>>>>;

async fn handle_msg(socket: &mut TcpStream, msg_block: MsgBlock) {
    // 处理消息的逻辑
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:8080").await?;
    println!("Server listening on 127.0.0.1:8080");

    let socket_map: SocketMap = Arc::new(Mutex::new(HashMap::new()));

    loop {
        let (socket, addr) = listener.accept().await?;
        println!("New client connected: {}", addr);

        let socket_arc = Arc::new(Mutex::new(socket));
        let socket_map_clone = Arc::clone(&socket_map);
        let socket_clone = Arc::clone(&socket_arc);

        tokio::spawn(async move {
            let mut client_id = 0;

            loop {
                let mut socket = socket_clone.lock().await;
                let mut header = [0u8; 20];
                match socket.read_exact(&mut header).await {
                    Ok(0) => break, // Connection closed
                    Ok(_) => (),
                    Err(e) => {
                        eprintln!("Failed to read from socket: {}", e);
                        break;
                    }
                }

                let mut cursor = Cursor::new(header);
                let magic = ReadBytesExt::read_u32::<BigEndian>(&mut cursor).unwrap();
                client_id = ReadBytesExt::read_u32::<BigEndian>(&mut cursor).unwrap();
                let data_size = ReadBytesExt::read_u32::<BigEndian>(&mut cursor).unwrap();
                let server_seq = ReadBytesExt::read_u32::<BigEndian>(&mut cursor).unwrap();
                let client_seq = ReadBytesExt::read_u32::<BigEndian>(&mut cursor).unwrap();

                if magic != 0x11223344 {
                    eprintln!("Invalid magic number");
                    break;
                }


                let mut data = vec![0u8; data_size as usize];
                if let Err(e) = socket.read_exact(&mut data).await {
                    eprintln!("Failed to read data: {}", e);
                    break;
                }

                let msg_block = MsgBlock {
                    magic,
                    client_id,
                    data_size,
                    server_seq,
                    client_seq,
                    data,
                };

                // 更新 socket_map 中的 client_id
                {
                    let mut map = socket_map_clone.lock().await;
                    if let Some(sockets) = map.get_mut(&0) {
                        if let Some(index) = sockets.iter().position(|s| Arc::ptr_eq(s, &socket_clone)) {
                            let socket = sockets.remove(index);
                            map.entry(client_id).or_insert_with(Vec::new).push(socket);
                        }
                    }
                }

                handle_msg(&mut *socket, msg_block).await;
            }

            // 当连接关闭时，从 HashMap 中移除这个 socket
            let mut map = socket_map_clone.lock().await;
            if let Some(sockets) = map.get_mut(&client_id) {
                sockets.retain(|s| !Arc::ptr_eq(s, &socket_clone));
                if sockets.is_empty() {
                    map.remove(&client_id);
                }
            }

            println!("Client {} disconnected", addr);
        });
    }
}