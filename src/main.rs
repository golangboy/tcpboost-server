use std::io::Cursor;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt};
use byteorder::{BigEndian, ReadBytesExt};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Mutex;
use std::collections::BTreeMap;
use lazy_static::lazy_static;
use tokio::io::AsyncWriteExt;


struct MsgBlock {
    magic: u32, //0x11223344
    cmd: u32,
    client_id: u32,
    data_size: u32,
    seq: u32,
    data: Vec<u8>,
}
lazy_static! {
    static ref CLIENT_MSG: Arc<Mutex<HashMap<u32, BTreeMap<u32, Vec<u8>>>>> = Arc::new(Mutex::new(HashMap::new()));
    static ref SOCKET_MAP:SocketMap=Arc::new(Mutex::new(HashMap::new()));
    static ref EXCEPT_ID:Arc<Mutex<HashMap<u32,u32>>> = Arc::new(Mutex::new(HashMap::new()));
    static ref SENDER_ID:Arc<Mutex<HashMap<u32,u32>>> = Arc::new(Mutex::new(HashMap::new()));
}
type SocketMap = Arc<Mutex<HashMap<u32, Vec<Arc<Mutex<TcpStream>>>>>>;

// CLIENT_MSG EXCEPT_ID SENDER_ID
async fn handle_msg(msg_block: MsgBlock) {
    let client_id = msg_block.client_id;
    let client_seq = msg_block.seq;
    let client_msg_map_clone = CLIENT_MSG.clone();
    let mut client_msg_map = client_msg_map_clone.lock().await;
    client_msg_map
        .entry(client_id)
        .or_insert_with(BTreeMap::new)
        .insert(client_seq, msg_block.data);
    let mut except_id_clone = EXCEPT_ID.clone();
    let mut except_id_map = except_id_clone.lock().await;
    let except_id = *except_id_map.entry(client_id).or_insert(0);

    if let Some(data_map) = client_msg_map.get_mut(&client_id) {
        if let Some(recv_data) = data_map.get(&except_id) {
            println!("Received data for client {}, sequence {}: {:?}", client_id, except_id, recv_data);
            write_to(client_id, "helloworld".as_bytes().to_vec()).await;
            data_map.remove(&except_id);
            except_id_map.insert(client_id, except_id + 1);
        } else {
            //println!("No data found for client {}, sequence {}", client_id, except_id);
        }
    } else {
        //println!("No data map found for client {}", client_id);
    }
}
async fn write_to(client_id: u32, data: Vec<u8>) {

    // 构造 MsgBlock 结构
    let msg_block = {
        let mut sender_id_clone = SENDER_ID.clone();
        let mut sender_id_map = sender_id_clone.lock().await;
        let seq = *(sender_id_map.entry(client_id).or_insert(0));

        // 增加 sender_id
        sender_id_map.insert(client_id, seq + 1);

        MsgBlock {
            magic: 0x11223344,
            cmd: 0, // 假设 0 为标准消息，根据需要调整
            client_id,
            data_size: data.len() as u32,
            seq,
            data,
        }
    };

    // 序列化 MsgBlock
    let mut serialized = Vec::new();
    serialized.extend_from_slice(&msg_block.magic.to_be_bytes());
    serialized.extend_from_slice(&msg_block.cmd.to_be_bytes());
    serialized.extend_from_slice(&msg_block.client_id.to_be_bytes());
    serialized.extend_from_slice(&msg_block.data_size.to_be_bytes());
    serialized.extend_from_slice(&msg_block.seq.to_be_bytes());
    serialized.extend_from_slice(&msg_block.data);

    // 获取客户端 ID 的所有 socket
    let socket_map_clone = SOCKET_MAP.clone();
    let socket_map = socket_map_clone.lock().await;
    if let Some(sockets) = socket_map.get(&client_id) {
        for socket_arc in sockets {
            // let mut socket = socket_arc.lock().await;
            // if let Err(e) = socket.write_all(&serialized).await {
            //     eprintln!("Failed to write to socket for client {}: {}", client_id, e);
            // }
        }
    } else {
        println!("No sockets found for client {}", client_id);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:8080").await?;
    println!("Server listening on 127.0.0.1:8080");


    let addr2socket: Arc<Mutex<HashMap<String, Arc<Mutex<tokio::net::TcpStream>>>>> = Arc::new(Mutex::new(HashMap::new()));
    let client_id2addr: Arc<Mutex<HashMap<u32, HashSet<String>>>> = Arc::new(Mutex::new(HashMap::new()));
    let client_addr2id: Arc<Mutex<HashMap<String, u32>>> = Arc::new(Mutex::new(HashMap::new()));
    loop {
        let (socket, addr) = listener.accept().await?;
        println!("New client connected: {}", addr);

        let socket_arc = Arc::new(Mutex::new(socket));
        let socket_clone = Arc::clone(&socket_arc);
        let client_id2addr_clone = client_id2addr.clone();
        let client_addr2id_clone = client_addr2id.clone();
        let client_addr2id_clone2 = client_addr2id.clone();
        let mut except_id_clone = EXCEPT_ID.clone();
        let socket_address: String = addr.to_string();
        {
            let addr2socket_clone = addr2socket.clone();
            let mut a = addr2socket_clone.lock().await;
            a.entry(socket_address.clone()).or_insert(socket_arc.clone());
        }
        tokio::spawn(async move {
            loop {
                let msg_block: MsgBlock;
                {
                    let mut client_id = 0;
                    let mut magic = 0;
                    let mut cmd = 0;
                    let mut data_size = 0;
                    let mut seq = 0;
                    let mut data: Vec<u8>;
                    {
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
                        magic = ReadBytesExt::read_u32::<BigEndian>(&mut cursor).unwrap();
                        cmd = ReadBytesExt::read_u32::<BigEndian>(&mut cursor).unwrap();
                        client_id = ReadBytesExt::read_u32::<BigEndian>(&mut cursor).unwrap();
                        data_size = ReadBytesExt::read_u32::<BigEndian>(&mut cursor).unwrap();
                        seq = ReadBytesExt::read_u32::<BigEndian>(&mut cursor).unwrap();
                        if magic != 0x11223344 {
                            eprintln!("Invalid magic number");
                            break;
                        }


                        data = vec![0u8; data_size as usize];
                        if let Err(e) = socket.read_exact(&mut data).await {
                            eprintln!("Failed to read data: {}", e);
                            break;
                        }
                    }

                    {
                        let client_id2addr_clone_c = client_id2addr_clone.clone();
                        let mut a = client_id2addr_clone_c.lock().await;
                        a.entry(client_id).or_insert_with(HashSet::new).insert(socket_address.clone());
                    }

                    {
                        let client_addr2id_clone_c = client_addr2id_clone.clone();
                        let mut b = client_addr2id_clone_c.lock().await;
                        if !b.contains_key(&socket_address) {
                            let socket_map_clone = Arc::clone(&SOCKET_MAP);
                            socket_map_clone.lock().await.entry(client_id).or_insert_with(Vec::new).push(socket_arc.clone());
                        }
                        b.entry(socket_address.clone()).or_insert(client_id);
                    }


                    msg_block = MsgBlock {
                        magic,
                        cmd,
                        client_id,
                        data_size,
                        seq,
                        data,
                    };
                }


                handle_msg(msg_block).await;
            }

            // 当连接关闭时，从 HashMap 中移除这个 socket
            println!("666666666");
            let client_id = client_addr2id_clone2.lock().await.get(&socket_address).unwrap().clone();
            let socket_map_clone2 = Arc::clone(&SOCKET_MAP);
            let mut map = socket_map_clone2.lock().await;
            println!("11111");
            let mut client_msg_map_clone = CLIENT_MSG.clone();
            println!("33333");
            if let Some(sockets) = map.get_mut(&client_id) {
                sockets.retain(|s| !Arc::ptr_eq(s, &socket_clone));
                if sockets.is_empty() {
                    map.remove(&client_id);
                    {
                        let mut except_id = except_id_clone.lock().await;
                        println!("22222");
                        except_id.remove(&client_id);
                    }
                    {
                        let mut client_msg = client_msg_map_clone.lock().await;
                        println!("777777777");
                        client_msg.remove(&client_id);
                    }
                    println!("Client {} disconnected", client_id);
                }
            }
        });
    }
}