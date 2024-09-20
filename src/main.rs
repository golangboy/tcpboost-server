use std::io::Cursor;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use byteorder::{BigEndian, ReadBytesExt};
use std::collections::{HashMap, HashSet, BTreeMap};
use std::sync::Arc;
use tokio::sync::Mutex;
use std::time::Duration;
use tokio::time::timeout;

const TIMEOUT_DURATION: Duration = Duration::from_secs(15);

struct MsgBlock {
    magic: u32,
    cmd: u32,
    client_id: u32,
    data_size: u32,
    seq: u32,
    data: Vec<u8>,
}

struct ClientManager {
    client_msg: Arc<Mutex<HashMap<u32, BTreeMap<u32, Vec<u8>>>>>,
    socket_map: Arc<Mutex<HashMap<u32, Vec<Arc<Mutex<TcpStream>>>>>>,
    except_id: Arc<Mutex<HashMap<u32, u32>>>,
    sender_id: Arc<Mutex<HashMap<u32, u32>>>,
}

impl ClientManager {
    fn new() -> Self {
        ClientManager {
            client_msg: Arc::new(Mutex::new(HashMap::new())),
            socket_map: Arc::new(Mutex::new(HashMap::new())),
            except_id: Arc::new(Mutex::new(HashMap::new())),
            sender_id: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn handle_msg(&self, msg_block: MsgBlock) {
        let client_id = msg_block.client_id;
        let client_seq = msg_block.seq;
        let mut client_msg_map = self.client_msg.lock().await;
        client_msg_map
            .entry(client_id)
            .or_insert_with(BTreeMap::new)
            .insert(client_seq, msg_block.data);
        let mut except_id_map = self.except_id.lock().await;
        let except_id = *except_id_map.entry(client_id).or_insert(0);
        if let Some(data_map) = client_msg_map.get_mut(&client_id) {
            if let Some(recv_data) = data_map.get(&except_id) {
                self.write_to(client_id, recv_data.clone()).await;
                // println!("回写");
                data_map.remove(&except_id);
                except_id_map.insert(client_id, except_id + 1);
            }
        }
    }

    async fn write_to(&self, client_id: u32, data: Vec<u8>) {
        let msg_block = {
            let mut sender_id_map = self.sender_id.lock().await;
            let seq = *sender_id_map.entry(client_id).or_insert(0);
            sender_id_map.insert(client_id, seq + 1);

            MsgBlock {
                magic: 0x11223344,
                cmd: 0,
                client_id,
                data_size: data.len() as u32,
                seq,
                data,
            }
        };

        let mut serialized = Vec::new();
        serialized.extend_from_slice(&msg_block.magic.to_be_bytes());
        serialized.extend_from_slice(&msg_block.cmd.to_be_bytes());
        serialized.extend_from_slice(&msg_block.client_id.to_be_bytes());
        serialized.extend_from_slice(&msg_block.data_size.to_be_bytes());
        serialized.extend_from_slice(&msg_block.seq.to_be_bytes());
        serialized.extend_from_slice(&msg_block.data);

        let socket_map = self.socket_map.lock().await;
        if let Some(sockets) = socket_map.get(&client_id) {
            for socket_arc in sockets {
                let mut socket = socket_arc.lock().await;
                if let Err(e) = socket.write_all(&serialized).await {
                    eprintln!("Failed to write to socket for client {}: {}", client_id, e);
                }
            }
        } else {
            println!("No sockets found for client {}", client_id);
        }
    }
}

struct Server {
    client_manager: Arc<ClientManager>,
    client_id2addr: Arc<Mutex<HashMap<u32, HashSet<String>>>>,
    client_addr2id: Arc<Mutex<HashMap<String, u32>>>,
}

impl Server {
    fn new() -> Self {
        Server {
            client_manager: Arc::new(ClientManager::new()),
            client_id2addr: Arc::new(Mutex::new(HashMap::new())),
            client_addr2id: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("0.0.0.0:8080").await?;
        println!("Server listening on 0.0.0.0:8080");

        loop {
            let (socket, addr) = listener.accept().await?;
            // println!("New client connected: {}", addr);

            let socket_arc = Arc::new(tokio::sync::Mutex::new(socket));
            let socket_clone = Arc::clone(&socket_arc);
            let client_manager = self.client_manager.clone();
            let client_id2addr_clone = self.client_id2addr.clone();
            let client_addr2id_clone = self.client_addr2id.clone();
            let client_addr2id_clone2 = self.client_addr2id.clone();
            let socket_address: String = addr.to_string();

            tokio::spawn(async move {
                Self::handle_client(
                    socket_clone,
                    client_manager,
                    client_id2addr_clone,
                    client_addr2id_clone,
                    client_addr2id_clone2,
                    socket_address,
                ).await;
            });
        }
    }

    async fn handle_client(
        socket_clone: Arc<Mutex<TcpStream>>,
        client_manager: Arc<ClientManager>,
        client_id2addr_clone: Arc<Mutex<HashMap<u32, HashSet<String>>>>,
        client_addr2id_clone: Arc<Mutex<HashMap<String, u32>>>,
        client_addr2id_clone2: Arc<Mutex<HashMap<String, u32>>>,
        socket_address: String,
    ) {
        let mut is_has_clientid = false;
        loop {
            let msg_block = match Self::read_msg_block(&socket_clone).await {
                Ok(Some(block)) => block,
                Ok(None) => break,
                Err(_e) => {
                    // eprintln!("Error reading message: {}", _e);
                    break;
                }
            };
            is_has_clientid = true;
            // println!("{} #### {}", socket_address, msg_block.client_id);
            {
                let mut b = client_addr2id_clone.lock().await;
                if !b.contains_key(&socket_address) {
                    let mut socket_map = client_manager.socket_map.lock().await;
                    socket_map.entry(msg_block.client_id).or_insert_with(Vec::new).push(socket_clone.clone());
                }
                b.entry(socket_address.clone()).or_insert(msg_block.client_id);
            }
            {
                let mut a = client_id2addr_clone.lock().await;
                a.entry(msg_block.client_id).or_insert_with(HashSet::new).insert(socket_address.clone());
            }

            client_manager.handle_msg(msg_block).await;
        }
        if is_has_clientid {
            // 当连接关闭时，从 HashMap 中移除这个 socket
            Self::remove_disconnected_client(&client_manager, &client_addr2id_clone2, &socket_address, &socket_clone).await;
        }
    }

    async fn read_msg_block(socket_clone: &Arc<Mutex<TcpStream>>) -> Result<Option<MsgBlock>, Box<dyn std::error::Error>> {
        let mut header = [0u8; 20];
        let mut socket = socket_clone.lock().await;
        match timeout(TIMEOUT_DURATION, socket.read_exact(&mut header)).await {
            Ok(Ok(0)) => return Ok(None), // Connection closed
            Ok(Ok(_)) => (),
            Ok(Err(e)) => return Err(Box::new(e)),
            Err(_) => return Err("Read operation timed out".into()),
        }

        let mut cursor = Cursor::new(header);
        let magic = ReadBytesExt::read_u32::<BigEndian>(&mut cursor)?;
        let cmd = ReadBytesExt::read_u32::<BigEndian>(&mut cursor)?;
        let client_id = ReadBytesExt::read_u32::<BigEndian>(&mut cursor)?;
        let data_size = ReadBytesExt::read_u32::<BigEndian>(&mut cursor)?;
        let seq = ReadBytesExt::read_u32::<BigEndian>(&mut cursor)?;

        if magic != 0x11223344 {
            return Err("Invalid magic number".into());
        }

        let mut data = vec![0u8; data_size as usize];
        socket.read_exact(&mut data).await?;

        Ok(Some(MsgBlock {
            magic,
            cmd,
            client_id,
            data_size,
            seq,
            data,
        }))
    }

    async fn remove_disconnected_client(
        client_manager: &ClientManager,
        client_addr2id: &Arc<Mutex<HashMap<String, u32>>>,
        socket_address: &str,
        socket_clone: &Arc<Mutex<TcpStream>>,
    ) {
        let mut client_msg = client_manager.client_msg.lock().await;
        let mut except_id = client_manager.except_id.lock().await;
        let mut sender_id = client_manager.sender_id.lock().await;
        let client_id = client_addr2id.lock().await.get(socket_address).unwrap().clone();
        let mut map = client_manager.socket_map.lock().await;
        if let Some(sockets) = map.get_mut(&client_id) {
            sockets.retain(|s| !Arc::ptr_eq(s, socket_clone));
            if sockets.is_empty() {
                map.remove(&client_id);
                except_id.remove(&client_id);
                client_msg.remove(&client_id);
                sender_id.remove(&client_id);
                println!("Client {} disconnected", client_id);
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server = Server::new();
    server.run().await
}