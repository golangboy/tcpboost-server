use std::collections::{BTreeMap, HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, mpsc};
use std::ptr;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt as _;
use std::time::Instant;

#[derive(Debug, Clone)]
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
    except_id: Arc<Mutex<HashMap<u32, u32>>>,
    sender_id: Arc<Mutex<HashMap<u32, u32>>>,
    connect_num: Arc<Mutex<HashMap<u32, u32>>>,
    broadcast_sender: Arc<Mutex<HashMap<u32, Vec<mpsc::Sender<Vec<u8>>>>>>,
    pending_messages: Arc<Mutex<HashMap<u32, VecDeque<(u32, Vec<u8>)>>>>,
    file: Arc<Mutex<tokio::fs::File>>,
    total_data_size: Arc<Mutex<usize>>,
    start_time: Arc<Mutex<Option<Instant>>>,
}

impl ClientManager {
    async fn new() -> Self {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open("aa.bin")
            .await
            .expect("Failed to open aa.bin");

        ClientManager {
            client_msg: Arc::new(Mutex::new(HashMap::new())),
            except_id: Arc::new(Mutex::new(HashMap::new())),
            sender_id: Arc::new(Mutex::new(HashMap::new())),
            connect_num: Arc::new(Mutex::new(HashMap::new())),
            broadcast_sender: Arc::new(Mutex::new(HashMap::new())),
            pending_messages: Arc::new(Mutex::new(HashMap::new())),
            file: Arc::new(Mutex::new(file)),
            total_data_size: Arc::new(Mutex::new(0)),
            start_time: Arc::new(Mutex::new(None)),
        }
    }

    async fn handle_msg(&self, msg_block: MsgBlock) {
        let client_id = msg_block.client_id;
        let client_seq = msg_block.seq;

        let mut start_time = self.start_time.lock().await;
        if start_time.is_none() {
            *start_time = Some(Instant::now());
        }
        drop(start_time);

        let mut total_data_size = self.total_data_size.lock().await;
        *total_data_size += msg_block.data.len();

        if *total_data_size >= 2000000 {
            let elapsed = self.start_time.lock().await.unwrap().elapsed();
            println!("Time taken to receive {:?} bytes: {:?}",*total_data_size, elapsed);
            std::process::exit(0);
        }
        drop(total_data_size);

        let mut pending_messages = self.pending_messages.lock().await;
        let pending = pending_messages.entry(client_id).or_insert_with(VecDeque::new);
        pending.push_back((client_seq, msg_block.data));

        self.process_pending_messages(client_id, pending).await;
    }

    async fn process_pending_messages(&self, client_id: u32, pending: &mut VecDeque<(u32, Vec<u8>)>) {
        let mut except_id_map = self.except_id.lock().await;
        let mut except_id = *except_id_map.entry(client_id).or_insert(0);

        let mut to_write = Vec::new();

        while let Some(&(seq, _)) = pending.front() {
            if seq == except_id {
                let (_, data) = pending.pop_front().unwrap();
                to_write.push(data);
                except_id += 1;
            } else if seq < except_id {
                pending.pop_front();
            } else {
                break;
            }
        }

        *except_id_map.get_mut(&client_id).unwrap() = except_id;
        drop(except_id_map);

        for data in to_write {
            self.write_to_file(data).await;
        }
    }

    async fn write_to_file(&self, data: Vec<u8>) {
        let mut file = self.file.lock().await;
        if let Err(e) = file.write_all(&data).await {
            eprintln!("Failed to write to aa.bin: {}", e);
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

        let senders = {
            let broadcast_sender = self.broadcast_sender.lock().await;
            broadcast_sender.get(&client_id).cloned().unwrap_or_default()
        };

        for sender in senders {
            if let Err(e) = sender.send(serialized.clone()).await {
                eprintln!("Failed to send message: {}", e);
            }
        }
    }

    async fn add_client_connection(&self, client_id: u32, tx: mpsc::Sender<Vec<u8>>) {
        let mut connect_num = self.connect_num.lock().await;
        *connect_num.entry(client_id).or_insert(0) += 1;

        let mut broadcast_sender = self.broadcast_sender.lock().await;
        broadcast_sender.entry(client_id).or_default().push(tx);
    }

    async fn remove_client_connection(&self, client_id: u32, tx: &mpsc::Sender<Vec<u8>>) {
        let mut connect_num = self.connect_num.lock().await;
        let mut broadcast_sender = self.broadcast_sender.lock().await;

        if let Some(count) = connect_num.get_mut(&client_id) {
            *count -= 1;
            if *count <= 0 {
                connect_num.remove(&client_id);
                self.client_msg.lock().await.remove(&client_id);
                self.except_id.lock().await.remove(&client_id);
                self.sender_id.lock().await.remove(&client_id);
                broadcast_sender.remove(&client_id);
                println!("Client {} fully disconnected", client_id);
            } else if let Some(senders) = broadcast_sender.get_mut(&client_id) {
                senders.retain(|s| !ptr::eq(s, tx));
            }
        }
    }
}

struct Server {
    client_manager: Arc<ClientManager>,
}

impl Server {
    async fn new() -> Self {
        Server {
            client_manager: Arc::new(ClientManager::new().await),
        }
    }

    async fn start(&self, addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(addr).await?;
        println!("Server listening on {}", addr);

        loop {
            let (socket, addr) = listener.accept().await?;
            let client_manager = self.client_manager.clone();

            tokio::spawn(async move {
                Self::handle_client(socket, client_manager, addr).await;
            });
        }
    }

    async fn handle_client(socket: TcpStream, client_manager: Arc<ClientManager>, addr: SocketAddr) {
        let (mut reader, mut writer) = socket.into_split();
        let (tx, mut rx) = mpsc::channel::<Vec<u8>>(100);

        let mut client_id = None;

        tokio::spawn(async move {
            while let Some(data) = rx.recv().await {
                if let Err(e) = writer.write_all(&data).await {
                    eprintln!("Failed to write to socket: {}", e);
                    break;
                }
            }
        });

        loop {
            let msg_block = match Self::read_msg_block(&mut reader).await {
                Ok(Some(block)) => block,
                Ok(None) => break,
                Err(e) => {
                    eprintln!("Error reading message: {}", e);
                    break;
                }
            };

            if client_id.is_none() {
                client_id = Some(msg_block.client_id);
                client_manager.add_client_connection(msg_block.client_id, tx.clone()).await;
                println!("New connection for client {}: {}", msg_block.client_id, addr);
            }

            client_manager.handle_msg(msg_block).await;
        }

        if let Some(id) = client_id {
            client_manager.remove_client_connection(id, &tx).await;
            println!("Connection closed for client {}: {}", id, addr);
        }
    }

    async fn read_msg_block(reader: &mut tokio::net::tcp::OwnedReadHalf) -> Result<Option<MsgBlock>, std::io::Error> {
        let mut header = [0u8; 20];
        if let Err(e) = reader.read_exact(&mut header).await {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                return Ok(None);
            }
            return Err(e);
        }

        let magic = u32::from_be_bytes([header[0], header[1], header[2], header[3]]);
        let cmd = u32::from_be_bytes([header[4], header[5], header[6], header[7]]);
        let client_id = u32::from_be_bytes([header[8], header[9], header[10], header[11]]);
        let data_size = u32::from_be_bytes([header[12], header[13], header[14], header[15]]);
        let seq = u32::from_be_bytes([header[16], header[17], header[18], header[19]]);

        let mut data = vec![0u8; data_size as usize];
        reader.read_exact(&mut data).await?;

        Ok(Some(MsgBlock {
            magic,
            cmd,
            client_id,
            data_size,
            seq,
            data,
        }))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server = Server::new().await;
    server.start("127.0.0.1:8080".parse()?).await?;
    Ok(())
}