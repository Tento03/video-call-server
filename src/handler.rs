use axum::{
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::IntoResponse,
};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc::unbounded_channel;
use tracing::{debug, info, warn};

use crate::messages::{Role, Signal, send, send_error};
use crate::rooms::{Peer, Room, Rooms, leave_room, random_code};

// Batas ukuran satu pesan. SDP biasanya beberapa KB, jadi 16 KB sudah longgar.
const MAX_MESSAGE_BYTES: usize = 16 * 1024;

// Batas panjang satu pesan chat (dalam karakter)
const MAX_CHAT_CHARS: usize = 500;

// Rapikan nama: buang spasi pinggir, tolak kalau kosong atau lebih dari 30 karakter
fn clean_name(raw: &str) -> Option<String> {
    let name = raw.trim();
    if name.is_empty() || name.chars().count() > 30 {
        None
    } else {
        Some(name.to_string())
    }
}

pub async fn ws_handler(ws: WebSocketUpgrade, State(rooms): State<Rooms>) -> impl IntoResponse {
    // Pesan yang lebih besar dari batas membuat koneksi itu diputus
    ws.max_message_size(MAX_MESSAGE_BYTES)
        .max_frame_size(MAX_MESSAGE_BYTES)
        .on_upgrade(move |socket| handle_socket(socket, rooms))
}

// Dijalankan untuk setiap klien yang tersambung
async fn handle_socket(socket: WebSocket, rooms: Rooms) {
    // Pecah socket: sink = untuk mengirim, stream = untuk menerima
    let (mut sink, mut stream) = socket.split();

    // Saluran internal: bagian lain program menaruh pesan di tx,
    // task di bawah mengambilnya dari rx lalu mengirim ke klien.
    let (tx, mut rx) = unbounded_channel::<Message>();

    let send_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if sink.send(msg).await.is_err() {
                break;
            }
        }
    });

    let my_id: u64 = rand::random();
    let mut my_room: Option<String> = None;

    while let Some(Ok(msg)) = stream.next().await {
        let text = match msg {
            Message::Text(t) => t,
            Message::Close(_) => break,
            _ => continue, // ping/binary diabaikan
        };

        // Pesan rusak tidak boleh bikin server crash: balas error, lanjut
        let signal: Signal = match serde_json::from_str(text.as_str()) {
            Ok(s) => s,
            Err(_) => {
                warn!("menerima pesan tidak valid dari klien");
                send_error(&tx, "pesan tidak valid");
                continue;
            }
        };

        match signal {
            Signal::Create { name } => {
                if my_room.is_some() {
                    send_error(&tx, "kamu sudah ada di room");
                    continue;
                }
                let Some(name) = clean_name(&name) else {
                    send_error(&tx, "nama harus 1-30 karakter");
                    continue;
                };
                let mut map = rooms.lock().unwrap();
                let code = loop {
                    let c = random_code();
                    if !map.contains_key(&c) {
                        break c;
                    }
                };
                map.insert(
                    code.clone(),
                    Room {
                        peers: vec![Peer {
                            id: my_id,
                            name,
                            role: Role::Interviewer, // pembuat room = pewawancara
                            tx: tx.clone(),
                        }],
                    },
                );
                drop(map); // lepas lock secepatnya
                my_room = Some(code.clone());
                info!(room = %code, "room dibuat");
                send(&tx, &Signal::Created { room: code });
            }

            Signal::Join { room, name } => {
                if my_room.is_some() {
                    send_error(&tx, "kamu sudah ada di room");
                    continue;
                }
                let Some(name) = clean_name(&name) else {
                    send_error(&tx, "nama harus 1-30 karakter");
                    continue;
                };
                let code = room.to_uppercase();
                if code.len() != 6 {
                    send_error(&tx, "kode room tidak valid");
                    continue;
                }
                let mut map = rooms.lock().unwrap();
                match map.get_mut(&code) {
                    None => send_error(&tx, "room tidak ditemukan"),
                    Some(r) if r.peers.len() >= 2 => send_error(&tx, "room penuh"),
                    Some(r) => {
                        info!(room = %code, "peserta bergabung");
                        for p in &r.peers {
                            // Beri tahu yang sudah ada: siapa yang baru masuk
                            send(
                                &p.tx,
                                &Signal::PeerJoined {
                                    name: name.clone(),
                                    role: Role::Candidate,
                                },
                            );
                            // Beri tahu yang baru masuk: siapa yang sudah ada
                            send(
                                &tx,
                                &Signal::Joined {
                                    name: p.name.clone(),
                                    role: p.role,
                                },
                            );
                        }
                        r.peers.push(Peer {
                            id: my_id,
                            name,
                            role: Role::Candidate, // yang bergabung = kandidat
                            tx: tx.clone(),
                        });
                        my_room = Some(code);
                    }
                }
            }

            // Offer, answer, dan ICE hanya diteruskan ke peserta lain
            Signal::Offer { .. } | Signal::Answer { .. } | Signal::Ice { .. } => {
                let Some(code) = &my_room else {
                    send_error(&tx, "kamu belum masuk room");
                    continue;
                };
                debug!(room = %code, "meneruskan pesan signaling");
                let map = rooms.lock().unwrap();
                if let Some(r) = map.get(code) {
                    for p in &r.peers {
                        if p.id != my_id {
                            // teruskan teks aslinya apa adanya
                            p.tx.send(Message::Text(text.clone())).ok();
                        }
                    }
                }
            }

            // Chat: validasi, lalu teruskan ke peserta lain beserta nama pengirim
            Signal::Chat { text: isi } => {
                let Some(code) = &my_room else {
                    send_error(&tx, "kamu belum masuk room");
                    continue;
                };
                let isi = isi.trim();
                if isi.is_empty() || isi.chars().count() > MAX_CHAT_CHARS {
                    send_error(&tx, "pesan chat harus 1-500 karakter");
                    continue;
                }
                let map = rooms.lock().unwrap();
                if let Some(r) = map.get(code) {
                    // Nama pengirim diambil dari data server, bukan dari klien
                    let sender = r
                        .peers
                        .iter()
                        .find(|p| p.id == my_id)
                        .map(|p| p.name.clone())
                        .unwrap_or_default();
                    let keluar = Signal::ChatMessage {
                        name: sender,
                        text: isi.to_string(),
                    };
                    for p in &r.peers {
                        if p.id != my_id {
                            send(&p.tx, &keluar);
                        }
                    }
                }
            }

            // Klien tidak boleh mengirim pesan khusus server
            _ => send_error(&tx, "jenis pesan tidak dikenal"),
        }
    }

    // Sampai sini klien putus (tutup tab, error, dll): bersihkan
    leave_room(&rooms, &my_room, my_id);
    send_task.abort();
    info!("klien putus");
}
