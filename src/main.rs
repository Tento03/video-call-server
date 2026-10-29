use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::IntoResponse,
    routing::get,
    Router,
};
use tower_http::services::ServeDir;
use futures_util::{SinkExt, StreamExt};
use rand::RngExt;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};

// ---------- Tipe data ----------

// Pesan signaling (JSON). Field "type" menentukan variannya.
// Contoh: {"type":"join","room":"AB12CD"}
#[derive(Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
enum Signal {
    // dari klien
    Create,
    Join { room: String },
    Offer { sdp: String },
    Answer { sdp: String },
    Ice { candidate: serde_json::Value },
    // dari server
    Created { room: String },
    PeerJoined,
    PeerLeft,
    Error { message: String },
}

// Saluran untuk mengirim pesan ke satu klien
type Tx = UnboundedSender<Message>;

struct Peer {
    id: u64,
    tx: Tx,
}

struct Room {
    peers: Vec<Peer>, // maksimal 2
}

// State bersama: peta kode room -> Room.
// Arc = bisa dibagi ke banyak task, Mutex = hanya satu yang akses pada satu waktu.
type Rooms = Arc<Mutex<HashMap<String, Room>>>;

// ---------- main ----------

#[tokio::main]
async fn main() {
    let rooms: Rooms = Arc::new(Mutex::new(HashMap::new()));

    let app = Router::new()
        .route("/health", get(health))
        .route("/ws", get(ws_handler))
        .fallback_service(ServeDir::new("static"))
        .with_state(rooms);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000")
        .await
        .unwrap();

    println!("Server jalan di http://localhost:3000");
    axum::serve(listener, app).await.unwrap();
}

async fn health() -> &'static str {
    "ok"
}

async fn ws_handler(ws: WebSocketUpgrade, State(rooms): State<Rooms>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, rooms))
}

// ---------- Fungsi bantu ----------

// Kirim satu Signal ke klien lewat salurannya
fn send(tx: &Tx, signal: &Signal) {
    let json = serde_json::to_string(signal).unwrap();
    // .ok() mengabaikan error: kalau klien sudah putus, tidak apa-apa
    tx.send(Message::Text(json.into())).ok();
}

fn send_error(tx: &Tx, message: &str) {
    send(
        tx,
        &Signal::Error {
            message: message.to_string(),
        },
    );
}

// Kode room 6 karakter, tanpa huruf/angka yang mirip (0, O, 1, I)
fn random_code() -> String {
    const CHARS: &[u8] = b"ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
    let mut rng = rand::rng();
    (0..6)
        .map(|_| CHARS[rng.random_range(0..CHARS.len())] as char)
        .collect()
}

// Keluarkan peserta dari room; hapus room kalau kosong, kalau tidak beri tahu yang tersisa
fn leave_room(rooms: &Rooms, room: &Option<String>, my_id: u64) {
    let Some(code) = room else { return };
    let mut map = rooms.lock().unwrap();

    let now_empty = if let Some(r) = map.get_mut(code) {
        r.peers.retain(|p| p.id != my_id);
        if r.peers.is_empty() {
            true
        } else {
            for p in &r.peers {
                send(&p.tx, &Signal::PeerLeft);
            }
            false
        }
    } else {
        false
    };

    if now_empty {
        map.remove(code);
    }
}

// ---------- Penanganan satu klien ----------

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
                send_error(&tx, "pesan tidak valid");
                continue;
            }
        };

        match signal {
            Signal::Create => {
                if my_room.is_some() {
                    send_error(&tx, "kamu sudah ada di room");
                    continue;
                }
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
                        peers: vec![Peer { id: my_id, tx: tx.clone() }],
                    },
                );
                drop(map); // lepas lock secepatnya
                my_room = Some(code.clone());
                send(&tx, &Signal::Created { room: code });
            }

            Signal::Join { room } => {
                if my_room.is_some() {
                    send_error(&tx, "kamu sudah ada di room");
                    continue;
                }
                let code = room.to_uppercase();
                let mut map = rooms.lock().unwrap();
                match map.get_mut(&code) {
                    None => send_error(&tx, "room tidak ditemukan"),
                    Some(r) if r.peers.len() >= 2 => send_error(&tx, "room penuh"),
                    Some(r) => {
                        // Beri tahu peserta yang sudah ada
                        for p in &r.peers {
                            send(&p.tx, &Signal::PeerJoined);
                        }
                        r.peers.push(Peer { id: my_id, tx: tx.clone() });
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

            // Klien tidak boleh mengirim pesan khusus server
            _ => send_error(&tx, "jenis pesan tidak dikenal"),
        }
    }

    // Sampai sini klien putus (tutup tab, error, dll): bersihkan
    leave_room(&rooms, &my_room, my_id);
    send_task.abort();
    println!("Klien putus");
}