use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::IntoResponse,
};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc::unbounded_channel;

use crate::messages::{send, send_error, Signal};
use crate::rooms::{leave_room, random_code, Peer, Room, Rooms};

pub async fn ws_handler(ws: WebSocketUpgrade, State(rooms): State<Rooms>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, rooms))
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