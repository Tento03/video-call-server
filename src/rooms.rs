use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use rand::RngExt;
use tracing::info;

use crate::messages::{send, Role, Signal, Tx};

pub struct Peer {
    pub id: u64,
    pub name: String,
    pub role: Role,
    pub tx: Tx,
}

pub struct Room {
    pub peers: Vec<Peer>, // maksimal 2
}

// State bersama: peta kode room -> Room.
// Arc = bisa dibagi ke banyak task, Mutex = hanya satu yang akses pada satu waktu.
pub type Rooms = Arc<Mutex<HashMap<String, Room>>>;

// Membuat peta room yang masih kosong
pub fn new_rooms() -> Rooms {
    Arc::new(Mutex::new(HashMap::new()))
}

// Kode room 6 karakter, tanpa huruf/angka yang mirip (0, O, 1, I)
pub fn random_code() -> String {
    const CHARS: &[u8] = b"ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
    let mut rng = rand::rng();
    (0..6)
        .map(|_| CHARS[rng.random_range(0..CHARS.len())] as char)
        .collect()
}

// Keluarkan peserta dari room; hapus room kalau kosong, kalau tidak beri tahu yang tersisa
pub fn leave_room(rooms: &Rooms, room: &Option<String>, my_id: u64) {
    let Some(code) = room else { return };
    let mut map = rooms.lock().unwrap();

    let now_empty = if let Some(r) = map.get_mut(code) {
        r.peers.retain(|p| p.id != my_id);
        info!(room = %code, tersisa = r.peers.len(), "peserta keluar");
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
        info!(room = %code, "room dihapus karena kosong");
    }
}