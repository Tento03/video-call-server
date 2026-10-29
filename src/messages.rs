use axum::extract::ws::Message;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::UnboundedSender;

// Pesan signaling (JSON). Field "type" menentukan variannya.
// Contoh: {"type":"join","room":"AB12CD"}
#[derive(Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum Signal {
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
pub type Tx = UnboundedSender<Message>;

// Kirim satu Signal ke klien lewat salurannya
pub fn send(tx: &Tx, signal: &Signal) {
    let json = serde_json::to_string(signal).unwrap();
    // .ok() mengabaikan error: kalau klien sudah putus, tidak apa-apa
    tx.send(Message::Text(json.into())).ok();
}

pub fn send_error(tx: &Tx, message: &str) {
    send(
        tx,
        &Signal::Error {
            message: message.to_string(),
        },
    );
}