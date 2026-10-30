use axum::extract::ws::Message;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::UnboundedSender;

// Peran di dalam wawancara
#[derive(Deserialize, Serialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Interviewer,
    Candidate,
}

// Pesan signaling (JSON). Field "type" menentukan variannya.
#[derive(Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum Signal {
    // dari klien
    Create { name: String },
    Join { room: String, name: String },
    Offer { sdp: String },
    Answer { sdp: String },
    Ice { candidate: serde_json::Value },
    Chat { text: String },
    // dari server
    Created { room: String },
    Joined { name: String, role: Role }, // untuk yang baru masuk: info lawan bicara
    PeerJoined { name: String, role: Role }, // untuk yang sudah ada: info yang baru masuk
    ChatMessage { name: String, text: String }, // chat dari peserta lain
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
