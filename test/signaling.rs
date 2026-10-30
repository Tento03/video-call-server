use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::net::TcpStream;
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};

type Client = WebSocketStream<MaybeTlsStream<TcpStream>>;

// ---------- Fungsi bantu ----------

// Nyalakan server di port acak yang kosong, kembalikan alamat WebSocket-nya
async fn spawn_server() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, video_call_server::app()).await.unwrap();
    });
    format!("ws://{addr}/ws")
}

async fn connect(url: &str) -> Client {
    let (ws, _) = connect_async(url).await.unwrap();
    ws
}

async fn send_json(client: &mut Client, value: Value) {
    client
        .send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
}

// Tunggu pesan berikutnya (maksimal 2 detik), lalu ubah dari teks JSON jadi Value
async fn recv_json(client: &mut Client) -> Value {
    let msg = tokio::time::timeout(Duration::from_secs(2), client.next())
        .await
        .expect("timeout: server tidak membalas dalam 2 detik")
        .expect("koneksi ditutup")
        .expect("error di koneksi");
    serde_json::from_str(msg.to_text().unwrap()).unwrap()
}

// Siapkan satu room berisi dua peserta: A (pewawancara) dan B (kandidat)
async fn room_with_two(url: &str) -> (Client, Client, String) {
    let mut a = connect(url).await;
    send_json(&mut a, json!({"type": "create", "name": "Rina"})).await;
    let created = recv_json(&mut a).await;
    let room = created["room"].as_str().unwrap().to_string();

    let mut b = connect(url).await;
    send_json(&mut b, json!({"type": "join", "room": room, "name": "Budi"})).await;
    recv_json(&mut b).await; // "joined" untuk B
    recv_json(&mut a).await; // "peer-joined" untuk A

    (a, b, room)
}

// ---------- Test ----------

#[tokio::test]
async fn buat_room_dan_join_saling_memberi_tahu() {
    let url = spawn_server().await;

    let mut a = connect(&url).await;
    send_json(&mut a, json!({"type": "create", "name": "Rina"})).await;
    let created = recv_json(&mut a).await;
    assert_eq!(created["type"], "created");
    let room = created["room"].as_str().unwrap().to_string();
    assert_eq!(room.len(), 6);

    let mut b = connect(&url).await;
    send_json(&mut b, json!({"type": "join", "room": room, "name": "Budi"})).await;

    // B diberi tahu siapa yang sudah ada di room
    let joined = recv_json(&mut b).await;
    assert_eq!(joined["type"], "joined");
    assert_eq!(joined["name"], "Rina");
    assert_eq!(joined["role"], "interviewer");

    // A diberi tahu siapa yang baru masuk
    let peer_joined = recv_json(&mut a).await;
    assert_eq!(peer_joined["type"], "peer-joined");
    assert_eq!(peer_joined["name"], "Budi");
    assert_eq!(peer_joined["role"], "candidate");
}

#[tokio::test]
async fn offer_dan_answer_diteruskan_ke_peserta_lain() {
    let url = spawn_server().await;
    let (mut a, mut b, _room) = room_with_two(&url).await;

    send_json(&mut a, json!({"type": "offer", "sdp": "sdp-dari-a"})).await;
    let diterima_b = recv_json(&mut b).await;
    assert_eq!(diterima_b["type"], "offer");
    assert_eq!(diterima_b["sdp"], "sdp-dari-a");

    send_json(&mut b, json!({"type": "answer", "sdp": "sdp-dari-b"})).await;
    let diterima_a = recv_json(&mut a).await;
    assert_eq!(diterima_a["type"], "answer");
    assert_eq!(diterima_a["sdp"], "sdp-dari-b");
}

#[tokio::test]
async fn peserta_ketiga_ditolak_karena_room_penuh() {
    let url = spawn_server().await;
    let (_a, _b, room) = room_with_two(&url).await;

    let mut c = connect(&url).await;
    send_json(&mut c, json!({"type": "join", "room": room, "name": "Candra"})).await;
    let reply = recv_json(&mut c).await;
    assert_eq!(reply["type"], "error");
    assert_eq!(reply["message"], "room penuh");
}

#[tokio::test]
async fn join_ke_room_yang_tidak_ada_ditolak() {
    let url = spawn_server().await;

    let mut c = connect(&url).await;
    send_json(&mut c, json!({"type": "join", "room": "ZZZZZZ", "name": "Candra"})).await;
    let reply = recv_json(&mut c).await;
    assert_eq!(reply["type"], "error");
    assert_eq!(reply["message"], "room tidak ditemukan");
}

#[tokio::test]
async fn nama_kosong_ditolak() {
    let url = spawn_server().await;

    let mut a = connect(&url).await;
    send_json(&mut a, json!({"type": "create", "name": "   "})).await;
    let reply = recv_json(&mut a).await;
    assert_eq!(reply["type"], "error");
    assert_eq!(reply["message"], "nama harus 1-30 karakter");
}

#[tokio::test]
async fn pesan_rusak_dibalas_error_dan_koneksi_tetap_hidup() {
    let url = spawn_server().await;

    let mut a = connect(&url).await;
    a.send(Message::Text("ini bukan json".into())).await.unwrap();
    let reply = recv_json(&mut a).await;
    assert_eq!(reply["type"], "error");
    assert_eq!(reply["message"], "pesan tidak valid");

    // Koneksi yang sama masih bisa dipakai setelah mengirim pesan rusak
    send_json(&mut a, json!({"type": "create", "name": "Rina"})).await;
    let created = recv_json(&mut a).await;
    assert_eq!(created["type"], "created");
}

#[tokio::test]
async fn peserta_keluar_mendadak_memberi_tahu_yang_lain() {
    let url = spawn_server().await;
    let (mut a, b, _room) = room_with_two(&url).await;

    // B hilang tiba-tiba (koneksi diputus tanpa pamit)
    drop(b);

    let reply = recv_json(&mut a).await;
    assert_eq!(reply["type"], "peer-left");
}

#[tokio::test]
async fn pesan_terlalu_besar_memutus_koneksi_tapi_server_selamat() {
    let url = spawn_server().await;

    let mut a = connect(&url).await;
    // Kirim pesan jauh di atas batas 16 KB
    a.send(Message::Text("x".repeat(20_000).into())).await.ok();

    let hasil = tokio::time::timeout(Duration::from_secs(2), a.next())
        .await
        .expect("timeout: koneksi tidak diputus");
    assert!(matches!(
        hasil,
        None | Some(Err(_)) | Some(Ok(Message::Close(_)))
    ));

    // Server tetap melayani klien baru
    let mut b = connect(&url).await;
    send_json(&mut b, json!({"type": "create", "name": "Budi"})).await;
    let created = recv_json(&mut b).await;
    assert_eq!(created["type"], "created");
}