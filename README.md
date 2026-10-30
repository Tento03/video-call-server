# Video Call Server (Rust)

[![CI](https://github.com/Tento03/video-call-server/actions/workflows/ci.yml/badge.svg)](https://github.com/Tento03/video-call-server/actions/workflows/ci.yml)
![License](https://img.shields.io/badge/license-MIT-blue)

Aplikasi video call 1-on-1 untuk wawancara. Server **Rust** menjadi *signaling server* (WebSocket), sedangkan audio dan video mengalir langsung antar browser lewat **WebRTC** (peer-to-peer). Tanpa akun, tanpa instalasi.

![Demo](docs/demo.gif)

> <img width="1365" height="626" alt="image" src="https://github.com/user-attachments/assets/5b19cb48-0238-478f-89f9-3b14c6a464a5" />
<img width="1362" height="625" alt="image" src="https://github.com/user-attachments/assets/801e87fc-5d3a-4e06-8cbf-7ddc297f11e0" />



## Fitur

- Buat room dengan kode 6 karakter dan link undangan yang bisa dibagikan
- Video dan suara peer-to-peer, mute mikrofon, matikan kamera
- Peran **Pewawancara** (pembuat room) dan **Kandidat** (yang bergabung), dengan nama tampilan
- Chat teks di dalam room
- Catatan pewawancara pribadi (tidak pernah dikirim ke server) yang bisa diunduh, dengan timer durasi
- Heartbeat ping/pong: koneksi yang mati diam-diam terdeteksi dan room dibersihkan
- Endpoint `/health` dan log terstruktur (`tracing`)

## Arsitektur

```
Browser A  <------ WebRTC (audio/video, P2P) ------>  Browser B
    |                                                      |
    +------ WebSocket (signaling) ----> Server Rust <------+
                                       (room, relay pesan)
```

Alur negosiasi:

1. A membuat room dan terhubung ke server lewat WebSocket.
2. B bergabung; server memberi tahu A bahwa ada peserta baru.
3. A membuat *offer* (SDP) dan mengirimnya lewat server ke B.
4. B membalas dengan *answer*; keduanya bertukar *ICE candidate* lewat server.
5. Koneksi P2P terbentuk. Setelah itu server tidak lagi dilalui audio/video.

Server **tidak mengerti isi SDP**; ia hanya meneruskan pesan ke peserta lain di room yang sama.

### Pesan signaling (JSON)

| Arah | Pesan |
|---|---|
| Klien ke server | `create {name}`, `join {room, name}`, `offer {sdp}`, `answer {sdp}`, `ice {candidate}`, `chat {text}` |
| Server ke klien | `created {room}`, `joined {name, role}`, `peer-joined {name, role}`, `chat-message {name, text}`, `peer-left`, `error {message}` (ditambah `offer`/`answer`/`ice` yang diteruskan) |

### Struktur kode

```
src/
  main.rs       pintu masuk: log dan menyalakan server
  lib.rs        merakit router (dipakai juga oleh test)
  messages.rs   tipe pesan (enum Signal, Role) dan fungsi pengirim
  rooms.rs      state room bersama, kode acak, pembersihan saat keluar
  handler.rs    logika per koneksi WebSocket (room, relay, chat, heartbeat)
static/
  index.html    frontend: HTML, CSS, dan JavaScript murni (WebRTC API)
tests/
  signaling.rs  test integrasi dengan klien WebSocket sungguhan
```

## Teknologi

Rust, `axum` (fitur `ws`), `tokio`, `serde` / `serde_json`, `tracing`, `tower-http`; frontend HTML + JavaScript murni; test dengan `tokio-tungstenite`; CI dengan GitHub Actions.

## Menjalankan

Syarat: [Rust](https://rustup.rs/) terpasang.

```bash
git clone https://github.com/Tento03/video-call-server.git
cd video-call-server
cargo run
```

Buka `http://localhost:3000` di dua tab (atau dua browser), buat room di salah satu, lalu bergabung dari yang lain.

Log lebih detail: `RUST_LOG=video_call_server=debug cargo run` (di PowerShell: `$env:RUST_LOG="video_call_server=debug"; cargo run`).

### Mencoba antar perangkat

Browser hanya mengizinkan kamera di `localhost` atau HTTPS. Untuk mencoba dari HP atau jaringan lain, buka tunnel HTTPS, misalnya dengan [Cloudflare Tunnel](https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/):

```bash
cloudflared tunnel --url http://localhost:3000
```

Buka alamat `https://....trycloudflare.com` yang muncul, lalu bagikan link room-nya.

## Pengujian

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```

Ada 12 test (11 integrasi, 1 unit) yang berjalan otomatis di CI. Yang diuji antara lain: buat dan join room, relay offer/answer, room penuh, room tidak ada, nama kosong, pesan rusak (koneksi tetap hidup), peserta keluar mendadak, pesan melebihi batas ukuran, dan chat (termasuk bahwa nama pengirim tidak bisa dipalsukan).

### Hasil uji antar jaringan

| Skenario | Hasil |
|---|---|
| Dua perangkat di WiFi yang sama | ISI: berhasil / gagal |
| Laptop (WiFi) dan HP (data seluler), lewat Cloudflare Tunnel | Berhasil: video dan suara tersambung hanya dengan STUN |
| Koneksi mati diam-diam (HP mode pesawat) | Server mendeteksi dalam sekitar satu menit dan lawan bicara diberi tahu |

ISI: tambahkan operator seluler, perangkat, dan browser yang dipakai (mis. Chrome Android, Edge Windows).

## Keputusan desain

- **Signaling saja di Rust.** Media tidak melewati server, jadi server ringan dan tidak perlu menulis media engine.
- **State room di memori** (`Arc<Mutex<HashMap>>`) dengan satu *channel* per peserta. Mutex tidak pernah dipegang saat menunggu (`.await`).
- **Semua jalur keluar berakhir di satu fungsi pembersihan** (`leave_room`): tab ditutup, jaringan putus, pesan terlalu besar, atau timeout heartbeat.
- **Nama pengirim chat diambil dari data server**, bukan dari pesan klien, sehingga tidak bisa dipalsukan.
- **Catatan pewawancara tidak melewati server.** Isinya sensitif, dan server memang tidak perlu menyimpannya.
- **Isi chat dan nama dirender dengan `textContent`**, tidak pernah `innerHTML`, untuk mencegah XSS.

## Keterbatasan yang diketahui

- **Tidak ada server TURN.** Pada jaringan dengan NAT yang sangat ketat (mis. sebagian jaringan kantor atau kampus), koneksi P2P bisa gagal. Tes saya berhasil dengan STUN saja, tetapi itu satu titik data, bukan jaminan.
- **Tidak ada autentikasi.** Siapa pun yang tahu kode room bisa bergabung (maksimal 2 peserta per room).
- **Belum ada rate limit** dan belum ada header keamanan (CSP).
- **Maksimal 2 peserta per room.**
- State room hilang saat server dimulai ulang.

## Rencana

- Rate limit, header keamanan, dan penyaringan nama
- Server TURN (coturn) untuk jaringan ketat
- Tahap 1 wawancara dengan AI, lalu tahap 2 video call dengan pewawancara
- Test otomatis khusus heartbeat

## Lisensi

[MIT](LICENSE)
