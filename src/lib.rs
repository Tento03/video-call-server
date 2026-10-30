pub mod handler;
pub mod messages;
pub mod rooms;

use axum::{routing::get, Router};
use tower_http::services::ServeDir;

// Membangun seluruh aplikasi web. Dipakai oleh main.rs dan oleh test.
// Setiap pemanggilan membuat daftar room yang baru dan kosong.
pub fn app() -> Router {
    let rooms = rooms::new_rooms();

    Router::new()
        .route("/health", get(health))
        .route("/ws", get(handler::ws_handler))
        .fallback_service(ServeDir::new("static"))
        .with_state(rooms)
}

async fn health() -> &'static str {
    "ok"
}