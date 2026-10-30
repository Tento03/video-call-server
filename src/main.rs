mod handler;
mod messages;
mod rooms;

use axum::{routing::get, Router};
use tower_http::services::ServeDir;

#[tokio::main]
async fn main() {
    // Nyalakan sistem log. Default: level info ke atas untuk crate kita.
    // Bisa diubah lewat variabel RUST_LOG, mis. video_call_server=debug
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "video_call_server=info".into()),
        )
        .init();

    let rooms = rooms::new_rooms();

    let app = Router::new()
        .route("/health", get(health))
        .route("/ws", get(handler::ws_handler))
        .fallback_service(ServeDir::new("static"))
        .with_state(rooms);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000")
        .await
        .unwrap();

    tracing::info!("Server jalan di http://localhost:3000");
    axum::serve(listener, app).await.unwrap();
}

async fn health() -> &'static str {
    "ok"
}