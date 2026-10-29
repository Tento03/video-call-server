mod handler;
mod messages;
mod rooms;

use axum::{routing::get, Router};
use tower_http::services::ServeDir;

#[tokio::main]
async fn main() {
    let rooms = rooms::new_rooms();

    let app = Router::new()
        .route("/health", get(health))
        .route("/ws", get(handler::ws_handler))
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