use video_call_server::app;

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

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();

    tracing::info!("Server jalan di http://localhost:3000");
    axum::serve(listener, app()).await.unwrap();
}
