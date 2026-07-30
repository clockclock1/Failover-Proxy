mod admin;
mod auth;
mod circuit;
mod config;
mod model_source;
mod proxy;
mod stats;

use axum::{
    extract::DefaultBodyLimit,
    http::{HeaderName, Method},
    routing::{get, post},
    Router,
};
use clap::Parser;
use config::{load_config, Config};
use reqwest::Client;
use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};
use tokio::sync::RwLock;
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Debug, Parser)]
#[command(author, version, about)]
struct Cli {
    #[arg(long, env = "HOST", default_value = "0.0.0.0")]
    host: String,
    #[arg(long, env = "PORT", default_value_t = 8787)]
    port: u16,
    #[arg(long, env = "DATA_DIR")]
    data_dir: Option<PathBuf>,
    #[arg(long, env = "CONFIG_PATH")]
    config_path: Option<PathBuf>,
    #[arg(long, env = "STATS_PATH")]
    stats_path: Option<PathBuf>,
    #[arg(long, env = "REQUEST_LOGS_PATH")]
    request_logs_path: Option<PathBuf>,
    #[arg(long, env = "MODEL_STATS_PATH")]
    model_stats_path: Option<PathBuf>,
    #[arg(long, env = "RUNTIME_STATS_PATH")]
    runtime_stats_path: Option<PathBuf>,
}

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<RwLock<Config>>,
    pub config_path: Arc<PathBuf>,
    pub runtime_stats_path: Arc<PathBuf>,
    pub stats: stats::StatsStore,
    pub circuit_breakers: circuit::CircuitBreakers,
    pub model_source: model_source::ModelSourceService,
    pub provider_health: model_source::ProviderHealthService,
    pub proxy_runtime: proxy::ProxyRuntime,
    pub auth: auth::AuthState,
    pub client: Client,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "failover_proxy=info,tower_http=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let cli = Cli::parse();
    let runtime_dir = std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|p| p.to_path_buf()))
        .unwrap_or(std::env::current_dir()?);
    let data_dir = cli.data_dir.unwrap_or_else(|| runtime_dir.join("data"));
    let config_path = cli
        .config_path
        .unwrap_or_else(|| data_dir.join("config.json"));
    let stats_path = cli
        .stats_path
        .unwrap_or_else(|| data_dir.join("stats.json"));
    let request_logs_path = cli
        .request_logs_path
        .unwrap_or_else(|| data_dir.join("request-logs.csv"));
    let model_stats_path = cli
        .model_stats_path
        .unwrap_or_else(|| data_dir.join("model-stats.csv"));
    let runtime_stats_path = cli
        .runtime_stats_path
        .unwrap_or_else(|| data_dir.join("runtime-stats.csv"));

    let cfg = load_config(&config_path).await?;
    let client = Client::builder()
        .pool_idle_timeout(Duration::from_secs(90))
        .pool_max_idle_per_host(32)
        .http2_adaptive_window(true)
        .http2_keep_alive_interval(Duration::from_secs(30))
        .http2_keep_alive_timeout(Duration::from_secs(10))
        .tcp_keepalive(Duration::from_secs(60))
        .gzip(true)
        .use_rustls_tls()
        .build()?;
    let stats = stats::StatsStore::load(
        stats_path.clone(),
        request_logs_path.clone(),
        model_stats_path.clone(),
        runtime_stats_path.clone(),
        cfg.log_settings.clone(),
    )
    .await?;
    stats.spawn_periodic_save();
    let config_state = Arc::new(RwLock::new(cfg));
    let provider_health = model_source::ProviderHealthService::new(client.clone());
    provider_health.spawn_periodic_refresh(config_state.clone());
    let state = AppState {
        config: config_state,
        config_path: Arc::new(config_path.clone()),
        runtime_stats_path: Arc::new(runtime_stats_path.clone()),
        stats,
        circuit_breakers: circuit::CircuitBreakers::default(),
        model_source: model_source::ModelSourceService::new(client.clone()),
        provider_health,
        proxy_runtime: proxy::ProxyRuntime::default(),
        auth: auth::AuthState::default(),
        client,
    };
    admin::spawn_runtime_state_cleanup(state.clone());

    let cors = CorsLayer::new()
        .allow_origin(tower_http::cors::Any)
        .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
        .allow_headers([
            HeaderName::from_static("authorization"),
            HeaderName::from_static("content-type"),
            HeaderName::from_static("x-admin-token"),
            HeaderName::from_static("x-admin-session"),
        ])
        .expose_headers([
            HeaderName::from_static("content-type"),
            HeaderName::from_static("x-proxy-target"),
            HeaderName::from_static("x-proxy-model"),
        ]);

    let app = Router::new()
        .route("/api/health", get(admin::health))
        .route("/api/login", post(admin::login))
        .route("/api/logout", post(admin::logout))
        .route("/api/session", get(admin::session))
        .route(
            "/api/shares/live-status",
            get(admin::list_live_status_shares).post(admin::create_live_status_share),
        )
        .route(
            "/api/shares/live-status/revoke",
            post(admin::revoke_live_status_share),
        )
        .route(
            "/api/share/live-status/{token}",
            get(admin::shared_live_status),
        )
        .route(
            "/api/config",
            get(admin::get_config).post(admin::post_config),
        )
        .route("/api/stats", get(admin::get_stats))
        .route("/api/stats/page/{page}", get(admin::get_page_stats))
        .route(
            "/api/log-settings",
            get(admin::get_log_settings).post(admin::post_log_settings),
        )
        .route("/api/logs/clear", post(admin::clear_logs))
        .route("/api/providers/health", post(admin::providers_health))
        .route("/api/model-tests/run", post(admin::model_tests_run))
        .route(
            "/api/model-source/preview",
            post(admin::model_source_preview),
        )
        .route(
            "/api/model-source/refresh",
            post(admin::model_source_refresh),
        )
        .route("/", get(admin::static_ui))
        .route("/index.html", get(admin::static_ui))
        .route("/dashboard", get(admin::static_ui))
        .route("/providers", get(admin::static_ui))
        .route("/model-tests", get(admin::static_ui))
        .route("/chains", get(admin::static_ui))
        .route("/model-stats", get(admin::static_ui))
        .route("/endpoints", get(admin::static_ui))
        .route("/live-status", get(admin::static_ui))
        .route(
            "/share/live-status/{token}",
            get(admin::live_status_share_page),
        )
        .route("/logs", get(admin::static_ui))
        .route("/app.css", get(admin::app_css))
        .route("/app.js", get(admin::app_js))
        .route("/app-core.js", get(admin::app_core_js))
        .route("/chunks/{*path}", get(admin::static_chunk))
        .route("/assets/{*path}", get(admin::static_asset))
        .route("/v1/models", get(proxy::list_models))
        .route("/models", get(proxy::list_models))
        .route("/v1/chat/completions", post(proxy::proxy_endpoint))
        .route("/chat/completions", post(proxy::proxy_endpoint))
        .route("/v1/responses", post(proxy::proxy_endpoint))
        .route("/responses", post(proxy::proxy_endpoint))
        .route("/v1/response", post(proxy::proxy_endpoint))
        .route("/response", post(proxy::proxy_endpoint))
        .route("/v1/completions", post(proxy::proxy_endpoint))
        .route("/completions", post(proxy::proxy_endpoint))
        .fallback(get(admin::static_ui))
        // The proxy accepts long context windows and file-like inputs. Do not
        // impose Axum's default request-body size cap; upstreams and callers
        // decide the practical request size.
        .layer(DefaultBodyLimit::disable())
        .layer(TraceLayer::new_for_http())
        .layer(cors)
        .with_state(state);

    let addr: SocketAddr = format!("{}:{}", cli.host, cli.port).parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    println!(
        "OpenAI failover proxy listening on http://{}:{}",
        cli.host, cli.port
    );
    println!("Admin UI: http://127.0.0.1:{}", cli.port);
    println!("Config: {}", config_path.display());
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(err) = tokio::signal::ctrl_c().await {
            tracing::warn!(error = %err, "failed to install Ctrl+C handler");
        }
    };

    #[cfg(unix)]
    let terminate = async {
        let mut signal = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler");
        signal.recv().await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
