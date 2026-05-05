//! DWBase node binary providing a minimal HTTP API over the DWBase engine.
//!
//! This wires together sled storage, HNSW vector search, local streams, a permissive
//! gatekeeper, and a dummy embedder. The API is intentionally minimal and intended for
//! local development/testing.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
#[cfg(feature = "metrics")]
use std::sync::OnceLock;

use axum::{
    body::Body,
    extract::{Extension, Path, State},
    http::{HeaderValue, Request, StatusCode},
    middleware::{self, Next},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use dwbase_core::{Atom, AtomId, Importance, Timestamp, WorldKey};
use dwbase_embedder_dummy::DummyEmbedder;
use dwbase_engine::{Answer, AtomFilter, DWBaseEngine, NewAtom, Question, WorldAction, WorldMeta};
use dwbase_security::{Capabilities, LocalGatekeeper, RateLimits, TrustStore};
use dwbase_storage_sled::{EnvKeyProvider, SledConfig, SledStorage};
use dwbase_stream_local::LocalStreamEngine;
use dwbase_vector_hnsw::HnswVectorEngine;
#[cfg(feature = "metrics")]
use metrics_exporter_prometheus::PrometheusHandle;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use tokio::sync::Mutex;
#[cfg(feature = "metrics")]
use tracing::info;
use tracing::{info_span, Instrument};
use uuid::Uuid;

#[derive(Debug, Deserialize)]
#[serde(default)]
struct NodeConfig {
    listen: Option<String>,
    data_dir: PathBuf,
    #[serde(default)]
    security: SecurityConfig,
    #[serde(default)]
    embedder: EmbedderConfig,
    #[serde(default)]
    health: HealthConfig,
    #[serde(default)]
    storage: StorageConfig,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            listen: Some("127.0.0.1:8080".into()),
            data_dir: PathBuf::from("./data"),
            security: SecurityConfig::default(),
            embedder: EmbedderConfig::default(),
            health: HealthConfig::default(),
            storage: StorageConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
struct StorageConfig {
    #[serde(default)]
    encryption_enabled: bool,
    key_id: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct SecurityConfig {
    read_worlds: Vec<String>,
    write_worlds: Vec<String>,
    importance_cap: Option<f32>,
    remember_per_sec: Option<f64>,
    ask_per_min: Option<f64>,
}

impl SecurityConfig {
    fn to_caps(&self) -> Capabilities {
        Capabilities {
            read_worlds: self.read_worlds.iter().cloned().map(WorldKey).collect(),
            write_worlds: self.write_worlds.iter().cloned().map(WorldKey).collect(),
            importance_cap: self.importance_cap,
            rate_limits: RateLimits {
                remember_per_sec: self.remember_per_sec,
                ask_per_min: self.ask_per_min,
            },
            ..Default::default()
        }
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
enum EmbedderConfig {
    #[default]
    Dummy,
}

impl EmbedderConfig {
    fn build(&self) -> DummyEmbedder {
        match self {
            EmbedderConfig::Dummy => DummyEmbedder::new(),
        }
    }
}

#[derive(Clone)]
struct AppState {
    engine: Arc<
        DWBaseEngine<
            SledStorage,
            HnswVectorEngine,
            LocalStreamEngine,
            LocalGatekeeper,
            DummyEmbedder,
        >,
    >,
    worlds: Arc<Mutex<Vec<WorldKey>>>,
    data_dir: PathBuf,
    health: HealthConfig,
    #[cfg(feature = "metrics")]
    metrics_handle: PrometheusHandle,
}

#[derive(Debug, Error)]
enum ApiError {
    #[error("{0}")]
    Engine(String),
    #[error("{0}")]
    BadRequest(String),
}

#[derive(Clone)]
struct CorrelationId(String);

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        let status = match self {
            ApiError::Engine(_) => StatusCode::INTERNAL_SERVER_ERROR,
            ApiError::BadRequest(_) => StatusCode::BAD_REQUEST,
        };
        let body = serde_json::json!({ "error": self.to_string() });
        (status, Json(body)).into_response()
    }
}

#[derive(Debug, Deserialize)]
struct ConfigWrapper {
    #[serde(default)]
    node: NodeConfig,
}

#[derive(Debug, Clone, Deserialize)]
struct HealthConfig {
    /// Mark degraded when disk usage exceeds this fraction (0.0-1.0).
    #[serde(default = "HealthConfig::default_disk_threshold")]
    disk_usage_degraded: f32,
    /// Mark degraded when index rebuild lag exceeds this many seconds.
    #[serde(default = "HealthConfig::default_index_lag_seconds")]
    index_rebuild_warn_seconds: u64,
}

impl HealthConfig {
    const fn default_disk_threshold() -> f32 {
        0.9
    }

    const fn default_index_lag_seconds() -> u64 {
        60
    }
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            disk_usage_degraded: Self::default_disk_threshold(),
            index_rebuild_warn_seconds: Self::default_index_lag_seconds(),
        }
    }
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: String,
    disk_usage: Option<DiskUsage>,
    index_rebuild_lag_ms: Option<u64>,
    message: Option<String>,
}

#[derive(Debug, Serialize)]
struct DiskUsage {
    used_bytes: u64,
    total_bytes: u64,
    percent_used: f32,
}

fn disk_usage(path: &PathBuf) -> Option<DiskUsage> {
    let total = fs2::total_space(path).ok()?;
    let avail = fs2::available_space(path).ok()?;
    if total == 0 {
        return None;
    }
    let used = total.saturating_sub(avail);
    let pct = (used as f64 / total as f64) as f32 * 100.0;
    Some(DiskUsage {
        used_bytes: used,
        total_bytes: total,
        percent_used: pct,
    })
}

async fn health_handler(State(state): State<AppState>) -> Json<HealthResponse> {
    let disk = disk_usage(&state.data_dir);
    let mut status = "ready".to_string();
    let mut message = None;
    let index_rebuild_lag_ms = state.engine.max_index_rebuild_lag_ms();
    if let Some(ref usage) = disk {
        let threshold = state.health.disk_usage_degraded * 100.0;
        if usage.percent_used >= threshold {
            status = "degraded".to_string();
            message = Some(format!(
                "disk usage {:.1}% >= {:.1}%",
                usage.percent_used, threshold
            ));
        }
        #[cfg(feature = "metrics")]
        dwbase_metrics::record_disk_usage(usage.used_bytes, usage.total_bytes);
    } else {
        status = "degraded".to_string();
        message = Some("unable to read disk usage".into());
    }

    if let Some(lag) = index_rebuild_lag_ms {
        let warn_ms = state.health.index_rebuild_warn_seconds * 1000;
        if lag > warn_ms {
            status = "degraded".into();
            message = Some(format!(
                "index rebuild lag {}ms exceeds {}ms threshold",
                lag, warn_ms
            ));
        }
    }

    #[cfg(feature = "metrics")]
    {
        // Swarm/quarantine not implemented; emit zeros to keep dashboards sane.
        dwbase_metrics::record_sync_lag(std::time::Duration::from_millis(0));
        dwbase_metrics::record_quarantine_count(0);
    }

    Json(HealthResponse {
        status,
        disk_usage: disk,
        index_rebuild_lag_ms,
        message,
    })
}

async fn healthz_handler() -> StatusCode {
    StatusCode::OK
}

async fn readyz_handler(State(state): State<AppState>) -> (StatusCode, Json<HealthResponse>) {
    let disk = disk_usage(&state.data_dir);
    let mut status = "ready".to_string();
    let mut message = None;
    let mut index_rebuild_lag_ms = state.engine.max_index_rebuild_lag_ms();
    if let Some(ref usage) = disk {
        let threshold = state.health.disk_usage_degraded * 100.0;
        if usage.percent_used >= threshold {
            status = "degraded".to_string();
            message = Some(format!(
                "disk usage {:.1}% >= {:.1}%",
                usage.percent_used, threshold
            ));
        }
    } else if message.is_none() {
        // Disk stats can be unavailable in some environments; treat this as "unknown"
        // rather than failing readiness.
        message = Some("disk usage unavailable".into());
    }

    let storage_ok = state.engine.storage_ready();
    if !storage_ok {
        status = "degraded".into();
        message = Some("storage unavailable".into());
    }

    let indexes = state.engine.index_status();
    let all_ready = indexes.iter().all(|m| m.ready);
    if !all_ready && message.is_none() {
        message = Some("index not ready yet".into());
    }
    if index_rebuild_lag_ms.is_none() && !all_ready {
        index_rebuild_lag_ms = Some(0);
    }

    let require_indexes = std::env::var("DWBASE_READY_REQUIRE_INDEX")
        .map(|v| v != "0" && !v.eq_ignore_ascii_case("false"))
        .unwrap_or(false);

    let code = if status == "ready" && storage_ok && (!require_indexes || all_ready) {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    let resp = HealthResponse {
        status,
        disk_usage: disk,
        index_rebuild_lag_ms,
        message,
    };
    (code, Json(resp))
}

async fn correlation_layer(mut req: Request<Body>, next: Next) -> impl IntoResponse {
    let header_key = axum::http::header::HeaderName::from_static("x-request-id");
    let cid = req
        .headers()
        .get(&header_key)
        .and_then(|v: &HeaderValue| v.to_str().ok())
        .map(|s: &str| s.to_string())
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    req.headers_mut()
        .insert(&header_key, HeaderValue::from_str(&cid).unwrap());
    req.extensions_mut().insert(CorrelationId(cid.clone()));
    let span = info_span!(
        "http.request",
        request_id = %cid,
        method = %req.method(),
        path = %req.uri().path()
    );
    next.run(req).instrument(span).await
}

async fn remember_handler(
    State(state): State<AppState>,
    Extension(correlation): Extension<CorrelationId>,
    Json(new_atom): Json<NewAtom>,
) -> std::result::Result<Json<AtomId>, ApiError> {
    let span = info_span!(
        "remember",
        request_id = %correlation.0,
        world = %new_atom.world.0
    );
    let id = state
        .engine
        .remember(new_atom.clone())
        .await
        .map_err(|e| ApiError::Engine(e.to_string()))?;
    {
        let mut worlds = state.worlds.lock().await;
        if !worlds.contains(&new_atom.world) {
            worlds.push(new_atom.world.clone());
        }
    }
    let _enter = span.enter();
    Ok(Json(id))
}

async fn ask_handler(
    State(state): State<AppState>,
    Extension(correlation): Extension<CorrelationId>,
    Json(question): Json<Question>,
) -> std::result::Result<Json<Answer>, ApiError> {
    let span = info_span!(
        "ask",
        request_id = %correlation.0,
        world = %question.world.0
    );
    let ans = state
        .engine
        .ask(question)
        .await
        .map_err(|e| ApiError::Engine(e.to_string()))?;
    let _enter = span.enter();
    Ok(Json(ans))
}

#[derive(Debug, Deserialize)]
struct ReplayRequest {
    world: WorldKey,
    #[serde(default)]
    filter: AtomFilter,
}

#[derive(Deserialize)]
struct WorldManageRequest {
    action: String,
    world: WorldKey,
    description: Option<String>,
    #[serde(default)]
    labels: Vec<String>,
}

#[derive(Deserialize)]
struct WorldPolicyRequest {
    world: WorldKey,
    retention_days: Option<u64>,
    min_importance: Option<f32>,
    #[serde(default)]
    replication_allow: Vec<String>,
    #[serde(default)]
    replication_deny: Vec<String>,
}

async fn replay_handler(
    State(state): State<AppState>,
    Extension(correlation): Extension<CorrelationId>,
    Json(req): Json<ReplayRequest>,
) -> std::result::Result<Json<Vec<Atom>>, ApiError> {
    let span = info_span!(
        "replay",
        request_id = %correlation.0,
        world = %req.world.0
    );
    let atoms = state
        .engine
        .replay(req.world, req.filter)
        .await
        .map_err(|e| ApiError::Engine(e.to_string()))?;
    let _enter = span.enter();
    Ok(Json(atoms))
}

async fn manage_world_handler(
    State(state): State<AppState>,
    Json(req): Json<WorldManageRequest>,
) -> std::result::Result<Json<()>, ApiError> {
    let action = match req.action.as_str() {
        "create" => WorldAction::Create(WorldMeta {
            world: req.world.clone(),
            description: req.description.clone(),
            labels: req.labels.clone(),
        }),
        "archive" => WorldAction::Archive(req.world.clone()),
        "resume" => WorldAction::Resume(req.world.clone()),
        other => return Err(ApiError::BadRequest(format!("unknown action: {other}"))),
    };

    state
        .engine
        .manage_world(action)
        .await
        .map_err(|e| ApiError::Engine(e.to_string()))?;

    let mut worlds = state.worlds.lock().await;
    match req.action.as_str() {
        "create" | "resume" if !worlds.contains(&req.world) => {
            worlds.push(req.world);
        }
        "archive" => {
            worlds.retain(|w| w != &req.world);
        }
        _ => {}
    }

    Ok(Json(()))
}

async fn world_policy_handler(
    State(state): State<AppState>,
    Json(req): Json<WorldPolicyRequest>,
) -> std::result::Result<Json<AtomId>, ApiError> {
    let target_world = req.world.clone();
    let policy_world = if req.world.0.starts_with("policy:") || req.world.0.ends_with("/policy") {
        req.world.clone()
    } else {
        WorldKey::new(format!("policy:{}", req.world.0))
    };
    let payload = serde_json::json!({
        "policy_world": policy_world.0,
        "target_world": target_world.0,
        "retention_days": req.retention_days,
        "min_importance": req.min_importance,
        "replication_allow": req.replication_allow,
        "replication_deny": req.replication_deny,
    });
    let mut labels = Vec::new();
    if let Some(days) = req.retention_days {
        labels.push(format!("policy:retention_days={days}"));
    }
    if let Some(min) = req.min_importance {
        labels.push(format!("policy:min_importance={min}"));
    }
    for allow in req.replication_allow {
        labels.push(format!("policy:replication_allow={allow}"));
    }
    for deny in req.replication_deny {
        labels.push(format!("policy:replication_deny={deny}"));
    }
    if labels.is_empty() {
        return Err(ApiError::BadRequest(
            "policy request must set at least one field".into(),
        ));
    }
    let new_atom = NewAtom {
        world: policy_world.clone(),
        worker: dwbase_core::WorkerKey::new("policy-cli"),
        kind: dwbase_core::AtomKind::Reflection,
        timestamp: Timestamp::new(
            OffsetDateTime::now_utc()
                .format(&Rfc3339)
                .unwrap_or_else(|_| "1970-01-01T00:00:00Z".into()),
        ),
        importance: Importance::clamped(0.2),
        payload_json: payload.to_string(),
        vector: None,
        flags: Vec::new(),
        labels,
        links: Vec::new(),
    };
    let id = state
        .engine
        .remember(new_atom)
        .await
        .map_err(|e| ApiError::Engine(e.to_string()))?;

    let mut worlds = state.worlds.lock().await;
    if !worlds.contains(&policy_world) {
        worlds.push(policy_world);
    }

    Ok(Json(id))
}

async fn list_worlds_handler(
    State(state): State<AppState>,
) -> std::result::Result<Json<Vec<WorldKey>>, ApiError> {
    let mut worlds = state
        .engine
        .worlds()
        .map_err(|e| ApiError::Engine(e.to_string()))?;
    // Merge in any worlds seen in-process that may not be persisted yet.
    let extra = state.worlds.lock().await;
    for w in extra.iter() {
        if worlds.contains(w) {
            continue;
        }
        let archived = state
            .engine
            .world_archived(w)
            .map_err(|e| ApiError::Engine(e.to_string()))?;
        if !archived {
            worlds.push(w.clone());
        }
    }
    worlds.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(Json(worlds))
}

async fn inspect_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> std::result::Result<Json<Option<Atom>>, ApiError> {
    let atoms = state
        .engine
        .get_atoms(&[AtomId::new(id)])
        .map_err(|e| ApiError::Engine(e.to_string()))?;
    Ok(Json(atoms.into_iter().next()))
}

async fn index_status_handler(
    State(state): State<AppState>,
) -> Json<Vec<dwbase_engine::IndexMetadata>> {
    Json(state.engine.index_status())
}

#[cfg(feature = "metrics")]
async fn metrics_handler(State(state): State<AppState>) -> impl IntoResponse {
    let body = state.metrics_handle.render();
    (
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "text/plain")],
        body,
    )
}

fn load_config(path: &str) -> anyhow::Result<NodeConfig> {
    let text = std::fs::read_to_string(path)?;
    let cfg: ConfigWrapper = toml::from_str(&text)?;
    Ok(cfg.node)
}

#[cfg(feature = "metrics")]
fn init_tracing() {
    static INIT: OnceLock<()> = OnceLock::new();
    let _ = INIT.get_or_init(|| {
        let _ = tracing_subscriber::fmt::try_init();
    });
}

#[cfg(feature = "metrics")]
fn init_metrics_recorder() -> PrometheusHandle {
    static HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();
    HANDLE
        .get_or_init(|| {
            metrics_exporter_prometheus::PrometheusBuilder::new()
                .install_recorder()
                .expect("install prometheus recorder")
        })
        .clone()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    #[cfg(feature = "metrics")]
    init_tracing();

    let args: Vec<String> = std::env::args().collect();
    let cfg_path = args
        .iter()
        .position(|a| a == "--config")
        .and_then(|idx| args.get(idx + 1))
        .map(|s| s.as_str())
        .unwrap_or("config.toml");
    let cfg = load_config(cfg_path)?;

    let mut sled_cfg = SledConfig::new(cfg.data_dir.clone());
    sled_cfg.encryption_enabled = cfg.storage.encryption_enabled;
    sled_cfg.key_id = cfg.storage.key_id.clone();
    let storage = SledStorage::open(sled_cfg, Arc::new(EnvKeyProvider))?;
    let vector = HnswVectorEngine::new();
    let stream = LocalStreamEngine::new();
    let trust_store = TrustStore::default();
    #[cfg(feature = "metrics")]
    {
        let worker_entries = trust_store
            .worker_scores
            .iter()
            .map(|(worker, score)| (worker.as_str(), *score));
        let node_entries = trust_store
            .node_scores
            .iter()
            .map(|(node, score)| (node.as_str(), *score));
        dwbase_metrics::record_trust_distribution(worker_entries.chain(node_entries));
    }
    let gatekeeper = LocalGatekeeper::new(cfg.security.to_caps(), trust_store);
    let embedder = cfg.embedder.build();
    let engine = Arc::new(DWBaseEngine::new(
        storage, vector, stream, gatekeeper, embedder,
    ));
    #[cfg(feature = "metrics")]
    let metrics_handle = init_metrics_recorder();

    let worlds = Arc::new(Mutex::new(Vec::new()));
    let state = AppState {
        engine,
        worlds,
        data_dir: cfg.data_dir.clone(),
        health: cfg.health.clone(),
        #[cfg(feature = "metrics")]
        metrics_handle,
    };

    let app = Router::new()
        .route("/remember", post(remember_handler))
        .route("/ask", post(ask_handler))
        .route("/replay", post(replay_handler))
        .route("/worlds", get(list_worlds_handler))
        .route("/worlds/manage", post(manage_world_handler))
        .route("/worlds/policy", post(world_policy_handler))
        .route("/atoms/{id}", get(inspect_handler))
        .route("/index/status", get(index_status_handler))
        .route("/health", get(health_handler))
        .route("/healthz", get(healthz_handler))
        .route("/readyz", get(readyz_handler))
        .route_layer(middleware::from_fn(correlation_layer));

    #[cfg(feature = "metrics")]
    let app = app.route("/metrics", get(metrics_handler));
    let app = app.with_state(state);

    let addr: SocketAddr = cfg
        .listen
        .as_deref()
        .unwrap_or("127.0.0.1:8080")
        .parse()
        .expect("invalid listen addr");

    println!("dwbase-node listening on {addr}");
    #[cfg(feature = "metrics")]
    info!(%addr, "dwbase-node listening");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let make = app.into_make_service();
    axum::serve(listener, make).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::State;
    use dwbase_core::{AtomKind, Importance, Timestamp, WorkerKey};
    use dwbase_engine::NewAtom;
    use tempfile::TempDir;

    #[test]
    fn parses_embedder_config() {
        let cfg: ConfigWrapper = toml::from_str(
            r#"
[node]
data_dir = "./data"
embedder = "dummy"
"#,
        )
        .expect("config");
        assert!(matches!(cfg.node.embedder, EmbedderConfig::Dummy));
    }

    fn build_state(tmp: &TempDir) -> AppState {
        let mut sled_cfg = SledConfig::new(tmp.path());
        sled_cfg.flush_on_write = true;
        let storage = SledStorage::open(sled_cfg, Arc::new(EnvKeyProvider)).expect("sled");
        let vector = HnswVectorEngine::new();
        let stream = LocalStreamEngine::new();
        let gatekeeper = LocalGatekeeper::new(Capabilities::default(), TrustStore::default());
        let embedder = DummyEmbedder::new();
        let engine = Arc::new(DWBaseEngine::new(
            storage, vector, stream, gatekeeper, embedder,
        ));
        #[cfg(feature = "metrics")]
        let metrics_handle = init_metrics_recorder();
        // Avoid relying on the host machine's free disk percentage for unit tests.
        let health = HealthConfig {
            disk_usage_degraded: 1.0,
            ..Default::default()
        };
        AppState {
            engine,
            worlds: Arc::new(Mutex::new(Vec::new())),
            data_dir: tmp.path().to_path_buf(),
            health,
            #[cfg(feature = "metrics")]
            metrics_handle,
        }
    }

    #[tokio::test]
    async fn readyz_returns_ok_on_fresh_state() {
        let tmp = TempDir::new().unwrap();
        let state = build_state(&tmp);
        let resp = readyz_handler(State(state)).await;
        assert_eq!(resp.0, StatusCode::OK);
    }

    #[tokio::test]
    async fn remember_and_ask_flow_sets_correlation() {
        let tmp = TempDir::new().unwrap();
        let state = build_state(&tmp);
        let cid = CorrelationId("test-cid".into());
        let world = WorldKey::new("w1");
        let atom = NewAtom {
            world: world.clone(),
            worker: WorkerKey::new("w"),
            kind: AtomKind::Observation,
            timestamp: Timestamp::new("2024-01-01T00:00:00Z"),
            importance: Importance::clamped(0.5),
            payload_json: "{}".into(),
            vector: None,
            flags: vec![],
            labels: vec![],
            links: vec![],
        };
        let _ = remember_handler(State(state.clone()), Extension(cid.clone()), Json(atom))
            .await
            .expect("remember");
        let ans = ask_handler(
            State(state),
            Extension(cid),
            Json(Question {
                world: world.clone(),
                text: "hi".into(),
                filter: None,
            }),
        )
        .await
        .expect("ask");
        assert_eq!(ans.0.world, world);
    }
}
