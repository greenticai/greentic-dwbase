use std::sync::Arc;
use std::time::{Duration, Instant};

use dwbase_core::{Atom, AtomId, AtomKind, Importance, Timestamp, WorkerKey, WorldKey};
use dwbase_engine::{DWBaseEngine, Embedder, NewAtom, Question, StreamEngine};
use dwbase_security::{Capabilities, LocalGatekeeper, TrustStore};
use dwbase_storage_sled::{DummyKeyProvider, SledConfig, SledStorage};
use dwbase_stream_local::LocalStreamEngine;
use dwbase_vector_hnsw::HnswVectorEngine;
use tempfile::TempDir;
use tokio::time::sleep;

#[derive(Clone)]
struct PerfEmbedder;

impl Embedder for PerfEmbedder {
    #[allow(clippy::type_complexity)]
    fn embed<'a>(
        &'a self,
        payload_json: &'a str,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = dwbase_engine::Result<Option<Vec<f32>>>> + Send + 'a>,
    > {
        let vec = payload_json
            .bytes()
            .take(4)
            .map(|b| b as f32 / 255.0)
            .collect::<Vec<_>>();
        Box::pin(async move { Ok(Some(vec)) })
    }
}

struct PerfCtx {
    _tmp: TempDir,
    engine: Arc<
        DWBaseEngine<
            SledStorage,
            HnswVectorEngine,
            LocalStreamEngine,
            LocalGatekeeper,
            PerfEmbedder,
        >,
    >,
    world: WorldKey,
}

impl PerfCtx {
    fn new() -> Self {
        let tmp = TempDir::new().expect("tmpdir");
        let storage = SledStorage::open(
            SledConfig::new(tmp.path()),
            Arc::new(DummyKeyProvider::default()),
        )
        .expect("sled");
        let vector = HnswVectorEngine::new();
        let stream = LocalStreamEngine::new();
        let gatekeeper = LocalGatekeeper::new(Capabilities::default(), TrustStore::default());
        let engine = Arc::new(DWBaseEngine::new(
            storage,
            vector,
            stream,
            gatekeeper,
            PerfEmbedder,
        ));
        Self {
            _tmp: tmp,
            engine,
            world: WorldKey::new("perf-world"),
        }
    }
}

fn make_atom(world: &WorldKey, idx: usize) -> NewAtom {
    NewAtom {
        world: world.clone(),
        worker: WorkerKey::new("perf-worker"),
        kind: AtomKind::Observation,
        timestamp: Timestamp::new(""),
        importance: Importance::clamped(0.5),
        payload_json: format!(r#"{{"text":"atom-{idx}"}}"#),
        vector: None,
        flags: vec![],
        labels: vec![],
        links: vec![],
    }
}

async fn remember_for(
    duration: Duration,
    engine: Arc<
        DWBaseEngine<
            SledStorage,
            HnswVectorEngine,
            LocalStreamEngine,
            LocalGatekeeper,
            PerfEmbedder,
        >,
    >,
    world: WorldKey,
) -> (usize, Vec<f64>) {
    let mut count = 0usize;
    let mut latencies = Vec::new();
    let start = Instant::now();
    while start.elapsed() < duration {
        let atom = make_atom(&world, count);
        let t0 = Instant::now();
        let _ = engine.remember(atom).await.unwrap();
        latencies.push(t0.elapsed().as_secs_f64() * 1_000.0);
        count += 1;
    }
    (count, latencies)
}

fn pct(values: &mut [f64], p: f64) -> f64 {
    values.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let idx = ((values.len() as f64 - 1.0) * p).round() as usize;
    values[idx]
}

fn warn_if(target: f64, actual: f64, label: &str) {
    if actual > target {
        println!(
            "[perf-warning] {label} p95 {:.2}ms exceeds target {:.2}ms",
            actual, target
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sustained_remember_rate() {
    let ctx = PerfCtx::new();
    let (count, mut latencies) = remember_for(
        Duration::from_secs(3),
        ctx.engine.clone(),
        ctx.world.clone(),
    )
    .await;
    let p95 = pct(&mut latencies, 0.95);
    let rate = count as f64 / 3.0;
    println!(
        "[perf] remember: total={}, rate={:.1} ops/s, p95={:.2}ms",
        count, rate, p95
    );
    warn_if(2.0, p95, "remember");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_ask_and_remember() {
    let ctx = PerfCtx::new();
    // warm up
    for i in 0..200 {
        let _ = ctx.engine.remember(make_atom(&ctx.world, i)).await.unwrap();
    }

    let duration = Duration::from_secs(3);
    let engine_for_ask = ctx.engine.clone();
    let world_for_ask = ctx.world.clone();
    let engine_for_remember = ctx.engine.clone();
    let world_for_remember = ctx.world.clone();

    let ask_task = tokio::spawn(async move {
        let mut count = 0usize;
        let mut lat = Vec::new();
        let start = Instant::now();
        while start.elapsed() < duration {
            let q = Question {
                world: world_for_ask.clone(),
                text: "perf ask".into(),
                filter: None,
            };
            let t0 = Instant::now();
            let _ = engine_for_ask.ask(q).await.unwrap();
            lat.push(t0.elapsed().as_secs_f64() * 1_000.0);
            count += 1;
        }
        (count, lat)
    });

    let remember_task = tokio::spawn(async move {
        remember_for(duration, engine_for_remember, world_for_remember).await
    });

    let (ask_res, remember_res) = tokio::join!(ask_task, remember_task);
    let (ask_count, mut ask_lat) = ask_res.expect("ask task");
    let (remember_count, mut remember_lat) = remember_res.expect("remember task");

    let ask_p95 = pct(&mut ask_lat, 0.95);
    let remember_p95 = pct(&mut remember_lat, 0.95);

    println!(
        "[perf] concurrent ask/remember: ask_total={}, ask_p95={:.2}ms, remember_total={}, remember_p95={:.2}ms",
        ask_count, ask_p95, remember_count, remember_p95
    );
    warn_if(3.0, ask_p95, "ask (hot)");
    warn_if(2.0, remember_p95, "remember");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn observe_fanout() {
    let ctx = PerfCtx::new();
    // create subscribers
    let subs: Vec<_> = (0..5)
        .map(|_| {
            ctx.engine
                .stream
                .subscribe(&ctx.world, dwbase_engine::AtomFilter::default())
                .unwrap()
        })
        .collect();

    let atom = Atom::builder(
        AtomId::new("fanout-1"),
        ctx.world.clone(),
        WorkerKey::new("fanout-worker"),
        AtomKind::Observation,
        Timestamp::new("2024-01-01T00:00:00Z"),
        Importance::clamped(0.5),
        r#"{"text":"fanout"}"#,
    )
    .build();

    let start = Instant::now();
    for _ in 0..50 {
        ctx.engine.observe(atom.clone()).await.unwrap();
    }
    // allow delivery
    sleep(Duration::from_millis(50)).await;

    let mut received = 0usize;
    for handle in subs {
        while ctx.engine.stream.poll(&handle).unwrap().is_some() {
            received += 1;
        }
    }
    let elapsed_ms = start.elapsed().as_secs_f64() * 1_000.0;
    println!(
        "[perf] observe fanout: delivered={}, elapsed={:.2}ms",
        received, elapsed_ms
    );
}
