use std::sync::Arc;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use dwbase_core::{Atom, AtomId, AtomKind, Importance, Timestamp, WorkerKey, WorldKey};
use dwbase_engine::{DWBaseEngine, Embedder, NewAtom, Question};
use dwbase_security::{Capabilities, LocalGatekeeper, TrustStore};
use dwbase_storage_sled::{DummyKeyProvider, SledConfig, SledStorage};
use dwbase_stream_local::LocalStreamEngine;
use dwbase_vector_hnsw::HnswVectorEngine;
use tempfile::TempDir;
use tokio::runtime::Runtime;

#[derive(Clone)]
struct BenchEmbedder {
    enable_vectors: bool,
}

impl Embedder for BenchEmbedder {
    #[allow(clippy::type_complexity)]
    fn embed<'a>(
        &'a self,
        payload_json: &'a str,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = dwbase_engine::Result<Option<Vec<f32>>>> + Send + 'a>,
    > {
        if !self.enable_vectors {
            return Box::pin(async { Ok(None) });
        }
        let vec = payload_json
            .as_bytes()
            .iter()
            .take(4)
            .map(|b| *b as f32 / 255.0)
            .collect::<Vec<_>>();
        Box::pin(async move {
            if vec.is_empty() {
                Ok(Some(vec![0.0]))
            } else {
                Ok(Some(vec))
            }
        })
    }
}

struct BenchCtx {
    _tmp: TempDir,
    engine: Arc<
        DWBaseEngine<
            SledStorage,
            HnswVectorEngine,
            LocalStreamEngine,
            LocalGatekeeper,
            BenchEmbedder,
        >,
    >,
}

impl BenchCtx {
    fn new(enable_vectors: bool) -> anyhow::Result<Self> {
        let dir = TempDir::new()?;
        let storage = SledStorage::open(
            SledConfig::new(dir.path()),
            Arc::new(DummyKeyProvider::default()),
        )?;
        let vector = HnswVectorEngine::new();
        let stream = LocalStreamEngine::new();
        let gatekeeper = LocalGatekeeper::new(Capabilities::default(), TrustStore::default());
        let embedder = BenchEmbedder { enable_vectors };
        let engine = Arc::new(DWBaseEngine::new(
            storage, vector, stream, gatekeeper, embedder,
        ));
        Ok(Self { _tmp: dir, engine })
    }
}

fn sample_new_atom(world: &WorldKey) -> NewAtom {
    NewAtom {
        world: world.clone(),
        worker: WorkerKey::new("worker-1"),
        kind: AtomKind::Observation,
        timestamp: Timestamp::new(""),
        importance: Importance::clamped(0.5),
        payload_json: r#"{"text":"hello"}"#.into(),
        vector: None,
        flags: vec![],
        labels: vec![],
        links: vec![],
    }
}

fn sample_atom(world: &WorldKey, id: usize) -> Atom {
    Atom::builder(
        AtomId::new(format!("atom-{id}")),
        world.clone(),
        WorkerKey::new("worker-1"),
        AtomKind::Observation,
        Timestamp::new("2024-01-01T00:00:00Z"),
        Importance::clamped(0.5),
        r#"{"text":"hello"}"#,
    )
    .build()
}

fn bench_remember(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let world = WorldKey::new("bench-world");
    let ctx = BenchCtx::new(false).unwrap();

    c.bench_function("remember_single_atom", |b| {
        b.to_async(&rt).iter(|| async {
            let _ = ctx.engine.remember(sample_new_atom(&world)).await.unwrap();
        })
    });

    for &batch in &[10usize, 100, 1_000] {
        c.bench_with_input(
            BenchmarkId::new("remember_batch_atoms", batch),
            &batch,
            |b, &size| {
                b.to_async(&rt).iter(|| async {
                    for _i in 0..size {
                        let mut atom = sample_new_atom(&world);
                        atom.payload_json = format!(r#"{{"text":"hello-{_i}"}}"#);
                        let _ = ctx.engine.remember(atom).await.unwrap();
                    }
                })
            },
        );
    }
}

fn bench_ask(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let world = WorldKey::new("ask-world");
    let ctx_hot = BenchCtx::new(false).unwrap();
    let ctx_vector = BenchCtx::new(true).unwrap();

    // Seed hot-path reflex data
    rt.block_on(async {
        for i in 0..500 {
            let mut atom = sample_new_atom(&world);
            atom.payload_json = format!(r#"{{"text":"hot-{i}"}}"#);
            let _ = ctx_hot.engine.remember(atom).await.unwrap();
        }
        for i in 0..500 {
            let mut atom = sample_new_atom(&world);
            atom.payload_json = format!(r#"{{"text":"vec-{i}"}}"#);
            let _ = ctx_vector.engine.remember(atom).await.unwrap();
        }
    });

    c.bench_function("ask_hot_path", |b| {
        b.to_async(&rt).iter(|| async {
            let question = Question {
                world: world.clone(),
                text: "hot question".into(),
                filter: None,
            };
            let _ = ctx_hot.engine.ask(question).await.unwrap();
        })
    });

    c.bench_function("ask_vector_path", |b| {
        b.to_async(&rt).iter(|| async {
            let question = Question {
                world: world.clone(),
                text: "vec question".into(),
                filter: None,
            };
            let _ = ctx_vector.engine.ask(question).await.unwrap();
        })
    });
}

fn bench_observe_replay(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let world = WorldKey::new("observe-world");
    let ctx = BenchCtx::new(false).unwrap();

    let atoms: Vec<Atom> = (0..500).map(|i| sample_atom(&world, i)).collect();

    c.bench_function("observe_throughput", |b| {
        b.to_async(&rt).iter(|| async {
            for atom in atoms.iter() {
                ctx.engine.observe(atom.clone()).await.unwrap();
            }
        })
    });

    // Seed storage for replay
    rt.block_on(async {
        for atom in atoms.iter() {
            let mut new = sample_new_atom(&world);
            new.payload_json = atom.payload_json().to_string();
            let _ = ctx.engine.remember(new).await.unwrap();
        }
    });

    let filter = dwbase_engine::AtomFilter {
        world: Some(world.clone()),
        limit: Some(200),
        ..Default::default()
    };

    let mut group = c.benchmark_group("replay_throughput");
    group.throughput(Throughput::Elements(200));
    group.bench_function("replay_throughput", |b| {
        b.to_async(&rt).iter(|| async {
            let _ = ctx
                .engine
                .replay(world.clone(), filter.clone())
                .await
                .unwrap();
        })
    });
    group.finish();
}

criterion_group!(benches, bench_remember, bench_ask, bench_observe_replay);
criterion_main!(benches);
