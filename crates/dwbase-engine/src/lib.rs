//! Engine traits and orchestrator skeleton for DWBase.
//!
//! The engine coordinates storage, vector search, streaming, gatekeeping, and embedding.
//! Atoms are immutable; updates should be represented as new atoms linked to predecessors.

use core::{future::Future, pin::Pin};
use dwbase_core::{
    Atom, AtomId, AtomKind, Importance, Link, LinkKind, Timestamp, WorkerKey, WorldKey,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{HashMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use std::time::Instant as StdInstant;
use thiserror::Error;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

pub type Result<T> = std::result::Result<T, DwbaseError>;

/// Errors surfaced by the DWBase engine layer.
#[derive(Debug, Error)]
pub enum DwbaseError {
    #[error("capability denied: {0}")]
    CapabilityDenied(String),
    #[error("invalid input: {0}")]
    InvalidInput(String),
    #[error("storage error: {0}")]
    Storage(String),
    #[error("vector error: {0}")]
    Vector(String),
    #[error("stream error: {0}")]
    Stream(String),
    #[error("internal error: {0}")]
    Internal(String),
}

/// Payload for a worker-submitted atom.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NewAtom {
    pub world: WorldKey,
    pub worker: WorkerKey,
    pub kind: AtomKind,
    pub timestamp: Timestamp,
    pub importance: Importance,
    pub payload_json: String,
    pub vector: Option<Vec<f32>>,
    pub flags: Vec<String>,
    pub labels: Vec<String>,
    pub links: Vec<Link>,
}

/// Filters used for selecting atoms.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct AtomFilter {
    pub world: Option<WorldKey>,
    pub kinds: Vec<AtomKind>,
    pub labels: Vec<String>,
    pub flags: Vec<String>,
    pub since: Option<Timestamp>,
    pub until: Option<Timestamp>,
    pub limit: Option<usize>,
}

/// Question posed to the system.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Question {
    pub world: WorldKey,
    pub text: String,
    pub filter: Option<AtomFilter>,
}

/// Answer returned for a question.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Answer {
    pub world: WorldKey,
    pub text: String,
    pub supporting_atoms: Vec<Atom>,
    #[serde(default)]
    pub warnings: Vec<String>,
}

/// Metadata describing a world.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WorldMeta {
    pub world: WorldKey,
    pub description: Option<String>,
    pub labels: Vec<String>,
}

/// Actions that may be taken on worlds.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum WorldAction {
    Create(WorldMeta),
    Archive(WorldKey),
    Resume(WorldKey),
}

/// In-memory reflex index for recent/high-importance atoms.
pub struct ReflexIndex {
    config: ReflexIndexConfig,
    buckets: RwLock<HashMap<WorldKey, HashMap<i64, Vec<Atom>>>>,
}

#[derive(Clone, Debug)]
pub struct ReflexIndexConfig {
    /// Total recency window to retain (seconds).
    pub recency_seconds: u64,
    /// Width of each bucket in seconds.
    pub bucket_width_seconds: u64,
}

impl Default for ReflexIndexConfig {
    fn default() -> Self {
        Self {
            recency_seconds: 300,
            bucket_width_seconds: 1,
        }
    }
}

#[derive(Default)]
struct ConflictIndex {
    supersedes: RwLock<HashMap<AtomId, AtomId>>,
    contradicts: RwLock<HashMap<AtomId, Vec<AtomId>>>,
    confirms: RwLock<HashMap<AtomId, Vec<AtomId>>>,
}

#[derive(Clone, Debug, Default)]
struct GcPolicy {
    retention_days: Option<u64>,
    min_importance: Option<f32>,
    replication_allow: Vec<String>,
    replication_deny: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndexMetadata {
    pub world: WorldKey,
    pub version: u64,
    pub embedder_version: String,
    pub last_rebuilt: Timestamp,
    pub ready: bool,
    #[serde(default)]
    pub rebuilding: bool,
    #[serde(default)]
    pub progress: f32,
    #[serde(default)]
    pub started_at: Option<Timestamp>,
    #[serde(default)]
    pub last_progress: Option<Timestamp>,
}

impl ConflictIndex {
    fn register(&self, atom: &Atom) {
        let mut supersedes = self.supersedes.write().expect("supersedes lock");
        let mut contradicts = self.contradicts.write().expect("contradicts lock");
        let mut confirms = self.confirms.write().expect("confirms lock");
        for link in atom.links() {
            match link.kind {
                LinkKind::Supersedes => {
                    supersedes.insert(link.target.clone(), atom.id().clone());
                }
                LinkKind::Contradicts => {
                    contradicts
                        .entry(link.target.clone())
                        .or_default()
                        .push(atom.id().clone());
                }
                LinkKind::Confirms => {
                    confirms
                        .entry(link.target.clone())
                        .or_default()
                        .push(atom.id().clone());
                }
                LinkKind::References => {}
            }
        }
    }

    fn superseded_by(&self, id: &AtomId) -> Option<AtomId> {
        self.supersedes
            .read()
            .expect("supersedes lock")
            .get(id)
            .cloned()
    }

    fn contradiction_count(&self, id: &AtomId) -> usize {
        self.contradicts
            .read()
            .expect("contradicts lock")
            .get(id)
            .map(|v| v.len())
            .unwrap_or(0)
    }

    fn confirmation_count(&self, id: &AtomId) -> usize {
        self.confirms
            .read()
            .expect("confirms lock")
            .get(id)
            .map(|v| v.len())
            .unwrap_or(0)
    }
}

#[derive(Clone, Debug, Default)]
pub struct ReflexFilter {
    pub world: Option<WorldKey>,
    pub kinds: Vec<AtomKind>,
    pub labels: Vec<String>,
    pub author: Option<WorkerKey>,
    pub min_importance: Option<f32>,
    pub since: Option<Timestamp>,
    pub exclude_flags: Vec<String>,
}

impl ReflexFilter {
    pub fn from_question(question: &Question) -> Self {
        let mut base = ReflexFilter {
            world: Some(question.world.clone()),
            ..Default::default()
        };
        if let Some(filter) = &question.filter {
            base.kinds = filter.kinds.clone();
            base.labels = filter.labels.clone();
            base.exclude_flags = filter.flags.clone();
            base.since = filter.since.clone();
        }
        base
    }
}

impl ReflexIndex {
    pub fn new(config: ReflexIndexConfig) -> Self {
        Self {
            config,
            buckets: RwLock::new(HashMap::new()),
        }
    }

    fn parse_ts(&self, ts: &Timestamp) -> Result<i64> {
        let dt = OffsetDateTime::parse(&ts.0, &Rfc3339)
            .map_err(|e| DwbaseError::InvalidInput(format!("invalid timestamp {}: {}", ts.0, e)))?;
        Ok(dt.unix_timestamp())
    }

    fn bucket_id(&self, ts: i64) -> i64 {
        let width = self.config.bucket_width_seconds.max(1) as i64;
        ts / width
    }

    fn trim_old_buckets(&self, world_buckets: &mut HashMap<i64, Vec<Atom>>, newest: i64) {
        let window_buckets =
            (self.config.recency_seconds / self.config.bucket_width_seconds.max(1)).max(1) as i64;
        let min_bucket = newest - window_buckets;
        #[cfg(feature = "metrics")]
        let before = world_buckets.len();
        world_buckets.retain(|bucket, _| *bucket >= min_bucket);
        #[cfg(feature = "metrics")]
        dwbase_metrics::record_gc_evictions(before.saturating_sub(world_buckets.len()) as u64);
    }

    /// Constant-time append (modulo lock acquisition).
    pub fn insert(&self, atom: Atom) -> Result<()> {
        let ts = self.parse_ts(atom.timestamp())?;
        let bucket = self.bucket_id(ts);
        let mut guard = self.buckets.write().expect("poisoned reflex index lock");
        let world_buckets = guard.entry(atom.world().clone()).or_default();
        world_buckets.entry(bucket).or_default().push(atom);
        self.trim_old_buckets(world_buckets, bucket);
        Ok(())
    }

    /// Filter atoms in the reflex window using the provided criteria.
    pub fn filter(&self, filter: &ReflexFilter) -> Result<Vec<Atom>> {
        let since_epoch = if let Some(since) = &filter.since {
            Some(self.parse_ts(since)?)
        } else {
            None
        };

        let guard = self.buckets.read().expect("poisoned reflex index lock");
        let world_views: Vec<(&WorldKey, &HashMap<i64, Vec<Atom>>)> = match &filter.world {
            Some(world) => guard
                .get(world)
                .map(|b| vec![(world, b)])
                .unwrap_or_default(),
            None => guard.iter().collect(),
        };

        let mut results = Vec::new();
        for (_world, buckets) in world_views {
            for atoms in buckets.values() {
                for atom in atoms {
                    if !filter.kinds.is_empty() && !filter.kinds.contains(atom.kind()) {
                        continue;
                    }
                    if let Some(author) = &filter.author {
                        if atom.worker() != author {
                            continue;
                        }
                    }
                    if let Some(min_imp) = filter.min_importance {
                        if atom.importance().get() < min_imp {
                            continue;
                        }
                    }
                    if let Some(since) = since_epoch {
                        let atom_ts = self.parse_ts(atom.timestamp())?;
                        if atom_ts < since {
                            continue;
                        }
                    }
                    if !filter.labels.is_empty()
                        && !filter.labels.iter().all(|l| atom.labels().contains(l))
                    {
                        continue;
                    }
                    if !filter.exclude_flags.is_empty()
                        && filter
                            .exclude_flags
                            .iter()
                            .any(|f| atom.flags().contains(f))
                    {
                        continue;
                    }
                    results.push(atom.clone());
                }
            }
        }
        Ok(results)
    }
}

/// Storage engine interface.
pub trait StorageEngine {
    fn append(&self, atom: Atom) -> Result<()>;
    fn get_by_ids(&self, ids: &[AtomId]) -> Result<Vec<Atom>>;
    fn scan(&self, world: &WorldKey, filter: &AtomFilter) -> Result<Vec<Atom>>;
    fn stats(&self, world: &WorldKey) -> Result<StorageStats>;
    /// Return atom ids for a world constrained by time window (ms since epoch).
    fn list_ids_in_window(&self, world: &WorldKey, window: &TimeWindow) -> Result<Vec<AtomId>>;
    /// Delete atoms by id; returns count removed.
    fn delete_atoms(&self, world: &WorldKey, ids: &[AtomId]) -> Result<usize>;
    /// List worlds known to the storage implementation.
    fn worlds(&self) -> Result<Vec<WorldKey>>;
}

/// Storage statistics.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct StorageStats {
    pub atom_count: usize,
    pub vector_count: usize,
}

/// Slice of time represented as milliseconds since unix epoch.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimeWindow {
    pub start_ms: i64,
    pub end_ms: i64,
}

impl TimeWindow {
    pub fn new(start_ms: i64, end_ms: i64) -> Self {
        Self { start_ms, end_ms }
    }
}

/// Window of time (inclusive bounds, milliseconds since epoch) covered by a summary.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SummaryWindow {
    pub start_ms: i64,
    pub end_ms: i64,
}

impl SummaryWindow {
    pub fn new(start_ms: i64, end_ms: i64) -> Self {
        Self { start_ms, end_ms }
    }
}

/// Advertises summary availability for a world.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SummaryAdvert {
    pub world: WorldKey,
    pub windows: Vec<SummaryWindow>,
    pub digest: String,
}

impl SummaryAdvert {
    pub fn new(world: WorldKey, windows: Vec<SummaryWindow>, digest: impl Into<String>) -> Self {
        Self {
            world,
            windows,
            digest: digest.into(),
        }
    }
}

/// In-memory catalog of summaries; read-only queries with simple upserts.
#[derive(Default)]
pub struct SummaryCatalog {
    entries: HashMap<WorldKey, Vec<SummaryAdvert>>,
}

impl SummaryCatalog {
    pub fn new() -> Self {
        Self::default()
    }

    /// Upsert a summary advert, replacing any existing advert for the same digest.
    pub fn upsert(&mut self, advert: SummaryAdvert) {
        let world_entry = self.entries.entry(advert.world.clone()).or_default();
        if let Some(pos) = world_entry.iter().position(|a| a.digest == advert.digest) {
            world_entry[pos] = advert;
        } else {
            world_entry.push(advert);
        }
    }

    /// List adverts known for a world (cloned).
    pub fn list(&self, world: &WorldKey) -> Vec<SummaryAdvert> {
        self.entries.get(world).cloned().unwrap_or_else(Vec::new)
    }

    /// List worlds with known adverts.
    pub fn worlds(&self) -> Vec<WorldKey> {
        self.entries.keys().cloned().collect()
    }
}

/// Vector index interface.
pub trait VectorEngine {
    fn upsert(&self, world: &WorldKey, atom_id: &AtomId, vector: &[f32]) -> Result<()>;
    fn search(
        &self,
        world: &WorldKey,
        query: &[f32],
        k: usize,
        filter: &AtomFilter,
    ) -> Result<Vec<AtomId>>;
    fn rebuild(&self, world: &WorldKey) -> Result<()>;
}

/// Stream processing interface.
pub trait StreamEngine {
    type Handle;

    fn publish(&self, atom: &Atom) -> Result<()>;
    fn subscribe(&self, world: &WorldKey, filter: AtomFilter) -> Result<Self::Handle>;
    fn poll(&self, handle: &Self::Handle) -> Result<Option<Atom>>;
    fn stop(&self, handle: Self::Handle) -> Result<()>;
}

/// Embedder for converting payloads to vectors.
pub trait Embedder {
    /// Produce an embedding vector for the payload JSON. Implementations may be async.
    #[allow(clippy::type_complexity)]
    #[cfg_attr(feature = "tokio", allow(async_fn_in_trait))]
    fn embed<'a>(
        &'a self,
        payload_json: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Vec<f32>>>> + Send + 'a>>;

    /// Return a stable identifier/version for the embedder model.
    fn model_version(&self) -> &'static str {
        "unknown"
    }
}

/// Gatekeeper for authorization/capability checks.
pub trait Gatekeeper {
    fn check_remember(&self, new_atom: &NewAtom) -> Result<()>;
    fn check_ask(&self, question: &Question) -> Result<()>;
    fn check_world_action(&self, action: &WorldAction) -> Result<()>;
}

/// Orchestrator tying together engine components.
pub struct DWBaseEngine<S, V, T, G, E> {
    pub storage: S,
    pub vector: V,
    pub stream: T,
    pub gatekeeper: G,
    pub embedder: E,
    pub reflex_index: ReflexIndex,
    conflict_index: ConflictIndex,
    suspicions: parking_lot::Mutex<HashMap<WorkerKey, VecDeque<StdInstant>>>,
    index_state: parking_lot::Mutex<HashMap<WorldKey, IndexMetadata>>,
    id_gen: AtomicU64,
}

impl<S, V, T, G, E> DWBaseEngine<S, V, T, G, E> {
    pub fn new(storage: S, vector: V, stream: T, gatekeeper: G, embedder: E) -> Self {
        Self {
            storage,
            vector,
            stream,
            gatekeeper,
            embedder,
            reflex_index: ReflexIndex::new(ReflexIndexConfig::default()),
            conflict_index: ConflictIndex::default(),
            suspicions: parking_lot::Mutex::new(HashMap::new()),
            index_state: parking_lot::Mutex::new(HashMap::new()),
            id_gen: AtomicU64::new(1),
        }
    }

    pub fn with_reflex_index(
        storage: S,
        vector: V,
        stream: T,
        gatekeeper: G,
        embedder: E,
        reflex_index: ReflexIndex,
    ) -> Self {
        Self {
            storage,
            vector,
            stream,
            gatekeeper,
            embedder,
            reflex_index,
            conflict_index: ConflictIndex::default(),
            suspicions: parking_lot::Mutex::new(HashMap::new()),
            index_state: parking_lot::Mutex::new(HashMap::new()),
            id_gen: AtomicU64::new(1),
        }
    }
}

impl<S, V, T, G, E> DWBaseEngine<S, V, T, G, E>
where
    S: StorageEngine,
    V: VectorEngine,
    T: StreamEngine,
    G: Gatekeeper,
    E: Embedder,
{
    fn new_id(&self) -> AtomId {
        let id = self.id_gen.fetch_add(1, Ordering::Relaxed);
        AtomId::new(format!("atom-{}", id))
    }

    fn register_conflicts(&self, atom: &Atom) {
        self.conflict_index.register(atom);
    }

    fn record_suspicion(&self, worker: &WorkerKey, now: StdInstant) -> usize {
        let mut map = self.suspicions.lock();
        let entry = map.entry(worker.clone()).or_default();
        entry.push_back(now);
        // keep 1s window
        while let Some(front) = entry.front().cloned() {
            if now.duration_since(front) > std::time::Duration::from_secs(1) {
                entry.pop_front();
            } else {
                break;
            }
        }
        entry.len()
    }

    fn is_impossible_payload(payload: &str) -> bool {
        let lower = payload.to_ascii_lowercase();
        lower.contains("nan") || lower.contains("infinity") || lower.contains("impossible")
    }

    fn low_trust(worker: &WorkerKey) -> bool {
        worker.0.to_ascii_lowercase().starts_with("lowtrust")
    }

    fn should_quarantine(&self, atom: &NewAtom, now: StdInstant) -> bool {
        let spam = self.record_suspicion(&atom.worker, now) > 5;
        let impossible = Self::is_impossible_payload(&atom.payload_json);
        let low_trust = Self::low_trust(&atom.worker) && atom.importance.get() > 0.7;
        spam || impossible || low_trust
    }

    fn trust_hint(&self, atom: &Atom) -> f32 {
        // Placeholder trust ordering: deterministic hash of worker id mapped into [0,1].
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        atom.worker().0.hash(&mut hasher);
        let v = hasher.finish();
        (v % 10_000) as f32 / 10_000.0
    }

    fn rank_score(&self, atom: &Atom) -> f32 {
        let mut score = atom.importance().get();
        if self.conflict_index.superseded_by(atom.id()).is_some() {
            score -= 1.0;
        }
        let contradictions = self.conflict_index.contradiction_count(atom.id());
        if contradictions > 0 {
            score -= 0.3 * contradictions as f32;
        }
        let confirms = self.conflict_index.confirmation_count(atom.id());
        if confirms > 0 {
            score += 0.2 * confirms as f32;
        }
        score
    }

    fn parse_policy(&self, atoms: &[Atom], world: &WorldKey) -> GcPolicy {
        // Defaults: keep atoms for 365 days, min importance 0.0 unless overridden.
        let mut policy = GcPolicy {
            retention_days: Some(365),
            ..Default::default()
        };
        let mut retention_min_days: Option<u64> = None;

        let mut policy_atoms: Vec<Atom> = atoms
            .iter()
            .filter(|a| a.world() == world)
            .cloned()
            .collect();

        // Policy worlds:
        // - policy:<world> (applies to that world)
        // - tenant:<id>/policy (applies to tenant-scoped worlds)
        let policy_world = WorldKey::new(format!("policy:{}", world.0));
        if let Ok(mut extra) = self.storage.scan(&policy_world, &AtomFilter::default()) {
            policy_atoms.append(&mut extra);
        }
        if let Some(tenant_world) = world.0.strip_prefix("tenant:").and_then(|rest| {
            rest.split_once('/')
                .map(|(tenant, _)| WorldKey::new(format!("tenant:{tenant}/policy")))
        }) {
            if let Ok(mut extra) = self.storage.scan(&tenant_world, &AtomFilter::default()) {
                policy_atoms.append(&mut extra);
            }
        }

        for atom in policy_atoms {
            for label in atom.labels() {
                if let Some(val) = label.strip_prefix("policy:retention_days=") {
                    if let Ok(days) = val.parse::<u64>() {
                        policy.retention_days = Some(days);
                    }
                }
                if let Some(val) = label.strip_prefix("policy:retention_min_days=") {
                    if let Ok(days) = val.parse::<u64>() {
                        retention_min_days = Some(days);
                    }
                }
                if let Some(val) = label.strip_prefix("policy:min_importance=") {
                    if let Ok(v) = val.parse::<f32>() {
                        policy.min_importance = Some(v);
                    }
                }
                if let Some(val) = label.strip_prefix("policy:replication_allow=") {
                    if !val.is_empty() {
                        policy.replication_allow.push(val.to_string());
                    }
                }
                if let Some(val) = label.strip_prefix("policy:replication_deny=") {
                    if !val.is_empty() {
                        policy.replication_deny.push(val.to_string());
                    }
                }
            }
        }

        if let Some(min_days) = retention_min_days {
            match policy.retention_days {
                Some(current) if current < min_days => policy.retention_days = Some(min_days),
                None => policy.retention_days = Some(min_days),
                _ => {}
            }
        }
        policy
    }

    fn is_policy_atom(atom: &Atom) -> bool {
        atom.labels()
            .iter()
            .any(|l| l.starts_with("policy:") || l == "world_meta")
    }

    fn world_status(&self, world: &WorldKey) -> Result<Option<String>> {
        let atoms = self
            .storage
            .scan(
                world,
                &AtomFilter {
                    world: Some(world.clone()),
                    kinds: Vec::new(),
                    labels: vec!["world_meta".into()],
                    flags: Vec::new(),
                    since: None,
                    until: None,
                    limit: None,
                },
            )
            .unwrap_or_default();
        let mut latest: Option<(OffsetDateTime, String)> = None;
        for atom in atoms {
            let ts = parse_ts(atom.timestamp()).unwrap_or(OffsetDateTime::UNIX_EPOCH);
            let status = atom
                .labels()
                .iter()
                .find_map(|l| l.strip_prefix("world_status:"))
                .map(|s| s.to_string());
            if let Some(status) = status {
                if latest.as_ref().map(|(t, _)| *t < ts).unwrap_or(true) {
                    latest = Some((ts, status));
                }
            }
        }
        Ok(latest.map(|(_, s)| s))
    }

    pub fn world_archived(&self, world: &WorldKey) -> Result<bool> {
        Ok(matches!(
            self.world_status(world)?,
            Some(status) if status == "archived"
        ))
    }

    /// List known worlds, excluding archived ones by default.
    pub fn worlds(&self) -> Result<Vec<WorldKey>> {
        self.worlds_filtered(false)
    }

    /// List known worlds with optional archived inclusion.
    pub fn worlds_filtered(&self, include_archived: bool) -> Result<Vec<WorldKey>> {
        let mut worlds = self.storage.worlds()?;
        worlds.sort_by(|a, b| a.0.cmp(&b.0));
        if include_archived {
            return Ok(worlds);
        }
        let mut filtered = Vec::new();
        for world in worlds {
            if self.world_archived(&world)? {
                continue;
            }
            filtered.push(world);
        }
        Ok(filtered)
    }

    fn now_timestamp(&self) -> Timestamp {
        Timestamp(
            OffsetDateTime::now_utc()
                .format(&Rfc3339)
                .unwrap_or_else(|_| "1970-01-01T00:00:00Z".into()),
        )
    }

    pub fn index_status(&self) -> Vec<IndexMetadata> {
        self.index_state.lock().values().cloned().collect()
    }

    fn ensure_index_entry(&self, world: &WorldKey) {
        let mut guard = self.index_state.lock();
        guard.entry(world.clone()).or_insert_with(|| IndexMetadata {
            world: world.clone(),
            version: 1,
            embedder_version: self.embedder.model_version().into(),
            last_rebuilt: self.now_timestamp(),
            ready: true,
            rebuilding: false,
            progress: 1.0,
            started_at: None,
            last_progress: None,
        });
    }

    fn maybe_rebuild_index(&self, world: &WorldKey) {
        self.ensure_index_entry(world);
        let mut guard = self.index_state.lock();
        if let Some(meta) = guard.get_mut(world) {
            if meta.embedder_version != self.embedder.model_version() {
                meta.ready = false;
                meta.rebuilding = true;
                meta.progress = 0.0;
                let now = self.now_timestamp();
                meta.started_at = Some(now.clone());
                meta.last_progress = Some(now.clone());
                let _ = self.vector.rebuild(world);
                meta.version += 1;
                meta.embedder_version = self.embedder.model_version().into();
                meta.last_rebuilt = self.now_timestamp();
                meta.ready = true;
                meta.rebuilding = false;
                meta.progress = 1.0;
                meta.last_progress = Some(meta.last_rebuilt.clone());
            }
        }
    }

    fn atom_from_new(&self, id: AtomId, mut new_atom: NewAtom) -> Atom {
        let mut builder = Atom::builder(
            id,
            new_atom.world,
            new_atom.worker,
            new_atom.kind,
            new_atom.timestamp,
            new_atom.importance,
            new_atom.payload_json,
        );
        builder = builder.vector(new_atom.vector);
        for flag in new_atom.flags.drain(..) {
            builder = builder.add_flag(flag);
        }
        for label in new_atom.labels.drain(..) {
            builder = builder.add_label(label);
        }
        for link in new_atom.links.drain(..) {
            builder = builder.add_typed_link(link.target, link.kind);
        }
        builder.build()
    }

    /// Remember (persist) a new atom submitted by a worker.
    pub async fn remember(&self, mut new_atom: NewAtom) -> Result<AtomId> {
        #[cfg(feature = "metrics")]
        let start = StdInstant::now();

        self.gatekeeper.check_remember(&new_atom)?;
        let now = StdInstant::now();

        // Fill in id and timestamp if missing.
        let id = self.new_id();
        if new_atom.timestamp.0.is_empty() {
            new_atom.timestamp = self.now_timestamp();
        }

        self.ensure_index_entry(&new_atom.world);
        self.maybe_rebuild_index(&new_atom.world);

        // Quarantine check
        if self.should_quarantine(&new_atom, now) {
            new_atom.world = WorldKey::new(format!("quarantine:{}", new_atom.world.0));
            if !new_atom.flags.contains(&"suspect".to_string()) {
                new_atom.flags.push("suspect".into());
            }
            if !new_atom.labels.contains(&"quarantine".to_string()) {
                new_atom.labels.push("quarantine".into());
            }
        }

        // Optional embedding.
        let embedded_vector = self.embedder.embed(&new_atom.payload_json).await?;
        if new_atom.vector.is_none() {
            new_atom.vector = embedded_vector;
        }

        let atom = self.atom_from_new(id.clone(), new_atom);

        #[cfg(feature = "metrics")]
        let freshness = parse_ts(atom.timestamp()).ok().and_then(|ts| {
            let age = OffsetDateTime::now_utc() - ts;
            (!age.is_negative()).then_some(age)
        });

        if let Some(vec) = atom.vector() {
            self.vector.upsert(atom.world(), atom.id(), vec)?;
        }
        self.storage.append(atom.clone())?;
        self.reflex_index.insert(atom.clone())?;
        self.register_conflicts(&atom);
        self.stream.publish(&atom)?;

        #[cfg(feature = "metrics")]
        {
            dwbase_metrics::record_remember_latency(start.elapsed());
            if let Some(age) = freshness {
                let age_std = std::time::Duration::from_secs_f64(age.as_seconds_f64());
                dwbase_metrics::record_index_freshness(age_std);
            }
        }

        Ok(id)
    }

    /// Answer a question based on stored atoms.
    pub async fn ask(&self, question: Question) -> Result<Answer> {
        #[cfg(feature = "metrics")]
        let start = StdInstant::now();

        let reflex_filter = ReflexFilter::from_question(&question);
        self.gatekeeper.check_ask(&question)?;

        let mut candidates = self.reflex_index.filter(&reflex_filter)?;
        let mut warnings = Vec::new();
        self.ensure_index_entry(&question.world);
        self.maybe_rebuild_index(&question.world);
        let ready_meta = self
            .index_state
            .lock()
            .get(&question.world)
            .cloned()
            .unwrap_or_else(|| IndexMetadata {
                world: question.world.clone(),
                version: 1,
                embedder_version: self.embedder.model_version().into(),
                last_rebuilt: self.now_timestamp(),
                ready: true,
                rebuilding: false,
                progress: 1.0,
                started_at: None,
                last_progress: None,
            });
        let ready = ready_meta.ready;

        // Fallback to storage scan if reflex is empty.
        if candidates.is_empty() {
            let mut filter = question.filter.clone().unwrap_or_default();
            filter.world = Some(question.world.clone());
            candidates = self.storage.scan(&question.world, &filter)?;
        }

        // Exclude quarantine worlds unless explicitly queried.
        let querying_quarantine = question.world.0.starts_with("quarantine:");
        if !querying_quarantine {
            candidates.retain(|a| !a.world().0.starts_with("quarantine:"));
        }

        // Vector search if we can embed question.
        if ready {
            if let Ok(Some(query_vec)) = self.embedder.embed(&question.text).await {
                let filter = question.filter.clone().unwrap_or_default();
                let ids = self
                    .vector
                    .search(&question.world, &query_vec, 10, &filter)?;
                if !ids.is_empty() {
                    let fetched = self.storage.get_by_ids(&ids)?;
                    candidates.extend(fetched);
                }
            }
        } else {
            warnings.push("index not ready; used fallback search".into());
            if ready_meta.rebuilding {
                warnings.push("index rebuilding in background".into());
            }
        }

        // if still empty, bounded storage scan.
        if candidates.is_empty() {
            let filter = question.filter.clone().unwrap_or_default();
            let mut storage_scan = self.storage.scan(&question.world, &filter)?;
            storage_scan.truncate(20);
            candidates.extend(storage_scan);
        }

        // Deduplicate by AtomId
        let mut seen = std::collections::HashSet::new();
        candidates.retain(|a| seen.insert(a.id().clone()));

        // Trust/confirmation-aware rerank: score -> timestamp -> importance -> trust hint.
        candidates.sort_by(|a, b| {
            let score_a = self.rank_score(a);
            let score_b = self.rank_score(b);
            score_b
                .partial_cmp(&score_a)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| {
                    let ts_a = parse_ts(a.timestamp()).unwrap_or(OffsetDateTime::UNIX_EPOCH);
                    let ts_b = parse_ts(b.timestamp()).unwrap_or(OffsetDateTime::UNIX_EPOCH);
                    ts_b.cmp(&ts_a)
                })
                .then_with(|| {
                    b.importance()
                        .get()
                        .partial_cmp(&a.importance().get())
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
                .then_with(|| {
                    self.trust_hint(b)
                        .partial_cmp(&self.trust_hint(a))
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
        });

        let answer = Answer {
            world: question.world,
            text: "answer-pending".into(),
            supporting_atoms: candidates,
            warnings,
        };
        #[cfg(feature = "metrics")]
        dwbase_metrics::record_ask_latency(start.elapsed());
        Ok(answer)
    }

    /// Return atom ids for a world within a time window.
    pub fn list_ids_in_window(&self, world: &WorldKey, window: &TimeWindow) -> Result<Vec<AtomId>> {
        self.storage.list_ids_in_window(world, window)
    }

    /// Shallow readiness probe: returns true if storage is reachable.
    pub fn storage_ready(&self) -> bool {
        self.storage.worlds().is_ok()
    }

    /// Maximum rebuild lag (ms) across worlds that are rebuilding or not ready.
    pub fn max_index_rebuild_lag_ms(&self) -> Option<u64> {
        let now = OffsetDateTime::now_utc();
        let guard = self.index_state.lock();
        let mut max_lag = None;
        for meta in guard.values() {
            if meta.ready && !meta.rebuilding {
                continue;
            }
            let ts = meta
                .last_progress
                .as_ref()
                .or(meta.started_at.as_ref())
                .unwrap_or(&meta.last_rebuilt);
            if let Ok(t) = parse_ts(ts) {
                let lag = now - t;
                let ms = lag.whole_milliseconds().max(0) as u64;
                if max_lag.map(|m| ms > m).unwrap_or(true) {
                    max_lag = Some(ms);
                }
            }
        }
        max_lag
    }

    /// Fetch atoms by id using the underlying storage engine.
    pub fn get_atoms(&self, ids: &[AtomId]) -> Result<Vec<Atom>> {
        self.storage.get_by_ids(ids)
    }

    /// Ingest atoms received from peers; idempotent (skips existing ids).
    pub async fn ingest_remote_atoms(&self, atoms: Vec<Atom>) -> Result<Vec<AtomId>> {
        let mut ingested = Vec::new();
        for atom in atoms {
            let id = atom.id().clone();
            if !self
                .storage
                .get_by_ids(std::slice::from_ref(&id))?
                .is_empty()
            {
                continue;
            }
            if let Some(vec) = atom.vector() {
                self.vector.upsert(atom.world(), atom.id(), vec)?;
            }
            self.storage.append(atom.clone())?;
            self.reflex_index.insert(atom.clone())?;
            self.register_conflicts(&atom);
            self.stream.publish(&atom)?;
            ingested.push(id);
        }
        Ok(ingested)
    }

    /// Run garbage collection once for all worlds. Returns number of atoms evicted.
    pub fn gc_once(&self, _max_disk_mb: Option<u64>) -> Result<usize> {
        let mut evicted = 0usize;
        let worlds = self.storage.worlds()?;
        for world in worlds {
            let mut atoms = self.storage.scan(&world, &AtomFilter::default())?;
            let policy = self.parse_policy(&atoms, &world);
            let referenced: std::collections::HashSet<_> = atoms
                .iter()
                .flat_map(|a| a.links().iter().map(|l| l.target.clone()))
                .collect();

            atoms.sort_by_key(|a| parse_ts(a.timestamp()).unwrap_or(OffsetDateTime::UNIX_EPOCH));
            let now = atoms
                .iter()
                .filter_map(|a| parse_ts(a.timestamp()).ok())
                .max()
                .unwrap_or_else(OffsetDateTime::now_utc);
            let mut to_delete = Vec::new();

            for atom in &atoms {
                if Self::is_policy_atom(atom) {
                    continue;
                }
                if referenced.contains(atom.id()) {
                    continue;
                }
                if atom.labels().iter().any(|l| l.starts_with("world_meta")) {
                    continue;
                }
                let expired = policy
                    .retention_days
                    .filter(|d| *d > 0)
                    .and_then(|days| {
                        parse_ts(atom.timestamp())
                            .ok()
                            .map(|ts| now - ts > time::Duration::days(days as i64))
                    })
                    .unwrap_or(false);
                let low_importance = policy
                    .min_importance
                    .map(|min| atom.importance().get() < min)
                    .unwrap_or(false);
                if expired || low_importance {
                    to_delete.push(atom.id().clone());
                    continue;
                }
            }
            if !to_delete.is_empty() {
                evicted += self.storage.delete_atoms(&world, &to_delete)?;
            }
        }
        #[cfg(feature = "metrics")]
        {
            dwbase_metrics::record_gc_evictions(evicted as u64);
        }
        Ok(evicted)
    }

    /// Observe (ingest) an atom and publish it to streams.
    pub async fn observe(&self, atom: Atom) -> Result<()> {
        self.stream.publish(&atom)?;
        Ok(())
    }

    /// Replay atoms for a world using a filter.
    pub async fn replay(&self, world: WorldKey, filter: AtomFilter) -> Result<Vec<Atom>> {
        self.storage.scan(&world, &filter)
    }

    /// Perform a world-level action (create/archive/resume).
    pub async fn manage_world(&self, action: WorldAction) -> Result<()> {
        self.gatekeeper.check_world_action(&action)?;

        let (world, next_status, description, mut extra_labels) = match action.clone() {
            WorldAction::Create(meta) => (
                meta.world,
                "active".to_string(),
                meta.description,
                meta.labels,
            ),
            WorldAction::Archive(world) => (world, "archived".to_string(), None, Vec::new()),
            WorldAction::Resume(world) => (world, "active".to_string(), None, Vec::new()),
        };

        // Short-circuit idempotent transitions.
        if let Some(current) = self.world_status(&world)? {
            if current == next_status && description.is_none() && extra_labels.is_empty() {
                return Ok(());
            }
        }

        let now = self.now_timestamp();
        let mut payload = json!({
            "action": next_status,
            "world": world.0,
            "at": now.0,
        });
        if let Some(d) = &description {
            payload["description"] = json!(d);
        }
        if !extra_labels.is_empty() {
            payload["labels"] = json!(extra_labels);
        }

        let mut builder = Atom::builder(
            self.new_id(),
            world.clone(),
            WorkerKey::new("system"),
            AtomKind::Reflection,
            now,
            Importance::clamped(0.1),
            payload.to_string(),
        )
        .add_label("world_meta".to_string())
        .add_label(format!("world_status:{next_status}"));

        for l in extra_labels.drain(..) {
            builder = builder.add_label(l);
        }

        let atom = builder.build();

        if let Some(vec) = atom.vector() {
            self.vector.upsert(atom.world(), atom.id(), vec)?;
        }
        self.storage.append(atom.clone())?;
        self.reflex_index.insert(atom.clone())?;
        self.register_conflicts(&atom);
        self.stream.publish(&atom)?;
        Ok(())
    }
}

fn parse_ts(ts: &Timestamp) -> Result<OffsetDateTime> {
    OffsetDateTime::parse(&ts.0, &Rfc3339)
        .map_err(|e| DwbaseError::InvalidInput(format!("invalid timestamp {}: {}", ts.0, e)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use dwbase_core::{Link, LinkKind};
    use std::collections::HashMap;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    };

    #[derive(Clone)]
    struct AllowGatekeeper;

    impl Gatekeeper for AllowGatekeeper {
        fn check_remember(&self, _new_atom: &NewAtom) -> Result<()> {
            Ok(())
        }

        fn check_ask(&self, _question: &Question) -> Result<()> {
            Ok(())
        }

        fn check_world_action(&self, _action: &WorldAction) -> Result<()> {
            Ok(())
        }
    }

    struct DummyEmbedder;

    impl Embedder for DummyEmbedder {
        #[allow(clippy::type_complexity)]
        fn embed<'a>(
            &'a self,
            payload_json: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<Option<Vec<f32>>>> + Send + 'a>> {
            Box::pin(async move {
                let parts: Vec<f32> = payload_json
                    .split(',')
                    .filter_map(|p| p.trim().parse::<f32>().ok())
                    .collect();
                if parts.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(parts))
                }
            })
        }

        fn model_version(&self) -> &'static str {
            "dummy-test"
        }
    }

    #[derive(Default)]
    struct MemStorage {
        atoms: Mutex<Vec<Atom>>,
    }

    impl StorageEngine for MemStorage {
        fn append(&self, atom: Atom) -> Result<()> {
            let mut atoms = self.atoms.lock().unwrap();
            atoms.push(atom);
            Ok(())
        }

        fn get_by_ids(&self, ids: &[AtomId]) -> Result<Vec<Atom>> {
            let atoms = self.atoms.lock().unwrap();
            Ok(atoms
                .iter()
                .filter(|a| ids.contains(a.id()))
                .cloned()
                .collect())
        }

        fn scan(&self, world: &WorldKey, filter: &AtomFilter) -> Result<Vec<Atom>> {
            let atoms = self.atoms.lock().unwrap();
            let mut results = Vec::new();
            for atom in atoms.iter() {
                if atom.world() != world {
                    continue;
                }
                if !filter.kinds.is_empty() && !filter.kinds.contains(atom.kind()) {
                    continue;
                }
                if !filter.labels.is_empty()
                    && !filter.labels.iter().all(|l| atom.labels().contains(l))
                {
                    continue;
                }
                if !filter.flags.is_empty()
                    && !filter.flags.iter().all(|f| atom.flags().contains(f))
                {
                    continue;
                }
                results.push(atom.clone());
            }
            Ok(results)
        }

        fn stats(&self, _world: &WorldKey) -> Result<StorageStats> {
            let atoms = self.atoms.lock().unwrap();
            Ok(StorageStats {
                atom_count: atoms.len(),
                vector_count: 0,
            })
        }

        fn list_ids_in_window(&self, world: &WorldKey, window: &TimeWindow) -> Result<Vec<AtomId>> {
            let atoms = self.atoms.lock().unwrap();
            Ok(atoms
                .iter()
                .filter(|a| a.world() == world)
                .filter(|a| {
                    let ts = Timestamp::new(a.timestamp().0.clone());
                    if let Ok(dt) = OffsetDateTime::parse(ts.0.as_str(), &Rfc3339) {
                        let ms = dt.unix_timestamp_nanos() / 1_000_000;
                        let start = window.start_ms as i128;
                        let end = window.end_ms as i128;
                        ms >= start && ms <= end
                    } else {
                        false
                    }
                })
                .map(|a| a.id().clone())
                .collect())
        }

        fn delete_atoms(&self, world: &WorldKey, ids: &[AtomId]) -> Result<usize> {
            let mut atoms = self.atoms.lock().unwrap();
            let before = atoms.len();
            atoms.retain(|a| !(a.world() == world && ids.contains(a.id())));
            Ok(before.saturating_sub(atoms.len()))
        }

        fn worlds(&self) -> Result<Vec<WorldKey>> {
            let atoms = self.atoms.lock().unwrap();
            Ok(atoms
                .iter()
                .map(|a| a.world().clone())
                .collect::<std::collections::HashSet<_>>()
                .into_iter()
                .collect())
        }
    }

    #[derive(Default)]
    struct MemVector {
        dims: Mutex<Option<usize>>,
        data: Mutex<HashMap<AtomId, Vec<f32>>>,
    }

    impl VectorEngine for MemVector {
        fn upsert(&self, _world: &WorldKey, atom_id: &AtomId, vector: &[f32]) -> Result<()> {
            let mut data = self.data.lock().unwrap();
            data.insert(atom_id.clone(), vector.to_vec());
            let mut dims = self.dims.lock().unwrap();
            dims.get_or_insert(vector.len());
            Ok(())
        }

        fn search(
            &self,
            _world: &WorldKey,
            query: &[f32],
            k: usize,
            _filter: &AtomFilter,
        ) -> Result<Vec<AtomId>> {
            let data = self.data.lock().unwrap();
            let mut pairs: Vec<(AtomId, f32)> = data
                .iter()
                .map(|(id, v)| {
                    let dist = v
                        .iter()
                        .zip(query.iter())
                        .map(|(a, b)| (a - b) * (a - b))
                        .sum();
                    (id.clone(), dist)
                })
                .collect();
            pairs.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
            Ok(pairs.into_iter().take(k).map(|(id, _)| id).collect())
        }

        fn rebuild(&self, _world: &WorldKey) -> Result<()> {
            Ok(())
        }
    }

    #[derive(Default)]
    struct MemStream {
        next: AtomicUsize,
        subscribers: Mutex<HashMap<usize, (AtomFilter, Vec<Atom>)>>,
    }

    impl StreamEngine for MemStream {
        type Handle = usize;

        fn publish(&self, atom: &Atom) -> Result<()> {
            let mut subs = self.subscribers.lock().unwrap();
            for (_id, (filter, queue)) in subs.iter_mut() {
                let mut include = true;
                if let Some(world) = &filter.world {
                    include &= atom.world() == world;
                }
                if !filter.kinds.is_empty() && !filter.kinds.contains(atom.kind()) {
                    include = false;
                }
                if include {
                    queue.push(atom.clone());
                }
            }
            Ok(())
        }

        fn subscribe(&self, _world: &WorldKey, filter: AtomFilter) -> Result<Self::Handle> {
            let id = self.next.fetch_add(1, Ordering::Relaxed);
            let mut subs = self.subscribers.lock().unwrap();
            subs.insert(id, (filter, Vec::new()));
            Ok(id)
        }

        fn poll(&self, handle: &Self::Handle) -> Result<Option<Atom>> {
            let mut subs = self.subscribers.lock().unwrap();
            if let Some((_, queue)) = subs.get_mut(handle) {
                if queue.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(queue.remove(0)))
                }
            } else {
                Err(DwbaseError::Stream(format!("unknown handle {handle}")))
            }
        }

        fn stop(&self, handle: Self::Handle) -> Result<()> {
            let mut subs = self.subscribers.lock().unwrap();
            subs.remove(&handle);
            Ok(())
        }
    }

    fn engine_components(
    ) -> DWBaseEngine<MemStorage, MemVector, MemStream, AllowGatekeeper, DummyEmbedder> {
        let storage = MemStorage::default();
        let vector = MemVector::default();
        let stream = MemStream::default();
        let gatekeeper = AllowGatekeeper;
        DWBaseEngine::new(storage, vector, stream, gatekeeper, DummyEmbedder)
    }

    fn new_atom_with(ts: &str, payload: &str, label: &str) -> NewAtom {
        NewAtom {
            world: WorldKey::new("w1"),
            worker: WorkerKey::new("worker-1"),
            kind: AtomKind::Observation,
            timestamp: Timestamp::new(ts),
            importance: Importance::new(0.5).unwrap(),
            payload_json: payload.into(),
            vector: None,
            flags: vec![],
            labels: vec![label.into()],
            links: vec![],
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn remember_and_ask_returns_ranked_atoms() {
        let engine = engine_components();
        let a1 = new_atom_with("2024-01-01T00:00:01Z", "0,0", "a1");
        let a2 = new_atom_with("2024-01-01T00:00:00Z", "10,10", "a2");

        let id1 = engine.remember(a1).await.expect("remember a1");
        let id2 = engine.remember(a2).await.expect("remember a2");

        assert_ne!(id1, id2);

        let question = Question {
            world: WorldKey::new("w1"),
            text: "0,0".into(),
            filter: None,
        };

        let answer = engine.ask(question).await.expect("ask");
        let ids: Vec<_> = answer
            .supporting_atoms
            .iter()
            .map(|a| a.id().clone())
            .collect();
        assert_eq!(ids.first().unwrap(), &id1);
        assert!(ids.contains(&id2));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn superseded_atoms_downranked() {
        let engine = engine_components();
        let base = new_atom_with("2024-01-01T00:00:00Z", "p", "base");
        let base_id = engine.remember(base).await.expect("base");

        let newer = NewAtom {
            world: WorldKey::new("w1"),
            worker: WorkerKey::new("worker-1"),
            kind: AtomKind::Observation,
            timestamp: Timestamp::new("2024-01-01T00:00:05Z"),
            importance: Importance::new(0.5).unwrap(),
            payload_json: "p-new".into(),
            vector: None,
            flags: vec![],
            labels: vec![],
            links: vec![Link::new(base_id.clone(), LinkKind::Supersedes)],
        };
        let newer_id = engine.remember(newer).await.expect("newer");

        let question = Question {
            world: WorldKey::new("w1"),
            text: "p".into(),
            filter: None,
        };

        let answer = engine.ask(question).await.expect("ask");
        let ids: Vec<_> = answer
            .supporting_atoms
            .iter()
            .map(|a| a.id().clone())
            .collect();
        assert_eq!(ids.first(), Some(&newer_id));
        assert!(ids.contains(&base_id));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn gc_retains_linked_and_policy_atoms() {
        let engine = engine_components();
        let base = new_atom_with("2024-01-01T00:00:00Z", "p", "keep");
        let base_id = engine.remember(base).await.unwrap();
        let linker = NewAtom {
            world: WorldKey::new("w1"),
            worker: WorkerKey::new("w1-worker"),
            kind: AtomKind::Observation,
            timestamp: Timestamp::new("2024-01-02T00:00:00Z"),
            importance: Importance::new(0.5).unwrap(),
            payload_json: "linker".into(),
            vector: None,
            flags: vec![],
            labels: vec![],
            links: vec![Link::new(base_id.clone(), LinkKind::References)],
        };
        engine.remember(linker).await.unwrap();

        // Add a retention policy atom.
        let policy = NewAtom {
            world: WorldKey::new("w1"),
            worker: WorkerKey::new("w1-worker"),
            kind: AtomKind::Reflection,
            timestamp: Timestamp::new("2024-01-03T00:00:00Z"),
            importance: Importance::new(0.5).unwrap(),
            payload_json: "policy".into(),
            vector: None,
            flags: vec![],
            labels: vec!["policy:retention_days=0".into()],
            links: vec![],
        };
        engine.remember(policy).await.unwrap();

        // GC should not delete the linked base despite retention=0 because it is referenced.
        let evicted = engine.gc_once(None).unwrap();
        assert_eq!(evicted, 0);
        let replayed = engine
            .replay(WorldKey::new("w1"), AtomFilter::default())
            .await
            .unwrap();
        assert_eq!(replayed.len(), 3);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn gc_applies_retention_and_min_importance() {
        let engine = engine_components();
        let old_low = NewAtom {
            world: WorldKey::new("w1"),
            worker: WorkerKey::new("w1-worker"),
            kind: AtomKind::Observation,
            timestamp: Timestamp::new("2020-01-01T00:00:00Z"),
            importance: Importance::new(0.1).unwrap(),
            payload_json: "old".into(),
            vector: None,
            flags: vec![],
            labels: vec![],
            links: vec![],
        };
        let recent = new_atom_with("2024-01-01T00:00:00Z", "new", "recent");
        engine.remember(old_low).await.unwrap();
        engine.remember(recent).await.unwrap();
        let policy = NewAtom {
            world: WorldKey::new("w1"),
            worker: WorkerKey::new("w1-worker"),
            kind: AtomKind::Reflection,
            timestamp: Timestamp::new("2024-01-02T00:00:00Z"),
            importance: Importance::new(0.5).unwrap(),
            payload_json: "policy".into(),
            vector: None,
            flags: vec![],
            labels: vec![
                "policy:retention_days=365".into(),
                "policy:min_importance=0.2".into(),
            ],
            links: vec![],
        };
        engine.remember(policy).await.unwrap();

        let evicted = engine.gc_once(None).unwrap();
        assert_eq!(evicted, 1);
        let remaining = engine
            .replay(WorldKey::new("w1"), AtomFilter::default())
            .await
            .unwrap();
        assert_eq!(remaining.len(), 2);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn contradicted_atoms_downranked_but_present() {
        let engine = engine_components();
        let base = new_atom_with("2024-01-01T00:00:00Z", "p", "base");
        let base_id = engine.remember(base).await.expect("base");

        let contradictor = NewAtom {
            world: WorldKey::new("w1"),
            worker: WorkerKey::new("worker-2"),
            kind: AtomKind::Observation,
            timestamp: Timestamp::new("2024-01-01T00:00:10Z"),
            importance: Importance::new(0.5).unwrap(),
            payload_json: "p-alt".into(),
            vector: None,
            flags: vec![],
            labels: vec![],
            links: vec![Link::new(base_id.clone(), LinkKind::Contradicts)],
        };
        let contra_id = engine.remember(contradictor).await.expect("contradictor");

        let question = Question {
            world: WorldKey::new("w1"),
            text: "p".into(),
            filter: None,
        };
        let answer = engine.ask(question).await.expect("ask");
        let ids: Vec<_> = answer
            .supporting_atoms
            .iter()
            .map(|a| a.id().clone())
            .collect();
        assert_eq!(ids.first(), Some(&contra_id));
        assert!(ids.contains(&base_id));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn suspicious_atoms_are_quarantined_and_excluded() {
        let engine = engine_components();
        let suspicious = NewAtom {
            world: WorldKey::new("wq"),
            worker: WorkerKey::new("lowtrust-bot"),
            kind: AtomKind::Observation,
            timestamp: Timestamp::new(""),
            importance: Importance::new(0.9).unwrap(),
            payload_json: "nan value".into(),
            vector: None,
            flags: vec![],
            labels: vec![],
            links: vec![],
        };
        let _ = engine.remember(suspicious).await.expect("quarantine");

        let q = Question {
            world: WorldKey::new("wq"),
            text: "nan".into(),
            filter: None,
        };
        let answer = engine.ask(q).await.expect("ask");
        assert!(
            answer.supporting_atoms.is_empty(),
            "quarantine filtered for normal world"
        );

        let q2 = Question {
            world: WorldKey::new("quarantine:wq"),
            text: "nan".into(),
            filter: None,
        };
        let answer_q = engine.ask(q2).await.expect("ask quarantine");
        assert_eq!(answer_q.supporting_atoms.len(), 1);
        assert!(answer_q.supporting_atoms[0]
            .flags()
            .iter()
            .any(|f| f == "suspect"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn manage_world_create_adds_world_and_meta() {
        let engine = engine_components();
        let world = WorldKey::new("tenant:demo/new");
        engine
            .manage_world(WorldAction::Create(WorldMeta {
                world: world.clone(),
                description: Some("demo".into()),
                labels: vec!["team:demo".into()],
            }))
            .await
            .expect("manage world");
        let worlds = engine.worlds().unwrap();
        assert!(worlds.contains(&world));
        assert!(!engine.world_archived(&world).unwrap());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn manage_world_archive_and_resume_flip_status() {
        let engine = engine_components();
        let world = WorldKey::new("tenant:demo/archive");
        engine
            .manage_world(WorldAction::Create(WorldMeta {
                world: world.clone(),
                description: None,
                labels: vec![],
            }))
            .await
            .unwrap();
        engine
            .manage_world(WorldAction::Archive(world.clone()))
            .await
            .unwrap();
        assert!(engine.world_archived(&world).unwrap());
        engine
            .manage_world(WorldAction::Resume(world.clone()))
            .await
            .unwrap();
        assert!(!engine.world_archived(&world).unwrap());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn worlds_excludes_archived_by_default() {
        let engine = engine_components();
        let active = WorldKey::new("tenant:demo/active");
        let archived = WorldKey::new("tenant:demo/archived");
        engine
            .manage_world(WorldAction::Create(WorldMeta {
                world: active.clone(),
                description: None,
                labels: vec![],
            }))
            .await
            .unwrap();
        engine
            .manage_world(WorldAction::Create(WorldMeta {
                world: archived.clone(),
                description: None,
                labels: vec![],
            }))
            .await
            .unwrap();
        engine
            .manage_world(WorldAction::Archive(archived.clone()))
            .await
            .unwrap();
        let worlds = engine.worlds().unwrap();
        assert!(worlds.contains(&active));
        assert!(!worlds.contains(&archived));

        let with_archived = engine.worlds_filtered(true).unwrap();
        assert!(with_archived.contains(&archived));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn policy_atoms_respect_policy_world_conventions() {
        let engine = engine_components();
        let world = WorldKey::new("tenant:acme/alpha");
        engine
            .manage_world(WorldAction::Create(WorldMeta {
                world: world.clone(),
                description: Some("alpha".into()),
                labels: vec!["team:alpha".into()],
            }))
            .await
            .unwrap();

        let per_world_policy = NewAtom {
            world: WorldKey::new(format!("policy:{}", world.0)),
            worker: WorkerKey::new("policy-tester"),
            kind: AtomKind::Reflection,
            timestamp: Timestamp::new("2024-02-01T00:00:00Z"),
            importance: Importance::clamped(0.2),
            payload_json: "policy".into(),
            vector: None,
            flags: vec![],
            labels: vec![
                "policy:retention_days=1".into(),
                "policy:min_importance=0.4".into(),
                "policy:replication_allow=tenant:acme/".into(),
            ],
            links: vec![],
        };
        engine.remember(per_world_policy).await.unwrap();

        let tenant_policy = NewAtom {
            world: WorldKey::new("tenant:acme/policy"),
            worker: WorkerKey::new("policy-tester"),
            kind: AtomKind::Reflection,
            timestamp: Timestamp::new("2024-02-02T00:00:00Z"),
            importance: Importance::clamped(0.2),
            payload_json: "policy".into(),
            vector: None,
            flags: vec![],
            labels: vec![
                "policy:replication_deny=tenant:acme/private".into(),
                "policy:retention_min_days=7".into(),
            ],
            links: vec![],
        };
        engine.remember(tenant_policy).await.unwrap();

        let policy = engine.parse_policy(&[], &world);
        assert_eq!(policy.retention_days, Some(7));
        assert_eq!(policy.min_importance, Some(0.4));
        assert!(policy
            .replication_allow
            .contains(&"tenant:acme/".to_string()));
        assert!(policy
            .replication_deny
            .contains(&"tenant:acme/private".to_string()));
    }

    #[test]
    fn embedder_change_triggers_index_metadata_update() {
        let engine = engine_components();
        let world = WorldKey::new("w-meta");
        engine.ensure_index_entry(&world);
        {
            let mut guard = engine.index_state.lock();
            if let Some(meta) = guard.get_mut(&world) {
                meta.embedder_version = "old".into();
                meta.ready = true;
                meta.progress = 0.0;
                meta.rebuilding = true;
                meta.started_at = Some(Timestamp::new("2024-01-01T00:00:00Z"));
            }
        }
        engine.maybe_rebuild_index(&world);
        let meta = engine
            .index_state
            .lock()
            .get(&world)
            .cloned()
            .expect("meta");
        assert!(meta.ready);
        assert_ne!(meta.embedder_version, "old");
        assert!(meta.version >= 1);
        assert_eq!(meta.progress, 1.0);
        assert!(!meta.rebuilding);
    }

    #[test]
    fn rebuild_lag_reports_inflight() {
        let engine = engine_components();
        let world = WorldKey::new("w-lag");
        {
            let mut guard = engine.index_state.lock();
            guard.insert(
                world.clone(),
                IndexMetadata {
                    world: world.clone(),
                    version: 1,
                    embedder_version: "v1".into(),
                    last_rebuilt: Timestamp::new("2024-01-01T00:00:00Z"),
                    ready: false,
                    rebuilding: true,
                    progress: 0.3,
                    started_at: Some(Timestamp::new("2024-01-02T00:00:00Z")),
                    last_progress: Some(Timestamp::new("2024-01-02T00:00:00Z")),
                },
            );
        }
        let lag = engine.max_index_rebuild_lag_ms();
        assert!(lag.is_some());
        assert!(lag.unwrap() > 0);
    }
}
