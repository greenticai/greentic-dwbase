use std::collections::{HashMap, VecDeque};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use anyhow::Error;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicUsize, Ordering};

use dwbase_core::Atom;
use dwbase_swarm::{AtomBatch, PeerId};

use crate::swarm::{NatsSwarmTransport, SwarmBus, SwarmMessage, SUBJECT_BROADCAST};

/// Controls which worlds a node will replicate to/from peers.
///
/// This is intentionally a small, inspectable hook: the default policy allows all worlds.
/// Components/nodes can plug in a policy backed by their gatekeeper/security configuration.
pub trait WorldAccessPolicy: Send + Sync {
    fn can_send_world(&self, _world: &str, _to: &PeerId) -> bool {
        true
    }

    fn can_receive_world(&self, _world: &str, _from: &PeerId) -> bool {
        true
    }
}

#[derive(Default)]
pub struct AllowAllWorldAccess;

impl WorldAccessPolicy for AllowAllWorldAccess {}

#[derive(Clone, Debug)]
pub struct SubscriptionRecord {
    pub patterns: Vec<String>,
    pub last_seen: Instant,
}

type ReplayMap = HashMap<String, VecDeque<(Instant, String)>>;

struct ReplayGuard {
    inner: Arc<Mutex<ReplayMap>>,
    window: Duration,
    max_entries: usize,
}

impl ReplayGuard {
    fn new(window: Duration, max_entries: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            window,
            max_entries,
        }
    }

    fn should_accept(&self, peer: &PeerId, correlation_id: &str) -> bool {
        let mut guard = self.inner.lock();
        let entry = guard.entry(peer.0.clone()).or_default();
        let now = Instant::now();
        while let Some((ts, _)) = entry.front() {
            if now.duration_since(*ts) > self.window || entry.len() > self.max_entries {
                entry.pop_front();
            } else {
                break;
            }
        }
        if entry.iter().any(|(_, cid)| cid == correlation_id) {
            return false;
        }
        entry.push_back((now, correlation_id.to_string()));
        if entry.len() > self.max_entries {
            entry.pop_front();
        }
        true
    }
}

#[derive(Default, Serialize, Deserialize)]
struct PersistedSubscriptionRecord {
    patterns: Vec<String>,
    last_seen_ms: u64,
}

#[derive(Default, Serialize, Deserialize)]
struct PersistedSwarmState {
    subscriptions: HashMap<String, PersistedSubscriptionRecord>,
}

#[derive(Clone)]
pub struct SubscriptionTable {
    inner: Arc<Mutex<HashMap<String, SubscriptionRecord>>>,
    expiry: Duration,
    state_path: Option<PathBuf>,
}

impl Default for SubscriptionTable {
    fn default() -> Self {
        Self::new(Duration::from_secs(30), None)
    }
}

impl SubscriptionTable {
    pub fn new(expiry: Duration, state_path: Option<PathBuf>) -> Self {
        let inner = Arc::new(Mutex::new(Self::load_from_disk(state_path.clone(), expiry)));
        Self {
            inner,
            expiry,
            state_path,
        }
    }

    pub fn upsert(&self, node_id: String, patterns: Vec<String>) {
        {
            let mut guard = self.inner.lock();
            guard.insert(
                node_id,
                SubscriptionRecord {
                    patterns,
                    last_seen: Instant::now(),
                },
            );
        }
        let _ = self.persist();
    }

    pub fn prune(&self) {
        let mut changed = false;
        {
            let mut guard = self.inner.lock();
            guard.retain(|_id, rec| {
                let keep = rec.last_seen.elapsed() <= self.expiry;
                if !keep {
                    changed = true;
                }
                keep
            });
        }
        if changed {
            let _ = self.persist();
        }
    }

    pub fn nodes_subscribed_to_world(&self, world: &str) -> Vec<String> {
        self.prune();
        let guard = self.inner.lock();
        guard
            .iter()
            .filter(|(_id, rec)| rec.patterns.iter().any(|p| matches_world(p, world)))
            .map(|(id, _)| id.clone())
            .collect()
    }

    pub fn snapshot(&self) -> HashMap<String, Vec<String>> {
        self.prune();
        let guard = self.inner.lock();
        guard
            .iter()
            .map(|(id, rec)| (id.clone(), rec.patterns.clone()))
            .collect()
    }

    fn persist(&self) -> anyhow::Result<()> {
        let Some(path) = &self.state_path else {
            return Ok(());
        };
        let guard = self.inner.lock();
        let now_ms = now_ms();
        let mut persisted = HashMap::new();
        for (id, rec) in guard.iter() {
            let elapsed_ms: u64 = rec
                .last_seen
                .elapsed()
                .as_millis()
                .try_into()
                .unwrap_or(u64::MAX);
            persisted.insert(
                id.clone(),
                PersistedSubscriptionRecord {
                    patterns: rec.patterns.clone(),
                    last_seen_ms: now_ms.saturating_sub(elapsed_ms),
                },
            );
        }
        let state = PersistedSwarmState {
            subscriptions: persisted,
        };
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let bytes = serde_json::to_vec_pretty(&state)?;
        fs::write(path, bytes)?;
        Ok(())
    }

    fn load_from_disk(
        path: Option<PathBuf>,
        expiry: Duration,
    ) -> HashMap<String, SubscriptionRecord> {
        let Some(path) = path else {
            return HashMap::new();
        };
        let Ok(bytes) = fs::read(&path) else {
            return HashMap::new();
        };
        let Ok(state) = serde_json::from_slice::<PersistedSwarmState>(&bytes) else {
            return HashMap::new();
        };
        let now_ms = now_ms();
        let mut map = HashMap::new();
        for (id, rec) in state.subscriptions {
            let age_ms = now_ms.saturating_sub(rec.last_seen_ms);
            let age = Duration::from_millis(age_ms);
            if age > expiry {
                continue;
            }
            map.insert(
                id,
                SubscriptionRecord {
                    patterns: rec.patterns,
                    last_seen: Instant::now() - age,
                },
            );
        }
        map
    }
}

pub fn matches_world(pattern: &str, world: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if let Some(prefix) = pattern.strip_suffix(":*") {
        return world.starts_with(prefix);
    }
    if let Some(prefix) = pattern.strip_suffix('*') {
        return world.starts_with(prefix);
    }
    pattern == world
}

pub struct Replicator {
    transport: NatsSwarmTransport,
    subscriptions: SubscriptionTable,
    self_patterns: Vec<String>,
    expiry: Duration,
    last_announce: Mutex<Instant>,
    incoming: Mutex<VecDeque<(PeerId, AtomBatch)>>,
    max_inbox: usize,
    dropped_inbox: AtomicUsize,
    dropped_replay: AtomicUsize,
    replay: ReplayGuard,
    policy: Arc<dyn WorldAccessPolicy>,
}

impl Replicator {
    pub fn new(
        transport: NatsSwarmTransport,
        self_patterns: Vec<String>,
        expiry: Duration,
    ) -> anyhow::Result<Self> {
        Self::with_policy(
            transport,
            self_patterns,
            expiry,
            Arc::new(AllowAllWorldAccess),
        )
    }

    pub fn with_policy(
        transport: NatsSwarmTransport,
        self_patterns: Vec<String>,
        expiry: Duration,
        policy: Arc<dyn WorldAccessPolicy>,
    ) -> anyhow::Result<Self> {
        Self::with_policy_and_state(
            transport,
            self_patterns,
            expiry,
            policy,
            None,
            1024,
            Duration::from_secs(300),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn with_policy_and_state(
        transport: NatsSwarmTransport,
        self_patterns: Vec<String>,
        expiry: Duration,
        policy: Arc<dyn WorldAccessPolicy>,
        state_path: Option<PathBuf>,
        max_inbox: usize,
        replay_window: Duration,
    ) -> anyhow::Result<Self> {
        let subscriptions = SubscriptionTable::new(expiry, state_path);
        let subs_clone = subscriptions.clone();
        let self_id = transport.self_id.clone();
        transport.bus.subscribe(
            SUBJECT_BROADCAST,
            Box::new(move |_subject, bytes, _reply_to| {
                let Ok(env) = rmp_serde::from_slice::<crate::swarm::SwarmEnvelope>(&bytes) else {
                    return;
                };
                if env.from == self_id.0 {
                    return;
                }
                let Ok(msg) = env.decode_message() else {
                    return;
                };
                if let SwarmMessage::SubscriptionIntent { patterns } = msg {
                    subs_clone.upsert(env.from, patterns);
                }
            }),
        )?;

        Ok(Self {
            transport,
            subscriptions,
            self_patterns,
            expiry,
            last_announce: Mutex::new(Instant::now() - expiry),
            incoming: Mutex::new(VecDeque::new()),
            max_inbox: max_inbox.max(1),
            dropped_inbox: AtomicUsize::new(0),
            dropped_replay: AtomicUsize::new(0),
            replay: ReplayGuard::new(replay_window, 2048),
            policy,
        })
    }

    pub fn subscriptions(&self) -> SubscriptionTable {
        self.subscriptions.clone()
    }

    pub fn announce(&self) -> anyhow::Result<()> {
        let mut guard = self.last_announce.lock();
        if guard.elapsed() < self.expiry / 2 {
            return Ok(());
        }
        *guard = Instant::now();
        let env = crate::swarm::SwarmEnvelope::new(
            self.transport.self_id.clone(),
            None,
            SwarmMessage::SubscriptionIntent {
                patterns: self.self_patterns.clone(),
            },
            None,
        )?;
        let bytes = rmp_serde::to_vec(&env).map_err(Error::from)?;
        self.transport.bus.publish(SUBJECT_BROADCAST, None, bytes)?;
        Ok(())
    }

    /// Poll the inbox and enqueue any incoming AtomBatch that passes local subscription + policy checks.
    pub fn poll_inbox(&self) -> anyhow::Result<()> {
        while let Some((_reply_to, env)) = self.transport.poll() {
            let from = PeerId::new(env.from.clone());
            if !self.replay.should_accept(&from, &env.correlation_id) {
                self.dropped_replay.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            let msg = env.decode_message()?;
            if let SwarmMessage::AtomBatch(batch) = msg {
                let world = batch.world.0.clone();
                if !self.should_ingest(&world) {
                    continue;
                }
                if !self.policy.can_receive_world(&world, &from) {
                    continue;
                }
                let mut incoming = self.incoming.lock();
                if incoming.len() >= self.max_inbox {
                    incoming.pop_front();
                    self.dropped_inbox.fetch_add(1, Ordering::Relaxed);
                }
                incoming.push_back((from, batch));
            }
        }
        Ok(())
    }

    pub fn should_ingest(&self, world: &str) -> bool {
        self.self_patterns.iter().any(|p| matches_world(p, world))
    }

    pub fn poll_atom_batch(&self) -> Option<(PeerId, AtomBatch)> {
        self.incoming.lock().pop_front()
    }

    pub fn dropped_replay(&self) -> usize {
        self.dropped_replay.load(Ordering::Relaxed)
    }

    pub fn dropped_inbox(&self) -> usize {
        self.dropped_inbox.load(Ordering::Relaxed)
    }

    pub fn replicate_new_atom(&self, atom: Atom) -> anyhow::Result<()> {
        let world = atom.world().0.clone();
        let targets = self.subscriptions.nodes_subscribed_to_world(&world);
        if targets.is_empty() {
            return Ok(());
        }
        let batch = AtomBatch {
            world: atom.world().clone(),
            atoms: vec![atom],
        };
        for node in targets {
            if node == self.transport.self_id.0 {
                continue;
            }
            let peer = PeerId::new(node);
            if !self.policy.can_send_world(&world, &peer) {
                continue;
            }
            let _ = self
                .transport
                .send_direct(peer, SwarmMessage::AtomBatch(batch.clone()));
        }
        Ok(())
    }
}

/// Helper to build a replicator for tests with MockBus.
pub fn replicator_with_bus(
    bus: Arc<dyn SwarmBus>,
    node_id: &str,
    patterns: Vec<String>,
) -> anyhow::Result<Replicator> {
    let transport = NatsSwarmTransport::new(bus, PeerId::new(node_id), 200.0)?;
    Replicator::new(transport, patterns, Duration::from_secs(30))
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::swarm::MockBus;
    use dwbase_core::{AtomId, AtomKind, Importance, Timestamp, WorkerKey, WorldKey};

    fn sample_atom(world: &str, id: &str) -> Atom {
        Atom::builder(
            AtomId::new(id),
            WorldKey::new(world),
            WorkerKey::new("w"),
            AtomKind::Observation,
            Timestamp::new("2024-01-01T00:00:00Z"),
            Importance::clamped(0.5),
            r#"{"x":1}"#,
        )
        .build()
    }

    #[test]
    fn only_subscribed_nodes_receive_atoms() {
        let bus = Arc::new(MockBus::default());

        let a = replicator_with_bus(bus.clone(), "node-a", vec![]).unwrap();
        let b = replicator_with_bus(bus.clone(), "node-b", vec!["world-x".into()]).unwrap();
        let c = replicator_with_bus(bus.clone(), "node-c", vec![]).unwrap();

        // B broadcasts subscription.
        b.announce().unwrap();
        std::thread::sleep(Duration::from_millis(10));

        // Replicate atom in world-x; only node-b should receive.
        a.replicate_new_atom(sample_atom("world-x", "a1")).unwrap();
        std::thread::sleep(Duration::from_millis(50));

        b.poll_inbox().unwrap();
        c.poll_inbox().unwrap();
        assert!(b.poll_atom_batch().is_some(), "node-b should receive atom");
        assert!(
            c.poll_atom_batch().is_none(),
            "node-c should not receive atom"
        );
    }

    #[test]
    fn receiver_policy_can_drop_worlds() {
        struct DenyAll;
        impl WorldAccessPolicy for DenyAll {
            fn can_receive_world(&self, _world: &str, _from: &PeerId) -> bool {
                false
            }
        }

        let bus = Arc::new(MockBus::default());
        let a = replicator_with_bus(bus.clone(), "node-a", vec![]).unwrap();
        let transport_b =
            NatsSwarmTransport::new(bus.clone(), PeerId::new("node-b"), 200.0).unwrap();
        let b = Replicator::with_policy(
            transport_b,
            vec!["world-x".into()],
            Duration::from_secs(30),
            Arc::new(DenyAll),
        )
        .unwrap();

        b.announce().unwrap();
        std::thread::sleep(Duration::from_millis(10));
        a.replicate_new_atom(sample_atom("world-x", "a1")).unwrap();
        std::thread::sleep(Duration::from_millis(50));

        b.poll_inbox().unwrap();
        assert!(b.poll_atom_batch().is_none(), "policy should drop batch");
    }

    #[test]
    fn subscriptions_persist_and_reload() {
        let bus = Arc::new(MockBus::default());
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("swarm.json");

        let transport_a =
            NatsSwarmTransport::new(bus.clone(), PeerId::new("node-a"), 200.0).unwrap();
        let rep_a = Replicator::with_policy_and_state(
            transport_a,
            vec!["*".into()],
            Duration::from_secs(30),
            Arc::new(AllowAllWorldAccess),
            Some(path.clone()),
            16,
            Duration::from_secs(60),
        )
        .unwrap();
        rep_a
            .subscriptions()
            .upsert("node-b".into(), vec!["world-x".into()]);
        drop(rep_a);

        let transport_b =
            NatsSwarmTransport::new(bus.clone(), PeerId::new("node-a"), 200.0).unwrap();
        let rep_b = Replicator::with_policy_and_state(
            transport_b,
            vec!["*".into()],
            Duration::from_secs(30),
            Arc::new(AllowAllWorldAccess),
            Some(path.clone()),
            16,
            Duration::from_secs(60),
        )
        .unwrap();
        let nodes = rep_b.subscriptions().nodes_subscribed_to_world("world-x");
        assert!(
            nodes.contains(&"node-b".into()),
            "subscription persisted across restart"
        );
    }

    #[test]
    fn duplicate_batches_are_dropped() {
        let bus = Arc::new(MockBus::default());
        let inbox = crate::swarm::inbox_subject("node-rx");
        let transport =
            NatsSwarmTransport::new(bus.clone(), PeerId::new("node-rx"), 200.0).unwrap();
        let rep = Replicator::with_policy_and_state(
            transport,
            vec!["world-x".into()],
            Duration::from_secs(30),
            Arc::new(AllowAllWorldAccess),
            None,
            8,
            Duration::from_secs(300),
        )
        .unwrap();

        let batch = AtomBatch {
            world: WorldKey::new("world-x"),
            atoms: vec![sample_atom("world-x", "a1")],
        };
        let mut env = crate::swarm::SwarmEnvelope::new(
            PeerId::new("node-tx"),
            Some(PeerId::new("node-rx")),
            SwarmMessage::AtomBatch(batch.clone()),
            None,
        )
        .unwrap();
        env.correlation_id = "dup-1".into();
        let bytes = rmp_serde::to_vec(&env).unwrap();
        bus.publish(&inbox, None, bytes.clone()).unwrap();
        bus.publish(&inbox, None, bytes).unwrap();

        rep.poll_inbox().unwrap();
        rep.poll_inbox().unwrap();
        let first = rep.poll_atom_batch();
        let second = rep.poll_atom_batch();
        assert!(first.is_some(), "first batch should be enqueued");
        assert!(second.is_none(), "duplicate should be dropped");
        assert!(rep.dropped_replay() >= 1, "replay counter increments");
    }

    #[test]
    fn inbox_is_bounded() {
        let bus = Arc::new(MockBus::default());
        let inbox = crate::swarm::inbox_subject("node-rx");
        let transport =
            NatsSwarmTransport::new(bus.clone(), PeerId::new("node-rx"), 200.0).unwrap();
        let rep = Replicator::with_policy_and_state(
            transport,
            vec!["*".into()],
            Duration::from_secs(30),
            Arc::new(AllowAllWorldAccess),
            None,
            1,
            Duration::from_secs(300),
        )
        .unwrap();

        for idx in 0..2 {
            let batch = AtomBatch {
                world: WorldKey::new("world-x"),
                atoms: vec![sample_atom("world-x", &format!("a{idx}"))],
            };
            let mut env = crate::swarm::SwarmEnvelope::new(
                PeerId::new("node-tx"),
                Some(PeerId::new("node-rx")),
                SwarmMessage::AtomBatch(batch),
                None,
            )
            .unwrap();
            env.correlation_id = format!("cid-{idx}");
            let bytes = rmp_serde::to_vec(&env).unwrap();
            bus.publish(&inbox, None, bytes).unwrap();
        }

        rep.poll_inbox().unwrap();
        let first = rep.poll_atom_batch();
        let second = rep.poll_atom_batch();
        assert!(first.is_some());
        assert!(second.is_none(), "bounded inbox drops overflow");
        assert_eq!(rep.dropped_inbox(), 1, "overflow accounted");
    }
}
