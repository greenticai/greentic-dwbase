//! Swarm coordination scaffold for DWBase; orchestration logic will be added later.

use dwbase_core::{Atom, AtomId, WorldKey};
use dwbase_engine::{SummaryAdvert, TimeWindow};
use serde::{Deserialize, Serialize};
use siphasher::sip::SipHasher13;
use std::collections::HashMap;
use std::fmt;
use std::hash::Hasher;
use std::net::SocketAddr;
use std::time::{Duration, Instant, SystemTime};
use thiserror::Error;

pub type Result<T> = std::result::Result<T, SwarmError>;

/// Errors surfaced by the swarm skeleton.
#[derive(Debug, Error)]
pub enum SwarmError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("encode error: {0}")]
    Encode(String),
    #[error("decode error: {0}")]
    Decode(String),
    #[error("datagram too large ({0} > {1})")]
    Oversize(usize, usize),
}

/// Identifier for a peer in the swarm.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PeerId(pub String);

impl PeerId {
    pub fn new<S: Into<String>>(s: S) -> Self {
        Self(s.into())
    }
}

impl fmt::Display for PeerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Metadata describing a peer.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PeerInfo {
    pub id: PeerId,
    pub addr: SocketAddr,
    pub trust_score: f32,
    pub worlds: Vec<String>,
    pub subscriptions: Vec<WorldSubscription>,
    pub summary_adverts: Vec<SummaryAdvert>,
}

/// A peer's subscription intent for a world (or pattern).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorldSubscription {
    pub world_pattern: String,
    pub kinds: Vec<InterestKind>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum InterestKind {
    Atoms,
    Summaries,
}

/// Initial handshake: declares identity, served worlds, and subscription intents.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Hello {
    pub peer: PeerInfo,
    pub subscriptions: Vec<WorldSubscription>,
}

/// Envelope wire format exchanged between peers.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Envelope {
    pub kind: EnvelopeKind,
    pub from: PeerId,
    pub to: Option<PeerId>,
    pub payload_bytes: Vec<u8>,
    pub sent_at: SystemTime,
}

impl Envelope {
    pub fn new(
        kind: EnvelopeKind,
        from: PeerId,
        to: Option<PeerId>,
        payload_bytes: Vec<u8>,
    ) -> Self {
        Self {
            kind,
            from,
            to,
            payload_bytes,
            sent_at: SystemTime::now(),
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        rmp_serde::to_vec(self).map_err(|e| SwarmError::Encode(e.to_string()))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        rmp_serde::from_slice(bytes).map_err(|e| SwarmError::Decode(e.to_string()))
    }
}

/// Basic envelope category marker.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum EnvelopeKind {
    Ping,
    Membership,
    Hello,
    SummaryAdvert,
    BloomOffer,
    MissingRequest,
    AtomBatch,
    Gossip,
    Custom(String),
}

/// Bloom filter wrapper for message payloads.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BloomFilter {
    bits: Vec<u8>,
    k: u32,
    m: u32,
}

impl BloomFilter {
    pub fn from_ids(ids: &[AtomId], fp_rate: f64) -> Self {
        let n = ids.len().max(1);
        let fp = fp_rate.clamp(0.0001, 0.5);
        let m = ((-(n as f64) * fp.ln()) / (std::f64::consts::LN_2.powi(2))).ceil() as u32;
        let m = m.max(256);
        let k = (((m as f64 / n as f64) * std::f64::consts::LN_2).ceil() as u32).max(1);
        let mut bf = Self {
            bits: vec![0; m.div_ceil(8) as usize],
            k,
            m,
        };
        for id in ids {
            bf.set(id);
        }
        bf
    }

    fn hash_pair(id: &AtomId) -> (u64, u64) {
        let mut h1 = SipHasher13::new_with_keys(0, 1);
        h1.write(id.0.as_bytes());
        let mut h2 = SipHasher13::new_with_keys(2, 3);
        h2.write(id.0.as_bytes());
        (h1.finish(), h2.finish())
    }

    fn indices(&self, id: &AtomId) -> impl Iterator<Item = u32> + '_ {
        let (h1, h2) = Self::hash_pair(id);
        (0..self.k).map(move |i| {
            let step = (i as u64).wrapping_mul(h2);
            ((h1.wrapping_add(step)) % self.m as u64) as u32
        })
    }

    fn set(&mut self, id: &AtomId) {
        let idxs: Vec<u32> = self.indices(id).collect();
        for idx in idxs {
            let byte = (idx / 8) as usize;
            let bit = idx % 8;
            self.bits[byte] |= 1 << bit;
        }
    }

    pub fn contains(&self, id: &AtomId) -> bool {
        for idx in self.indices(id) {
            let byte = (idx / 8) as usize;
            let bit = idx % 8;
            if self.bits[byte] & (1 << bit) == 0 {
                return false;
            }
        }
        true
    }

    pub fn missing(&self, ids: &[AtomId]) -> Vec<AtomId> {
        ids.iter()
            .filter(|id| !self.contains(id))
            .cloned()
            .collect()
    }
}

/// Offer of bloom filter for a world and window.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BloomOffer {
    pub world: WorldKey,
    pub window: TimeWindow,
    pub bloom: BloomFilter,
}

/// Request for specific missing atoms.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MissingRequest {
    pub world: WorldKey,
    pub atom_ids: Vec<AtomId>,
}

/// Batch of atoms to ship to a peer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AtomBatch {
    pub world: WorldKey,
    pub atoms: Vec<Atom>,
}

/// Clock abstraction to make membership logic testable.
pub trait Clock: Clone + Send + Sync + 'static {
    fn now(&self) -> Instant;
}

#[derive(Clone, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

#[derive(Debug, Clone)]
struct PeerState {
    info: PeerInfo,
    last_seen: Instant,
    failure_count: u32,
    backoff_until: Option<Instant>,
}

/// Tracks known peers, last-seen timestamps, and backoff windows.
pub struct Membership<C: Clock = SystemClock> {
    peers: HashMap<PeerId, PeerState>,
    expiry: Duration,
    base_backoff: Duration,
    clock: C,
}

impl Membership<SystemClock> {
    pub fn new(expiry: Duration, base_backoff: Duration) -> Self {
        Self::with_clock(SystemClock, expiry, base_backoff)
    }
}

impl<C: Clock> Membership<C> {
    pub fn with_clock(clock: C, expiry: Duration, base_backoff: Duration) -> Self {
        Self {
            peers: HashMap::new(),
            expiry,
            base_backoff,
            clock,
        }
    }

    /// Insert or update a peer record; returns true if it was newly added.
    pub fn upsert(&mut self, info: PeerInfo) -> bool {
        let now = self.clock.now();
        let id = info.id.clone();
        let is_new = !self.peers.contains_key(&id);
        let entry = self.peers.entry(id.clone()).or_insert(PeerState {
            info: info.clone(),
            last_seen: now,
            failure_count: 0,
            backoff_until: None,
        });

        entry.info = info;
        entry.last_seen = now;
        entry.failure_count = 0;
        entry.backoff_until = None;
        is_new
    }

    /// Merge a Hello message into membership, resetting backoff/last_seen.
    pub fn merge_hello(&mut self, hello: Hello) -> bool {
        let mut info = hello.peer;
        info.subscriptions = hello.subscriptions.clone();
        self.upsert(info)
    }

    /// Apply a summary advert from a known peer, storing it in their record.
    pub fn apply_summary_advert(&mut self, from: &PeerId, advert: SummaryAdvert) {
        if let Some(state) = self.peers.get_mut(from) {
            state
                .info
                .summary_adverts
                .retain(|a| a.digest != advert.digest || a.world != advert.world);
            state.info.summary_adverts.push(advert);
            state.last_seen = self.clock.now();
        }
    }

    /// Mark a peer as seen now (resets failures/backoff).
    pub fn mark_seen(&mut self, id: &PeerId) {
        if let Some(state) = self.peers.get_mut(id) {
            state.last_seen = self.clock.now();
            state.failure_count = 0;
            state.backoff_until = None;
        }
    }

    /// Increment failure count and set an exponential backoff window.
    pub fn mark_failure(&mut self, id: &PeerId) {
        if let Some(state) = self.peers.get_mut(id) {
            state.failure_count = state.failure_count.saturating_add(1);
            let shift = state.failure_count.saturating_sub(1).min(16);
            let factor = 1u32 << shift;
            let backoff = self
                .base_backoff
                .saturating_mul(factor)
                .max(self.base_backoff);
            state.backoff_until = Some(self.clock.now() + backoff);
        }
    }

    /// Remove peers whose last_seen exceeds the expiry window; returns removed ids.
    pub fn expire(&mut self) -> Vec<PeerId> {
        let now = self.clock.now();
        let expiry = self.expiry;
        let mut removed = Vec::new();
        self.peers.retain(|id, state| {
            let keep = now.duration_since(state.last_seen) <= expiry;
            if !keep {
                removed.push(id.clone());
            }
            keep
        });
        removed
    }

    pub fn is_backing_off(&self, id: &PeerId) -> bool {
        let now = self.clock.now();
        self.peers
            .get(id)
            .and_then(|s| s.backoff_until)
            .map(|until| until > now)
            .unwrap_or(false)
    }

    pub fn peers(&self) -> Vec<PeerInfo> {
        self.peers.values().map(|s| s.info.clone()).collect()
    }

    pub fn eligible_peers(&self) -> Vec<PeerInfo> {
        let now = self.clock.now();
        self.peers
            .values()
            .filter(|s| s.backoff_until.map(|b| b <= now).unwrap_or(true))
            .map(|s| s.info.clone())
            .collect()
    }

    pub fn len(&self) -> usize {
        self.peers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.peers.is_empty()
    }
}

#[cfg(any(feature = "swarm", test))]
use async_trait::async_trait;
#[cfg(any(feature = "swarm", test))]
use std::sync::Arc;
#[cfg(any(feature = "swarm", test))]
use tokio::net::UdpSocket;

/// Transport abstraction used by the gossip loop.
#[cfg(any(feature = "swarm", test))]
#[async_trait]
pub trait Transport: Send + Sync {
    async fn send(&self, target: SocketAddr, envelope: Envelope) -> Result<()>;
    async fn recv(&self) -> Result<(SocketAddr, Envelope)>;
}

/// Minimal UDP transport stub for local/dev scenarios.
#[cfg(any(feature = "swarm", test))]
#[derive(Clone)]
pub struct UdpTransport {
    socket: Arc<UdpSocket>,
    max_datagram: usize,
}

#[cfg(any(feature = "swarm", test))]
impl UdpTransport {
    pub async fn bind(addr: SocketAddr, max_datagram: usize) -> Result<Self> {
        let socket = UdpSocket::bind(addr).await?;
        Ok(Self {
            socket: Arc::new(socket),
            max_datagram,
        })
    }

    pub fn local_addr(&self) -> Result<SocketAddr> {
        self.socket.local_addr().map_err(SwarmError::from)
    }
}

#[cfg(any(feature = "swarm", test))]
#[async_trait]
impl Transport for UdpTransport {
    async fn send(&self, target: SocketAddr, envelope: Envelope) -> Result<()> {
        let bytes = envelope.encode()?;
        if bytes.len() > self.max_datagram {
            return Err(SwarmError::Oversize(bytes.len(), self.max_datagram));
        }
        self.socket.send_to(&bytes, target).await?;
        Ok(())
    }

    async fn recv(&self) -> Result<(SocketAddr, Envelope)> {
        let mut buf = vec![0u8; self.max_datagram];
        let (len, addr) = self.socket.recv_from(&mut buf).await?;
        let env = Envelope::decode(&buf[..len])?;
        Ok((addr, env))
    }
}

/// Tick-based gossip loop skeleton.
#[cfg(any(feature = "swarm", test))]
pub struct GossipLoop<T, C: Clock = SystemClock> {
    _transport: T,
    membership: Membership<C>,
    tick_interval: Duration,
}

#[cfg(any(feature = "swarm", test))]
impl<T, C> GossipLoop<T, C>
where
    T: Transport,
    C: Clock,
{
    pub fn new(transport: T, membership: Membership<C>, tick_interval: Duration) -> Self {
        Self {
            _transport: transport,
            membership,
            tick_interval,
        }
    }

    pub async fn tick(&mut self) -> Result<()> {
        let expired = self.membership.expire();
        if !expired.is_empty() {
            // placeholder hook for future metrics/logging
        }
        // Future work: pull from recv/send queues, drive membership gossip.
        tokio::time::sleep(self.tick_interval).await;
        Ok(())
    }

    pub async fn run_once(&mut self) -> Result<()> {
        self.tick().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use dwbase_core::{AtomId, AtomKind, Importance, Timestamp, WorkerKey, WorldKey};
    use dwbase_engine::{SummaryCatalog, SummaryWindow};
    use std::net::{IpAddr, Ipv4Addr};
    use std::sync::Mutex;
    use time::{format_description::well_known::Rfc3339, OffsetDateTime};
    use tokio::sync::{mpsc, Mutex as AsyncMutex};

    #[derive(Clone)]
    struct FakeClock {
        now: std::sync::Arc<Mutex<Instant>>,
    }

    impl FakeClock {
        fn new() -> Self {
            Self {
                now: std::sync::Arc::new(Mutex::new(Instant::now())),
            }
        }

        fn advance(&self, duration: Duration) {
            let mut guard = self.now.lock().unwrap();
            *guard += duration;
        }
    }

    impl Clock for FakeClock {
        fn now(&self) -> Instant {
            *self.now.lock().unwrap()
        }
    }

    #[derive(Clone)]
    struct LoopbackTransport {
        addr: SocketAddr,
        rx: std::sync::Arc<AsyncMutex<mpsc::UnboundedReceiver<(SocketAddr, Envelope)>>>,
        tx: mpsc::UnboundedSender<(SocketAddr, Envelope)>,
    }

    fn loopback_pair() -> (LoopbackTransport, LoopbackTransport) {
        let (tx_a, rx_a) = mpsc::unbounded_channel();
        let (tx_b, rx_b) = mpsc::unbounded_channel();
        let addr_a = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9000);
        let addr_b = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9001);
        (
            LoopbackTransport {
                addr: addr_a,
                rx: std::sync::Arc::new(AsyncMutex::new(rx_a)),
                tx: tx_b,
            },
            LoopbackTransport {
                addr: addr_b,
                rx: std::sync::Arc::new(AsyncMutex::new(rx_b)),
                tx: tx_a,
            },
        )
    }

    #[async_trait]
    impl Transport for LoopbackTransport {
        async fn send(&self, _target: SocketAddr, envelope: Envelope) -> Result<()> {
            self.tx
                .send((self.addr, envelope))
                .map_err(|e| SwarmError::Encode(e.to_string()))
        }

        async fn recv(&self) -> Result<(SocketAddr, Envelope)> {
            let mut rx = self.rx.lock().await;
            rx.recv()
                .await
                .ok_or_else(|| SwarmError::Decode("loopback channel closed".into()))
        }
    }

    fn ts_from_ms(ms: i64) -> Timestamp {
        let secs = ms / 1000;
        let nanos = (ms % 1000) * 1_000_000;
        let total = (secs as i128 * 1_000_000_000i128) + nanos as i128;
        let dt =
            OffsetDateTime::from_unix_timestamp_nanos(total).unwrap_or(OffsetDateTime::UNIX_EPOCH);
        Timestamp(dt.format(&Rfc3339).unwrap())
    }

    fn atom_with(id: &str, world: &str, ms: i64) -> Atom {
        Atom::builder(
            AtomId::new(id),
            WorldKey::new(world),
            WorkerKey::new("worker"),
            AtomKind::Observation,
            ts_from_ms(ms),
            Importance::new(1.0).unwrap(),
            "{}",
        )
        .build()
    }

    struct SimpleNode {
        world: WorldKey,
        atoms: Vec<Atom>,
    }

    impl SimpleNode {
        fn new(world: &str, atoms: Vec<Atom>) -> Self {
            Self {
                world: WorldKey::new(world),
                atoms,
            }
        }

        fn ids_in_window(&self, window: &TimeWindow) -> Vec<AtomId> {
            self.atoms
                .iter()
                .filter_map(|a| {
                    if let Ok(dt) = OffsetDateTime::parse(a.timestamp().0.as_str(), &Rfc3339) {
                        let ms = (dt.unix_timestamp_nanos() / 1_000_000) as i64;
                        if ms >= window.start_ms && ms <= window.end_ms {
                            return Some(a.id().clone());
                        }
                    }
                    None
                })
                .collect()
        }

        fn bloom_offer(&self, window: TimeWindow, fp: f64) -> BloomOffer {
            let ids = self.ids_in_window(&window);
            BloomOffer {
                world: self.world.clone(),
                window,
                bloom: BloomFilter::from_ids(&ids, fp),
            }
        }

        fn missing_atoms(&self, bloom: &BloomFilter, window: &TimeWindow) -> Vec<Atom> {
            let ids = self.ids_in_window(window);
            let missing = bloom.missing(&ids);
            self.atoms
                .iter()
                .filter(|a| missing.contains(a.id()))
                .cloned()
                .collect()
        }

        fn ingest_batch(&mut self, batch: AtomBatch) {
            for atom in batch.atoms {
                if !self.atoms.iter().any(|a| a.id() == atom.id()) {
                    self.atoms.push(atom);
                }
            }
        }
    }

    fn peer(id: &str, port: u16) -> PeerInfo {
        PeerInfo {
            id: PeerId::new(id),
            addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port),
            trust_score: 1.0,
            worlds: vec!["default".into()],
            subscriptions: vec![],
            summary_adverts: vec![],
        }
    }

    #[test]
    fn membership_add_update_and_expire() {
        let clock = FakeClock::new();
        let mut membership = Membership::with_clock(
            clock.clone(),
            Duration::from_secs(30),
            Duration::from_secs(5),
        );

        let mut p1 = peer("node-a", 7000);
        assert!(membership.upsert(p1.clone()));
        assert_eq!(membership.len(), 1);

        // Update trust score and ensure it overwrites.
        p1.trust_score = 0.6;
        assert!(!membership.upsert(p1.clone()));
        assert_eq!(membership.peers()[0].trust_score, 0.6);

        // Expire after window passes.
        clock.advance(Duration::from_secs(31));
        let expired = membership.expire();
        assert_eq!(expired, vec![p1.id.clone()]);
        assert!(membership.is_empty());
    }

    #[test]
    fn membership_backoff_and_recovery() {
        let clock = FakeClock::new();
        let mut membership = Membership::with_clock(
            clock.clone(),
            Duration::from_secs(30),
            Duration::from_secs(5),
        );
        let p1 = peer("node-b", 7001);
        membership.upsert(p1.clone());

        membership.mark_failure(&p1.id);
        assert!(membership.is_backing_off(&p1.id));

        // Backoff expires then mark seen clears failures.
        clock.advance(Duration::from_secs(6));
        assert!(!membership.is_backing_off(&p1.id));
        membership.mark_seen(&p1.id);
        assert!(!membership.is_backing_off(&p1.id));
    }

    #[test]
    fn envelope_roundtrip() {
        let env = Envelope::new(
            EnvelopeKind::Ping,
            PeerId::new("node-a"),
            Some(PeerId::new("node-b")),
            vec![1, 2, 3],
        );
        let bytes = env.encode().unwrap();
        let decoded = Envelope::decode(&bytes).unwrap();
        assert_eq!(env.kind, decoded.kind);
        assert_eq!(env.from, decoded.from);
        assert_eq!(env.to, decoded.to);
        assert_eq!(env.payload_bytes, decoded.payload_bytes);
    }

    #[test]
    fn hello_merge_is_idempotent() {
        let clock = FakeClock::new();
        let mut membership = Membership::with_clock(
            clock.clone(),
            Duration::from_secs(30),
            Duration::from_secs(5),
        );
        let mut p1 = peer("node-hello", 7100);
        p1.worlds = vec!["w1".into()];
        let hello = Hello {
            peer: p1.clone(),
            subscriptions: vec![WorldSubscription {
                world_pattern: "w*".into(),
                kinds: vec![InterestKind::Summaries],
            }],
        };
        assert!(membership.merge_hello(hello.clone()));
        assert_eq!(membership.len(), 1);

        // Second hello updates worlds/subscriptions without duplicating.
        p1.worlds.push("w2".into());
        let updated = Hello {
            peer: p1.clone(),
            subscriptions: hello.subscriptions.clone(),
        };
        assert!(!membership.merge_hello(updated));
        let worlds: Vec<String> = membership.peers()[0].worlds.clone();
        assert_eq!(worlds, vec!["w1".to_string(), "w2".to_string()]);
    }

    #[test]
    fn summary_advert_updates_catalog_and_membership() {
        let clock = FakeClock::new();
        let mut membership = Membership::with_clock(
            clock.clone(),
            Duration::from_secs(30),
            Duration::from_secs(5),
        );
        let mut catalog = SummaryCatalog::new();
        let p1 = peer("node-sum", 7200);
        membership.upsert(p1.clone());

        let advert = SummaryAdvert::new(
            WorldKey::new("w-sum"),
            vec![SummaryWindow::new(0, 100)],
            "digest-1",
        );
        membership.apply_summary_advert(&p1.id, advert.clone());
        catalog.upsert(advert.clone());

        let stored = membership.peers()[0].summary_adverts.clone();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].digest, "digest-1");

        let known = catalog.list(&WorldKey::new("w-sum"));
        assert_eq!(known.len(), 1);
        assert_eq!(known[0].digest, "digest-1");
    }

    #[tokio::test]
    async fn exchange_hello_and_summary_advert() -> Result<()> {
        let (t1, t2) = loopback_pair();

        let addr1 = t1.addr;
        let addr2 = t2.addr;

        let mut m1 = Membership::new(Duration::from_secs(60), Duration::from_secs(5));
        let mut m2 = Membership::new(Duration::from_secs(60), Duration::from_secs(5));
        let mut catalog = SummaryCatalog::new();

        let hello = Hello {
            peer: PeerInfo {
                id: PeerId::new("node-a"),
                addr: addr1,
                trust_score: 1.0,
                worlds: vec!["w1".into()],
                subscriptions: vec![],
                summary_adverts: vec![],
            },
            subscriptions: vec![WorldSubscription {
                world_pattern: "w1".into(),
                kinds: vec![InterestKind::Atoms, InterestKind::Summaries],
            }],
        };

        // Send hello from node A to node B.
        t1.send(
            addr2,
            Envelope::new(
                EnvelopeKind::Hello,
                hello.peer.id.clone(),
                None,
                rmp_serde::to_vec(&hello).unwrap(),
            ),
        )
        .await?;

        let (_, env) = t2.recv().await?;
        assert_eq!(env.kind, EnvelopeKind::Hello);
        let decoded: Hello = rmp_serde::from_slice(&env.payload_bytes).unwrap();
        m2.merge_hello(decoded.clone());
        assert_eq!(m2.len(), 1);

        // Send summary advert from node B back to A.
        m1.upsert(PeerInfo {
            id: PeerId::new("node-b"),
            addr: addr2,
            trust_score: 1.0,
            worlds: vec!["w1".into()],
            subscriptions: vec![],
            summary_adverts: vec![],
        });
        let advert = SummaryAdvert::new(
            WorldKey::new("w1"),
            vec![SummaryWindow::new(0, 1000)],
            "digest-hello",
        );
        t2.send(
            addr1,
            Envelope::new(
                EnvelopeKind::SummaryAdvert,
                PeerId::new("node-b"),
                Some(PeerId::new("node-a")),
                rmp_serde::to_vec(&advert).unwrap(),
            ),
        )
        .await?;
        let (_, env2) = t1.recv().await?;
        assert_eq!(env2.kind, EnvelopeKind::SummaryAdvert);
        let decoded_adv: SummaryAdvert = rmp_serde::from_slice(&env2.payload_bytes).unwrap();
        catalog.upsert(decoded_adv.clone());
        m1.apply_summary_advert(&hello.peer.id, decoded_adv.clone());

        assert_eq!(catalog.list(&WorldKey::new("w1")).len(), 1);
        assert_eq!(m1.peers().len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn delta_sync_via_bloom_and_atom_batch() -> Result<()> {
        let window = TimeWindow::new(0, 10_000);
        let node_a = SimpleNode::new(
            "w-delta",
            vec![
                atom_with("a1", "w-delta", 1_000),
                atom_with("a2", "w-delta", 2_000),
            ],
        );
        let mut node_b = SimpleNode::new("w-delta", vec![atom_with("a1", "w-delta", 1_000)]);

        let (t_a, t_b) = loopback_pair();

        // B sends bloom of what it has to A.
        let offer = node_b.bloom_offer(window, 0.01);
        t_b.send(
            t_a.addr,
            Envelope::new(
                EnvelopeKind::BloomOffer,
                PeerId::new("node-b"),
                Some(PeerId::new("node-a")),
                rmp_serde::to_vec(&offer).unwrap(),
            ),
        )
        .await?;

        // A receives offer and responds with AtomBatch of missing atoms.
        let (_, env) = t_a.recv().await?;
        let decoded: BloomOffer = rmp_serde::from_slice(&env.payload_bytes).unwrap();
        let missing_atoms = node_a.missing_atoms(&decoded.bloom, &decoded.window);
        let batch = AtomBatch {
            world: decoded.world.clone(),
            atoms: missing_atoms,
        };
        t_a.send(
            t_b.addr,
            Envelope::new(
                EnvelopeKind::AtomBatch,
                PeerId::new("node-a"),
                Some(PeerId::new("node-b")),
                rmp_serde::to_vec(&batch).unwrap(),
            ),
        )
        .await?;

        // B ingests the batch and ends up in sync.
        let (_, env2) = t_b.recv().await?;
        let decoded_batch: AtomBatch = rmp_serde::from_slice(&env2.payload_bytes).unwrap();
        node_b.ingest_batch(decoded_batch);

        assert_eq!(node_b.atoms.len(), node_a.atoms.len());
        Ok(())
    }
}
