use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use parking_lot::{Mutex, RwLock};
use rand::Rng;
use serde::{Deserialize, Serialize};

use dwbase_engine::SummaryAdvert;
use dwbase_swarm::{AtomBatch, BloomOffer, MissingRequest, PeerId};

#[cfg(feature = "nats")]
use futures::StreamExt;

pub const SUBJECT_BROADCAST: &str = "dwbase.swarm.broadcast";

pub fn inbox_subject(node_id: &str) -> String {
    format!("dwbase.swarm.{}.inbox", node_id)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SwarmMessage {
    Ping { nonce: u64 },
    SubscriptionIntent { patterns: Vec<String> },
    SummaryAdvert(SummaryAdvert),
    BloomOffer(BloomOffer),
    MissingRequest(MissingRequest),
    AtomBatch(AtomBatch),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SwarmEnvelope {
    pub kind: String,
    pub from: String,
    pub to: Option<String>,
    pub correlation_id: String,
    pub reply_to: Option<String>,
    pub payload: Vec<u8>,
    pub sent_at_ms: u64,
}

impl SwarmEnvelope {
    pub fn new(
        from: PeerId,
        to: Option<PeerId>,
        message: SwarmMessage,
        reply_to: Option<String>,
    ) -> anyhow::Result<Self> {
        let payload = bincode::serde::encode_to_vec(&message, bincode::config::standard())
            .context("encode swarm message")?;
        let now_ms = (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()) as u64;
        Ok(Self {
            kind: match &message {
                SwarmMessage::Ping { .. } => "Ping".into(),
                SwarmMessage::SubscriptionIntent { .. } => "SubscriptionIntent".into(),
                SwarmMessage::SummaryAdvert(_) => "SummaryAdvert".into(),
                SwarmMessage::BloomOffer(_) => "BloomOffer".into(),
                SwarmMessage::MissingRequest(_) => "MissingRequest".into(),
                SwarmMessage::AtomBatch(_) => "AtomBatch".into(),
            },
            from: from.0,
            to: to.map(|p| p.0),
            correlation_id: new_correlation_id(),
            reply_to,
            payload,
            sent_at_ms: now_ms,
        })
    }

    pub fn decode_message(&self) -> anyhow::Result<SwarmMessage> {
        bincode::serde::decode_from_slice(&self.payload, bincode::config::standard())
            .map(|(m, _)| m)
            .context("decode swarm message")
    }
}

fn new_correlation_id() -> String {
    let mut bytes = [0u8; 16];
    rand::rng().fill_bytes(&mut bytes);
    hex::encode(bytes)
}

#[derive(Clone)]
pub struct RateLimiter {
    inner: Arc<Mutex<TokenBucket>>,
}

impl RateLimiter {
    pub fn per_second(rate: f64) -> Self {
        Self {
            inner: Arc::new(Mutex::new(TokenBucket::new(rate))),
        }
    }

    pub fn allow(&self) -> bool {
        self.inner.lock().allow()
    }
}

struct TokenBucket {
    capacity: f64,
    tokens: f64,
    last_refill: Instant,
    refill_per_sec: f64,
}

impl TokenBucket {
    fn new(rate_per_sec: f64) -> Self {
        let cap = rate_per_sec.max(1.0);
        Self {
            capacity: cap,
            tokens: cap,
            last_refill: Instant::now(),
            refill_per_sec: rate_per_sec,
        }
    }

    fn allow(&mut self) -> bool {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.refill_per_sec).min(self.capacity);
        self.last_refill = now;
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

pub trait SwarmBus: Send + Sync {
    fn publish(&self, subject: &str, reply_to: Option<&str>, bytes: Vec<u8>) -> anyhow::Result<()>;
    fn subscribe(&self, subject: &str, handler: Box<MessageHandler>) -> anyhow::Result<()>;
}

pub type MessageHandler = dyn Fn(String, Vec<u8>, Option<String>) + Send + Sync;

/// Real NATS bus (async-nats) using an internal Tokio runtime.
#[cfg(feature = "nats")]
pub struct NatsBus {
    client: async_nats::Client,
    runtime: tokio::runtime::Runtime,
}

#[cfg(feature = "nats")]
impl NatsBus {
    pub fn connect(url: &str) -> anyhow::Result<Self> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        let client = rt.block_on(async { async_nats::connect(url).await })?;
        Ok(Self {
            client,
            runtime: rt,
        })
    }
}

#[cfg(feature = "nats")]
impl SwarmBus for NatsBus {
    fn publish(&self, subject: &str, reply_to: Option<&str>, bytes: Vec<u8>) -> anyhow::Result<()> {
        let subject = subject.to_string();
        let reply_to = reply_to.map(|s| s.to_string());
        self.runtime.block_on(async {
            match reply_to {
                Some(reply) => self
                    .client
                    .publish_with_reply(subject, reply, bytes.into())
                    .await
                    .map(|_| ()),
                None => self.client.publish(subject, bytes.into()).await.map(|_| ()),
            }
        })?;
        Ok(())
    }

    fn subscribe(&self, subject: &str, handler: Box<MessageHandler>) -> anyhow::Result<()> {
        let client = self.client.clone();
        let subject = subject.to_string();
        let handler = Arc::new(handler);
        self.runtime.spawn(async move {
            if let Ok(mut sub) = client.subscribe(subject.clone()).await {
                while let Some(msg) = sub.next().await {
                    let reply = msg.reply.map(|r| r.to_string());
                    handler(subject.clone(), msg.payload.to_vec(), reply);
                }
            }
        });
        Ok(())
    }
}

/// In-memory bus for tests and WASI-friendly runs.
#[derive(Default, Clone)]
pub struct MockBus {
    subs: Arc<RwLock<HashMap<String, Vec<Arc<MessageHandler>>>>>,
}

impl SwarmBus for MockBus {
    fn publish(&self, subject: &str, reply_to: Option<&str>, bytes: Vec<u8>) -> anyhow::Result<()> {
        if let Some(list) = self.subs.read().get(subject) {
            for cb in list.iter() {
                cb(
                    subject.to_string(),
                    bytes.clone(),
                    reply_to.map(|s| s.to_string()),
                );
            }
        }
        Ok(())
    }

    fn subscribe(&self, subject: &str, handler: Box<MessageHandler>) -> anyhow::Result<()> {
        self.subs
            .write()
            .entry(subject.to_string())
            .or_default()
            .push(handler.into());
        Ok(())
    }
}

pub struct NatsSwarmTransport {
    pub(crate) bus: Arc<dyn SwarmBus>,
    pub self_id: PeerId,
    limiter: RateLimiter,
    backoff_until: Mutex<Option<Instant>>,
    // last received messages
    pub(crate) rx: Arc<Mutex<Vec<(String, SwarmEnvelope)>>>,
}

impl NatsSwarmTransport {
    pub fn new(bus: Arc<dyn SwarmBus>, self_id: PeerId, msgs_per_sec: f64) -> anyhow::Result<Self> {
        let inbox = inbox_subject(&self_id.0);
        let rx = Arc::new(Mutex::new(Vec::new()));
        let rx_clone = rx.clone();
        bus.subscribe(
            &inbox,
            Box::new(move |_subject, bytes, reply_to| {
                if let Ok((env, _)) = bincode::serde::decode_from_slice::<SwarmEnvelope, _>(
                    &bytes,
                    bincode::config::standard(),
                ) {
                    rx_clone.lock().push((reply_to.unwrap_or_default(), env));
                }
            }),
        )?;
        Ok(Self {
            bus,
            self_id,
            limiter: RateLimiter::per_second(msgs_per_sec),
            backoff_until: Mutex::new(None),
            rx,
        })
    }

    pub fn send_direct(&self, to: PeerId, msg: SwarmMessage) -> anyhow::Result<()> {
        if !self.limiter.allow() {
            return Ok(());
        }
        if self.is_backing_off() {
            return Ok(());
        }
        let dest = inbox_subject(&to.0);
        let env = SwarmEnvelope::new(self.self_id.clone(), Some(to), msg, None)?;
        let bytes = bincode::serde::encode_to_vec(&env, bincode::config::standard())
            .map_err(anyhow::Error::from)?;
        if let Err(e) = self.bus.publish(&dest, None, bytes) {
            self.mark_failure();
            return Err(e);
        }
        Ok(())
    }

    pub fn request(
        &self,
        to: PeerId,
        msg: SwarmMessage,
        timeout: Duration,
    ) -> anyhow::Result<SwarmEnvelope> {
        if self.is_backing_off() {
            anyhow::bail!("backing off");
        }
        let mut reply_id = [0u8; 8];
        rand::rng().fill_bytes(&mut reply_id);
        let reply_subject = format!(
            "dwbase.swarm.reply.{}.{}",
            self.self_id.0,
            hex::encode(reply_id)
        );

        let rx = Arc::new(Mutex::new(Vec::<SwarmEnvelope>::new()));
        let rx_clone = rx.clone();
        self.bus.subscribe(
            &reply_subject,
            Box::new(move |_sub, bytes, _reply_to| {
                if let Ok((env, _)) = bincode::serde::decode_from_slice::<SwarmEnvelope, _>(
                    &bytes,
                    bincode::config::standard(),
                ) {
                    rx_clone.lock().push(env);
                }
            }),
        )?;

        let dest = inbox_subject(&to.0);
        let env = SwarmEnvelope::new(
            self.self_id.clone(),
            Some(to),
            msg,
            Some(reply_subject.clone()),
        )?;
        let bytes = bincode::serde::encode_to_vec(&env, bincode::config::standard())
            .map_err(anyhow::Error::from)?;
        self.bus.publish(&dest, Some(&reply_subject), bytes)?;

        let start = Instant::now();
        while start.elapsed() < timeout {
            if let Some(env) = rx.lock().pop() {
                return Ok(env);
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        self.mark_failure();
        anyhow::bail!("request timeout");
    }

    pub fn poll(&self) -> Option<(String, SwarmEnvelope)> {
        self.rx.lock().pop()
    }

    pub fn respond(
        &self,
        reply_to: &str,
        msg: SwarmMessage,
        correlation_id: String,
    ) -> anyhow::Result<()> {
        let mut env = SwarmEnvelope::new(self.self_id.clone(), None, msg, None)?;
        env.correlation_id = correlation_id;
        let bytes = bincode::serde::encode_to_vec(&env, bincode::config::standard())
            .map_err(anyhow::Error::from)?;
        self.bus.publish(reply_to, None, bytes)?;
        Ok(())
    }

    fn is_backing_off(&self) -> bool {
        self.backoff_until
            .lock()
            .map(|u| u > Instant::now())
            .unwrap_or(false)
    }

    fn mark_failure(&self) {
        let mut guard = self.backoff_until.lock();
        let next = Instant::now() + Duration::from_millis(200);
        *guard = Some(next);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dwbase_core::WorldKey;

    #[test]
    fn ping_and_summary_advert_roundtrip() {
        let bus = Arc::new(MockBus::default());
        let a = NatsSwarmTransport::new(bus.clone(), PeerId::new("a"), 100.0).unwrap();
        let b = NatsSwarmTransport::new(bus.clone(), PeerId::new("b"), 100.0).unwrap();

        a.send_direct(PeerId::new("b"), SwarmMessage::Ping { nonce: 1 })
            .unwrap();
        let (_reply_to, env) = b.poll().expect("b should receive ping");
        assert_eq!(env.kind, "Ping");

        // Request/reply summary advert
        let advert = SummaryAdvert::new(
            WorldKey::new("w1"),
            vec![dwbase_engine::SummaryWindow {
                start_ms: 0,
                end_ms: 1000,
            }],
            "d1",
        );

        // b listens and replies when asked
        a.send_direct(
            PeerId::new("b"),
            SwarmMessage::SummaryAdvert(advert.clone()),
        )
        .unwrap();
        let (_r, env2) = b.poll().expect("b should receive advert");
        let decoded = env2.decode_message().unwrap();
        match decoded {
            SwarmMessage::SummaryAdvert(a) => assert_eq!(a.digest, advert.digest),
            other => panic!("unexpected message {other:?}"),
        }

        // request/reply using MissingRequest and AtomBatch as a ping-like
        std::thread::spawn({
            let b = b;
            move || {
                let start = Instant::now();
                while start.elapsed() < Duration::from_millis(300) {
                    if let Some((reply_to, req)) = b.poll() {
                        if !reply_to.is_empty() {
                            let _ = b.respond(
                                &reply_to,
                                SwarmMessage::Ping { nonce: 42 },
                                req.correlation_id,
                            );
                            return;
                        }
                    }
                    std::thread::sleep(Duration::from_millis(5));
                }
            }
        });

        let resp = a
            .request(
                PeerId::new("b"),
                SwarmMessage::Ping { nonce: 9 },
                Duration::from_millis(500),
            )
            .unwrap();
        assert_eq!(resp.kind, "Ping");
    }

    /// Optional integration test against a real NATS server.
    /// Run with:
    /// `NATS_URL=nats://127.0.0.1:4222 cargo test -p dwbase-swarm-nats --features nats -- --ignored`
    #[cfg(feature = "nats")]
    #[test]
    #[ignore]
    fn real_nats_ping_and_reply() {
        let Ok(url) = std::env::var("NATS_URL") else {
            eprintln!("skipping: NATS_URL not set");
            return;
        };
        let bus = Arc::new(NatsBus::connect(&url).expect("connect nats"));

        let a = NatsSwarmTransport::new(bus.clone(), PeerId::new("a"), 50.0).unwrap();
        let b = NatsSwarmTransport::new(bus.clone(), PeerId::new("b"), 50.0).unwrap();

        a.send_direct(PeerId::new("b"), SwarmMessage::Ping { nonce: 123 })
            .unwrap();

        // Wait for b to receive ping.
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(2) {
            if let Some((_reply, env)) = b.poll() {
                assert_eq!(env.kind, "Ping");
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }

        // b responds to a request ping.
        std::thread::spawn({
            let b = b;
            move || {
                let start = Instant::now();
                while start.elapsed() < Duration::from_secs(2) {
                    if let Some((reply_to, req)) = b.poll() {
                        if !reply_to.is_empty() {
                            let _ = b.respond(
                                &reply_to,
                                SwarmMessage::Ping { nonce: 777 },
                                req.correlation_id,
                            );
                            return;
                        }
                    }
                    std::thread::sleep(Duration::from_millis(10));
                }
            }
        });

        let resp = a
            .request(
                PeerId::new("b"),
                SwarmMessage::Ping { nonce: 9 },
                Duration::from_secs(2),
            )
            .expect("request");
        assert_eq!(resp.kind, "Ping");
    }
}
