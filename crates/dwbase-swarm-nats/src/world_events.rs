use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use rand::Rng;
use serde::{Deserialize, Serialize};

use dwbase_core::Atom;

use crate::swarm::{RateLimiter, SwarmBus};

fn world_token(world_key: &str) -> String {
    // Keep NATS subjects stable and safe even if world keys contain '.', '/', etc.
    // Token = hex(world_key bytes).
    hex::encode(world_key.as_bytes())
}

pub fn world_events_subject(world_key: &str) -> String {
    format!("dwbase.world.{}.events", world_token(world_key))
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AtomEvent {
    pub world_key: String,
    pub from_node: String,
    pub sent_at_ms: u64,
    pub atom: Atom,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AtomEventBatch {
    pub batch_id: String,
    pub events: Vec<AtomEvent>,
}

pub fn encode_event_batch(batch: &AtomEventBatch) -> anyhow::Result<Vec<u8>> {
    Ok(bincode::serde::encode_to_vec(
        batch,
        bincode::config::standard(),
    )?)
}

pub fn decode_event_batch(bytes: &[u8]) -> anyhow::Result<AtomEventBatch> {
    Ok(bincode::serde::decode_from_slice(bytes, bincode::config::standard()).map(|(v, _)| v)?)
}

fn new_batch_id() -> String {
    let mut bytes = [0u8; 8];
    rand::rng().fill_bytes(&mut bytes);
    hex::encode(bytes)
}

/// Simple, synchronous world-event broadcaster with rate limiting and opportunistic batching.
///
/// Designed to work in WASM/WASI environments without relying on async executors.
pub struct WorldEventBroadcaster {
    bus: Arc<dyn SwarmBus>,
    from_node: String,
    limiter: RateLimiter,
    flush_every: Duration,
    max_batch: usize,
    inner: Mutex<HashMap<String, (Instant, Vec<AtomEvent>)>>,
}

impl WorldEventBroadcaster {
    pub fn new(bus: Arc<dyn SwarmBus>, from_node: String, msgs_per_sec: f64) -> Self {
        Self {
            bus,
            from_node,
            limiter: RateLimiter::per_second(msgs_per_sec),
            flush_every: Duration::from_millis(50),
            max_batch: 32,
            inner: Mutex::new(HashMap::new()),
        }
    }

    pub fn publish_atom(&self, atom: Atom) -> anyhow::Result<()> {
        if !self.limiter.allow() {
            return Ok(());
        }
        let world_key = atom.world().0.clone();
        let event = AtomEvent {
            world_key: world_key.clone(),
            from_node: self.from_node.clone(),
            sent_at_ms: (std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis()) as u64,
            atom,
        };

        let mut guard = self.inner.lock();
        let entry = guard
            .entry(world_key.clone())
            .or_insert_with(|| (Instant::now(), Vec::new()));
        entry.1.push(event);

        let should_flush = entry.1.len() >= self.max_batch || entry.0.elapsed() >= self.flush_every;
        if should_flush {
            let (_t0, events) = guard
                .remove(&world_key)
                .unwrap_or_else(|| (Instant::now(), Vec::new()));
            drop(guard);
            self.flush(world_key, events)?;
        }
        Ok(())
    }

    pub fn flush_all(&self) -> anyhow::Result<()> {
        let mut to_flush = Vec::new();
        {
            let mut guard = self.inner.lock();
            for (world, (_t0, events)) in guard.drain() {
                if !events.is_empty() {
                    to_flush.push((world, events));
                }
            }
        }
        for (world, events) in to_flush {
            self.flush(world, events)?;
        }
        Ok(())
    }

    fn flush(&self, world_key: String, events: Vec<AtomEvent>) -> anyhow::Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        let subject = world_events_subject(&world_key);
        let batch = AtomEventBatch {
            batch_id: new_batch_id(),
            events,
        };
        let bytes = encode_event_batch(&batch)?;
        self.bus.publish(&subject, None, bytes)?;
        Ok(())
    }
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
    fn encode_decode_roundtrip() {
        let batch = AtomEventBatch {
            batch_id: "b1".into(),
            events: vec![AtomEvent {
                world_key: "w".into(),
                from_node: "n".into(),
                sent_at_ms: 1,
                atom: sample_atom("w", "a1"),
            }],
        };
        let bytes = encode_event_batch(&batch).unwrap();
        let decoded = decode_event_batch(&bytes).unwrap();
        assert_eq!(decoded.batch_id, "b1");
        assert_eq!(decoded.events.len(), 1);
        assert_eq!(decoded.events[0].atom.id().0, "a1");
    }

    #[test]
    fn broadcaster_publishes_on_world_subject() {
        let bus = Arc::new(MockBus::default()) as Arc<dyn SwarmBus>;
        let got = Arc::new(Mutex::new(Vec::<AtomEventBatch>::new()));
        let got_clone = got.clone();
        let subj = world_events_subject("w");
        bus.subscribe(
            &subj,
            Box::new(move |_s, bytes, _reply| {
                if let Ok(batch) = decode_event_batch(&bytes) {
                    got_clone.lock().push(batch);
                }
            }),
        )
        .unwrap();

        let b = WorldEventBroadcaster::new(bus, "node-a".into(), 100.0);
        b.publish_atom(sample_atom("w", "a1")).unwrap();
        b.flush_all().unwrap();

        let batches = got.lock();
        assert!(!batches.is_empty());
        assert_eq!(batches[0].events[0].atom.id().0, "a1");
    }
}
