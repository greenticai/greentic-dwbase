//! Sled-backed append-only storage engine for DWBase.
//!
//! Atoms are immutable and written once; replay is driven by a per-world log that records
//! append order. Storage keys:
//! - `world/{world}/atoms/{atom_id}` -> serialized `Atom`
//! - `world/{world}/log/{seq}` -> `atom_id` (sequence is zero-padded decimal for ordering)

use std::{path::PathBuf, sync::Arc};

use aes_gcm::{
    aead::{Aead, KeyInit},
    Aes256Gcm, Nonce,
};
use crc32fast::Hasher as Crc32;
use dwbase_core::{Atom, AtomId, WorldKey};
use dwbase_engine::{AtomFilter, DwbaseError, Result, StorageEngine, StorageStats};
use rand::RngExt;
use rmp_serde::{decode, encode};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sled::IVec;
use thiserror::Error;

const LOG_SEQ_WIDTH: usize = 20;
const ENC_MAGIC: &[u8] = b"ENC1";

#[derive(Debug, Clone)]
pub struct SledConfig {
    pub path: PathBuf,
    pub flush_on_write: bool,
    pub encryption_enabled: bool,
    pub key_id: Option<String>,
}

impl SledConfig {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            flush_on_write: true,
            encryption_enabled: false,
            key_id: None,
        }
    }
}

pub trait KeyProvider: Send + Sync {
    fn key_bytes(&self, key_id: &str) -> Result<Vec<u8>>;
}

#[derive(Debug, Clone, Default)]
pub struct DummyKeyProvider {
    keys: std::collections::HashMap<String, Vec<u8>>,
}

impl DummyKeyProvider {
    pub fn with_key(mut self, key_id: impl Into<String>, key_bytes: [u8; 32]) -> Self {
        self.keys.insert(key_id.into(), key_bytes.to_vec());
        self
    }
}

impl KeyProvider for DummyKeyProvider {
    fn key_bytes(&self, key_id: &str) -> Result<Vec<u8>> {
        self.keys
            .get(key_id)
            .cloned()
            .ok_or_else(|| DwbaseError::InvalidInput(format!("missing key for id {key_id}")))
    }
}

#[derive(Debug, Default)]
pub struct EnvKeyProvider;

impl KeyProvider for EnvKeyProvider {
    fn key_bytes(&self, key_id: &str) -> Result<Vec<u8>> {
        let env_key = format!(
            "DWBASE_KEY_{}",
            key_id.to_ascii_uppercase().replace('-', "_")
        );
        let raw = std::env::var(&env_key).map_err(|_| {
            DwbaseError::InvalidInput(format!("env var {env_key} missing for key id {key_id}"))
        })?;
        hex::decode(raw.trim()).map_err(|e| DwbaseError::InvalidInput(e.to_string()))
    }
}

/// Sled-backed implementation of `StorageEngine`.
pub struct SledStorage {
    db: sled::Db,
    flush_on_write: bool,
    encryption_enabled: bool,
    key_id: Option<String>,
    key_provider: Arc<dyn KeyProvider>,
}

#[derive(Debug, Serialize, Deserialize)]
struct AtomIndexEntry {
    world: WorldKey,
    seq: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct LogFrame {
    atom_id: AtomId,
    checksum: u32,
    len: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct EncryptedBlob {
    key_id: String,
    nonce: [u8; 12],
    cipher: Vec<u8>,
}

#[derive(Debug, Error)]
enum StorageError {
    #[error("sled error: {0}")]
    Sled(#[from] sled::Error),
    #[error("serialization error: {0}")]
    Encode(#[from] encode::Error),
    #[error("deserialization error: {0}")]
    Decode(#[from] decode::Error),
    #[error("utf8 error: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
    #[error("str utf8 error: {0}")]
    StrUtf8(#[from] std::str::Utf8Error),
}

impl From<StorageError> for DwbaseError {
    fn from(err: StorageError) -> Self {
        DwbaseError::Storage(err.to_string())
    }
}

impl SledStorage {
    pub fn open(config: SledConfig, key_provider: Arc<dyn KeyProvider>) -> Result<Self> {
        if config.encryption_enabled && config.key_id.is_none() {
            return Err(DwbaseError::InvalidInput(
                "encryption enabled but key_id missing".into(),
            ));
        }
        let db = sled::open(&config.path).map_err(StorageError::from)?;
        let storage = Self {
            db,
            flush_on_write: config.flush_on_write,
            encryption_enabled: config.encryption_enabled,
            key_id: config.key_id.clone(),
            key_provider,
        };
        storage.repair_logs()?;
        // Best-effort index build to avoid legacy scans.
        let _ = storage.rebuild_index();
        Ok(storage)
    }

    pub fn open_with_env(config: SledConfig) -> Result<Self> {
        Self::open(config, Arc::new(EnvKeyProvider))
    }

    fn key_from_bytes(bytes: Vec<u8>) -> Result<[u8; 32]> {
        if bytes.len() != 32 {
            return Err(DwbaseError::InvalidInput(
                "encryption key must be 32 bytes (AES-256-GCM)".into(),
            ));
        }
        let mut key = [0u8; 32];
        key.copy_from_slice(&bytes);
        Ok(key)
    }

    fn derive_data_key(master: [u8; 32], key_id: &str, nonce: &[u8]) -> aes_gcm::Key<Aes256Gcm> {
        let mut hasher = Sha256::new();
        hasher.update(master);
        hasher.update(key_id.as_bytes());
        hasher.update(nonce);
        let derived = hasher.finalize();
        let mut key = [0u8; 32];
        key.copy_from_slice(&derived);
        *aes_gcm::Key::<Aes256Gcm>::from_slice(&key)
    }

    fn encrypt_bytes(&self, plain: &[u8]) -> Result<Vec<u8>> {
        let key_id = self
            .key_id
            .as_ref()
            .ok_or_else(|| DwbaseError::InvalidInput("key_id missing".into()))?;
        let key_bytes = self.key_provider.key_bytes(key_id)?;
        let master = Self::key_from_bytes(key_bytes)?;
        let mut nonce_bytes = [0u8; 12];
        let mut rng = rand::rng();
        rng.fill(&mut nonce_bytes);
        let data_key = Self::derive_data_key(master, key_id, &nonce_bytes);
        let cipher = Aes256Gcm::new(&data_key)
            .encrypt(Nonce::from_slice(&nonce_bytes), plain)
            .map_err(|e| DwbaseError::Storage(e.to_string()))?;
        let blob = EncryptedBlob {
            key_id: key_id.clone(),
            nonce: nonce_bytes,
            cipher,
        };
        let mut out = ENC_MAGIC.to_vec();
        let blob_bytes = rmp_serde::to_vec(&blob).map_err(StorageError::from)?;
        out.extend(blob_bytes);
        Ok(out)
    }

    fn decrypt_bytes(&self, bytes: &[u8]) -> Result<Vec<u8>> {
        let blob: EncryptedBlob = rmp_serde::from_slice(bytes).map_err(StorageError::from)?;
        let key_bytes = self.key_provider.key_bytes(&blob.key_id)?;
        let master = Self::key_from_bytes(key_bytes)?;
        let data_key = Self::derive_data_key(master, &blob.key_id, &blob.nonce);
        Aes256Gcm::new(&data_key)
            .decrypt(Nonce::from_slice(&blob.nonce), blob.cipher.as_ref())
            .map_err(|e| DwbaseError::Storage(e.to_string()))
    }

    fn atom_key(world: &WorldKey, atom_id: &AtomId) -> Vec<u8> {
        format!("world/{}/atoms/{}", world.0, atom_id.0).into_bytes()
    }

    fn log_prefix(world: &WorldKey) -> Vec<u8> {
        format!("world/{}/log/", world.0).into_bytes()
    }

    fn log_key(world: &WorldKey, seq: u64) -> Vec<u8> {
        format!(
            "world/{}/log/{:0width$}",
            world.0,
            seq,
            width = LOG_SEQ_WIDTH
        )
        .into_bytes()
    }

    fn next_seq(&self, world: &WorldKey) -> Result<u64> {
        let prefix = Self::log_prefix(world);
        let mut iter = self.db.scan_prefix(prefix);
        let last = iter
            .next_back()
            .and_then(|res| res.ok())
            .and_then(|(key, _)| Self::seq_from_log_key(&key));
        Ok(last.map_or(0, |n| n + 1))
    }

    fn seq_from_log_key(key: &IVec) -> Option<u64> {
        let s = std::str::from_utf8(key.as_ref()).ok()?;
        let seq_part = s.rsplit('/').next()?;
        seq_part.parse().ok()
    }

    fn decode_frame(&self, bytes: &[u8]) -> Result<LogFrame> {
        rmp_serde::from_slice(bytes)
            .map_err(StorageError::from)
            .map_err(Into::into)
    }

    fn repair_logs(&self) -> Result<()> {
        let mut worlds = std::collections::HashSet::new();
        for entry in self.db.scan_prefix("world/") {
            let (key, _) = entry.map_err(StorageError::from)?;
            if let Some(world) = Self::world_from_key(&key) {
                worlds.insert(world);
            }
        }
        for world in worlds {
            self.repair_world_log(&world)?;
        }
        Ok(())
    }

    fn world_from_key(key: &IVec) -> Option<WorldKey> {
        let s = std::str::from_utf8(key.as_ref()).ok()?;
        let mut parts = s.split('/');
        let first = parts.next()?;
        let world = parts.next()?;
        if first == "world" {
            Some(WorldKey(world.to_string()))
        } else {
            None
        }
    }

    fn repair_world_log(&self, world: &WorldKey) -> Result<()> {
        let prefix = Self::log_prefix(world);
        let mut _last_good: Option<u64> = None;
        let mut bad_seq: Option<u64> = None;
        for entry in self.db.scan_prefix(prefix.clone()) {
            let (key, val) = entry.map_err(StorageError::from)?;
            let seq = match Self::seq_from_log_key(&key) {
                Some(s) => s,
                None => continue,
            };
            let frame = match self.decode_frame(val.as_ref()) {
                Ok(f) => f,
                Err(_) => {
                    bad_seq = Some(seq);
                    break;
                }
            };
            let atom_bytes = match self.db.get(Self::atom_key(world, &frame.atom_id)) {
                Ok(Some(b)) => b,
                _ => {
                    bad_seq = Some(seq);
                    break;
                }
            };
            let checksum = Self::checksum(atom_bytes.as_ref());
            if checksum != frame.checksum {
                bad_seq = Some(seq);
                break;
            }
            _last_good = Some(seq);
        }

        if let Some(bad) = bad_seq {
            let mut to_delete = Vec::new();
            for entry in self.db.scan_prefix(prefix.clone()) {
                let (key, _) = entry.map_err(StorageError::from)?;
                if let Some(seq) = Self::seq_from_log_key(&key) {
                    if seq >= bad {
                        to_delete.push(key);
                    }
                }
            }
            for key in to_delete {
                let _ = self.db.remove(key);
            }
            eprintln!("repair: truncated log for {} at seq {}", world.0, bad);
        }
        Ok(())
    }

    fn encode_atom(&self, atom: &Atom) -> Result<Vec<u8>> {
        let plain = rmp_serde::to_vec(atom).map_err(StorageError::from)?;
        if self.encryption_enabled {
            self.encrypt_bytes(&plain)
        } else {
            Ok(plain)
        }
    }

    fn decode_atom(&self, bytes: &[u8]) -> Result<Atom> {
        if bytes.starts_with(ENC_MAGIC) {
            let decrypted = self.decrypt_bytes(&bytes[ENC_MAGIC.len()..])?;
            return rmp_serde::from_slice(&decrypted)
                .map_err(StorageError::from)
                .map_err(Into::into);
        }
        rmp_serde::from_slice(bytes)
            .map_err(StorageError::from)
            .map_err(Into::into)
    }

    fn flush_if_needed(&self) -> Result<()> {
        if self.flush_on_write {
            self.db.flush().map_err(StorageError::from)?;
        }
        Ok(())
    }

    fn checksum(bytes: &[u8]) -> u32 {
        let mut h = Crc32::new();
        h.update(bytes);
        h.finalize()
    }

    /// Append a batch of atoms to the same world, returning their ids.
    pub fn append_atoms(&self, world: &WorldKey, atoms: Vec<Atom>) -> Result<Vec<AtomId>> {
        let mut seq = self.next_seq(world)?;
        let mut ids = Vec::with_capacity(atoms.len());
        for atom in atoms {
            let atom_world = atom.world().clone();
            if &atom_world != world {
                return Err(DwbaseError::InvalidInput(format!(
                    "atom world {} does not match target world {}",
                    atom_world.0, world.0
                )));
            }
            let id = atom.id().clone();
            let bytes = self.encode_atom(&atom)?;
            let frame = LogFrame {
                atom_id: id.clone(),
                checksum: Self::checksum(&bytes),
                len: bytes.len() as u64,
            };
            let frame_bytes = rmp_serde::to_vec(&frame).map_err(StorageError::from)?;
            self.db
                .insert(Self::atom_key(world, &id), bytes)
                .map_err(StorageError::from)?;
            self.db
                .insert(Self::log_key(world, seq), frame_bytes)
                .map_err(StorageError::from)?;
            self.record_index(world, &id, seq)?;
            seq += 1;
            ids.push(id);
        }
        self.flush_if_needed()?;
        Ok(ids)
    }

    fn atom_from_store(&self, id: &AtomId, world: &WorldKey) -> Result<Option<Atom>> {
        let key = Self::atom_key(world, id);
        let bytes = match self.db.get(key).map_err(StorageError::from)? {
            Some(b) => b,
            None => return Ok(None),
        };
        self.decode_atom(bytes.as_ref()).map(Some)
    }

    fn matches_filter(atom: &Atom, filter: &AtomFilter) -> bool {
        if !filter.kinds.is_empty() && !filter.kinds.contains(atom.kind()) {
            return false;
        }
        if !filter.labels.is_empty() && !filter.labels.iter().all(|l| atom.labels().contains(l)) {
            return false;
        }
        if !filter.flags.is_empty() && !filter.flags.iter().all(|f| atom.flags().contains(f)) {
            return false;
        }
        if let Some(since) = &filter.since {
            if atom.timestamp().0 < since.0 {
                return false;
            }
        }
        if let Some(until) = &filter.until {
            if atom.timestamp().0 > until.0 {
                return false;
            }
        }
        true
    }

    #[cfg(test)]
    fn corrupt_log_entry(&self, world: &WorldKey, seq: u64, bytes: &[u8]) {
        let _ = self.db.insert(Self::log_key(world, seq), bytes);
    }

    fn atom_index_key(id: &AtomId) -> Vec<u8> {
        format!("idx/atom/{}", id.0).into_bytes()
    }

    fn index_entry(world: &WorldKey, seq: u64) -> Result<Vec<u8>> {
        let entry = AtomIndexEntry {
            world: world.clone(),
            seq,
        };
        rmp_serde::to_vec(&entry)
            .map_err(StorageError::from)
            .map_err(Into::into)
    }

    fn decode_index(bytes: &[u8]) -> Result<AtomIndexEntry> {
        rmp_serde::from_slice(bytes)
            .map_err(StorageError::from)
            .map_err(Into::into)
    }

    fn record_index(&self, world: &WorldKey, id: &AtomId, seq: u64) -> Result<()> {
        let key = Self::atom_index_key(id);
        let val = Self::index_entry(world, seq)?;
        self.db.insert(key, val).map_err(StorageError::from)?;
        Ok(())
    }

    fn clear_index(&self) -> Result<()> {
        let mut to_delete = Vec::new();
        for entry in self.db.scan_prefix("idx/atom/") {
            let (k, _) = entry.map_err(StorageError::from)?;
            to_delete.push(k);
        }
        for k in to_delete {
            let _ = self.db.remove(k);
        }
        Ok(())
    }

    /// Rebuild the atom id index from existing worlds/logs; returns entries written.
    pub fn rebuild_index(&self) -> Result<u64> {
        self.clear_index()?;
        let mut written = 0u64;
        for world in self.worlds()? {
            let prefix = Self::log_prefix(&world);
            for entry in self.db.scan_prefix(prefix) {
                let (key, val) = entry.map_err(StorageError::from)?;
                let seq = match Self::seq_from_log_key(&key) {
                    Some(s) => s,
                    None => continue,
                };
                let frame = match self.decode_frame(val.as_ref()) {
                    Ok(f) => f,
                    Err(_) => continue,
                };
                self.record_index(&world, &frame.atom_id, seq)?;
                written += 1;
            }
        }
        Ok(written)
    }
}

impl StorageEngine for SledStorage {
    fn append(&self, atom: Atom) -> Result<()> {
        let world = atom.world().clone();
        self.append_atoms(&world, vec![atom])?;
        Ok(())
    }

    fn get_by_ids(&self, ids: &[AtomId]) -> Result<Vec<Atom>> {
        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            let mut found = None;
            if let Some(idx_bytes) = self
                .db
                .get(Self::atom_index_key(id))
                .map_err(StorageError::from)?
            {
                if let Ok(entry) = Self::decode_index(idx_bytes.as_ref()) {
                    if let Some(atom) = self.atom_from_store(id, &entry.world)? {
                        found = Some(atom);
                    }
                }
            }
            if found.is_none() {
                // World is encoded in atom entry path, but not in id; legacy fallback scans worlds.
                for world_atom in self.db.scan_prefix(b"world/") {
                    let (key, _) = world_atom.map_err(StorageError::from)?;
                    if !key.ends_with(format!("/atoms/{}", id.0).as_bytes()) {
                        continue;
                    }
                    let s = match std::str::from_utf8(key.as_ref()) {
                        Ok(v) => v,
                        Err(_) => continue,
                    };
                    let parts: Vec<_> = s.split('/').collect();
                    let world = WorldKey(parts.get(1).unwrap_or(&"").to_string());
                    found = self.atom_from_store(id, &world)?;
                    if found.is_some() {
                        break;
                    }
                }
            }
            if let Some(atom) = found {
                out.push(atom);
            }
        }
        Ok(out)
    }

    fn scan(&self, world: &WorldKey, filter: &AtomFilter) -> Result<Vec<Atom>> {
        if let Some(f_world) = &filter.world {
            if f_world != world {
                return Ok(Vec::new());
            }
        }
        let prefix = Self::log_prefix(world);
        let mut results = Vec::new();
        for entry in self.db.scan_prefix(prefix) {
            let (_, frame_bytes) = entry.map_err(StorageError::from)?;
            let frame = match self.decode_frame(frame_bytes.as_ref()) {
                Ok(f) => f,
                Err(_) => continue,
            };
            let atom_id = frame.atom_id;
            if let Some(atom) = self.atom_from_store(&atom_id, world)? {
                if Self::matches_filter(&atom, filter) {
                    results.push(atom);
                    if let Some(limit) = filter.limit {
                        if results.len() >= limit {
                            break;
                        }
                    }
                }
            }
        }
        Ok(results)
    }

    fn stats(&self, world: &WorldKey) -> Result<StorageStats> {
        let mut atom_count = 0usize;
        let mut vector_count = 0usize;
        let prefix = format!("world/{}/atoms/", world.0);
        for entry in self.db.scan_prefix(prefix.as_bytes()) {
            let (_, bytes) = entry.map_err(StorageError::from)?;
            atom_count += 1;
            if let Ok(atom) = self.decode_atom(bytes.as_ref()) {
                if atom.vector().is_some() {
                    vector_count += 1;
                }
            }
        }
        Ok(StorageStats {
            atom_count,
            vector_count,
        })
    }

    fn list_ids_in_window(
        &self,
        world: &WorldKey,
        window: &dwbase_engine::TimeWindow,
    ) -> Result<Vec<AtomId>> {
        let mut ids = Vec::new();
        let prefix = Self::log_prefix(world);
        for entry in self.db.scan_prefix(prefix) {
            let (_key, val) = entry.map_err(StorageError::from)?;
            let frame = match self.decode_frame(val.as_ref()) {
                Ok(f) => f,
                Err(_) => continue,
            };
            if let Some(atom) = self.atom_from_store(&frame.atom_id, world)? {
                let ts = dwbase_core::Timestamp::new(atom.timestamp().0.clone());
                if let Ok(dt) = time::OffsetDateTime::parse(
                    ts.0.as_str(),
                    &time::format_description::well_known::Rfc3339,
                ) {
                    let ms = (dt.unix_timestamp_nanos() / 1_000_000) as i64;
                    if ms >= window.start_ms && ms <= window.end_ms {
                        ids.push(frame.atom_id.clone());
                    }
                }
            }
        }
        Ok(ids)
    }

    fn delete_atoms(&self, world: &WorldKey, ids: &[AtomId]) -> Result<usize> {
        let mut removed = 0usize;
        for id in ids {
            let _ = self.db.remove(Self::atom_key(world, id));
            let _ = self.db.remove(Self::atom_index_key(id));
        }
        let prefix = Self::log_prefix(world);
        for entry in self.db.scan_prefix(prefix.clone()) {
            let (key, val) = entry.map_err(StorageError::from)?;
            let frame = match self.decode_frame(val.as_ref()) {
                Ok(f) => f,
                Err(_) => continue,
            };
            if ids.contains(&frame.atom_id) {
                let _ = self.db.remove(key);
                removed += 1;
            }
        }
        Ok(removed)
    }

    fn worlds(&self) -> Result<Vec<WorldKey>> {
        let mut worlds = std::collections::HashSet::new();
        for entry in self.db.scan_prefix("world/") {
            let (key, _) = entry.map_err(StorageError::from)?;
            if let Some(w) = Self::world_from_key(&key) {
                worlds.insert(w);
            }
        }
        Ok(worlds.into_iter().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dwbase_core::{AtomKind, Importance, Timestamp, WorkerKey};
    use std::sync::Arc;
    use tempfile::TempDir;

    fn sample_atom(id: &str, world: &str, ts: &str, importance: f32) -> Atom {
        Atom::builder(
            AtomId::new(id),
            WorldKey::new(world),
            WorkerKey::new("worker"),
            AtomKind::Observation,
            Timestamp::new(ts),
            Importance::new(importance).unwrap(),
            r#"{"hello":"world"}"#,
        )
        .add_flag("f1")
        .add_label("l1")
        .build()
    }

    fn new_store() -> (SledStorage, TempDir) {
        let tmp = TempDir::new().unwrap();
        let storage = SledStorage::open(
            SledConfig::new(tmp.path()),
            Arc::new(DummyKeyProvider::default()),
        )
        .unwrap();
        (storage, tmp)
    }

    #[test]
    fn append_and_replay_preserves_order() {
        let (storage, _tmp) = new_store();
        let world = WorldKey::new("w1");
        let a1 = sample_atom("a1", "w1", "2024-01-01T00:00:00Z", 0.4);
        let a2 = sample_atom("a2", "w1", "2024-01-01T00:00:01Z", 0.6);

        storage
            .append_atoms(&world, vec![a1.clone(), a2.clone()])
            .unwrap();

        let replayed = storage
            .scan(&world, &AtomFilter::default())
            .expect("replay");
        assert_eq!(replayed.len(), 2);
        assert_eq!(replayed[0].id(), a1.id());
        assert_eq!(replayed[1].id(), a2.id());
    }

    #[test]
    fn get_by_ids_returns_atoms() {
        let (storage, _tmp) = new_store();
        let world = WorldKey::new("w1");
        let a1 = sample_atom("a1", "w1", "2024-01-01T00:00:00Z", 0.4);
        let a2 = sample_atom("a2", "w1", "2024-01-01T00:00:01Z", 0.6);
        storage
            .append_atoms(&world, vec![a1.clone(), a2.clone()])
            .unwrap();

        let atoms = storage
            .get_by_ids(&[AtomId::new("a2"), AtomId::new("a1")])
            .unwrap();
        assert_eq!(atoms.len(), 2);
        assert_eq!(atoms[0].id(), a2.id());
        assert_eq!(atoms[1].id(), a1.id());
    }

    #[test]
    fn get_by_ids_prefers_indexed_world() {
        let (storage, _tmp) = new_store();
        let w1 = WorldKey::new("a");
        let w2 = WorldKey::new("z");
        let dup1 = sample_atom("dup", "a", "2024-01-01T00:00:00Z", 0.1);
        let dup2 = sample_atom("dup", "z", "2024-01-01T00:00:01Z", 0.2);
        storage.append_atoms(&w1, vec![dup1]).unwrap();
        storage.append_atoms(&w2, vec![dup2.clone()]).unwrap();

        let atoms = storage.get_by_ids(&[AtomId::new("dup")]).unwrap();
        assert_eq!(atoms.len(), 1);
        assert_eq!(atoms[0].world(), dup2.world());
    }

    #[test]
    fn replay_with_limit_and_filters() {
        let (storage, _tmp) = new_store();
        let world = WorldKey::new("w1");
        storage
            .append_atoms(
                &world,
                vec![
                    sample_atom("a1", "w1", "2024-01-01T00:00:00Z", 0.4),
                    sample_atom("a2", "w1", "2024-01-01T00:00:01Z", 0.6),
                    sample_atom("a3", "w1", "2024-01-01T00:00:02Z", 0.7),
                ],
            )
            .unwrap();

        let filter = AtomFilter {
            world: None,
            kinds: vec![AtomKind::Observation],
            labels: vec!["l1".to_string()],
            flags: vec!["f1".to_string()],
            since: Some(Timestamp::new("2024-01-01T00:00:01Z")),
            until: None,
            limit: Some(1),
        };
        let replayed = storage.scan(&world, &filter).unwrap();
        assert_eq!(replayed.len(), 1);
        assert_eq!(replayed[0].id(), &AtomId::new("a2"));
    }

    #[test]
    fn rebuild_index_populates_entries() {
        let (storage, _tmp) = new_store();
        let world = WorldKey::new("w1");
        storage
            .append_atoms(
                &world,
                vec![
                    sample_atom("a1", "w1", "2024-01-01T00:00:00Z", 0.4),
                    sample_atom("a2", "w1", "2024-01-01T00:00:01Z", 0.6),
                ],
            )
            .unwrap();

        storage.clear_index().unwrap();
        assert!(storage
            .db
            .get(SledStorage::atom_index_key(&AtomId::new("a1")))
            .unwrap()
            .is_none());

        let rebuilt = storage.rebuild_index().unwrap();
        assert_eq!(rebuilt, 2);

        let entry_bytes = storage
            .db
            .get(SledStorage::atom_index_key(&AtomId::new("a1")))
            .unwrap()
            .expect("rebuilt index entry");
        let entry = SledStorage::decode_index(entry_bytes.as_ref()).unwrap();
        assert_eq!(entry.world, world);
        assert_eq!(entry.seq, 0);
    }

    #[test]
    fn delete_atoms_clears_index_entries() {
        let (storage, _tmp) = new_store();
        let world = WorldKey::new("w1");
        storage
            .append_atoms(
                &world,
                vec![
                    sample_atom("a1", "w1", "2024-01-01T00:00:00Z", 0.4),
                    sample_atom("a2", "w1", "2024-01-01T00:00:01Z", 0.6),
                ],
            )
            .unwrap();

        let removed = storage.delete_atoms(&world, &[AtomId::new("a1")]).unwrap();
        assert_eq!(removed, 1);
        assert!(storage
            .db
            .get(SledStorage::atom_index_key(&AtomId::new("a1")))
            .unwrap()
            .is_none());
        let atoms = storage.get_by_ids(&[AtomId::new("a1")]).unwrap();
        assert!(atoms.is_empty());
    }

    #[test]
    fn detects_and_truncates_corrupt_log_tail() {
        let (storage, tmp) = new_store();
        let world = WorldKey::new("w1");
        storage
            .append_atoms(
                &world,
                vec![
                    sample_atom("a1", "w1", "2024-01-01T00:00:00Z", 0.4),
                    sample_atom("a2", "w1", "2024-01-01T00:00:01Z", 0.6),
                ],
            )
            .unwrap();

        // Corrupt the last log frame.
        storage.corrupt_log_entry(&world, 1, b"badframe");
        drop(storage);

        let storage2 = SledStorage::open(
            SledConfig::new(tmp.path()),
            Arc::new(DummyKeyProvider::default()),
        )
        .unwrap();
        let replayed = storage2
            .scan(&world, &AtomFilter::default())
            .expect("scan after repair");
        assert_eq!(replayed.len(), 1);
        assert_eq!(replayed[0].id(), &AtomId::new("a1"));
    }

    #[test]
    fn encrypted_roundtrip_and_on_disk_ciphertext() {
        let tmp = TempDir::new().unwrap();
        let mut cfg = SledConfig::new(tmp.path());
        cfg.encryption_enabled = true;
        cfg.key_id = Some("k1".into());
        let provider = DummyKeyProvider::default().with_key("k1", [7u8; 32]);

        let storage =
            SledStorage::open(cfg.clone(), Arc::new(provider.clone())).expect("open encrypted");
        let world = WorldKey::new("w1");
        let atom = sample_atom("a1", "w1", "2024-01-01T00:00:00Z", 0.4);
        storage.append_atoms(&world, vec![atom.clone()]).unwrap();

        // Validate ciphertext on disk.
        let stored = storage
            .db
            .get(SledStorage::atom_key(&world, atom.id()))
            .unwrap()
            .expect("stored");
        assert!(
            stored.starts_with(ENC_MAGIC),
            "encrypted payload should be prefixed with magic"
        );
        drop(storage);

        // Re-open and read back with the same key.
        let storage2 = SledStorage::open(cfg, Arc::new(provider)).expect("reopen");
        let replayed = storage2.scan(&world, &AtomFilter::default()).unwrap();
        assert_eq!(replayed.len(), 1);
        assert_eq!(replayed[0].id(), atom.id());
    }

    #[test]
    fn encrypted_read_with_wrong_key_fails() {
        let tmp = TempDir::new().unwrap();
        let mut cfg = SledConfig::new(tmp.path());
        cfg.encryption_enabled = true;
        cfg.key_id = Some("k1".into());
        let provider = DummyKeyProvider::default().with_key("k1", [8u8; 32]);

        let storage = SledStorage::open(cfg.clone(), Arc::new(provider)).expect("open encrypted");
        let world = WorldKey::new("w1");
        let atom = sample_atom("a1", "w1", "2024-01-01T00:00:00Z", 0.4);
        storage.append_atoms(&world, vec![atom]).unwrap();
        drop(storage);

        let wrong_provider = DummyKeyProvider::default().with_key("k1", [9u8; 32]);
        let storage2 = SledStorage::open(cfg, Arc::new(wrong_provider)).expect("reopen");
        let err = storage2.scan(&world, &AtomFilter::default()).unwrap_err();
        assert!(
            format!("{err:?}").contains("Storage"),
            "expected storage error when decrypting with wrong key"
        );
    }
}
