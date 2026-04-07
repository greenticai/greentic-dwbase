//! Core immutable data model for DWBase.
//!
//! Atoms are immutable records; updates are represented by creating new atoms that
//! link to predecessors rather than mutating in place.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Identifier for an atom (stringly typed for WASM friendliness).
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AtomId(pub String);

impl AtomId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
}

impl fmt::Display for AtomId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Identifier for the world/space an atom belongs to.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WorldKey(pub String);

impl WorldKey {
    pub fn new(key: impl Into<String>) -> Self {
        Self(key.into())
    }
}

impl fmt::Display for WorldKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Identifier for the worker/agent that produced the atom.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WorkerKey(pub String);

impl WorkerKey {
    pub fn new(key: impl Into<String>) -> Self {
        Self(key.into())
    }
}

impl fmt::Display for WorkerKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Kind of atom within DWBase.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AtomKind {
    Observation,
    Reflection,
    Plan,
    Action,
    Message,
}

/// Relationship between atoms.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LinkKind {
    Supersedes,
    References,
    Confirms,
    Contradicts,
}

/// A typed link to another atom.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Link {
    pub target: AtomId,
    pub kind: LinkKind,
}

impl Link {
    pub fn new(target: AtomId, kind: LinkKind) -> Self {
        Self { target, kind }
    }
}

/// ISO-8601 UTC timestamp wrapper.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Timestamp(pub String);

impl Timestamp {
    pub fn new(ts: impl Into<String>) -> Self {
        Self(ts.into())
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Importance score in the closed range [0.0, 1.0].
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct Importance(f32);

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum ImportanceError {
    #[error("importance must be between 0.0 and 1.0 inclusive, got {0}")]
    OutOfRange(f32),
    #[error("importance cannot be NaN")]
    NotANumber,
}

impl Importance {
    /// Validates the provided value is finite and within [0.0, 1.0].
    pub fn new(value: f32) -> Result<Self, ImportanceError> {
        if value.is_nan() {
            return Err(ImportanceError::NotANumber);
        }
        if !(0.0..=1.0).contains(&value) {
            return Err(ImportanceError::OutOfRange(value));
        }
        Ok(Self(value))
    }

    /// Clamps the provided value into the valid range; NaN becomes 0.0.
    pub fn clamped(value: f32) -> Self {
        if value.is_nan() {
            return Self(0.0);
        }
        Self(value.clamp(0.0, 1.0))
    }

    pub fn get(self) -> f32 {
        self.0
    }
}

impl fmt::Display for Importance {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Immutable atom record.
///
/// Atoms are constructed once and never mutated; updates are represented by new atoms
/// referencing older atoms through `links`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Atom {
    id: AtomId,
    world: WorldKey,
    worker: WorkerKey,
    kind: AtomKind,
    timestamp: Timestamp,
    importance: Importance,
    payload_json: String,
    vector: Option<Vec<f32>>,
    flags: Vec<String>,
    labels: Vec<String>,
    links: Vec<Link>,
}

impl Atom {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: AtomId,
        world: WorldKey,
        worker: WorkerKey,
        kind: AtomKind,
        timestamp: Timestamp,
        importance: Importance,
        payload_json: impl Into<String>,
        vector: Option<Vec<f32>>,
        flags: Vec<String>,
        labels: Vec<String>,
        links: Vec<Link>,
    ) -> Self {
        Self {
            id,
            world,
            worker,
            kind,
            timestamp,
            importance,
            payload_json: payload_json.into(),
            vector,
            flags,
            labels,
            links,
        }
    }

    pub fn builder(
        id: AtomId,
        world: WorldKey,
        worker: WorkerKey,
        kind: AtomKind,
        timestamp: Timestamp,
        importance: Importance,
        payload_json: impl Into<String>,
    ) -> AtomBuilder {
        AtomBuilder {
            id,
            world,
            worker,
            kind,
            timestamp,
            importance,
            payload_json: payload_json.into(),
            vector: None,
            flags: Vec::new(),
            labels: Vec::new(),
            links: Vec::new(),
        }
    }

    pub fn id(&self) -> &AtomId {
        &self.id
    }

    pub fn world(&self) -> &WorldKey {
        &self.world
    }

    pub fn worker(&self) -> &WorkerKey {
        &self.worker
    }

    pub fn kind(&self) -> &AtomKind {
        &self.kind
    }

    pub fn timestamp(&self) -> &Timestamp {
        &self.timestamp
    }

    pub fn importance(&self) -> Importance {
        self.importance
    }

    pub fn payload_json(&self) -> &str {
        &self.payload_json
    }

    pub fn vector(&self) -> Option<&[f32]> {
        self.vector.as_deref()
    }

    pub fn flags(&self) -> &[String] {
        &self.flags
    }

    pub fn labels(&self) -> &[String] {
        &self.labels
    }

    pub fn links(&self) -> &[Link] {
        &self.links
    }
}

/// Builder for immutable atoms.
pub struct AtomBuilder {
    id: AtomId,
    world: WorldKey,
    worker: WorkerKey,
    kind: AtomKind,
    timestamp: Timestamp,
    importance: Importance,
    payload_json: String,
    vector: Option<Vec<f32>>,
    flags: Vec<String>,
    labels: Vec<String>,
    links: Vec<Link>,
}

impl AtomBuilder {
    pub fn vector(mut self, vector: Option<Vec<f32>>) -> Self {
        self.vector = vector;
        self
    }

    pub fn add_flag(mut self, flag: impl Into<String>) -> Self {
        self.flags.push(flag.into());
        self
    }

    pub fn add_label(mut self, label: impl Into<String>) -> Self {
        self.labels.push(label.into());
        self
    }

    pub fn add_link(mut self, link: AtomId) -> Self {
        self.links.push(Link::new(link, LinkKind::References));
        self
    }

    pub fn add_typed_link(mut self, target: AtomId, kind: LinkKind) -> Self {
        self.links.push(Link::new(target, kind));
        self
    }

    pub fn build(self) -> Atom {
        Atom {
            id: self.id,
            world: self.world,
            worker: self.worker,
            kind: self.kind,
            timestamp: self.timestamp,
            importance: self.importance,
            payload_json: self.payload_json,
            vector: self.vector,
            flags: self.flags,
            labels: self.labels,
            links: self.links,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_atom() -> Atom {
        Atom::builder(
            AtomId::new("a1"),
            WorldKey::new("world-1"),
            WorkerKey::new("worker-1"),
            AtomKind::Observation,
            Timestamp::new("2024-01-01T00:00:00Z"),
            Importance::new(0.5).expect("importance"),
            r#"{"foo":"bar"}"#,
        )
        .vector(Some(vec![1.0, 2.0, 3.0]))
        .add_flag("immutable")
        .add_label("test")
        .add_link(AtomId::new("previous"))
        .build()
    }

    #[test]
    fn importance_validation() {
        assert!(Importance::new(0.0).is_ok());
        assert!(Importance::new(1.0).is_ok());
        assert!(Importance::new(1.1).is_err());
        assert!(Importance::new(f32::NAN).is_err());
        assert_eq!(Importance::clamped(1.5).get(), 1.0);
        assert_eq!(Importance::clamped(-1.0).get(), 0.0);
        assert_eq!(Importance::clamped(f32::NAN).get(), 0.0);
    }

    #[test]
    fn atom_kind_serde_names_are_stable() {
        let kinds = [
            (AtomKind::Observation, "observation"),
            (AtomKind::Reflection, "reflection"),
            (AtomKind::Plan, "plan"),
            (AtomKind::Action, "action"),
            (AtomKind::Message, "message"),
        ];

        for (kind, expected) in kinds {
            let json = serde_json::to_string(&kind).unwrap();
            assert_eq!(json, format!("\"{expected}\""));
        }
    }

    #[test]
    fn atom_roundtrip_rmp() {
        let atom = sample_atom();
        let bytes = rmp_serde::to_vec(&atom).expect("serialize");
        let decoded: Atom = rmp_serde::from_slice(&bytes).expect("deserialize");
        assert_eq!(atom, decoded);
    }
}
