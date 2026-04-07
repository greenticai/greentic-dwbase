//! Host-side WIT bindings and adapter for DWBase.
//!
//! This crate currently provides type conversions aligned with the WIT definitions and a simple
//! adapter trait to connect a DWBaseEngine-like implementation to WIT-facing handlers. It also
//! includes a parser smoke test to ensure the WIT files stay in sync.

use dwbase_core::{
    Atom, AtomId, AtomKind, Importance, Link, LinkKind, Timestamp, WorkerKey, WorldKey,
};
use dwbase_engine::{Answer, AtomFilter, NewAtom, Question, Result};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum WitHostError {
    #[error("mapping error: {0}")]
    Mapping(String),
}

/// Trait to be implemented by host-side engine so WIT handlers can delegate calls.
pub trait EngineApi: Send + Sync {
    fn remember(&self, atom: NewAtom) -> Result<AtomId>;
    fn ask(&self, question: Question) -> Result<Answer>;
    fn observe(&self, atom: Atom) -> Result<()>;
    fn replay(&self, world: WorldKey, filter: AtomFilter) -> Result<Vec<Atom>>;
}

/// Adapter mapping between WIT-facing types and engine types.
pub struct WitHostAdapter<E: EngineApi> {
    engine: E,
}

impl<E: EngineApi> WitHostAdapter<E> {
    pub fn new(engine: E) -> Self {
        Self { engine }
    }

    pub fn remember(&self, atom: NewAtom) -> Result<AtomId> {
        self.engine.remember(atom)
    }

    pub fn ask(&self, question: Question) -> Result<Answer> {
        self.engine.ask(question)
    }

    pub fn observe(&self, atom: Atom) -> Result<()> {
        self.engine.observe(atom)
    }

    pub fn replay(&self, world: WorldKey, filter: AtomFilter) -> Result<Vec<Atom>> {
        self.engine.replay(world, filter)
    }
}

/// Helpers to construct core types from WIT-like payloads, useful for host-side glue code.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WitNewAtom {
    pub world_key: WorldKey,
    pub worker: WorkerKey,
    pub kind: AtomKind,
    pub timestamp: Timestamp,
    pub importance: f32,
    pub payload_json: String,
    pub vector: Option<Vec<f32>>,
    pub flags_list: Vec<String>,
    pub labels: Vec<String>,
    pub links: Vec<WitLink>,
}

/// WIT-shaped atom for host conversions.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WitAtom {
    pub id: AtomId,
    pub world_key: WorldKey,
    pub worker: WorkerKey,
    pub kind: AtomKind,
    pub timestamp: Timestamp,
    pub importance: f32,
    pub payload_json: String,
    pub vector: Option<Vec<f32>>,
    pub flags_list: Vec<String>,
    pub labels: Vec<String>,
    pub links: Vec<WitLink>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WitLink {
    pub target: AtomId,
    pub kind: LinkKind,
}

impl From<Atom> for WitAtom {
    fn from(value: Atom) -> Self {
        Self {
            id: value.id().clone(),
            world_key: value.world().clone(),
            worker: value.worker().clone(),
            kind: value.kind().clone(),
            timestamp: value.timestamp().clone(),
            importance: value.importance().get(),
            payload_json: value.payload_json().to_string(),
            vector: value.vector().map(|v| v.to_vec()),
            flags_list: value.flags().to_vec(),
            labels: value.labels().to_vec(),
            links: value
                .links()
                .iter()
                .map(|l| WitLink {
                    target: l.target.clone(),
                    kind: l.kind.clone(),
                })
                .collect(),
        }
    }
}

impl TryFrom<WitAtom> for Atom {
    type Error = WitHostError;

    fn try_from(value: WitAtom) -> std::result::Result<Self, Self::Error> {
        let mut builder = Atom::builder(
            value.id,
            value.world_key,
            value.worker,
            value.kind,
            value.timestamp,
            Importance::new(value.importance).map_err(|e| WitHostError::Mapping(e.to_string()))?,
            value.payload_json,
        );
        builder = builder.vector(value.vector);
        for f in value.flags_list {
            builder = builder.add_flag(f);
        }
        for l in value.labels {
            builder = builder.add_label(l);
        }
        for link in value.links {
            builder = builder.add_typed_link(link.target, link.kind);
        }
        Ok(builder.build())
    }
}

impl TryFrom<WitNewAtom> for NewAtom {
    type Error = WitHostError;

    fn try_from(value: WitNewAtom) -> std::result::Result<Self, Self::Error> {
        Ok(NewAtom {
            world: value.world_key,
            worker: value.worker,
            kind: value.kind,
            timestamp: value.timestamp,
            importance: Importance::new(value.importance)
                .map_err(|e| WitHostError::Mapping(e.to_string()))?,
            payload_json: value.payload_json,
            vector: value.vector,
            flags: value.flags_list,
            labels: value.labels,
            links: value
                .links
                .into_iter()
                .map(|l| Link::new(l.target, l.kind))
                .collect(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs, path::Path};
    use wit_parser::UnresolvedPackageGroup;

    struct DummyEngine;

    impl EngineApi for DummyEngine {
        fn remember(&self, atom: NewAtom) -> Result<AtomId> {
            let id = AtomId::new(format!("dummy-{}", atom.world.0));
            Ok(id)
        }
        fn ask(&self, _question: Question) -> Result<Answer> {
            Ok(Answer {
                world: WorldKey::new("w"),
                text: "ok".into(),
                supporting_atoms: vec![],
                warnings: vec![],
            })
        }
        fn observe(&self, _atom: Atom) -> Result<()> {
            Ok(())
        }
        fn replay(&self, _world: WorldKey, _filter: AtomFilter) -> Result<Vec<Atom>> {
            Ok(vec![])
        }
    }

    #[test]
    fn wit_files_parse() {
        let path = Path::new("../../wit/dwbase-core.wit");
        let contents = fs::read_to_string(path).unwrap();
        let group = UnresolvedPackageGroup::parse(path, &contents).unwrap();
        assert_eq!(group.main.name.name.as_str(), "core");
    }

    #[test]
    fn adapter_calls_engine() {
        let adapter = WitHostAdapter::new(DummyEngine);
        let new_atom = NewAtom {
            world: WorldKey::new("w"),
            worker: WorkerKey::new("wk"),
            kind: AtomKind::Observation,
            timestamp: Timestamp::new("2024-01-01T00:00:00Z"),
            importance: Importance::new(0.5).unwrap(),
            payload_json: "{}".into(),
            vector: None,
            flags: vec![],
            labels: vec![],
            links: vec![],
        };
        let id = adapter.remember(new_atom).unwrap();
        assert!(!id.0.is_empty());

        let answer = adapter
            .ask(Question {
                world: WorldKey::new("w"),
                text: "hello".into(),
                filter: None,
            })
            .unwrap();
        assert_eq!(answer.world.0, "w");
    }
}
