//! Guest-side helpers for DWBase WIT interfaces.
//!
//! Currently provides a minimal wrapper around the WIT definitions plus a parser smoke test to
//! ensure the interface stays in sync.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClientConfig {
    pub world: String,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            world: "default".into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path};
    use wit_parser::UnresolvedPackageGroup;

    #[test]
    fn wit_files_parse() {
        let path = Path::new("../../wit/dwbase-types.wit");
        let contents = fs::read_to_string(path).unwrap();
        let group = UnresolvedPackageGroup::parse(path, &contents).unwrap();
        let mut names = Vec::new();
        names.push(group.main.name.name.as_str());
        for pkg in &group.nested {
            names.push(pkg.name.name.as_str());
        }
        assert!(names.contains(&"types"));
    }
}
