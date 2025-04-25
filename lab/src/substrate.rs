use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use sustainity_models::gather;

use crate::errors;

// Indentifies a substrate file.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DataSetId(usize);

impl DataSetId {
    #[must_use]
    pub fn new(id: usize) -> Self {
        Self(id)
    }
}

#[derive(Debug)]
pub struct Substrate {
    pub id: DataSetId,
    pub path: std::path::PathBuf,
    pub name: String,
    pub source: gather::Source,
}

pub struct Substrates {
    list: Vec<Substrate>,
}

impl Substrates {
    pub fn prepare(
        directory: &std::path::Path,
    ) -> Result<(Self, SubstratesReport), errors::ProcessingError> {
        let mut report = SubstratesReport::default();
        let mut list = Vec::new();

        for entry in std::fs::read_dir(directory)
            .map_err(|e| errors::ProcessingError::Io(e, directory.to_owned()))?
        {
            let entry = entry.map_err(|e| errors::ProcessingError::Io(e, directory.to_owned()))?;
            let path = entry.path();
            if path.is_file() {
                if let Some(stem) = path.file_stem() {
                    if let Some(stem) = stem.to_str() {
                        list.push(Substrate {
                            id: DataSetId::new(list.len()),
                            path: path.clone(),
                            name: stem.to_owned(),
                            source: gather::Source::from_string(stem),
                        });
                    } else {
                        report.add_path_not_unicode(path.clone());
                    }
                } else {
                    report.add_no_file_stem(path.clone());
                }
            }
        }

        Ok((Self { list }, report))
    }

    #[must_use]
    pub fn list(&self) -> &[Substrate] {
        &self.list
    }

    #[must_use]
    pub fn get_path_for_id(&self, data_set_id: DataSetId) -> Option<&std::path::Path> {
        for substrate in &self.list {
            if substrate.id == data_set_id {
                return Some(&substrate.path);
            }
        }
        None
    }

    #[must_use]
    pub fn get_name_for_id(&self, data_set_id: DataSetId) -> Option<&str> {
        for substrate in &self.list {
            if substrate.id == data_set_id {
                return Some(&substrate.name);
            }
        }
        None
    }

    #[must_use]
    pub fn get_id_for_name(&self, name: &str) -> Option<&DataSetId> {
        for substrate in &self.list {
            if substrate.name == name {
                return Some(&substrate.id);
            }
        }
        None
    }
}

// TODO: Rework as repotts per data source
#[must_use]
#[derive(Debug, Default)]
pub struct SubstratesReport {
    no_stem: BTreeSet<std::path::PathBuf>,
    not_unicode: BTreeSet<std::path::PathBuf>,
}

impl SubstratesReport {
    pub fn add_no_file_stem(&mut self, path: std::path::PathBuf) {
        self.no_stem.insert(path);
    }

    pub fn add_path_not_unicode(&mut self, path: std::path::PathBuf) {
        self.not_unicode.insert(path);
    }

    pub fn report(&self) {
        log::warn!("Substrates report:");

        if !self.no_stem.is_empty() {
            log::warn!(" no stem:");
            for path in &self.no_stem {
                log::warn!("  - {path:?}");
            }
        }
        if !self.not_unicode.is_empty() {
            log::warn!(" not unicode:");
            for path in &self.not_unicode {
                log::warn!("  - {path:?}");
            }
        }
        log::warn!("End of the report");
    }
}
