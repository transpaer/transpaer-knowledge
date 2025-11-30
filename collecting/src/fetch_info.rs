// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::errors::{IoOrSerdeError, MapIo, MapSerde};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct FetchData {
    access_time: String,
}

impl FetchData {
    fn now() -> Self {
        let access_time = chrono::Utc::now().to_rfc3339();
        Self { access_time }
    }
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct FetchInfo {
    bcorp: Option<FetchData>,
    eu_ecolabel: Option<FetchData>,
    open_food_facts: Option<FetchData>,
    open_food_repo: Option<FetchData>,
}

impl FetchInfo {
    /// Reads the fetch info from the passed file.
    ///
    /// # Errors
    ///
    /// Returns `Err` if fails to read from `path` or parse the contents
    pub fn read(path: &std::path::Path) -> Result<Self, IoOrSerdeError> {
        if path.exists() {
            let contents = std::fs::read_to_string(path).map_with_path(path)?;
            let parsed: Self = serde_yaml::from_str(&contents).map_with_path(path)?;
            Ok(parsed)
        } else {
            Ok(Self::default())
        }
    }

    /// Writes the fetch info to the passed file.
    ///
    /// # Errors
    ///
    /// Returns `Err` if fails to write to the passed file or serialize the contents.
    pub fn write(&self, path: &std::path::Path) -> Result<(), IoOrSerdeError> {
        let contents = serde_yaml::to_string(self).map_serde()?;
        std::fs::write(path, contents).map_with_path(path)?;
        Ok(())
    }

    pub fn update_bcorp(&mut self) {
        self.bcorp = Some(FetchData::now());
    }

    pub fn update_eu_ecolabel(&mut self) {
        self.eu_ecolabel = Some(FetchData::now());
    }

    pub fn update_open_food_facts(&mut self) {
        self.open_food_facts = Some(FetchData::now());
    }

    pub fn update_open_food_repo(&mut self) {
        self.open_food_repo = Some(FetchData::now());
    }
}
