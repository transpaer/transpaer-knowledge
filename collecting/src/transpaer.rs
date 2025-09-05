// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/// Data structures for parsing transpaer data.
pub mod data {
    use serde::{Deserialize, Serialize};

    /// Enumerates all library topics.
    #[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
    pub enum LibraryTopic {
        #[serde(rename = "wiki")]
        Wiki,

        #[serde(rename = "open_food_facts")]
        OpenFoodFacts,

        #[serde(rename = "bcorp")]
        Bcorp,

        #[serde(rename = "eu_ecolabel")]
        EuEcolabel,

        #[serde(rename = "tco")]
        Tco,

        #[serde(rename = "fti")]
        Fti,
    }

    impl LibraryTopic {
        #[must_use]
        #[allow(clippy::missing_panics_doc)]
        pub fn to_str(&self) -> &'static str {
            #[allow(clippy::expect_used)]
            serde_variant::to_variant_name(&self)
                .expect("Converting enum to string should always succeed")
        }
    }

    #[derive(Serialize, Deserialize, Clone, Debug)]
    pub struct Link {
        #[serde(rename = "title")]
        pub title: String,

        #[serde(rename = "link")]
        pub link: String,
    }

    /// Transpaer topic entry.
    #[derive(Serialize, Deserialize, Clone, Debug)]
    pub struct LibraryInfo {
        #[serde(rename = "id")]
        pub id: LibraryTopic,

        #[serde(rename = "title")]
        pub title: String,

        #[serde(rename = "summary")]
        pub summary: String,

        #[serde(rename = "links")]
        pub links: Option<Vec<Link>>,
    }

    /// Mapping connecting company or product name to curresponding Wikidata ID.
    ///
    /// This is an accepted match with high accuracy..
    #[derive(Clone, Debug)]
    pub struct Match {
        /// Wikidata ID.
        pub wiki_id: transpaer_wikidata::data::Id,

        /// Match accuracy.
        pub match_accuracy: f64,
    }

    /// Mapping connecting company or product name to curresponding Wikidata ID.
    ///
    /// This is a tentative match and potentially contains multiple candidates.
    #[derive(Serialize, Deserialize, Clone, Debug)]
    pub struct NameMatching {
        /// Company or product name.
        #[serde(rename = "name")]
        pub name: String,

        /// Wikidata IDs.
        #[serde(
            rename = "ids",
            deserialize_with = "transpaer_wikidata::data::deserialize_vec_id_from_vec_string"
        )]
        pub ids: Vec<transpaer_wikidata::data::Id>,

        /// Measure of certainty that the matched IDs really belong to the correct entry.
        #[serde(rename = "similarity")]
        pub similarity: f64,
    }

    impl NameMatching {
        /// Check if match has high enough similarity.
        #[must_use]
        pub fn matched(&self) -> Option<Match> {
            if self.similarity > 0.85 && self.ids.len() == 1 {
                self.ids.first().map(|id| Match { wiki_id: *id, match_accuracy: self.similarity })
            } else {
                None
            }
        }
    }

    #[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
    pub enum Regions {
        #[serde(rename = "all")]
        World,

        #[serde(rename = "unknown")]
        Unknown,

        #[serde(rename = "list")]
        List(Vec<String>),
    }

    #[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
    pub struct CountryEntry {
        pub tag: String,

        #[serde(skip_serializing_if = "Option::is_none", default)]
        pub description: Option<String>,

        #[serde(skip_serializing_if = "Option::is_none", default)]
        pub regions: Option<Regions>,

        pub count: usize,
    }

    #[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default)]
    pub struct Countries {
        pub countries: Vec<CountryEntry>,
    }

    #[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
    pub struct CategoryEntry {
        pub tag: String,

        #[serde(skip_serializing_if = "Option::is_none", default)]
        pub description: Option<String>,

        #[serde(skip_serializing_if = "Option::is_none", default)]
        pub categories: Option<Vec<crate::categories::Category>>,

        pub count: usize,

        #[serde(skip_serializing_if = "Option::is_none", default)]
        pub delete: Option<bool>,
    }

    #[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default)]
    pub struct Categories {
        pub categories: Vec<CategoryEntry>,
    }
}

/// Readers for loading transpaer data.
pub mod reader {
    use std::collections::HashMap;

    use super::data::{Categories, Countries, LibraryInfo, NameMatching, Regions};
    use crate::errors::{IoOrSerdeError, MapIo, MapSerde};

    /// Loads the transpaer library data from a file.
    ///
    /// # Errors
    ///
    /// Returns `Err` if fails to read from `path` or parse the contents.
    pub fn parse_library(path: &std::path::Path) -> Result<Vec<LibraryInfo>, IoOrSerdeError> {
        let contents = std::fs::read_to_string(path).map_with_path(path)?;
        let parsed: Vec<LibraryInfo> = serde_yaml::from_str(&contents).map_with_path(path)?;
        Ok(parsed)
    }

    /// Loads a mapping from company or product name to corresponding Wikidata ID..
    ///
    /// # Errors
    ///
    /// Returns `Err` if fails to read from `path` or parse the contents.
    pub fn parse_id_map(path: &std::path::Path) -> Result<Vec<NameMatching>, IoOrSerdeError> {
        let contents = std::fs::read_to_string(path).map_with_path(path)?;
        let parsed: Vec<NameMatching> = serde_yaml::from_str(&contents).map_with_path(path)?;
        Ok(parsed)
    }

    /// Loads the file with mapping from country tags to Transpaer regions.
    ///
    /// # Errors
    ///
    /// Returns `Err` if fails to read from `path` or parse the contents.
    pub fn parse_countries(path: &std::path::Path) -> Result<Countries, IoOrSerdeError> {
        let contents = std::fs::read_to_string(path).map_with_path(path)?;
        let parsed: Countries = serde_yaml::from_str(&contents).map_with_path(path)?;
        Ok(parsed)
    }

    /// Loads the file with mapping from source categories to to Transpaer categories.
    ///
    /// # Errors
    ///
    /// Returns `Err` if fails to read from `path` or parse the contents.
    pub fn parse_categories(path: &std::path::Path) -> Result<Categories, IoOrSerdeError> {
        let contents = std::fs::read_to_string(path).map_with_path(path)?;
        let parsed: Categories = serde_yaml::from_str(&contents).map_with_path(path)?;
        Ok(parsed)
    }

    pub struct RegionMapEntry {
        regions: Option<Regions>,
    }

    pub struct RegionMap {
        map: HashMap<String, RegionMapEntry>,
    }

    impl RegionMap {
        #[must_use]
        pub fn from_countries(countries: Countries) -> Self {
            let mut map = HashMap::new();
            for country in countries.countries {
                map.insert(country.tag, RegionMapEntry { regions: country.regions });
            }
            Self { map }
        }

        #[must_use]
        pub fn contains_tag(&self, tag: &str) -> bool {
            self.map.contains_key(tag)
        }

        #[must_use]
        pub fn get_regions(&self, tag: &str) -> Option<&Regions> {
            self.map.get(tag).and_then(|value| value.regions.as_ref())
        }
    }

    pub struct CategoryMapEntry {
        pub description: Option<String>,
        pub categories: Option<Vec<crate::categories::Category>>,
        pub delete: Option<bool>,
    }

    pub struct CategoryMap {
        map: HashMap<String, CategoryMapEntry>,
    }

    impl CategoryMap {
        #[must_use]
        pub fn from_categories(categories: Categories) -> Self {
            let mut map = HashMap::new();
            for entry in categories.categories {
                map.insert(
                    entry.tag,
                    CategoryMapEntry {
                        description: entry.description,
                        categories: entry.categories,
                        delete: entry.delete,
                    },
                );
            }
            Self { map }
        }

        #[must_use]
        pub fn contains_tag(&self, tag: &str) -> bool {
            self.map.contains_key(tag)
        }

        #[must_use]
        pub fn get(&self, tag: &str) -> Option<&CategoryMapEntry> {
            self.map.get(tag)
        }
    }
}

/// Writers for saving transpaer data.
pub mod writer {
    use super::data::{Categories, Countries};
    use crate::errors::{IoOrSerdeError, MapIo, MapSerde};

    /// Saves the given countries to the given path.
    ///
    /// # Errors
    ///
    /// Returns `Err` if fails to write to the `path` or serialize the contents.
    pub fn save_countries(
        countries: &Countries,
        path: &std::path::Path,
    ) -> Result<(), IoOrSerdeError> {
        let contents = serde_yaml::to_string(countries).map_serde()?;
        std::fs::write(path, contents).map_with_path(path)?;
        Ok(())
    }

    /// Saves the given categories to the given path.
    ///
    /// # Errors
    ///
    /// Returns `Err` if fails to write to the `path` or serialize the contents.
    pub fn save_categories(
        categories: &Categories,
        path: &std::path::Path,
    ) -> Result<(), IoOrSerdeError> {
        let contents = serde_yaml::to_string(categories).map_serde()?;
        std::fs::write(path, contents).map_with_path(path)?;
        Ok(())
    }
}
