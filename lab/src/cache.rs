//! Contains code ralated to parsing and saving cache data.

use serde::{Deserialize, Serialize};

use sustainity_collecting::errors::{IoOrSerdeError, MapSerde};

/// Cached data from search over Wikidata data.
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Wikidata {
    /// Manufacturer IDs.
    #[serde(deserialize_with = "sustainity_wikidata::data::deserialize_vec_id_from_vec_string")]
    pub manufacturer_ids: Vec<sustainity_wikidata::data::Id>,

    /// All entry classes.
    #[serde(deserialize_with = "sustainity_wikidata::data::deserialize_vec_id_from_vec_string")]
    pub classes: Vec<sustainity_wikidata::data::Id>,
}

/// Reads in the cache data.
///
/// # Errors
///
/// Returns `Err` if fails to read from `path` or parse the contents.
pub fn load(path: &std::path::Path) -> Result<Wikidata, IoOrSerdeError> {
    if path.exists() {
        let contents = std::fs::read_to_string(path)?;
        let cache = serde_json::from_str(&contents).map_with_path(path)?;
        Ok(cache)
    } else {
        Ok(Wikidata::default())
    }
}
