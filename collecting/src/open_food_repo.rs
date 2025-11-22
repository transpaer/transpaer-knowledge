// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/// Data structures for parsing Open Food Repo data.
pub mod data {
    use std::collections::HashMap;

    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct Image {
        pub categories: Vec<String>,
        pub large: String,
        pub medium: String,
        pub thumb: String,
        pub xlarge: String,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct Nutrient {
        pub name_translations: HashMap<String, String>,
        pub per_hundred: Option<f64>,
        pub per_portion: Option<f64>,
        pub unit: String,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct Nutrients {
        pub carbohydrates: Option<Nutrient>,
        pub energy: Option<Nutrient>,
        pub energy_calories_kcal: Option<Nutrient>,
        pub fat: Option<Nutrient>,
        pub fiber: Option<Nutrient>,
        pub protein: Option<Nutrient>,
        pub saturated_fat: Option<Nutrient>,
        pub sodium: Option<Nutrient>,
        pub sugars: Option<Nutrient>,
    }

    /// Entry in a Open Food Repo data.
    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct Entry {
        pub id: usize,
        pub barcode: String,
        pub country: String,
        pub created_at: String,
        pub updated_at: String,
        pub images: Vec<Image>,
        pub name_translations: HashMap<String, String>,
        pub display_name_translations: HashMap<String, String>,
        pub ingredients_translations: HashMap<String, String>,
        pub nutrients: Nutrients,
        pub alcohol_by_volume: f64,
        pub quantity: f64,
        pub portion_quantity: f64,
        pub unit: String,
        pub portion_unit: String,
        pub hundred_unit: String,
        pub status: String,
    }
}

/// Reader to loading `BCorp` data.
pub mod reader {
    use super::data::Entry;
    use crate::errors::{IoOrSerdeError, MapIo};

    /// Loads the EU Ecolabel data from a file asynchroneusly.
    ///
    /// # Errors
    ///
    /// Returns `Err` if fails to read from `path` or parse the contents.
    pub async fn load<C, F>(path: &std::path::Path, callback: C) -> Result<usize, IoOrSerdeError>
    where
        C: Fn(Entry) -> F,
        F: std::future::Future<Output = ()>,
    {
        let mut result: usize = 0;
        for entry in serde_jsonlines::json_lines::<Entry, _>(path).map_with_path(path)? {
            let entry =
                entry.map_err(|e| IoOrSerdeError::ReadJsonLines(e, path.into(), result + 1))?;
            callback(entry).await;
            result += 1;
        }
        Ok(result)
    }
}
