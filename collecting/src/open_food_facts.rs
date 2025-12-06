// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/// Data structures for parsing Open Food Facts data.
pub mod data {
    use std::collections::HashSet;

    use serde::{Deserialize, Serialize};

    /// Record in Open Food Facts data.
    #[derive(Serialize, Deserialize, Clone, Debug)]
    pub struct Record {
        pub code: String,
        pub url: String,
        pub creator: String,
        pub created_t: String,
        pub created_datetime: String,
        pub last_modified_t: String,
        pub last_modified_datetime: String,
        pub last_modified_by: String,
        pub last_updated_t: String,
        pub last_updated_datetime: String,
        pub product_name: String,
        pub abbreviated_product_name: String,
        pub generic_name: String,
        pub quantity: String,
        pub packaging: String,
        pub packaging_tags: String,
        pub packaging_en: String,
        pub packaging_text: String,
        pub brands: String,
        pub brands_tags: String,
        pub brands_en: String,
        pub categories: String,
        pub categories_tags: String,
        pub categories_en: String,
        pub origins: String,
        pub origins_tags: String,
        pub origins_en: String,
        pub manufacturing_places: String,
        pub manufacturing_places_tags: String,
        pub labels: String,
        pub labels_tags: String,
        pub labels_en: String,
        pub emb_codes: String,
        pub emb_codes_tags: String,
        pub first_packaging_code_geo: String,
        pub cities: String,
        pub cities_tags: String,
        pub purchase_places: String,
        pub stores: String,
        pub countries: String,
        pub countries_tags: String,
        pub countries_en: String,
        pub ingredients_text: String,
        pub ingredients_tags: String,
        pub ingredients_analysis_tags: String,
        pub allergens: String,
        pub allergens_en: String,
        pub traces: String,
        pub traces_tags: String,
        pub traces_en: String,
        pub serving_size: String,
        pub serving_quantity: String,
        pub no_nutrition_data: String,
        pub additives_n: String,
        pub additives: String,
        pub additives_tags: String,
        pub additives_en: String,
        pub nutriscore_score: String,
        pub nutriscore_grade: String,
        pub nova_group: String,
        pub pnns_groups_1: String,
        pub pnns_groups_2: String,
        pub food_groups: String,
        pub food_groups_tags: String,
        pub food_groups_en: String,
        pub states: String,
        pub states_tags: String,
        pub states_en: String,
        pub brand_owner: String,
        pub environmental_score_score: String,
        pub environmental_score_grade: String,
        pub nutrient_levels_tags: String,
        pub product_quantity: String,
        pub owner: String,
        pub data_quality_errors_tags: String,
        pub unique_scans_n: String,
        pub popularity_tags: String,
        pub completeness: String,
        pub last_image_t: String,
        pub last_image_datetime: String,
        pub main_category: String,
        pub main_category_en: String,
        pub image_url: String,
        pub image_small_url: String,
        pub image_ingredients_url: String,
        pub image_ingredients_small_url: String,
        pub image_nutrition_url: String,
        pub image_nutrition_small_url: String,
    }

    impl Record {
        /// Extracts brand owner names and brand names.
        #[must_use]
        pub fn extract_brand_labels(&self) -> Vec<String> {
            let mut labels = HashSet::<String>::new();
            if !self.brand_owner.is_empty() {
                labels.insert(self.brand_owner.clone());
            }
            for brand in self.brands.split(',') {
                if !brand.is_empty() {
                    labels.insert(brand.trim().to_owned());
                }
            }
            labels.into_iter().collect()
        }

        /// Extracts productnio country tags.
        #[must_use]
        pub fn extract_production_countries(&self) -> Vec<String> {
            if self.manufacturing_places_tags.is_empty() {
                Vec::new()
            } else {
                self.manufacturing_places_tags.split(',').map(String::from).collect()
            }
        }

        /// Extracts sell country tags.
        #[must_use]
        pub fn extract_sell_countries(&self) -> Vec<String> {
            if self.countries_tags.is_empty() {
                Vec::new()
            } else {
                self.countries_tags.split(',').map(String::from).collect()
            }
        }

        /// Extracts category tags.
        #[must_use]
        pub fn extract_categories(&self) -> Vec<String> {
            if self.categories_tags.is_empty() {
                Vec::new()
            } else {
                self.categories_tags.split(',').map(String::from).collect()
            }
        }
    }
}

/// Loader for loading Open Food Facts data.
pub mod loader {
    use std::future::Future;

    use crate::errors::{IoOrSerdeError, MapIo, MapSerde};

    /// Compression method used in the source file..
    #[derive(Clone, Debug)]
    enum CompressionMethod {
        /// `csv` file.
        None,

        /// `json.gz` file.
        Gz,
    }

    #[derive(Debug)]
    pub struct Loader {
        /// Compression method to use.
        compression_method: CompressionMethod,

        /// Path to the loaded file. Needed only for error reporting.
        path: std::path::PathBuf,
    }

    impl Loader {
        /// Constructs a new `Loader`.
        ///
        /// # Errors
        ///
        /// Returns `Err` if fails to read from `path`.
        pub fn load(path: &std::path::Path) -> Result<Self, IoOrSerdeError> {
            let compression_method = match path.extension().and_then(std::ffi::OsStr::to_str) {
                Some("csv") => CompressionMethod::None,
                Some("gz") => CompressionMethod::Gz,
                method => {
                    return Err(IoOrSerdeError::CompressionMethod(
                        method.map(std::string::ToString::to_string),
                    ))
                }
            };
            let path = path.to_owned();
            Ok(Self { compression_method, path })
        }

        /// Performs the loading of the data.
        ///
        /// # Errors
        ///
        /// Returns `Err` if fails to read from file or parse the read data.
        pub async fn run<C, F>(mut self, callback: C) -> Result<usize, IoOrSerdeError>
        where
            C: Fn(csv::StringRecord, csv::StringRecord) -> F,
            F: Future<Output = ()>,
        {
            match self.compression_method {
                CompressionMethod::Gz => self.run_gz(callback).await,
                CompressionMethod::None => self.run_none(callback).await,
            }
        }

        async fn run_none<C, F>(&mut self, callback: C) -> Result<usize, IoOrSerdeError>
        where
            C: Fn(csv::StringRecord, csv::StringRecord) -> F,
            F: std::future::Future<Output = ()>,
        {
            let mut result: usize = 0;
            let mut reader = csv::ReaderBuilder::new()
                .delimiter(b'\t')
                .quoting(false)
                .from_path(&self.path)
                .map_with_path(&self.path)?;
            let headers = reader.headers().map_with_path(&self.path)?.clone();
            for record in reader.into_records() {
                callback(headers.clone(), record.map_with_path(&self.path)?).await;
                result += 1;
            }
            Ok(result)
        }

        async fn run_gz<C, F>(&mut self, callback: C) -> Result<usize, IoOrSerdeError>
        where
            C: Fn(csv::StringRecord, csv::StringRecord) -> F,
            F: std::future::Future<Output = ()>,
        {
            let mut result: usize = 0;

            let file = std::fs::File::open(&self.path).map_with_path(&self.path)?;
            let mut file_reader = std::io::BufReader::new(file);
            let decoder = flate2::bufread::GzDecoder::new(&mut file_reader);
            let decoder_reader = std::io::BufReader::new(decoder);
            let mut csv_reader = csv::ReaderBuilder::new()
                .delimiter(b'\t')
                .quoting(false)
                .from_reader(decoder_reader);

            let headers = csv_reader.headers().map_with_path(&self.path)?.clone();
            for record in csv_reader.into_records() {
                match record {
                    Ok(record) => {
                        callback(headers.clone(), record).await;
                        result += 1;
                    }
                    Err(err) => {
                        log::error!("Open Food Facts error: {err:?}");
                    }
                }
            }

            Ok(result)
        }
    }
}
