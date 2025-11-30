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
        pub product_name: String,
        pub abbreviated_product_name: String,
        pub generic_name: String,
        pub brands: String,
        pub brands_tags: String,
        pub categories: String,
        pub categories_tags: String,
        pub categories_en: String,
        pub manufacturing_places: String,
        pub manufacturing_places_tags: String,
        pub countries: String,
        pub countries_tags: String,
        pub countries_en: String,
        pub ingredients_text: String,
        pub ingredients_tags: String,
        pub ingredients_analysis_tags: String,
        pub food_groups: String,
        pub food_groups_tags: String,
        pub food_groups_en: String,
        pub brand_owner: String,
        pub ecoscore_score: String,
        pub ecoscore_grade: String,
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

    use super::data::Record;
    use crate::errors::{IoOrSerdeError, MapIo, MapSerde};

    /// Iterator over Open Food Facts CSV file records.
    /// XXX rm?
    pub struct Iter {
        path: std::path::PathBuf,
        reader: csv::DeserializeRecordsIntoIter<std::fs::File, Record>,
    }

    impl Iterator for Iter {
        type Item = Result<Record, IoOrSerdeError>;

        fn next(&mut self) -> Option<Self::Item> {
            self.reader.next().map(|e| e.map_with_path(&self.path))
        }
    }

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
            let mut csv_reader =
                csv::ReaderBuilder::new().delimiter(b'\t').from_reader(decoder_reader);

            let headers = csv_reader.headers().map_with_path(&self.path)?.clone();
            for record in csv_reader.into_records() {
                callback(headers.clone(), record.map_with_path(&self.path)?).await;
                result += 1;
            }

            Ok(result)
        }
    }
}
