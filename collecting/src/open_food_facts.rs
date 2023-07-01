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
        pub fn extract_labels(&self) -> Vec<String> {
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
    }
}

/// Reader for loading Open Food Facts data.
pub mod reader {
    use super::data::Record;
    use crate::errors::IoOrSerdeError;

    /// Iterator over Open Food Facts CSV file records.
    pub struct Iter {
        reader: csv::DeserializeRecordsIntoIter<std::fs::File, Record>,
    }

    impl Iterator for Iter {
        type Item = Result<Record, IoOrSerdeError>;

        fn next(&mut self) -> Option<Self::Item> {
            self.reader.next().map(|e| e.map_err(Into::into))
        }
    }

    /// Loads the Open Food Facts data from a file synchroneusly.
    ///
    /// # Errors
    ///
    /// Returns `Err` if fails to read from `path` or parse the contents.
    pub fn parse<P: AsRef<std::path::Path>>(path: P) -> Result<Iter, IoOrSerdeError> {
        Ok(Iter {
            reader: csv::ReaderBuilder::new().delimiter(b'\t').from_path(path)?.into_deserialize(),
        })
    }

    /// Loads the Open Food Facts data from a file asynchroneusly.
    ///
    /// # Errors
    ///
    /// Returns `Err` if fails to read from `path` or parse the contents.
    pub async fn load<C, F>(path: std::path::PathBuf, callback: C) -> Result<usize, IoOrSerdeError>
    where
        C: Fn(csv::StringRecord, csv::StringRecord) -> F,
        F: std::future::Future<Output = ()>,
    {
        let mut result: usize = 0;
        let mut reader = csv::ReaderBuilder::new().delimiter(b'\t').from_path(path)?;
        let headers = reader.headers()?.clone();
        for record in reader.into_records() {
            callback(headers.clone(), record?).await;
            result += 1;
        }
        Ok(result)
    }
}
