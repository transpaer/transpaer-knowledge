use std::collections::HashMap;

use serde::{de::DeserializeOwned, Serialize};
use thiserror::Error;

use crate::store;

/// Errors related to key-value store.
#[derive(Error, Debug)]
pub enum BucketError {
    #[error("Failed to serde the entry: {0}")]
    Serde2(#[from] postcard::Error),

    #[error("KV operation failed: {0}")]
    Store(#[from] kv::Error),
}

pub struct Bucket<'a, K, V> {
    bucket: kv::Bucket<'a, Vec<u8>, Vec<u8>>,
    phantom: std::marker::PhantomData<(K, V)>,
}

impl<K, V> Bucket<'_, K, V> {
    pub fn obtain(store: &kv::Store, name: &str) -> Result<Self, BucketError> {
        let bucket = store.bucket::<Vec<u8>, Vec<u8>>(Some(name))?;
        Ok(Bucket { bucket, phantom: std::marker::PhantomData })
    }

    pub fn flush(&self) -> Result<(), BucketError> {
        self.bucket.flush()?;
        Ok(())
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.bucket.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.bucket.is_empty()
    }

    pub fn get(&self, key: &K) -> Result<Option<V>, BucketError>
    where
        K: Serialize,
        V: DeserializeOwned,
    {
        let key_data = postcard::to_stdvec(key)?;
        let value_data = self.bucket.get(&key_data)?;
        Ok(if let Some(value_data) = value_data {
            Some(postcard::from_bytes(&value_data)?)
        } else {
            None
        })
    }

    pub fn remove(&self, key: &K) -> Result<Option<V>, BucketError>
    where
        K: Serialize,
        V: DeserializeOwned,
    {
        let key_data = postcard::to_stdvec(key)?;
        let value_data = self.bucket.remove(&key_data)?;
        Ok(if let Some(value_data) = value_data {
            Some(postcard::from_bytes(&value_data)?)
        } else {
            None
        })
    }

    pub fn insert(&self, key: &K, value: &V) -> Result<(), BucketError>
    where
        K: Serialize,
        V: Serialize,
    {
        let key_data = postcard::to_stdvec(key)?;
        let value_data = postcard::to_stdvec(value)?;
        self.bucket.set(&key_data, &value_data)?;
        Ok(())
    }

    pub fn gather(&self) -> Result<HashMap<K, V>, BucketError>
    where
        K: DeserializeOwned + Eq + std::hash::Hash,
        V: DeserializeOwned,
    {
        let mut result = HashMap::new();
        for item in self.bucket.iter() {
            let item = item?;
            let key = postcard::from_bytes(&item.key::<Vec<u8>>()?)?;
            let value = postcard::from_bytes(&item.value::<Vec<u8>>()?)?;
            result.insert(key, value);
        }
        Ok(result)
    }
}

#[derive(Debug, Clone)]
pub struct DbStore {
    store: kv::Store,
}

impl DbStore {
    pub fn new(path: &std::path::Path) -> Result<Self, BucketError> {
        Ok(Self { store: kv::Store::new(kv::Config::new(path))? })
    }

    pub fn get_organisation_bucket(
        &self,
    ) -> Result<Bucket<store::OrganisationId, store::Organisation>, BucketError> {
        Bucket::obtain(&self.store, "organisation.id => organisation")
    }

    pub fn get_keyword_to_organisation_ids_bucket(
        &self,
    ) -> Result<Bucket<String, Vec<store::OrganisationId>>, BucketError> {
        Bucket::obtain(&self.store, "keyword => [organisation.id]")
    }

    pub fn get_vat_id_to_organisation_id_bucket(
        &self,
    ) -> Result<Bucket<store::VatId, store::OrganisationId>, BucketError> {
        Bucket::obtain(&self.store, "organisation.vat_id => organisation.id")
    }

    pub fn get_wiki_id_to_organisation_id_bucket(
        &self,
    ) -> Result<Bucket<store::WikiId, store::OrganisationId>, BucketError> {
        Bucket::obtain(&self.store, "organisation.wiki_id => organisation.id")
    }

    pub fn get_www_domain_to_organisation_id_bucket(
        &self,
    ) -> Result<Bucket<store::Domain, store::OrganisationId>, BucketError> {
        Bucket::obtain(&self.store, "organisation.www_domain => organisation.id")
    }

    pub fn get_categories_bucket(
        &self,
    ) -> Result<Bucket<String, Vec<store::ProductId>>, BucketError> {
        Bucket::obtain(&self.store, "product.category => [product.id]")
    }

    pub fn get_product_bucket(
        &self,
    ) -> Result<Bucket<store::ProductId, store::Product>, BucketError> {
        Bucket::obtain(&self.store, "product.id => product")
    }

    pub fn get_keyword_to_product_ids_bucket(
        &self,
    ) -> Result<Bucket<String, Vec<store::ProductId>>, BucketError> {
        Bucket::obtain(&self.store, "keyword => [product.id]")
    }

    pub fn get_ean_to_product_id_bucket(
        &self,
    ) -> Result<Bucket<store::Ean, store::ProductId>, BucketError> {
        Bucket::obtain(&self.store, "product.ean => product.id")
    }

    pub fn get_gtin_to_product_id_bucket(
        &self,
    ) -> Result<Bucket<store::Gtin, store::ProductId>, BucketError> {
        Bucket::obtain(&self.store, "product.gtin => product.id")
    }

    pub fn get_wiki_id_to_product_id_bucket(
        &self,
    ) -> Result<Bucket<store::WikiId, store::ProductId>, BucketError> {
        Bucket::obtain(&self.store, "product.wiki_id => product.id")
    }
}

#[derive(Debug, Clone)]
pub struct AppStore {
    store: kv::Store,
}

impl AppStore {
    pub fn new(path: &std::path::Path) -> Result<Self, BucketError> {
        Ok(Self { store: kv::Store::new(kv::Config::new(path))? })
    }

    pub fn get_library_bucket(
        &self,
    ) -> Result<Bucket<store::LibraryTopic, store::LibraryItem>, BucketError> {
        Bucket::obtain(&self.store, "library.topic => library.item")
    }

    pub fn get_presentation_bucket(
        &self,
    ) -> Result<Bucket<store::LibraryTopic, store::Presentation>, BucketError> {
        Bucket::obtain(&self.store, "library.topic => library.presentation")
    }
}
