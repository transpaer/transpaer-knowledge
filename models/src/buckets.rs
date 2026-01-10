// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::HashMap;

use serde::{Serialize, de::DeserializeOwned};
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

#[derive(Clone)]
pub struct Bucket<'a, K, V> {
    bucket: kv::Bucket<'a, Vec<u8>, Vec<u8>>,
    phantom: std::marker::PhantomData<(K, V)>,
}

impl<'a, K, V> Bucket<'a, K, V> {
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

    pub fn edit(&self, key: K) -> Result<Option<BucketEntry<'a, K, V>>, BucketError>
    where
        K: Clone + Serialize,
        V: Clone + Serialize + DeserializeOwned,
    {
        let key_data = postcard::to_stdvec(&key)?;
        let value_data = self.bucket.get(&key_data)?;
        Ok(if let Some(value_data) = value_data {
            Some(BucketEntry {
                key,
                value: postcard::from_bytes(&value_data)?,
                key_data,
                bucket: self.clone(),
            })
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

    pub fn iter(&self) -> BucketIter<K, V>
    where
        K: Serialize,
        V: Serialize,
    {
        BucketIter { iter: self.bucket.iter(), phantom: std::marker::PhantomData }
    }

    pub fn iter_autosave(self) -> BucketIterAutosave<'a, K, V>
    where
        K: Clone + Serialize,
        V: Clone + Serialize,
    {
        BucketIterAutosave { iter: self.bucket.iter(), bucket: self }
    }
}

pub struct BucketIter<K, V> {
    iter: kv::Iter<Vec<u8>, Vec<u8>>,
    phantom: std::marker::PhantomData<(K, V)>,
}

impl<K, V> BucketIter<K, V>
where
    K: Serialize + DeserializeOwned + Eq + std::hash::Hash,
    V: Serialize + DeserializeOwned,
{
    fn go(&mut self) -> Result<Option<(K, V)>, BucketError> {
        Ok(if let Some(item) = self.iter.next().transpose()? {
            let key = postcard::from_bytes(&item.key::<Vec<u8>>()?)?;
            let value = postcard::from_bytes(&item.value::<Vec<u8>>()?)?;
            Some((key, value))
        } else {
            None
        })
    }
}

impl<K, V> Iterator for BucketIter<K, V>
where
    K: Serialize + DeserializeOwned + Eq + std::hash::Hash,
    V: Serialize + DeserializeOwned,
{
    type Item = Result<(K, V), BucketError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.go().transpose()
    }
}

pub struct BucketIterAutosave<'a, K, V>
where
    K: Clone,
    V: Clone,
{
    iter: kv::Iter<Vec<u8>, Vec<u8>>,
    bucket: Bucket<'a, K, V>,
}

impl<'a, K, V> BucketIterAutosave<'a, K, V>
where
    K: Clone + Serialize + DeserializeOwned + Eq + std::hash::Hash,
    V: Clone + Serialize + DeserializeOwned,
{
    fn go(&mut self) -> Result<Option<BucketEntry<'a, K, V>>, BucketError> {
        Ok(if let Some(item) = self.iter.next().transpose()? {
            let key_data = item.key::<Vec<u8>>()?;
            let key = postcard::from_bytes(&key_data)?;
            let value = postcard::from_bytes(&item.value::<Vec<u8>>()?)?;
            Some(BucketEntry { key, value, key_data, bucket: self.bucket.clone() })
        } else {
            None
        })
    }
}

impl<'a, K, V> Iterator for BucketIterAutosave<'a, K, V>
where
    K: Clone + Serialize + DeserializeOwned + Eq + std::hash::Hash,
    V: Clone + Serialize + DeserializeOwned,
{
    type Item = Result<BucketEntry<'a, K, V>, BucketError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.go().transpose()
    }
}

pub struct BucketEntry<'a, K, V>
where
    K: Clone + Serialize,
    V: Clone + Serialize,
{
    pub key: K,
    pub value: V,
    key_data: Vec<u8>,
    bucket: Bucket<'a, K, V>,
}

impl<K, V> BucketEntry<'_, K, V>
where
    K: Clone + Serialize,
    V: Clone + Serialize,
{
    pub fn store(&mut self) -> Result<(), BucketError> {
        let value_data = postcard::to_stdvec(&self.value)?;
        self.bucket.bucket.set(&self.key_data, &value_data)?;
        Ok(())
    }

    pub fn consume(mut self) -> Result<(), BucketError> {
        self.store()
    }
}

impl<K, V> Drop for BucketEntry<'_, K, V>
where
    K: Clone + Serialize,
    V: Clone + Serialize,
{
    fn drop(&mut self) {
        self.store().expect("Failed to automatically save a bucket entry");
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
    ) -> Result<Bucket<'_, store::OrganisationId, store::Organisation>, BucketError> {
        Bucket::obtain(&self.store, "organisation.id => organisation")
    }

    pub fn get_keyword_to_organisation_ids_bucket(
        &self,
    ) -> Result<Bucket<'_, String, Vec<store::OrganisationId>>, BucketError> {
        Bucket::obtain(&self.store, "keyword => [organisation.id]")
    }

    pub fn get_vat_id_to_organisation_id_bucket(
        &self,
    ) -> Result<Bucket<'_, store::VatId, store::OrganisationId>, BucketError> {
        Bucket::obtain(&self.store, "organisation.vat_id => organisation.id")
    }

    pub fn get_wiki_id_to_organisation_id_bucket(
        &self,
    ) -> Result<Bucket<'_, store::WikiId, store::OrganisationId>, BucketError> {
        Bucket::obtain(&self.store, "organisation.wiki_id => organisation.id")
    }

    pub fn get_www_domain_to_organisation_id_bucket(
        &self,
    ) -> Result<Bucket<'_, store::Domain, store::OrganisationId>, BucketError> {
        Bucket::obtain(&self.store, "organisation.www_domain => organisation.id")
    }

    pub fn get_categories_bucket(
        &self,
    ) -> Result<Bucket<'_, String, store::Category>, BucketError> {
        Bucket::obtain(&self.store, "product.category => [product.id]")
    }

    pub fn get_product_bucket(
        &self,
    ) -> Result<Bucket<'_, store::ProductId, store::Product>, BucketError> {
        Bucket::obtain(&self.store, "product.id => product")
    }

    pub fn get_keyword_to_product_ids_bucket(
        &self,
    ) -> Result<Bucket<'_, String, Vec<store::ProductId>>, BucketError> {
        Bucket::obtain(&self.store, "keyword => [product.id]")
    }

    pub fn get_ean_to_product_id_bucket(
        &self,
    ) -> Result<Bucket<'_, store::Ean, store::ProductId>, BucketError> {
        Bucket::obtain(&self.store, "product.ean => product.id")
    }

    pub fn get_gtin_to_product_id_bucket(
        &self,
    ) -> Result<Bucket<'_, store::Gtin, store::ProductId>, BucketError> {
        Bucket::obtain(&self.store, "product.gtin => product.id")
    }

    pub fn get_wiki_id_to_product_id_bucket(
        &self,
    ) -> Result<Bucket<'_, store::WikiId, store::ProductId>, BucketError> {
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
    ) -> Result<Bucket<'_, store::LibraryTopic, store::LibraryItem>, BucketError> {
        Bucket::obtain(&self.store, "library.topic => library.item")
    }

    pub fn get_presentation_bucket(
        &self,
    ) -> Result<Bucket<'_, store::LibraryTopic, store::Presentation>, BucketError> {
        Bucket::obtain(&self.store, "library.topic => library.presentation")
    }
}

#[cfg(test)]
mod tests {
    use super::Bucket;

    #[derive(Debug, Clone)]
    pub struct TestStore {
        store: kv::Store,
    }

    impl TestStore {
        pub fn new() -> Self {
            Self { store: kv::Store::new(kv::Config::new(tempfile::tempdir().unwrap())).unwrap() }
        }

        pub fn get_test_bucket<'a>(&'a self) -> Bucket<'a, u32, String> {
            Bucket::obtain(&self.store, "test").unwrap()
        }
    }

    /// Check if bucket clones edit the same entries.
    ///
    /// This should be possible per guaranties of the `kv` crate.
    #[test]
    fn bucket_clone() {
        let store = TestStore::new();

        let bucket1 = store.get_test_bucket();
        let bucket2 = bucket1.clone();
        let bucket3 = store.get_test_bucket();

        bucket1.insert(&3, &String::from("3")).unwrap();
        bucket1.insert(&4, &String::from("4")).unwrap();
        bucket1.insert(&5, &String::from("5")).unwrap();
        bucket1.insert(&6, &String::from("6")).unwrap();

        {
            let mut iter = bucket2.iter();
            assert_eq!(iter.next().transpose().unwrap(), Some((3, String::from("3"))));
            assert_eq!(iter.next().transpose().unwrap(), Some((4, String::from("4"))));
            assert_eq!(iter.next().transpose().unwrap(), Some((5, String::from("5"))));
            assert_eq!(iter.next().transpose().unwrap(), Some((6, String::from("6"))));
            assert_eq!(iter.next().transpose().unwrap(), None);
            assert_eq!(iter.next().transpose().unwrap(), None);
        }

        {
            let mut iter = bucket3.iter();
            assert_eq!(iter.next().transpose().unwrap(), Some((3, String::from("3"))));
            assert_eq!(iter.next().transpose().unwrap(), Some((4, String::from("4"))));
            assert_eq!(iter.next().transpose().unwrap(), Some((5, String::from("5"))));
            assert_eq!(iter.next().transpose().unwrap(), Some((6, String::from("6"))));
            assert_eq!(iter.next().transpose().unwrap(), None);
            assert_eq!(iter.next().transpose().unwrap(), None);
        }
    }

    /// Check if iteration works properly and edited entries are available during next iteration.
    #[test]
    fn bucket_iter_and_edit() {
        let store = TestStore::new();
        let bucket = store.get_test_bucket();

        bucket.insert(&3, &String::from("3")).unwrap();
        bucket.insert(&4, &String::from("4")).unwrap();
        bucket.insert(&5, &String::from("5")).unwrap();
        bucket.insert(&6, &String::from("6")).unwrap();

        {
            let mut iter = bucket.iter();
            assert_eq!(iter.next().transpose().unwrap(), Some((3, String::from("3"))));
            assert_eq!(iter.next().transpose().unwrap(), Some((4, String::from("4"))));
            assert_eq!(iter.next().transpose().unwrap(), Some((5, String::from("5"))));
            assert_eq!(iter.next().transpose().unwrap(), Some((6, String::from("6"))));
            assert_eq!(iter.next().transpose().unwrap(), None);
            assert_eq!(iter.next().transpose().unwrap(), None);
        }

        {
            let mut editor = bucket.edit(4).unwrap().unwrap();
            editor.value = String::from("44");
        }

        {
            let mut editor = bucket.edit(5).unwrap().unwrap();
            // Editing the key should have no impact on where the value is saved.
            editor.key = 9;
            editor.value = String::from("55");
        }

        {
            let mut iter = bucket.iter();
            assert_eq!(iter.next().transpose().unwrap(), Some((3, String::from("3"))));
            assert_eq!(iter.next().transpose().unwrap(), Some((4, String::from("44"))));
            assert_eq!(iter.next().transpose().unwrap(), Some((5, String::from("55"))));
            assert_eq!(iter.next().transpose().unwrap(), Some((6, String::from("6"))));
            assert_eq!(iter.next().transpose().unwrap(), None);
            assert_eq!(iter.next().transpose().unwrap(), None);
        }
    }

    /// Check if autosave iteration works properly.
    #[test]
    fn bucket_iter_autosave() {
        let store = TestStore::new();
        let bucket = store.get_test_bucket();

        bucket.insert(&3, &String::from("3")).unwrap();
        bucket.insert(&4, &String::from("4")).unwrap();
        bucket.insert(&5, &String::from("5")).unwrap();
        bucket.insert(&6, &String::from("6")).unwrap();

        {
            for item in bucket.clone().iter_autosave() {
                let mut item = item.unwrap();
                item.value = (11 * item.key).to_string();
            }
        }

        {
            let mut iter = bucket.iter();
            assert_eq!(iter.next().transpose().unwrap(), Some((3, String::from("33"))));
            assert_eq!(iter.next().transpose().unwrap(), Some((4, String::from("44"))));
            assert_eq!(iter.next().transpose().unwrap(), Some((5, String::from("55"))));
            assert_eq!(iter.next().transpose().unwrap(), Some((6, String::from("66"))));
            assert_eq!(iter.next().transpose().unwrap(), None);
            assert_eq!(iter.next().transpose().unwrap(), None);
        }
    }
}
