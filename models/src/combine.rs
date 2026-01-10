// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::{HashMap, hash_map::Entry};

pub trait Combine {
    /// Combines two objects of the same type.
    fn combine(o1: Self, o2: Self) -> Self;
}

pub trait TryCombine: Sized {
    type Error: Sized;

    /// Combines two objects of the same type.
    fn try_combine(o1: Self, o2: Self) -> Result<Self, Self::Error>;
}

impl<T> Combine for Option<T> {
    fn combine(o1: Option<T>, o2: Option<T>) -> Self {
        o1.or(o2)
    }
}

/// Combines two hash maps returning an error if keys repeat.
pub fn try_combine_disjoint_hashmaps<K, V, S>(
    mut m1: HashMap<K, V, S>,
    m2: HashMap<K, V, S>,
) -> Result<HashMap<K, V, S>, K>
where
    K: Eq + std::hash::Hash + Clone,
    S: std::hash::BuildHasher,
{
    for (key, value2) in m2 {
        match m1.entry(key) {
            Entry::Occupied(entry) => return Err(entry.key().clone()),
            Entry::Vacant(entry) => {
                entry.insert(value2);
            }
        }
    }
    Ok(m1)
}
