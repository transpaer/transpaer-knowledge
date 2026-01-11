// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! This modules contains definitions of data stored in the internal database.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    str::FromStr,
};

use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::combine::Combine;

#[cfg(feature = "into-api")]
use transpaer_api::models as api;

#[cfg(feature = "from-substrate")]
use transpaer_schema as schema;

use crate::ids;

pub type LibraryTopic = String;

// TODO: Validate the domain when deserializing.
pub type Domain = String;

/// Points to a source of some data.
///
/// If the source is mentioned here, we process it in a special way.
/// The sources without special processing are marked as `Other`.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum Source {
    /// Transpaer.
    Transpaer,

    /// BCorp.
    BCorp,

    /// EU Ecolabel.
    EuEcolabel,

    /// Fashion Transparency Index.
    Fti,

    /// Open Food Facts.
    OpenFoodFacts,

    /// Open Food Repo.
    OpenFoodRepo,

    /// TCO.
    Tco,

    /// Wikidata.
    Wikidata,

    /// The "Simple Environmentalist" youtube channel.
    SimpleEnvironmentalist,

    Other,
}

impl Source {
    pub fn from_stem(string: &str) -> Self {
        match string {
            "transpaer" => Source::Transpaer,
            "bcorp" => Source::BCorp,
            "eu_ecolabel" => Source::EuEcolabel,
            "fti" => Source::Fti,
            "open_food_facts" => Source::OpenFoodFacts,
            "open_food_repo" => Source::OpenFoodRepo,
            "tco" => Source::Tco,
            "wikidata" => Source::Wikidata,
            "simple_environmentalist" => Source::SimpleEnvironmentalist,
            _ => {
                log::warn!("Source `{string}` is not covered");
                Source::Other
            }
        }
    }

    pub fn is_bcorp(&self) -> bool {
        matches!(self, Self::BCorp)
    }

    pub fn is_euecolabel(&self) -> bool {
        matches!(self, Self::EuEcolabel)
    }

    pub fn is_fti(&self) -> bool {
        matches!(self, Self::Fti)
    }

    pub fn is_tco(&self) -> bool {
        matches!(self, Self::Tco)
    }

    #[cfg(feature = "into-api")]
    pub fn get_icon_link(&self) -> Option<String> {
        match self {
            Self::SimpleEnvironmentalist => Some("https://yt3.googleusercontent.com/TAUPgsU3oOD-CYNfUo1V9rpgtH-IHbAjUdo92nusdtz9e25tLjQ_uRx0ZpnAf5DnBp6tUAQUt28=s160-c-k-c0x00ffffff-no-rj".to_string()),
            _ => None,
        }
    }
}

#[cfg(feature = "into-api")]
impl Source {
    pub fn to_label(&self) -> String {
        match self {
            Self::Transpaer => "transpaer",
            Self::BCorp => "bcorp",
            Self::EuEcolabel => "eu_ecolabel",
            Self::Fti => "fti",
            Self::OpenFoodFacts => "open_food_facts",
            Self::OpenFoodRepo => "open_food_repo",
            Self::Tco => "tco",
            Self::Wikidata => "wikidata",
            Self::SimpleEnvironmentalist => "simple_environmentalist",
            Self::Other => "other",
        }
        .to_owned()
    }

    pub fn into_api(&self) -> api::DataSource {
        api::DataSource(self.to_label())
    }
}

/// Text together with it's source.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Text {
    /// Text.
    pub text: String,

    /// Source of the text.
    pub sources: Vec<Source>,
}

#[cfg(feature = "into-api")]
impl Text {
    pub fn new(text: &str, source: Source) -> Self {
        Self { text: text.to_string(), sources: vec![source] }
    }

    pub fn new_many(text: &str, sources: Vec<Source>) -> Self {
        Self { text: text.to_string(), sources }
    }

    pub fn into_api_long(self) -> api::LongText {
        let text = match api::LongString::from_str(&self.text) {
            Ok(ok) => ok,
            Err(err) => {
                log::error!("Could not convert to a LongString: {err}");
                default_long_string()
            }
        };

        api::LongText { text, sources: sources_to_api(&self.sources) }
    }

    pub fn into_api_short(self) -> api::ShortText {
        let text = match api::ShortString::from_str(&self.text) {
            Ok(ok) => ok,
            Err(err) => {
                log::error!("Could not convert to a ShortString: {err}");
                default_short_string()
            }
        };

        api::ShortText { text, sources: sources_to_api(&self.sources) }
    }
}

/// Image together with it's source.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Image {
    /// Name of the images.
    ///
    /// Together with the source it's possible to reconstruct images URL.
    pub image: String,

    /// Source of the image.
    pub source: Source,
}

#[cfg(feature = "into-api")]
impl Image {
    pub fn into_api(self) -> api::Image {
        api::Image { image: self.image, source: self.source.into_api() }
    }
}

/// Website together with it's source.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Website {
    /// The website domain..
    pub website: String,

    /// Source of the website.
    pub sources: Vec<Source>,
}

#[cfg(feature = "into-api")]
impl Website {
    pub fn into_api_short_string(self) -> api::ShortString {
        match api::ShortString::from_str(&self.website) {
            Ok(website) => website,
            Err(err) => {
                log::error!("Could not convert a website to a ShortString: {err}");
                default_short_string()
            }
        }
    }

    pub fn into_api_id(self) -> api::Id {
        match api::Id::from_str(&self.website) {
            Ok(website) => website,
            Err(err) => {
                log::error!("Could not convert a website to an Id {err}");
                default_id()
            }
        }
    }
}

/// Organisatio ID with its source.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SourcedOrganisationId {
    /// The website domain..
    pub id: ids::OrganisationId,

    /// Source of the website.
    pub sources: Vec<Source>,
}

/// Vat ID with its source.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Vat {
    /// The website domain..
    pub id: ids::VatId,

    /// Source of the website.
    pub sources: Vec<Source>,
}

#[cfg(feature = "into-api")]
impl Vat {
    pub fn into_api(self) -> api::Id {
        api::Id::from_str(&self.id.to_canonical_string()).expect("Converting Wiki ID")
    }
}

/// Wiki ID with its source.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SourcedWikiId {
    /// The website domain..
    pub id: ids::WikiId,

    /// Source of the website.
    pub sources: Vec<Source>,
}

impl SourcedWikiId {
    pub fn new(id: ids::WikiId, source: Source) -> Self {
        Self { id, sources: vec![source] }
    }

    pub fn new_many(id: ids::WikiId, sources: Vec<Source>) -> Self {
        Self { id, sources }
    }
}

#[cfg(feature = "into-api")]
impl SourcedWikiId {
    pub fn into_api(self) -> api::Id {
        api::Id::from_str(&self.id.to_canonical_string()).expect("Converting Wiki ID")
    }
}

/// EAN with its source.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SourcedEan {
    /// The website domain..
    pub id: ids::Ean,

    /// Source of the website.
    pub sources: Vec<Source>,
}

#[cfg(feature = "into-api")]
impl SourcedEan {
    pub fn into_api(self) -> api::Id {
        api::Id::from_str(&self.id.to_canonical_string()).expect("Converting EAN")
    }
}

/// GTIN with its source.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SourcedGtin {
    /// The website domain..
    pub id: ids::Gtin,

    /// Source of the website.
    pub sources: Vec<Source>,
}

#[cfg(feature = "into-api")]
impl SourcedGtin {
    pub fn into_api(self) -> api::Id {
        api::Id::from_str(&self.id.to_canonical_string()).expect("Converting GTIN")
    }
}

/// Country code with its source.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Country {
    /// The website domain..
    pub country: isocountry::CountryCode,

    /// Source of the website.
    pub sources: Vec<Source>,
}

#[cfg(feature = "into-api")]
impl Country {
    pub fn into_api(self) -> isocountry::CountryCode {
        self.country
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct MultiMap<K: Ord, V: Ord>(BTreeMap<K, BTreeSet<V>>);

impl<K, V> Default for MultiMap<K, V>
where
    K: Ord,
    V: Ord,
{
    fn default() -> Self {
        Self::new_empty()
    }
}

impl<K, V> MultiMap<K, V>
where
    K: Ord,
    V: Ord,
{
    pub fn new_single(key: K, value: V) -> Self {
        Self(maplit::btreemap! { key => maplit::btreeset! { value } })
    }

    pub fn new_or_empty(key: Option<K>, value: V) -> Self {
        if let Some(key) = key { Self::new_single(key, value) } else { Self::new_empty() }
    }

    pub fn new_empty() -> Self {
        Self(BTreeMap::new())
    }

    pub fn new_from_map(map: BTreeMap<K, BTreeSet<V>>) -> Self {
        Self(map)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn insert(&mut self, key: K, value: V) {
        use std::collections::btree_map::Entry;

        match self.0.entry(key) {
            Entry::Vacant(entry) => {
                entry.insert(maplit::btreeset! { value });
            }
            Entry::Occupied(mut entry) => {
                entry.get_mut().insert(value);
            }
        }
    }

    pub fn contains<Q>(&self, key: &Q) -> bool
    where
        Q: Ord + ?Sized,
        K: std::borrow::Borrow<Q>,
    {
        self.0.contains_key(key)
    }
}

impl<K, V> MultiMap<K, V>
where
    K: Ord + Clone,
    V: Ord + Clone,
{
    pub fn new_many(keys: Vec<K>, value: V) -> Self {
        Self(keys.into_iter().map(|key| (key, maplit::btreeset! { value.clone() })).collect())
    }

    pub fn keys(&self) -> BTreeSet<K> {
        self.0.keys().map(|k| (*k).clone()).collect()
    }
}

impl<K, V> Combine for MultiMap<K, V>
where
    K: Ord,
    V: Ord,
{
    fn combine(mut m1: Self, m2: Self) -> Self {
        use std::collections::btree_map::Entry;

        for (key, value2) in m2.0 {
            match m1.0.entry(key) {
                Entry::Occupied(mut entry) => {
                    entry.get_mut().extend(value2);
                }
                Entry::Vacant(entry) => {
                    entry.insert(value2);
                }
            }
        }
        m1
    }
}

impl<K> MultiMap<K, Source>
where
    K: Ord,
{
    pub fn collect_sources(&self) -> Vec<Source> {
        let mut result = BTreeSet::<Source>::new();
        for value in self.0.values() {
            result.extend(value.clone());
        }
        result.into_iter().collect()
    }
}

impl MultiMap<ids::OrganisationId, Source> {
    pub fn into_vec_organisation_ids(self) -> Vec<SourcedOrganisationId> {
        self.0
            .into_iter()
            .map(|(id, sources)| {
                let sources = sources.into_iter().collect();
                SourcedOrganisationId { id, sources }
            })
            .collect()
    }
}

impl MultiMap<ids::VatId, Source> {
    pub fn into_vec_vat(self) -> Vec<Vat> {
        self.0
            .into_iter()
            .map(|(id, sources)| {
                let sources = sources.into_iter().collect();
                Vat { id, sources }
            })
            .collect()
    }
}

impl MultiMap<ids::WikiId, Source> {
    pub fn into_vec_wiki(self) -> Vec<SourcedWikiId> {
        self.0
            .into_iter()
            .map(|(id, sources)| {
                let sources = sources.into_iter().collect();
                SourcedWikiId { id, sources }
            })
            .collect()
    }
}

impl MultiMap<ids::Ean, Source> {
    pub fn into_vec_ean(self) -> Vec<SourcedEan> {
        self.0
            .into_iter()
            .map(|(id, sources)| {
                let sources = sources.into_iter().collect();
                SourcedEan { id, sources }
            })
            .collect()
    }
}

impl MultiMap<ids::Gtin, Source> {
    pub fn into_vec_gtin(self) -> Vec<SourcedGtin> {
        self.0
            .into_iter()
            .map(|(id, sources)| {
                let sources = sources.into_iter().collect();
                SourcedGtin { id, sources }
            })
            .collect()
    }
}

impl MultiMap<String, Source> {
    pub fn into_vec_text(self) -> Vec<Text> {
        self.0
            .into_iter()
            .map(|(text, sources)| {
                let sources = sources.into_iter().collect();
                Text { text, sources }
            })
            .collect()
    }

    pub fn into_vec_website(self) -> Vec<Website> {
        self.0
            .into_iter()
            .map(|(website, sources)| {
                let sources = sources.into_iter().collect();
                Website { website, sources }
            })
            .collect()
    }
}

impl MultiMap<isocountry::CountryCode, Source> {
    pub fn into_vec_country(self) -> Vec<Country> {
        self.0
            .into_iter()
            .map(|(country, sources)| {
                let sources = sources.into_iter().collect();
                Country { country, sources }
            })
            .collect()
    }
}

impl MultiMap<ShoppingKey, ShoppingData> {
    pub fn into_vec_shopping_entry(self) -> Vec<ShoppingEntry> {
        self.0
            .into_iter()
            .map(|(key, data)| {
                let sources = data.iter().map(|d| d.source.clone()).collect();
                #[allow(unstable_name_collisions)]
                let description = data
                    .into_iter()
                    .map(|d| d.description)
                    .intersperse("\n\n".to_owned())
                    .collect();
                ShoppingEntry { id: key.id, shop: key.shop, description, sources }
            })
            .collect()
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub enum Regions {
    /// Available world-wide
    World,

    /// Region could not be identified
    #[default]
    Unknown,

    /// List of regions
    List(Vec<isocountry::CountryCode>),
}

impl Regions {
    pub fn is_available_in(&self, region: Option<&str>) -> bool {
        match self {
            Self::World => true,
            Self::Unknown => false,
            Self::List(codes) => region
                .map(|region| codes.iter().any(|code| code.alpha3() == region))
                .unwrap_or(false),
        }
    }

    pub fn is_unknown(&self) -> bool {
        match self {
            Self::Unknown => true,
            Self::World | Self::List(_) => false,
        }
    }
}

impl Combine for Regions {
    fn combine(o1: Self, o2: Self) -> Self {
        match &o2 {
            Self::World => o2,
            Self::Unknown => o1,
            Self::List(list2) => match o1 {
                Self::World => o1,
                Self::Unknown => o2,
                Self::List(mut list1) => {
                    list1.extend(list2);
                    list1.sort_unstable();
                    list1.dedup();
                    Self::List(list1)
                }
            },
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct Availability {
    /// Regions where the product is available.
    pub regions: Regions,

    /// Sources.
    pub sources: BTreeSet<Source>,
}

impl Combine for Availability {
    fn combine(mut a1: Self, a2: Self) -> Self {
        let regions = Combine::combine(a1.regions, a2.regions);
        a1.sources.extend(a2.sources);
        Self { regions, sources: a1.sources }
    }
}

/// Data about a `BCorp` company.
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct BCorpCert {
    /// Name identifying the company.
    pub id: String,

    /// Link to the BCorp page about the company.
    pub report_url: String,
}

#[cfg(feature = "into-api")]
impl BCorpCert {
    pub fn into_api(self) -> api::Medallion {
        let bcorp = match (api::Id::from_str(&self.id), api::LongString::from_str(&self.report_url))
        {
            (Ok(id), Ok(report_url)) => Some(api::BCorpMedallion { id, report_url }),
            (id, report_url) => {
                log::error!("Could not convert medallion: {id:?}, {report_url:?}");
                None
            }
        };

        api::Medallion {
            variant: api::MedallionVariant::BCorp,
            bcorp,
            eu_ecolabel: None,
            fti: None,
            transpaer: None,
            tco: None,
        }
    }
}

/// Data about a company certified by EU Ecolabel.
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct EuEcolabelCert;

#[cfg(feature = "into-api")]
impl EuEcolabelCert {
    pub fn into_api(self) -> api::Medallion {
        api::Medallion {
            variant: api::MedallionVariant::EuEcolabel,
            bcorp: None,
            eu_ecolabel: Some(api::EuEcolabelMedallion { match_accuracy: None }),
            fti: None,
            transpaer: None,
            tco: None,
        }
    }
}

/// Data about a company scored by Fashion Transparency Index.
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct FtiCert {
    /// Score (from 0% to 100%).
    pub score: i64,
}

#[cfg(feature = "into-api")]
impl FtiCert {
    pub fn into_api(self) -> api::Medallion {
        api::Medallion {
            variant: api::MedallionVariant::Fti,
            bcorp: None,
            eu_ecolabel: None,
            fti: Some(api::FtiMedallion { score: self.score }),
            transpaer: None,
            tco: None,
        }
    }
}

/// Data about a company which products were certified by TCO.
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct TcoCert {
    /// Name identifying the company.
    pub brand_name: String,
}

#[cfg(feature = "into-api")]
impl TcoCert {
    pub fn into_api(self) -> api::Medallion {
        let tco = match api::ShortString::from_str(&self.brand_name) {
            Ok(brand_name) => Some(api::TcoMedallion { brand_name }),
            Err(err) => {
                log::error!("Could not convert a brand name to a ShortString: {err}");
                None
            }
        };

        api::Medallion {
            variant: api::MedallionVariant::Tco,
            bcorp: None,
            eu_ecolabel: None,
            fti: None,
            transpaer: None,
            tco,
        }
    }
}

/// Lists known certifications.
#[derive(Serialize, Deserialize, Debug, Clone, Default, Eq, PartialEq)]
pub struct Certifications {
    /// Manufacturer certifiad by BCorp.
    pub bcorp: Option<BCorpCert>,

    /// Manufacturer certified by EU Ecolabel.
    pub eu_ecolabel: Option<EuEcolabelCert>,

    /// Organisation scored by Fashion Transparency Index.
    pub fti: Option<FtiCert>,

    /// Manufacturer certifiad by TCO.
    pub tco: Option<TcoCert>,
}

impl Certifications {
    /// Returns number of given certifications.
    ///
    /// TODO: FTI is not a certification.
    #[must_use]
    pub fn get_num(&self) -> usize {
        usize::from(self.bcorp.is_some())
            + usize::from(self.eu_ecolabel.is_some())
            + usize::from(self.fti.is_some())
            + usize::from(self.tco.is_some())
    }

    /// Copies certifications.
    ///
    /// EU Ecolabel is not inherited - this certification is assigned directly to products, not companies.
    pub fn inherit(&mut self, other: &Self) {
        if other.bcorp.is_some() {
            self.bcorp.clone_from(&other.bcorp);
        }
        if other.fti.is_some() {
            self.fti.clone_from(&other.fti);
        }
        if other.tco.is_some() {
            self.tco.clone_from(&other.tco);
        }
    }
}

impl Combine for Certifications {
    fn combine(o1: Self, o2: Self) -> Self {
        Self {
            bcorp: Combine::combine(o1.bcorp, o2.bcorp),
            eu_ecolabel: Combine::combine(o1.eu_ecolabel, o2.eu_ecolabel),
            fti: Combine::combine(o1.fti, o2.fti),
            tco: Combine::combine(o1.tco, o2.tco),
        }
    }
}

#[cfg(feature = "into-api")]
impl Certifications {
    pub fn into_api_medallions(self) -> Vec<api::Medallion> {
        let mut medallions = Vec::new();
        if let Some(bcorp) = self.bcorp {
            medallions.push(bcorp.into_api());
        }
        if let Some(eu_ecolabel) = self.eu_ecolabel {
            medallions.push(eu_ecolabel.into_api());
        }
        if let Some(fti) = self.fti {
            medallions.push(fti.into_api());
        }
        if let Some(tco) = self.tco {
            medallions.push(tco.into_api());
        }
        medallions
    }

    pub fn to_api_badges(&self) -> Vec<api::BadgeName> {
        let mut badges = Vec::new();
        if self.bcorp.is_some() {
            badges.push(api::BadgeName::Bcorp);
        }
        if self.eu_ecolabel.is_some() {
            badges.push(api::BadgeName::Eu);
        }
        if self.tco.is_some() {
            badges.push(api::BadgeName::Tco);
        }
        badges
    }

    pub fn to_api_scores(&self) -> Vec<api::Score> {
        let mut scores = Vec::with_capacity(1);
        if let Some(fti) = &self.fti {
            scores.push(api::Score { scorer_name: api::ScorerName::Fti, score: fti.score });
        }
        scores
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Mention {
    /// Title of the mention.
    pub title: String,

    /// External link to the mention.
    pub link: String,
}

#[cfg(feature = "into-api")]
impl Mention {
    pub fn into_api(self) -> api::Mention {
        api::Mention { title: self.title, link: self.link }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Medium {
    /// Describes the medium.
    pub source: Source,

    /// List of all mentions from this medium.
    pub mentions: Vec<Mention>,
}

#[cfg(feature = "into-api")]
impl Medium {
    pub fn into_api(self) -> api::Medium {
        api::Medium {
            icon: self.source.get_icon_link(),
            mentions: self.mentions.into_iter().map(|mention| mention.into_api()).collect(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum VerifiedShop {
    Fairphone,
    Amazon,
}

impl VerifiedShop {
    fn from_schema(shop: &schema::VerifiedShop) -> Self {
        match shop {
            schema::VerifiedShop::Fairphone => Self::Fairphone,
            schema::VerifiedShop::Amazon => Self::Amazon,
        }
    }
}

#[cfg(feature = "into-api")]
impl VerifiedShop {
    pub fn into_api(self) -> api::VerifiedShop {
        match self {
            Self::Fairphone => api::VerifiedShop::Fairphone,
            Self::Amazon => api::VerifiedShop::Amazon,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ShoppingKey {
    pub id: String,
    pub shop: VerifiedShop,
}

impl ShoppingKey {
    pub fn from_schema(entry: &schema::ShoppingEntry) -> Self {
        Self { shop: VerifiedShop::from_schema(&entry.shop), id: entry.id.clone() }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ShoppingData {
    pub description: String,
    pub source: Source,
}

impl ShoppingData {
    pub fn from_schema(entry: &schema::ShoppingEntry, source: Source) -> Self {
        Self { description: entry.description.clone(), source }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ShoppingEntry {
    pub id: String,
    pub shop: VerifiedShop,
    pub description: String,
    pub sources: Vec<Source>,
}

#[cfg(feature = "into-api")]
impl ShoppingEntry {
    pub fn into_api(self) -> api::ShoppingEntry {
        let link = match &self.shop {
            VerifiedShop::Fairphone => format!("https://shop.fairphone.com/{}", self.id),
            VerifiedShop::Amazon => format!("https://www.amazon.nl/-/en/_/dp/{}", self.id),
        };
        let shop = self.shop.into_api();
        let description = str_to_short_string(self.description);
        api::ShoppingEntry { shop, link, description }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[repr(u8)]
pub enum TranspaerScoreCategory {
    Root,
    DataAvailability,
    ProducerKnown,
    ProductionPlaceKnown,
    IdKnown,
    CategoryAssigned,
    Category,
    WarrantyLength,
    NumCerts,
    AtLeastOneCert,
    AtLeastTwoCerts,
}

#[cfg(feature = "into-api")]
impl TranspaerScoreCategory {
    pub fn into_api(self) -> api::TranspaerScoreCategory {
        match self {
            Self::Root => unimplemented!(), //< This category is never passed to the API
            Self::DataAvailability => api::TranspaerScoreCategory::DataAvailability,
            Self::ProducerKnown => api::TranspaerScoreCategory::ProducerKnown,
            Self::ProductionPlaceKnown => api::TranspaerScoreCategory::ProductionPlaceKnown,
            Self::IdKnown => api::TranspaerScoreCategory::IdKnown,
            Self::CategoryAssigned => api::TranspaerScoreCategory::CategoryAssigned,
            Self::Category => api::TranspaerScoreCategory::Category,
            Self::WarrantyLength => api::TranspaerScoreCategory::WarrantyLength,
            Self::NumCerts => api::TranspaerScoreCategory::NumCerts,
            Self::AtLeastOneCert => api::TranspaerScoreCategory::AtLeastOneCert,
            Self::AtLeastTwoCerts => api::TranspaerScoreCategory::AtLeastTwoCerts,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TranspaerScoreBranch {
    /// Subbranches of the tree.
    pub branches: Vec<TranspaerScoreBranch>,

    /// Category representing this branch.
    pub category: TranspaerScoreCategory,

    /// Weight of this branch.
    pub weight: i32,

    /// Calculated subscore of this branch.
    pub score: f64,
}

#[cfg(feature = "into-api")]
impl TranspaerScoreBranch {
    pub fn into_api(self) -> api::TranspaerScoreBranch {
        api::TranspaerScoreBranch {
            branches: self.branches.into_iter().map(|b| b.into_api()).collect(),
            category: self.category.into_api(),
            weight: self.weight as i64,
            score: self.score,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TranspaerScore {
    /// Score tree.
    pub tree: Vec<TranspaerScoreBranch>,

    /// Total calculated score.
    pub total: f64,
}

#[cfg(feature = "into-api")]
impl TranspaerScore {
    pub fn into_api_score(self) -> api::TranspaerScore {
        api::TranspaerScore {
            tree: self.tree.into_iter().map(|t| t.into_api()).collect(),
            total: self.total,
        }
    }

    fn into_api_medallion(self) -> api::Medallion {
        api::Medallion {
            variant: api::MedallionVariant::Transpaer,
            transpaer: Some(api::TranspaerMedallion { score: self.into_api_score() }),
            bcorp: None,
            eu_ecolabel: None,
            fti: None,
            tco: None,
        }
    }
}

impl Default for TranspaerScore {
    fn default() -> Self {
        Self { tree: Vec::default(), total: 0.0 }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Significance(f64);

impl Significance {
    pub fn new(value: f64) -> Self {
        Self(value)
    }

    pub fn add(&mut self, value: f64) {
        self.0 += value;
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct TranspaerProductData {
    pub score: TranspaerScore,
    pub significance: HashMap<Source, Significance>,
}

// TODO: Introduce score for organisations
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct TranspaerOrganisationData {
    pub significance: HashMap<Source, Significance>,
}

/// Represents a set of IDs of an organisation.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct GatherOrganisationIds {
    /// VAT IDs.
    pub vat_ids: MultiMap<ids::VatId, Source>,

    /// Organisation ID.
    pub wiki: MultiMap<ids::WikiId, Source>,

    /// Web domains.
    pub domains: MultiMap<Domain, Source>,
}

impl GatherOrganisationIds {
    pub fn store(self) -> StoreOrganisationIds {
        let mut vat_ids = self.vat_ids.into_vec_vat();
        let mut wiki = self.wiki.into_vec_wiki();
        let mut domains = self.domains.into_vec_website();

        vat_ids.sort();
        wiki.sort();
        domains.sort();

        StoreOrganisationIds { vat_ids, wiki, domains }
    }
}

impl Combine for GatherOrganisationIds {
    fn combine(o1: Self, o2: Self) -> Self {
        let wiki = Combine::combine(o1.wiki, o2.wiki);
        let vat_ids = Combine::combine(o1.vat_ids, o2.vat_ids);
        let domains = Combine::combine(o1.domains, o2.domains);
        Self { wiki, vat_ids, domains }
    }
}

/// Represents a set of IDs of an organisation.
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct StoreOrganisationIds {
    /// Organisation ID.
    pub wiki: Vec<SourcedWikiId>,

    /// VAT IDs.
    pub vat_ids: Vec<Vat>,

    /// Web domains.
    pub domains: Vec<Website>,
}

#[cfg(feature = "into-api")]
impl StoreOrganisationIds {
    pub fn into_api(self) -> api::OrganisationIds {
        api::OrganisationIds {
            wiki: self.wiki.into_iter().map(|id| id.into_api()).collect(),
            vat: self.vat_ids.into_iter().map(|id| id.into_api()).collect(),
            domains: self.domains.into_iter().map(|id| id.into_api_id()).collect(),
        }
    }
}

/// Represents an organisation (e.g. manufacturer, shop).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatherOrganisation {
    /// Organisation IDs.
    pub ids: GatherOrganisationIds,

    /// Names of the organisation.
    pub names: MultiMap<String, Source>,

    /// Descriptions of the organisation.
    pub descriptions: MultiMap<String, Source>,

    /// Logo images.
    pub images: BTreeSet<Image>,

    /// Websites.
    pub websites: MultiMap<String, Source>,

    /// Products of this organistion.
    pub products: BTreeSet<ids::ProductId>,

    /// Countries where the organisation is registered in.
    pub origins: MultiMap<isocountry::CountryCode, Source>,

    /// Known certifications.
    pub certifications: Certifications,

    /// Mantions in media.
    pub media: BTreeSet<Medium>,

    /// The Transpaer data.
    pub transpaer: TranspaerOrganisationData,
}

impl GatherOrganisation {
    pub fn store(self) -> StoreOrganisation {
        let ids = self.ids.store();
        let mut names: Vec<_> = self.names.into_vec_text();
        let mut descriptions: Vec<_> = self.descriptions.into_vec_text();
        let mut images: Vec<_> = self.images.into_iter().collect();
        let mut websites: Vec<_> = self.websites.into_vec_website();
        let mut products: Vec<_> = self.products.into_iter().collect();
        let mut origins: Vec<_> = self.origins.into_vec_country();
        let mut media: Vec<_> = self.media.into_iter().collect();
        let certifications = self.certifications;
        let transpaer = self.transpaer;

        names.sort();
        descriptions.sort();
        images.sort();
        products.sort();
        websites.sort();
        origins.sort();
        media.sort();

        StoreOrganisation {
            ids,
            names,
            descriptions,
            images,
            websites,
            origins,
            products,
            certifications,
            media,
            transpaer,
        }
    }
}

impl Combine for GatherOrganisation {
    fn combine(mut o1: Self, o2: Self) -> Self {
        let ids = Combine::combine(o1.ids, o2.ids);

        let names = Combine::combine(o1.names, o2.names);
        let descriptions = Combine::combine(o1.descriptions, o2.descriptions);
        let websites = Combine::combine(o1.websites, o2.websites);
        let origins = Combine::combine(o1.origins, o2.origins);
        let certifications = Combine::combine(o1.certifications, o2.certifications);

        // This data is filled after merging all organisations.
        let transpaer = TranspaerOrganisationData::default();

        o1.images.extend(o2.images);
        o1.products.extend(o2.products);
        o1.media.extend(o2.media);

        Self {
            ids,
            names,
            descriptions,
            images: o1.images,
            websites,
            products: o1.products,
            origins,
            certifications,
            media: o1.media,
            transpaer,
        }
    }
}

/// Represents an organisation (e.g. manufacturer, shop).
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StoreOrganisation {
    /// Organisation IDs.
    pub ids: StoreOrganisationIds,

    /// Names of the organisation.
    pub names: Vec<Text>,

    /// Descriptions of the organisation.
    pub descriptions: Vec<Text>,

    /// Logo images.
    pub images: Vec<Image>,

    /// Websites.
    pub websites: Vec<Website>,

    /// Countries where the organisation is registered in.
    pub origins: Vec<Country>,

    /// Products of this organistion.
    pub products: Vec<ids::ProductId>,

    /// Known certifications.
    pub certifications: Certifications,

    /// Mantions in media.
    pub media: Vec<Medium>,

    /// The Transpaer data.
    pub transpaer: TranspaerOrganisationData,
}

#[cfg(feature = "into-api")]
fn default_id() -> api::Id {
    api::Id::from_str("").expect("Id from an empty string")
}

#[cfg(feature = "into-api")]
fn default_short_string() -> api::ShortString {
    api::ShortString::from_str("").expect("ShortString from an empty string")
}

#[cfg(feature = "into-api")]
fn default_long_string() -> api::LongString {
    api::LongString::from_str("").expect("LongString from an empty string")
}

#[cfg(feature = "into-api")]
fn sources_to_api(sources: &[Source]) -> api::DataSources {
    api::DataSources(sources.iter().map(|s| s.to_label()).collect())
}

#[cfg(feature = "into-api")]
fn str_to_short_string(s: String) -> api::ShortString {
    api::ShortString::from_str(&s).expect("Converting strings")
}

#[cfg(feature = "into-api")]
fn str_to_long_string(s: String) -> api::LongString {
    api::LongString::from_str(&s).expect("Converting strings")
}

#[cfg(feature = "into-api")]
fn text_to_short_string(text: &Text) -> api::ShortString {
    api::ShortString::from_str(&text.text).expect("Converting texts")
}

#[cfg(feature = "into-api")]
fn text_to_long_text(text: &Text) -> api::LongText {
    api::LongText {
        text: api::LongString::from_str(&text.text).expect("Converting texts"),
        sources: sources_to_api(&text.sources),
    }
}

#[cfg(feature = "into-api")]
fn country_code_to_region_code(country: Country) -> api::RegionCode {
    api::RegionCode::from_str(country.country.alpha3()).expect("alpha3 code must have length of 3")
}

#[cfg(feature = "into-api")]
impl StoreOrganisation {
    pub fn into_api_short(self) -> api::OrganisationShort {
        api::OrganisationShort {
            organisation_ids: self.ids.into_api(),
            name: self.names.first().map_or_else(default_short_string, text_to_short_string),
            description: self.descriptions.first().map(text_to_long_text),
            badges: self.certifications.to_api_badges(),
            scores: self.certifications.to_api_scores(),
        }
    }

    pub fn into_api_full(self, products: Vec<api::ProductShort>) -> api::OrganisationFull {
        api::OrganisationFull {
            organisation_ids: self.ids.into_api(),
            names: self.names.into_iter().map(|n| n.into_api_short()).collect(),
            descriptions: self.descriptions.into_iter().map(|d| d.into_api_long()).collect(),
            images: self.images.into_iter().map(|i| i.into_api()).collect(),
            websites: self.websites.into_iter().map(|w| w.into_api_short_string()).collect(),
            origins: self.origins.into_iter().map(country_code_to_region_code).collect(),
            medallions: self.certifications.into_api_medallions(),
            media: self.media.into_iter().map(|m| m.into_api()).collect(),
            products,
        }
    }
}

/// Represents a set of product IDs.
#[derive(Default, Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct GatherProductIds {
    /// GTIN of the product.
    pub eans: MultiMap<ids::Ean, Source>,

    /// GTIN of the product.
    pub gtins: MultiMap<ids::Gtin, Source>,

    /// Wiki ID.
    pub wiki: MultiMap<ids::WikiId, Source>,
}

impl GatherProductIds {
    pub fn is_empty(&self) -> bool {
        self.eans.is_empty() && self.gtins.is_empty() && self.wiki.is_empty()
    }

    pub fn store(self) -> StoreProductIds {
        let mut eans = self.eans.into_vec_ean();
        let mut gtins = self.gtins.into_vec_gtin();
        let mut wiki = self.wiki.into_vec_wiki();

        eans.sort();
        gtins.sort();
        wiki.sort();

        StoreProductIds { eans, gtins, wiki }
    }
}

impl Combine for GatherProductIds {
    fn combine(o1: Self, o2: Self) -> Self {
        let eans = Combine::combine(o1.eans, o2.eans);
        let gtins = Combine::combine(o1.gtins, o2.gtins);
        let wiki = Combine::combine(o1.wiki, o2.wiki);
        Self { eans, gtins, wiki }
    }
}

/// Represents a set of product IDs.
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct StoreProductIds {
    /// GTIN of the product.
    pub eans: Vec<SourcedEan>,

    /// GTIN of the product.
    pub gtins: Vec<SourcedGtin>,

    /// Wiki ID.
    pub wiki: Vec<SourcedWikiId>,
}

#[cfg(feature = "into-api")]
impl StoreProductIds {
    pub fn to_api(self) -> api::ProductIds {
        api::ProductIds {
            eans: self.eans.into_iter().map(|id| id.into_api()).collect(),
            gtins: self.gtins.into_iter().map(|id| id.into_api()).collect(),
            wiki: self.wiki.into_iter().map(|id| id.into_api()).collect(),
        }
    }
}

/// Represents a product.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GatherProduct {
    /// Product ID.
    pub ids: GatherProductIds,

    /// Names of the product.
    pub names: MultiMap<String, Source>,

    /// Descriptions of the product.
    pub descriptions: MultiMap<String, Source>,

    /// Product images.
    pub images: BTreeSet<Image>,

    /// Product categories.
    pub categories: MultiMap<String, Source>,

    /// Regions where the product is available.
    pub availability: Availability,

    /// Regions where the product is manufactures.
    pub origins: MultiMap<isocountry::CountryCode, Source>,

    /// Known certifications.
    pub certifications: Certifications,

    /// DB IDs of manufacturers.
    pub manufacturers: MultiMap<ids::OrganisationId, Source>,

    /// Info about possible ways to buy this product.
    pub shopping: MultiMap<ShoppingKey, ShoppingData>,

    /// Mentions in media.
    pub media: BTreeSet<Medium>,

    /// Wikidata IDs newer version products.
    pub follows: BTreeSet<ids::ProductId>,

    /// Wikidata IDs older version products.
    pub followed_by: BTreeSet<ids::ProductId>,

    /// The Transpaer data.
    pub transpaer: TranspaerProductData,
}

impl GatherProduct {
    pub fn store(self) -> StoreProduct {
        let ids = self.ids.store();
        let mut names = self.names.into_vec_text();
        let descriptions = self.descriptions.into_vec_text();
        let mut images: Vec<_> = self.images.into_iter().collect();
        let mut categories = self.categories.into_vec_text();
        let availability = self.availability;
        let origins = self.origins.into_vec_country();
        let certifications = self.certifications;
        let mut manufacturers = self.manufacturers.into_vec_organisation_ids();
        let mut shopping = self.shopping.into_vec_shopping_entry();
        let mut media: Vec<_> = self.media.into_iter().collect();
        let mut follows: Vec<_> = self.follows.into_iter().collect();
        let mut followed_by: Vec<_> = self.followed_by.into_iter().collect();
        let transpaer = self.transpaer;

        names.sort();
        images.sort();
        categories.sort();
        manufacturers.sort();
        shopping.sort();
        media.sort();
        follows.sort();
        followed_by.sort();

        StoreProduct {
            ids,
            names,
            descriptions,
            images,
            categories,
            availability,
            origins,
            certifications,
            manufacturers,
            shopping,
            media,
            follows,
            followed_by,
            transpaer,
        }
    }

    pub fn all_categories(&self, category_separator: char) -> BTreeSet<String> {
        let sep = category_separator.to_string();
        let mut result = BTreeSet::new();
        for category in self.categories.keys() {
            let mut buffer = String::with_capacity(category.len());
            for part in category.split(category_separator) {
                if !buffer.is_empty() {
                    buffer += &sep;
                }
                buffer += part;
                result.insert(buffer.clone());
            }
        }
        result
    }
}

impl Combine for GatherProduct {
    fn combine(mut o1: Self, o2: Self) -> Self {
        let ids = Combine::combine(o1.ids, o2.ids);
        let names = Combine::combine(o1.names, o2.names);
        let descriptions = Combine::combine(o1.descriptions, o2.descriptions);
        let categories = Combine::combine(o1.categories, o2.categories);
        let origins = Combine::combine(o1.origins, o2.origins);
        let availability = Combine::combine(o1.availability, o2.availability);
        let certifications = Combine::combine(o1.certifications, o2.certifications);
        let manufacturers = Combine::combine(o1.manufacturers, o2.manufacturers);
        let shopping = Combine::combine(o1.shopping, o2.shopping);

        // This data is filled after merging all organisations.
        let transpaer = TranspaerProductData::default();

        o1.images.extend(o2.images);
        o1.media.extend(o2.media);
        o1.follows.extend(o2.follows);
        o1.followed_by.extend(o2.followed_by);

        Self {
            ids,
            names,
            descriptions,
            images: o1.images,
            categories,
            availability,
            origins,
            certifications,
            manufacturers,
            shopping,
            media: o1.media,
            follows: o1.follows,
            followed_by: o1.followed_by,
            transpaer,
        }
    }
}

/// Represents a product.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StoreProduct {
    /// Product ID.
    pub ids: StoreProductIds,

    /// Names of the product.
    pub names: Vec<Text>,

    /// Descriptions of the product.
    pub descriptions: Vec<Text>,

    /// Product images.
    pub images: Vec<Image>,

    /// Product categories.
    pub categories: Vec<Text>,

    /// Regions where the product is available.
    pub availability: Availability,

    /// Regions where the product is produced.
    pub origins: Vec<Country>,

    /// Known certifications.
    pub certifications: Certifications,

    /// DB IDs of manufacturers.
    pub manufacturers: Vec<SourcedOrganisationId>,

    /// Info about possible ways to buy this product.
    pub shopping: Vec<ShoppingEntry>,

    /// Mentions in media.
    pub media: Vec<Medium>,

    /// Wikidata IDs newer version products.
    pub follows: Vec<ids::ProductId>,

    /// Wikidata IDs older version products.
    pub followed_by: Vec<ids::ProductId>,

    /// The Transpaer data.
    pub transpaer: TranspaerProductData,
}

#[cfg(feature = "into-api")]
impl StoreProduct {
    pub fn into_api_short(self) -> api::ProductShort {
        api::ProductShort {
            product_ids: self.ids.to_api(),
            name: self.names.first().map_or_else(default_short_string, text_to_short_string),
            description: self.descriptions.first().map(text_to_long_text),
            badges: self.certifications.to_api_badges(),
            scores: self.certifications.to_api_scores(),
        }
    }

    pub fn into_api_full(
        self,
        manufacturers: Vec<api::OrganisationShort>,
        alternatives: Vec<api::CategoryAlternatives>,
    ) -> api::ProductFull {
        let mut medallions = self.certifications.into_api_medallions();
        medallions.push(self.transpaer.score.into_api_medallion());

        api::ProductFull {
            product_ids: self.ids.to_api(),
            names: self.names.into_iter().map(|n| n.into_api_short()).collect(),
            descriptions: self.descriptions.into_iter().map(|d| d.into_api_long()).collect(),
            images: self.images.into_iter().map(|i| i.into_api()).collect(),
            origins: self.origins.into_iter().map(country_code_to_region_code).collect(),
            shopping: self.shopping.into_iter().map(|l| l.into_api()).collect(),
            media: self.media.into_iter().map(|m| m.into_api()).collect(),
            manufacturers,
            alternatives,
            medallions,
        }
    }

    pub fn score(&self) -> f64 {
        0.0 + 0.9 * self.certifications.bcorp.is_some() as u32 as f64
            + 0.9 * self.certifications.eu_ecolabel.is_some() as u32 as f64
            + 0.6 * self.certifications.fti.as_ref().map(|c| 0.01 * c.score as f64).unwrap_or(0.0)
            + 0.3 * self.certifications.tco.is_some() as u32 as f64
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq)]
pub enum CategoryStatus {
    Exploratory,
    Incomplete,
    Satisfactory,
    Complete,
    Broad,
}

#[cfg(feature = "into-api")]
impl CategoryStatus {
    pub fn into_api(self) -> api::CategoryStatus {
        match self {
            Self::Exploratory => api::CategoryStatus::Exploratory,
            Self::Incomplete => api::CategoryStatus::Incomplete,
            Self::Satisfactory => api::CategoryStatus::Satisfactory,
            Self::Complete => api::CategoryStatus::Complete,
            Self::Broad => api::CategoryStatus::Broad,
        }
    }
}

/// Stores all relevant info about a category.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Category {
    /// Progress of the work on this category.
    pub status: CategoryStatus,

    /// List of subcategories.
    pub subcategories: Vec<String>,

    /// List of products in this categories.
    ///
    /// If `None`, the the category does not need products, e.g. it's a very broad category
    /// and product comparisons don't make sense.
    pub products: Option<Vec<ids::ProductId>>,
}

/// One enttry in `PresentationData::Scored`.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ScoredPresentationEntry {
    /// Organisation ID.
    pub wiki_id: ids::WikiId,

    /// Name of the organisation (as originally listed by the certifier).
    pub name: String,

    /// Score from the certifier.
    pub score: i64,
}

#[cfg(feature = "into-api")]
impl ScoredPresentationEntry {
    pub fn into_api(self) -> api::PresentationEntry {
        api::PresentationEntry {
            wiki_id: api::Id::from_str(&self.wiki_id.to_canonical_string())
                .expect("Converting to Wikidata ID"),
            name: str_to_short_string(self.name),
            score: self.score,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct PresentationData {
    pub entries: Vec<ScoredPresentationEntry>,
}

#[cfg(feature = "into-api")]
impl PresentationData {
    fn into_api(self) -> Vec<api::PresentationEntry> {
        self.entries.into_iter().map(|e| e.into_api()).collect()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Presentation {
    /// Topic ID.
    pub id: LibraryTopic,

    /// Data to be presented.
    pub data: PresentationData,
}

#[cfg(feature = "into-api")]
impl Presentation {
    pub fn into_api(self) -> api::Presentation {
        api::Presentation { data: self.data.into_api() }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ReferenceLink {
    /// Title of the reference
    pub title: String,

    /// Link to the reference
    pub link: String,
}

#[cfg(feature = "into-api")]
impl ReferenceLink {
    pub fn into_api(self) -> api::ReferenceLink {
        api::ReferenceLink { title: self.title, link: self.link }
    }
}

/// Represents a topic info.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LibraryItem {
    /// Topic ID.
    pub id: LibraryTopic,

    /// Article title.
    pub title: String,

    /// Short one line summary of the article.
    pub summary: String,

    /// Contents of the article in markdown format.
    pub article: String,

    /// Links to external references to the same topic.
    pub links: Vec<ReferenceLink>,
}

#[cfg(feature = "into-api")]
impl LibraryItem {
    pub fn into_api_short(self) -> api::LibraryItemShort {
        api::LibraryItemShort {
            id: api::LibraryTopic::from(self.id),
            title: str_to_short_string(self.title),
            summary: str_to_short_string(self.summary),
        }
    }

    pub fn into_api_full(self, presentation: Option<api::Presentation>) -> api::LibraryItemFull {
        api::LibraryItemFull {
            id: api::LibraryTopic::from(self.id),
            title: str_to_short_string(self.title),
            summary: str_to_short_string(self.summary),
            article: str_to_long_string(self.article),
            links: self.links.into_iter().map(|link| link.into_api()).collect(),
            presentation,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serde_presentation_with_scored_data_json() {
        let original_presentation = Presentation {
            id: "topic".to_owned(),
            data: PresentationData {
                entries: vec![
                    ScoredPresentationEntry {
                        wiki_id: ids::WikiId::new(1),
                        name: "name1".to_owned(),
                        score: 80,
                    },
                    ScoredPresentationEntry {
                        wiki_id: ids::WikiId::new(2),
                        name: "name2".to_owned(),
                        score: 20,
                    },
                ],
            },
        };

        let original_string = r#"{"id":"topic","data":{"entries":[{"wiki_id":1,"name":"name1","score":80},{"wiki_id":2,"name":"name2","score":20}]}}"#.to_owned();

        let serialized_string = serde_json::to_string(&original_presentation).unwrap();
        assert_eq!(serialized_string, original_string);

        let deserialized_presentation: Presentation =
            serde_json::from_str(&original_string).unwrap();
        assert_eq!(deserialized_presentation, original_presentation);
    }

    #[test]
    fn serde_presentation_with_scored_data_postcard() {
        let original_presentation = Presentation {
            id: "topic".to_owned(),
            data: PresentationData {
                entries: vec![
                    ScoredPresentationEntry {
                        wiki_id: ids::WikiId::new(1),
                        name: "name1".to_owned(),
                        score: 80,
                    },
                    ScoredPresentationEntry {
                        wiki_id: ids::WikiId::new(2),
                        name: "name2".to_owned(),
                        score: 20,
                    },
                ],
            },
        };

        let serialized_presentation = postcard::to_stdvec(&original_presentation).unwrap();
        let deserialized_presentation: Presentation =
            postcard::from_bytes(&serialized_presentation).unwrap();
        assert_eq!(deserialized_presentation, original_presentation);
    }

    #[test]
    fn products_all_categories() {
        let product = GatherProduct {
            categories: MultiMap::new_from_map(maplit::btreemap! {
                "aaa/bbb/ccc".to_string() => maplit::btreeset!{},
                "aaa/bbb".to_string() => maplit::btreeset!{},
                "ddd/eee".to_string() => maplit::btreeset!{},
            }),
            ..GatherProduct::default()
        };
        let expected = maplit::btreeset! {
                "aaa/bbb/ccc".to_string(),
                "aaa/bbb".to_string(),
                "aaa".to_string(),
                "ddd/eee".to_string(),
                "ddd".to_string(),
        };
        let obtained = product.all_categories('/');
        assert_eq!(expected, obtained);
    }
}
