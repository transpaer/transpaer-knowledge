use crate::ids;

const ORGANISATIONS_COLLECTION: &str = "organisations";
const ORGANISATION_KEYWORDS_COLLECTION: &str = "organisation_keywords";
const ORGANISATION_VAT_IDS_COLLECTION: &str = "organisation_vat_ids";
const ORGANISATION_WIKI_IDS_COLLECTION: &str = "organisation_wiki_ids";
const ORGANISATION_WWWS_COLLECTION: &str = "organisation_wwws";
const PRODUCTS_COLLECTION: &str = "products";
const PRODUCT_KEYWORDS_COLLECTION: &str = "product_keywords";
const PRODUCT_EANS_COLLECTION: &str = "product_eans";
const PRODUCT_GTINS_COLLECTION: &str = "product_gtins";
const PRODUCT_WIKI_IDS_COLLECTION: &str = "product_wiki_ids";
const CATEGORIES_COLLECTION: &str = "categories";

pub struct KeyId {
    pub key: String,
    pub id: String,
}

pub fn organisation(org_id: &ids::OrganisationId) -> KeyId {
    let key = org_id.to_canonical_string();
    let id = format!("{}/{}", ORGANISATIONS_COLLECTION, key);
    KeyId { key, id }
}

pub fn organisation_vat(vat: &ids::VatId) -> KeyId {
    let key = vat.to_canonical_string();
    let id = format!("{}/{}", ORGANISATION_VAT_IDS_COLLECTION, key);
    KeyId { key, id }
}

pub fn organisation_wiki(wiki: &ids::WikiId) -> KeyId {
    let key = wiki.to_canonical_string();
    let id = format!("{}/{}", ORGANISATION_WIKI_IDS_COLLECTION, key);
    KeyId { key, id }
}

pub fn organisation_www(domain: &str) -> KeyId {
    let key = domain.to_owned();
    let id = format!("{}/{}", ORGANISATION_WWWS_COLLECTION, key);
    KeyId { key, id }
}

pub fn organisation_keyword(keyword: &str) -> KeyId {
    let digest = md5::compute(keyword.as_bytes());
    let key = format!("{:x}", digest);
    let id = format!("{}/{}", ORGANISATION_KEYWORDS_COLLECTION, key);
    KeyId { key, id }
}

pub fn product(prod_id: &ids::ProductId) -> KeyId {
    let key = prod_id.to_canonical_string();
    let id = format!("{}/{}", PRODUCTS_COLLECTION, key);
    KeyId { key, id }
}

pub fn product_ean(ean: &ids::Ean) -> KeyId {
    let key = ean.to_canonical_string();
    let id = format!("{}/{}", PRODUCT_EANS_COLLECTION, key);
    KeyId { key, id }
}

pub fn product_gtin(gtin: &ids::Gtin) -> KeyId {
    let key = gtin.to_canonical_string();
    let id = format!("{}/{}", PRODUCT_GTINS_COLLECTION, key);
    KeyId { key, id }
}

pub fn product_wiki(wiki: &ids::WikiId) -> KeyId {
    let key = wiki.to_canonical_string();
    let id = format!("{}/{}", PRODUCT_WIKI_IDS_COLLECTION, key);
    KeyId { key, id }
}

pub fn product_keyword(keyword: &str) -> KeyId {
    let digest = md5::compute(keyword.as_bytes());
    let key = format!("{:x}", digest);
    let id = format!("{}/{}", PRODUCT_KEYWORDS_COLLECTION, key);
    KeyId { key, id }
}

pub fn category(category: &str) -> KeyId {
    let key = category.to_owned();
    let id = format!("{}/{}", CATEGORIES_COLLECTION, key);
    KeyId { key, id }
}
