use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug)]
pub enum SearchResultVariant {
    Organisation,
    Product,
}

impl SearchResultVariant {
    pub fn convert(
        self,
        input: Vec<SearchResult>,
    ) -> Vec<sustainity_api::models::TextSearchResult> {
        let variant = sustainity_api::models::TextSearchResultVariant::from(self);
        let mut output = Vec::with_capacity(input.len());
        for result in input {
            output.push(sustainity_api::models::TextSearchResult {
                id: result.id,
                label: result.name.map_or_else(String::default, |t| t.text),
                variant,
            });
        }
        output
    }
}

impl From<SearchResultVariant> for sustainity_api::models::TextSearchResultVariant {
    fn from(value: SearchResultVariant) -> Self {
        match value {
            SearchResultVariant::Organisation => Self::Organisation,
            SearchResultVariant::Product => Self::Product,
        }
    }
}

/// Represents a search result.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SearchResult {
    /// DB entry ID.
    #[serde(rename = "id")]
    pub id: String,

    /// Product name.
    #[serde(rename = "name")]
    pub name: Option<sustainity_models::read::Text>,
}
