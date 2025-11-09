// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use transpaer_collecting::{
    errors::{MapIo, MapSerde},
    fetch_info::FetchInfo,
};

use crate::{config, errors};

const OPEN_FOOD_REPO_INITIAL_PAGE: &str =
    "https://www.foodrepo.org/api/v3/products?page[number]=1&page[size]=200";

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct OpenFoodRepoProducts {
    data: Vec<serde_json::Value>,
    links: OpenFoodRepoProductsLinks,
    meta: serde_json::Value,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct OpenFoodRepoProductsLinks {
    last: Option<String>,
    next: Option<String>,
    #[serde(rename = "self")]
    this: String,
}

pub struct Absorber;

impl Absorber {
    /// Runs the `absorb` command.
    pub async fn run(config: &config::AbsorbingConfig) -> Result<(), errors::ProcessingError> {
        match &config.sub {
            config::AbsorbingSubconfig::OpenFoodRepo(subconfig) => {
                Self::run_open_food_repo(&config.origin, &config.meta, subconfig).await?;
            }
        }
        Ok(())
    }

    async fn run_open_food_repo(
        origin: &config::OriginConfig,
        meta: &config::MetaConfig,
        config: &config::AbsorbingOpenFoodRepoConfig,
    ) -> Result<(), errors::AbsorbingError> {
        let path = &origin.open_food_repo_path;
        let client = reqwest::ClientBuilder::new().user_agent("transpaer-lab").build()?;

        println!("Fetching data");
        let mut entries = 0;
        let mut next = Some(OPEN_FOOD_REPO_INITIAL_PAGE.to_string());
        while let Some(address) = next {
            let resp = client
                .get(address)
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .header(reqwest::header::AUTHORIZATION, format!("Token token={}", config.api_key))
                .send()
                .await?;
            let text = resp.text().await?;
            let products: OpenFoodRepoProducts = serde_json::from_str(&text).map_serde()?;
            if entries == 0 {
                serde_jsonlines::write_json_lines(path, &products.data).map_with_path(path)?;
            } else {
                serde_jsonlines::append_json_lines(path, &products.data).map_with_path(path)?;
            }
            next = products.links.next;
            entries += products.data.len();
            println!(" - {entries} entries processed");
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }

        let mut info = FetchInfo::read(&meta.absorbents)?;
        info.update_open_food_repo();
        info.write(&meta.absorbents)?;

        Ok(())
    }
}
