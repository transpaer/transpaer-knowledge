// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::io::Write;

use transpaer_collecting::{
    errors::{MapIo, MapSerde},
    fetch_info::FetchInfo,
};

use crate::{config, errors};

const USER_AGENT: &str = "transpaer-lab";
const BCORP_DOWNLOAD_URL: &str = "https://download.data.world/file_download/blab/b-corp-impact-data/B%20Corp%20Impact%20Data.csv?dwr=US";
const EU_ECOLABEL_DOWNLOAD_URL: &str =
    "https://publicstorage.data.env.service.ec.europa.eu/ecolabel/exports/most-recent-export.csv";
const OPEN_FOOD_FACTS_DOWNLOAD_URL: &str =
    "https://static.openfoodfacts.org/data/en.openfoodfacts.org.products.csv.gz";
const OPEN_FOOD_REPO_INITIAL_PAGE: &str =
    "https://www.foodrepo.org/api/v3/products?page[number]=1&page[size]=200";
const WIKIDATA_DOWNLOAD_URL: &str =
    "https://dumps.wikimedia.org/wikidatawiki/entities/20251117/wikidata-20251117-all.json.gz";

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
            config::AbsorbingSubconfig::BCorp(subconfig) => {
                Self::run_bcorp(&config.origin, &config.meta, subconfig).await?;
            }
            config::AbsorbingSubconfig::EuEcolabel(subconfig) => {
                Self::run_eu_ecolabel(&config.origin, &config.meta, subconfig).await?;
            }
            config::AbsorbingSubconfig::OpenFoodFacts(subconfig) => {
                Self::run_open_food_facts(&config.origin, &config.meta, subconfig).await?;
            }
            config::AbsorbingSubconfig::OpenFoodRepo(subconfig) => {
                Self::run_open_food_repo(&config.origin, &config.meta, subconfig).await?;
            }
            config::AbsorbingSubconfig::Wikidata(subconfig) => {
                Self::run_wikidata(&config.origin, &config.meta, subconfig).await?;
            }
        }
        Ok(())
    }

    async fn run_bcorp(
        origin: &config::OriginConfig,
        meta: &config::MetaConfig,
        config: &config::AbsorbingBCorpConfig,
    ) -> Result<(), errors::AbsorbingError> {
        let path = &origin.bcorp_path;
        let client = reqwest::ClientBuilder::new().user_agent(USER_AGENT).build()?;

        println!("Fetching data");
        let resp = client
            .get(BCORP_DOWNLOAD_URL)
            .header(reqwest::header::AUTHORIZATION, format!("Bearer {}", config.token))
            .send()
            .await?;
        let contents = resp.text().await?;

        println!("Saving data");
        std::fs::write(path, contents).map_with_path(path)?;

        println!("Updating fetch info");
        let mut info = FetchInfo::read(&meta.absorbents)?;
        info.update_bcorp();
        info.write(&meta.absorbents)?;

        Ok(())
    }

    async fn run_eu_ecolabel(
        origin: &config::OriginConfig,
        meta: &config::MetaConfig,
        _config: &config::AbsorbingEuEcolabelConfig,
    ) -> Result<(), errors::AbsorbingError> {
        let path = &origin.eu_ecolabel_path;
        let client = reqwest::ClientBuilder::new().user_agent(USER_AGENT).build()?;

        println!("Fetching data");
        let resp = client.get(EU_ECOLABEL_DOWNLOAD_URL).send().await?;
        let contents = resp.text().await?;

        println!("Saving data");
        std::fs::write(path, contents).map_with_path(path)?;

        println!("Updating fetch info");
        let mut info = FetchInfo::read(&meta.absorbents)?;
        info.update_eu_ecolabel();
        info.write(&meta.absorbents)?;

        Ok(())
    }

    #[allow(clippy::cast_precision_loss)]
    async fn run_open_food_facts(
        origin: &config::OriginConfig,
        meta: &config::MetaConfig,
        _config: &config::AbsorbingOpenFoodFactsConfig,
    ) -> Result<(), errors::AbsorbingError> {
        let path = &origin.open_food_facts_path;
        let client = reqwest::ClientBuilder::new().user_agent(USER_AGENT).build()?;

        println!("Fetching data");
        let mut resp = client.get(OPEN_FOOD_FACTS_DOWNLOAD_URL).send().await?;
        let content_length = resp.content_length();

        println!("Saving data");
        let mut file = std::fs::File::create(path)
            .map_err(|e| errors::AbsorbingError::CreateFile(e, path.into()))?;
        let mut bytes: usize = 0;
        while let Some(chunk) = resp.chunk().await? {
            let buf = &chunk;
            bytes += buf.len();
            file.write_all(buf).map_with_path(path)?;
            if let Some(content_length) = &content_length {
                print!(" Downloading: {:>6.2}%\r", 100.0 * bytes as f64 / *content_length as f64);
            } else {
                print!(" Downloading: {bytes}B\r");
            }
        }
        println!();

        println!("Updating fetch info");
        let mut info = FetchInfo::read(&meta.absorbents)?;
        info.update_open_food_facts();
        info.write(&meta.absorbents)?;

        Ok(())
    }

    async fn run_open_food_repo(
        origin: &config::OriginConfig,
        meta: &config::MetaConfig,
        config: &config::AbsorbingOpenFoodRepoConfig,
    ) -> Result<(), errors::AbsorbingError> {
        let path = &origin.open_food_repo_path;
        let client = reqwest::ClientBuilder::new().user_agent(USER_AGENT).build()?;

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

        println!("Updating fetch info");
        let mut info = FetchInfo::read(&meta.absorbents)?;
        info.update_open_food_repo();
        info.write(&meta.absorbents)?;

        Ok(())
    }

    #[allow(clippy::cast_precision_loss)]
    async fn run_wikidata(
        origin: &config::OriginConfig,
        meta: &config::MetaConfig,
        _config: &config::AbsorbingWikidataConfig,
    ) -> Result<(), errors::AbsorbingError> {
        let path = &origin.wikidata_path;
        let client = reqwest::ClientBuilder::new().user_agent(USER_AGENT).build()?;

        println!("Fetching data");
        let mut resp = client.get(WIKIDATA_DOWNLOAD_URL).send().await?;
        let content_length = resp.content_length();

        println!("Saving data");
        let mut file = std::fs::File::create(path)
            .map_err(|e| errors::AbsorbingError::CreateFile(e, path.into()))?;
        let mut bytes: usize = 0;
        while let Some(chunk) = resp.chunk().await? {
            let buf = &chunk;
            bytes += buf.len();
            file.write_all(buf).map_with_path(path)?;
            if let Some(content_length) = &content_length {
                print!(" Downloading: {:>6.2}%\r", 100.0 * bytes as f64 / *content_length as f64);
            } else {
                print!(" Downloading: {bytes}B\r");
            }
        }
        println!();

        println!("Updating fetch info");
        let mut info = FetchInfo::read(&meta.absorbents)?;
        info.update_open_food_facts();
        info.write(&meta.absorbents)?;

        Ok(())
    }
}
