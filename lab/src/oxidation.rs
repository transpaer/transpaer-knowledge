// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use transpaer_models::{buckets, store};

use crate::{advisors, config, errors};

pub struct Oxidizer;

impl Oxidizer {
    /// Runs the oxidation command.
    ///
    /// # Errors
    ///
    /// Returns `Err` if reading, parsing or saving required data failed.
    pub fn run(config: &config::OxidationConfig) -> Result<(), errors::ProcessingError> {
        let store = buckets::AppStore::new(&config.app_storage)?;
        Self::transcribe_library(&store, config)?;
        Self::create_presentations(&store, config)?;
        Ok(())
    }

    fn transcribe_library(
        store: &buckets::AppStore,
        config: &config::OxidationConfig,
    ) -> Result<(), errors::ProcessingError> {
        let library = store.get_library_bucket()?;
        let transpaer = advisors::TranspaerLibraryAdvisor::load(&config.library_file_path)?;
        for info in transpaer.get_info() {
            let id: &str = serde_variant::to_variant_name(&info.id)?;
            let article_path = config.library_dir_path.join(id).with_extension("md");
            crate::utils::file_exists(&article_path)?;

            let article = std::fs::read_to_string(&article_path)
                .map_err(|e| errors::ProcessingError::Io(e, article_path.clone()))?;
            let topic = info.id.to_str().to_owned();
            let links = info
                .links
                .clone()
                .unwrap_or_default()
                .into_iter()
                .map(|link| store::ReferenceLink { link: link.link, title: link.title })
                .collect();

            log::info!(" - `{topic}` from `{}`", article_path.display());
            library.insert(
                &topic,
                &store::LibraryItem {
                    id: topic.clone(),
                    title: info.title.clone(),
                    summary: info.summary.clone(),
                    article,
                    links,
                },
            )?;
        }
        log::info!("Saving {} topics", library.len());
        library.flush()?;
        Ok(())
    }

    fn create_presentations(
        store: &buckets::AppStore,
        config: &config::OxidationConfig,
    ) -> Result<(), errors::ProcessingError> {
        let fti = advisors::FashionTransparencyIndexAdvisor::load(
            &config.fashion_transparency_index_path,
        )?;

        let fti = fti.prepare_presentation();

        let presentations = store.get_presentation_bucket()?;
        presentations.insert(&fti.id.clone(), &fti)?;
        presentations.flush()?;
        Ok(())
    }
}
