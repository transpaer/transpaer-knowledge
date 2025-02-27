use sustainity_models::{buckets, store};

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
        let sustainity = advisors::SustainityLibraryAdvisor::load(&config.library_file_path)?;
        for info in sustainity.get_info() {
            let id: &str = serde_variant::to_variant_name(&info.id)?;
            let article_path = config.library_dir_path.join(id).with_extension("md");
            crate::utils::path_exists(&article_path)?;

            let article = std::fs::read_to_string(&article_path)?;
            let topic = info.id.to_str().to_owned();
            library.insert(
                &topic,
                &store::LibraryItem {
                    id: topic.clone(),
                    title: info.title.clone(),
                    summary: info.summary.clone(),
                    article,
                },
            )?;
        }
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
