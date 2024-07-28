use sustainity_models::gather as models;

use crate::{advisors, config, errors};

pub struct Oxidizer;

impl Oxidizer {
    /// Runs the oxidation command.
    ///
    /// # Errors
    ///
    /// Returns `Err` if reading, parsing or saving required data failed.
    pub fn run(config: &config::OxidationConfig) -> Result<(), errors::ProcessingError> {
        Self::transcribe_library(config)?;
        Self::create_presentations(config)?;
        Ok(())
    }

    fn transcribe_library(config: &config::OxidationConfig) -> Result<(), errors::ProcessingError> {
        let sustainity = advisors::SustainityLibraryAdvisor::load(&config.library_file_path)?;
        let mut library = Vec::<models::LibraryItem>::new();
        for info in sustainity.get_info() {
            let id: &str = serde_variant::to_variant_name(&info.id)?;
            let article_path = config.library_dir_path.join(id).with_extension("md");
            crate::utils::path_exists(&article_path)?;

            let article = std::fs::read_to_string(&article_path)?;
            library.push(models::LibraryItem {
                id: info.id.to_str().into(),
                title: info.title.clone(),
                summary: info.summary.clone(),
                article,
            });
        }
        serde_jsonlines::write_json_lines(&config.library_target_path, library)?;
        Ok(())
    }

    fn create_presentations(
        config: &config::OxidationConfig,
    ) -> Result<(), errors::ProcessingError> {
        let fti = advisors::FashionTransparencyIndexAdvisor::load(
            &config.fashion_transparency_index_path,
        )?;

        let mut presentations = vec![fti.prepare_presentation()];

        presentations.sort_by(|a, b| a.id.cmp(&b.id));

        serde_jsonlines::write_json_lines(&config.presentations_path, &presentations)?;
        Ok(())
    }
}
