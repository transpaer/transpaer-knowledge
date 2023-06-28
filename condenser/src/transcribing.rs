use crate::{advisors, config, errors, knowledge};

pub struct Transcriptor;

impl Transcriptor {
    pub fn transcribe(config: &config::TranscriptionConfig) -> Result<(), errors::ProcessingError> {
        let sustainity = advisors::SustainityAdvisor::load(&config.library_file_path)?;
        let mut library = Vec::<knowledge::LibraryInfo>::new();
        for info in sustainity.get_info() {
            let id: &str = serde_variant::to_variant_name(&info.id)?;
            let article_path = config.library_dir_path.join(id).with_extension("md");
            crate::utils::path_exists(&article_path)?;

            let article = std::fs::read_to_string(&article_path)?;
            library.push(knowledge::LibraryInfo {
                id: info.id.clone(),
                title: info.title.clone(),
                summary: info.summary.clone(),
                article,
            });
        }
        serde_jsonlines::write_json_lines(&config.library_target_path, library)?;
        Ok(())
    }
}
