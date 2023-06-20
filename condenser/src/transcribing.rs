use crate::{advisors, config, errors, knowledge};

fn convert_info(
    info: &sustainity_collecting::sustainity::data::LibraryInfo,
) -> knowledge::LibraryInfo {
    knowledge::LibraryInfo {
        id: info.id.clone(),
        title: info.title.clone(),
        article: info.article.clone(),
    }
}

pub struct Transcriptor;

impl Transcriptor {
    pub fn transcribe(config: &config::TranscriptionConfig) -> Result<(), errors::ProcessingError> {
        let sustainity = advisors::SustainityAdvisor::load(&config.library_source_path)?;
        let library: Vec<knowledge::LibraryInfo> =
            sustainity.get_info().iter().map(convert_info).collect();
        serde_jsonlines::write_json_lines(&config.library_target_path, library)?;
        Ok(())
    }
}
