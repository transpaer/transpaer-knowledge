use crate::{advisors, config, errors, knowledge};

fn convert_info(info: &consumers_collecting::consumers::data::Info) -> knowledge::Info {
    knowledge::Info {
        id: info.id.clone(),
        title: info.title.clone(),
        article: info.article.clone(),
    }
}

pub struct Transcriptor;

impl Transcriptor {
    pub fn transcribe(config: &config::TranscriptionConfig) -> Result<(), errors::ProcessingError> {
        let consumers = advisors::ConsumersAdvisor::load(&config.consumers_path)?;
        let info: Vec<knowledge::Info> = consumers.get_info().iter().map(convert_info).collect();
        let contents = serde_json::to_string_pretty(&info)?;
        std::fs::write(&config.target_info_path, contents)?;
        Ok(())
    }
}
