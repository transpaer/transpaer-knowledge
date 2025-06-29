use snafu::prelude::*;

use sustainity_models::{buckets::BucketError, ids::ParseIdError};

#[derive(Debug)]
pub enum InputVariant {
    WikiId,
    Ean,
    Gtin,
    VatId,
}

impl std::fmt::Display for InputVariant {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum BackendError {
    #[snafu(context(false), display("Bucket: {source}"))]
    Bucket { source: BucketError },

    #[snafu(display("Parsing request input `{input}` as {variant}: {source}"))]
    ParsingInput { source: ParseIdError, input: String, variant: InputVariant },

    #[snafu(context(false), display("Model conversion: {source}"))]
    Convert { source: sustainity_models::models::IntoApiError },
}

impl From<BackendError> for swagger::ApiError {
    fn from(error: BackendError) -> Self {
        let message = error.to_string();
        log::error!("{message}");
        Self(message)
    }
}
