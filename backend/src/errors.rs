use snafu::prelude::*;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum DbError {
    #[snafu(display("Connection: {source}"))]
    Connect { source: arangors::ClientError },

    #[snafu(display("Database: {source}"))]
    Database { source: arangors::ClientError, name: String },

    #[snafu(display("Query: {source}\n{query}"))]
    Query { source: arangors::ClientError, query: String },
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum BackendError {
    #[snafu(context(false), display("Database: {source}"))]
    Db { source: DbError },

    #[snafu(context(false), display("Model conversion: {source}"))]
    Convert { source: sustainity_models::models::IntoApiError },
}

impl From<BackendError> for swagger::ApiError {
    fn from(error: BackendError) -> Self {
        let message = error.to_string();
        log::error!("{}", message);
        Self(message)
    }
}
