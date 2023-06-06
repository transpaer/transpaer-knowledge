//! This module contains definition of some commonly used errors..

use thiserror::Error;

/// Describes an error occured during parsing an Id.
#[derive(Error, Debug, Eq, PartialEq)]
pub enum ParseIdError {
    /// Part of the ID was expected to be a number but wasn't.
    #[error("Failed to parse number from `{0}`: {1}")]
    Num(String, std::num::ParseIntError),

    /// Length of the ID was wrong.
    #[error("The ID `{0}` has wrong length")]
    Length(String),

    /// The ID didn't contain the expected prefix.
    #[error("The ID `{0}` has unexpected prefix")]
    Prefix(String),
}
