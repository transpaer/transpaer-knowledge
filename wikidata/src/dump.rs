// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::{
    future::Future,
    io::{BufRead, Seek},
};

use thiserror::Error;

/// Error returned id fig checking failed.
/// Error returned when a problem with IO or sending over channel occured.
#[derive(Error, Debug)]
pub enum LoaderError {
    #[error("In file `{1}`.\nIO error: {0}")]
    Io(std::io::Error, std::path::PathBuf),

    #[error("Unknown compression method: {0:?}")]
    CompressionMethod(Option<String>),
}

/// Compression method used in the dump.
#[derive(Clone, Debug)]
enum CompressionMethod {
    /// `json` or `jsonl` file.
    None,

    /// `json.gz` file.
    Gz,

    /// `json.bz2` file.
    Bz2,
}

/// Reads Wikidata dump file in.
///
/// The Wikidata dump file, which is a compressed json file, is very big. The unpacked version would
/// take more than 1TB. To make it possible to process this file, this reader parses the json file
/// while uncompressing the dump, without need for extracting the compressed file in advance.
///
/// The Wikidata dumpfile is compressed using either `gzip` or `bz2` algorithm. The `gzip` vesion is
/// composed of many confactenated zips, which in not supported by `flate2-rs`
/// (`https://github.com/rust-lang/flate2-rs/issues/23`). Parsing such concatenated zip structure
/// had to be implemented within this reader.
#[derive(Debug)]
pub struct Loader {
    /// Reader of the zip file.
    reader: std::io::BufReader<std::fs::File>,

    /// Compression method to use.
    compression_method: CompressionMethod,

    /// Path to the loaded file. Needed only for error reporting.
    path: std::path::PathBuf,
}

impl Loader {
    /// Constructs a new `Loader`.
    ///
    /// # Errors
    ///
    /// Returns `Err` if fails to read from `path`.
    pub fn load(path: &std::path::Path) -> Result<Self, LoaderError> {
        let compression_method = match path.extension().and_then(std::ffi::OsStr::to_str) {
            Some("json" | "jsonl") => CompressionMethod::None,
            Some("gz") => CompressionMethod::Gz,
            Some("bz2") => CompressionMethod::Bz2,
            method => {
                return Err(LoaderError::CompressionMethod(
                    method.map(std::string::ToString::to_string),
                ))
            }
        };

        let path = path.to_owned();
        let file = std::fs::File::open(&path).map_err(|e| LoaderError::Io(e, path.clone()))?;
        let reader = std::io::BufReader::new(file);

        Ok(Self { reader, compression_method, path })
    }

    /// Parses the Wikidata dump file while unzipping it and sends the parsed out entries to the
    /// passed channel.
    ///
    /// # Errors
    ///
    /// Returns `Err` if fails to
    /// - read from the file
    /// - unzip the file
    /// - seek position in the file
    /// - send a message over channel
    pub async fn run<C, F>(mut self, callback: C) -> Result<usize, LoaderError>
    where
        C: Fn(String) -> F,
        F: Future<Output = ()>,
    {
        match self.compression_method {
            CompressionMethod::Gz => self.run_gz(callback).await,
            CompressionMethod::Bz2 => self.run_bz2(callback).await,
            CompressionMethod::None => self.run_none(callback).await,
        }
    }

    async fn run_gz<C, F>(&mut self, mut callback: C) -> Result<usize, LoaderError>
    where
        C: Fn(String) -> F,
        F: Future<Output = ()>,
    {
        let mut entries: usize = 0;

        self.reader
            .seek(std::io::SeekFrom::End(0))
            .map_err(|e| LoaderError::Io(e, self.path.clone()))?;
        let file_size =
            self.reader.stream_position().map_err(|e| LoaderError::Io(e, self.path.clone()))?;
        self.reader
            .seek(std::io::SeekFrom::Start(0))
            .map_err(|e| LoaderError::Io(e, self.path.clone()))?;

        loop {
            let decoder = flate2::bufread::GzDecoder::new(&mut self.reader);
            for line in std::io::BufReader::new(decoder).lines() {
                let line = line.map_err(|e| LoaderError::Io(e, self.path.clone()))?;
                entries += Self::handle_line(&mut callback, &line).await?;
            }

            let stream_position =
                self.reader.stream_position().map_err(|e| LoaderError::Io(e, self.path.clone()))?;
            if stream_position == file_size {
                break;
            }
        }
        Ok(entries)
    }

    async fn run_bz2<C, F>(&mut self, mut callback: C) -> Result<usize, LoaderError>
    where
        C: Fn(String) -> F,
        F: Future<Output = ()>,
    {
        let mut entries: usize = 0;

        let decoder = bzip2::bufread::MultiBzDecoder::new(&mut self.reader);
        for line in std::io::BufReader::new(decoder).lines() {
            let line = line.map_err(|e| LoaderError::Io(e, self.path.clone()))?;
            entries += Self::handle_line(&mut callback, &line).await?;
        }

        Ok(entries)
    }

    async fn run_none<C, F>(&mut self, mut callback: C) -> Result<usize, LoaderError>
    where
        C: Fn(String) -> F,
        F: Future<Output = ()>,
    {
        let mut entries: usize = 0;

        for line in std::io::BufReader::new(&mut self.reader).lines() {
            let line = line.map_err(|e| LoaderError::Io(e, self.path.clone()))?;
            entries += Self::handle_line(&mut callback, &line).await?;
        }

        Ok(entries)
    }

    fn should_ignore_line(line: &str) -> bool {
        line == "," || line == "[" || line == "]" || line.is_empty()
    }

    async fn handle_line<C, F>(callback: &mut C, line: &str) -> Result<usize, LoaderError>
    where
        C: Fn(String) -> F,
        F: Future<Output = ()>,
    {
        if Self::should_ignore_line(line) {
            return Ok(0);
        }

        let json_str =
            if line.ends_with(',') { line.strip_suffix(',').unwrap_or("") } else { line };

        callback(json_str.to_string()).await;
        Ok(1)
    }
}
