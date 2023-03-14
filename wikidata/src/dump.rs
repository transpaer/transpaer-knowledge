use std::io::{BufRead, Seek};

use thiserror::Error;

/// Error returned id fig checking failed.
/// Error returned when a problem with IO or sending over channel occured.
#[derive(Error, Debug)]
pub enum IoOrChannelError {
    #[error("IO error: {0}")]
    Io(std::io::Error),

    #[error("Channel sending error: {0}")]
    Channel(async_channel::SendError<std::string::String>),
}

impl From<std::io::Error> for IoOrChannelError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<async_channel::SendError<std::string::String>> for IoOrChannelError {
    fn from(error: async_channel::SendError<std::string::String>) -> Self {
        Self::Channel(error)
    }
}

/// Reads in Wikidata dump file.
///
/// The Wikidata dump file, which is a zipped json file, is very big. The unpacked version would
/// take more than 1TB. To make it possivle to process this file, this reader parses the json file
/// already while uncompressing the zip.
///
/// The Wikidata dumpfile zip is composed of many confactenated zips, which in not supported by
/// `flate2-rs` (`https://github.com/rust-lang/flate2-rs/issues/23`). Parsing such concatenated zip
/// structure had to be implemented withing this reader.
pub struct Loader {
    /// Reader of the zip file.
    reader: std::io::BufReader<std::fs::File>,
}

impl Loader {
    /// Constructs a new `Loader`.
    ///
    /// # Errors
    ///
    /// Returns `Err` if fails to read from `path`.
    pub fn load<P: AsRef<std::path::Path>>(path: P) -> Result<Self, std::io::Error> {
        let file = std::fs::File::open(path)?;
        let reader = std::io::BufReader::new(file);

        Ok(Self { reader })
    }

    /// Parses the Wikidata dump file while unzipping it and sends the parsed out entries to the
    /// passed channel.
    ///
    /// # Errors
    ///
    /// Returns `Err` if fails to
    /// - read from the file
    /// - seek position in the file
    /// - send a message over channel
    pub async fn run_with_channel(
        &mut self,
        tx: async_channel::Sender<String>,
    ) -> Result<usize, IoOrChannelError> {
        let mut entries: usize = 0;

        self.reader.seek(std::io::SeekFrom::End(0))?;
        let file_size = self.reader.stream_position()?;
        self.reader.seek(std::io::SeekFrom::Start(0))?;

        loop {
            let decoder = flate2::bufread::GzDecoder::new(&mut self.reader);
            for line in std::io::BufReader::new(decoder).lines() {
                entries += Self::handle_line(&tx, &line?).await?;
            }

            if self.reader.stream_position()? == file_size {
                break;
            }
        }
        tx.close();
        Ok(entries)
    }

    fn should_ignore_line(line: &str) -> bool {
        line == "," || line == "[" || line == "]" || line.is_empty()
    }

    async fn handle_line(
        tx: &async_channel::Sender<String>,
        line: &str,
    ) -> Result<usize, IoOrChannelError> {
        if Self::should_ignore_line(line) {
            return Ok(0);
        }

        let json_str =
            if line.ends_with(',') { line.strip_suffix(',').unwrap_or("") } else { line };

        tx.send(json_str.to_string()).await?;
        Ok(1)
    }
}
