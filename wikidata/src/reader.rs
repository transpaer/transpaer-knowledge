use std::io::{BufRead, Seek};

/// Reads in Wikidata dump file.
///
/// The Wikidata dump file, which is a zipped json file, is very big. The unpacked version would
/// take more than 1TB. To make it possivle to process this file, this reader parses the json file
/// already while uncompressing the zip.
///
/// The Wikidata dumpfile zip is composed of many confactenated zips, which in not supported by
/// `flate2-rs` (https://github.com/rust-lang/flate2-rs/issues/23). PArsing such concatenated zip
/// structure had to be implemented withing this reader.
pub struct WikidataReader {
    /// Reader of the zip file.
    reader: std::io::BufReader<std::fs::File>,
}

impl WikidataReader {
    /// Constructs a new `WikidataReader`.
    pub fn new<P: AsRef<std::path::Path>>(path: P) -> Self {
        let file = std::fs::File::open(path).unwrap();
        let reader = std::io::BufReader::new(file);

        Self { reader }
    }

    /// Parses the Wikidata dump file while unzipping it and sends the parsed out entries to the
    /// passed channel.
    pub async fn run_with_channel(
        &mut self,
        tx: async_channel::Sender<String>,
    ) -> Result<usize, std::io::Error> {
        let mut entries: usize = 0;

        self.reader.seek(std::io::SeekFrom::End(0)).unwrap();
        let file_size = self.reader.stream_position().unwrap();
        self.reader.seek(std::io::SeekFrom::Start(0)).unwrap();

        loop {
            let decoder = flate2::bufread::GzDecoder::new(&mut self.reader);
            for line in std::io::BufReader::new(decoder).lines() {
                entries += Self::handle_line(&tx, &line?).await;
            }

            if self.reader.stream_position().unwrap() == file_size {
                break;
            }
        }
        tx.close();
        Ok(entries)
    }

    fn should_ignore_line(line: &str) -> bool {
        line == "," || line == "[" || line == "]" || line == ""
    }

    async fn handle_line(tx: &async_channel::Sender<String>, line: &str) -> usize {
        if Self::should_ignore_line(line) {
            return 0;
        }

        let json_str = if line.ends_with(",") { line.strip_suffix(",").unwrap() } else { line };

        tx.send(json_str.to_string()).await.unwrap();
        return 1;
    }
}
