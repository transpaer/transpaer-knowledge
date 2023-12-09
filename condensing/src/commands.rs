use clap::{Parser, Subcommand};

/// Arguments of the `prefilter` command.
#[derive(Parser, Debug)]
#[command(
    about = "First step of fitering",
    long_about = "Wikidata data set is very big and processing it takes a lot of time. \
                  To mitigate that problem we preprocess that data by filtering out the entriess \
                  that we are not interested in. We do that intwo steps and this the first of those steps."
)]
pub struct PrefilteringArgs {
    /// Origin data directory.
    #[arg(long)]
    pub origin: String,

    /// Cache directory.
    #[arg(long)]
    pub cache: String,
}

/// Arguments of the `filter` command.
#[derive(Parser, Debug)]
#[command(
    about = "Second step of fitering",
    long_about = "Wikidata data set is very big and processing it takes a lot of time. \
                  To mitigate that problem we preprocess that data by filtering out the entriess \
                  that we are not interested in. We do that intwo steps and this the second of those steps."
)]
pub struct FilteringArgs {
    /// Origin data directory.
    #[arg(long)]
    pub origin: String,

    /// Source data directory.
    #[arg(long)]
    pub source: String,

    /// Cache directory.
    #[arg(long)]
    pub cache: String,
}

/// Arguments of the `filter` command.
#[derive(Parser, Debug)]
#[command(
    about = "Update source files",
    long_about = "Some data we are processing need to be augmented we additional information \
                  which we prepare manually. With new versions of the original data those manually created \
                  data may become insufficient or obsolete. This command updates the data and points to \
                  any further manual updates required.\n\nCurrently this command updates mapping from \
                  Open Food Facts countries to Sustaininty regions."
)]
pub struct UpdatingArgs {
    /// Origin data directory.
    #[arg(long)]
    pub origin: String,

    /// Source data directory.
    #[arg(long)]
    pub source: String,

    /// Cache directory.
    #[arg(long)]
    pub cache: String,
}

/// Arguments of the `condense` command.
#[derive(Parser, Debug)]
#[command(
    about = "Process big input data sources",
    long_about = "Processes all available data sources to create an new version of Sustainity database"
)]
pub struct CondensationArgs {
    /// Origin data directory.
    #[arg(long)]
    pub origin: String,

    /// Source data directory.
    #[arg(long)]
    pub source: String,

    /// Cache directory.
    #[arg(long)]
    pub cache: String,

    /// Target data directory.
    #[arg(long)]
    pub target: String,
}

/// Arguments of the `transcribe` command.
#[derive(Parser, Debug)]
#[command(
    about = "Convert backend config files to format storable in the database",
    long_about = "Some of the data we store in the database (e.g. texts and articles we show on the web page) \
                  can be processed quickly because don't require access to large data sources like Wikidata. \
                  This command runs this processing basically transcribing some human readable files into \
                  a format that can be imported by the database."
)]
pub struct TranscriptionArgs {
    /// Source data directory.
    #[arg(long)]
    pub source: String,

    /// Library data directory.
    #[arg(long)]
    pub library: String,

    /// Target data directory.
    #[arg(long)]
    pub target: String,
}

/// Arguments of the `analyse` command.
#[derive(Parser, Debug)]
#[command(
    about = "Run an analysis of input data",
    long_about = "Runs an analysis of input data to find ways to improve the processing of those data.\n\n\
                  Currently this command only looks for entry classes in Wikidata and looks for those \
                  contain but do not correspond to any product category."
)]
pub struct AnalysisArgs {
    /// Cache directory.
    #[arg(long)]
    pub cache: String,
}

/// Arguments of the `connect` command.
#[derive(Parser, Debug)]
#[command(
    about = "Try to connect companies of products known mainly only by name to entries in Wikidata",
    long_about = "Using fuzzy estimations tries to connect companies and products from data sources like \
                  Open Food Facts and EU Ecolabel data (which frequently don't contain identifiers) \
                  to entries in Wikidata. The methods used cannot guaranty correctness of connections, \
                  so in the future we would like to avoid using this approach."
)]
pub struct ConnectionArgs {
    #[arg(long)]
    pub wikidata_path: String,

    #[arg(long)]
    pub origin: String,

    #[arg(long)]
    pub source: String,
}

/// All arguments of the program.
#[derive(Subcommand, Debug)]
pub enum Commands {
    Prefilter(PrefilteringArgs),
    Filter(FilteringArgs),
    Update(UpdatingArgs),
    Condense(CondensationArgs),
    Transcribe(TranscriptionArgs),
    Analyze(AnalysisArgs),
    Connect(ConnectionArgs),
}

/// Program arguments.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// Commands.
    #[command(subcommand)]
    pub command: Commands,
}
