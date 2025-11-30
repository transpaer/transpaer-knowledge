// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use clap::{Parser, Subcommand, ValueEnum};

/// Arguments of the `absorb` command.
#[derive(Parser, Debug)]
#[command(
    about = "Fetch data",
    long_about = "In some cases we create substrate files from existing data sets.
                  This command allows to easily fetch data from a picked source."
)]
pub struct AbsorbingArgs {
    /// Origin data directory.
    #[arg(long)]
    pub origin: String,

    /// Meta data directory.
    #[arg(long)]
    pub meta: String,

    /// Subommands.
    #[command(subcommand)]
    pub command: AbsorbingCommands,
}

/// Arguments of the `eu-ecolabel` subcommand of the `absorb` command.
#[derive(Parser, Debug)]
#[command(about = "Download the EU EcoLabel data", long_about = "Download the EU EcoLabel data")]
pub struct AbsorbingEuEcolabelArgs {}

/// Arguments of the `bcorp` subcommand of the `absorb` command.
#[derive(Parser, Debug)]
#[command(about = "Download the BCorp Impact Data", long_about = "Download the BCorp Impact Data")]
pub struct AbsorbingBCorpArgs {
    /// `data.world` authentication token..
    #[arg(long)]
    pub token: String,
}

/// Arguments of the `open-food-facts` subcommand of the `absorb` command.
#[derive(Parser, Debug)]
#[command(
    about = "Download the Open Food Facts data",
    long_about = "Download the OpenFoodFacts data"
)]
pub struct AbsorbingOpenFoodFactsArgs {}

/// Arguments of the `open-food-repo` subcommand of the `absorb` command.
#[derive(Parser, Debug)]
#[command(
    about = "Fetch data from the Open Food Repo",
    long_about = "Fetch data from the Open Food Repo"
)]
pub struct AbsorbingOpenFoodRepoArgs {
    /// Open Food Repo API key.
    #[arg(long)]
    pub api_key: String,
}

/// Arguments of the `wikidata` subcommand of the `absorb` command.
#[derive(Parser, Debug)]
#[command(about = "Download the Wikidata data", long_about = "Download the Wikidata data")]
pub struct AbsorbingWikidataArgs {}

/// Subcommands of the `absorb` command.
#[derive(Subcommand, Debug)]
pub enum AbsorbingCommands {
    #[clap(name = "bcorp")]
    BCorp(AbsorbingBCorpArgs),
    EuEcolabel(AbsorbingEuEcolabelArgs),
    OpenFoodFacts(AbsorbingOpenFoodFactsArgs),
    OpenFoodRepo(AbsorbingOpenFoodRepoArgs),
    Wikidata(AbsorbingWikidataArgs),
}

/// Arguments of the `extract` command.
#[derive(Parser, Debug)]
#[command(
    about = "First step of filtering",
    long_about = "Wikidata data set is very big and processing it takes a lot of time. \
                  To mitigate that problem we preprocess that data by filtering out the entriess \
                  that we are not interested in. We do that intwo steps and this the first of those steps."
)]
pub struct ExtractingArgs {
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
    about = "Second step of filtering",
    long_about = "Wikidata data set is very big and processing it takes a lot of time. \
                  To mitigate that problem we preprocess that data by filtering out the entriess \
                  that we are not interested in. We do that intwo steps and this the second of those steps."
)]
pub struct FilteringArgs {
    /// Origin data directory.
    #[arg(long)]
    pub origin: String,

    /// Meta data directory.
    #[arg(long)]
    pub meta: String,

    /// Cache directory.
    #[arg(long)]
    pub cache: String,

    /// Substrate directory.
    #[arg(long)]
    pub substrate: String,
}

/// Arguments of the `update` command.
#[derive(Parser, Debug)]
#[command(
    about = "Update meta files",
    long_about = "Some data we are processing need to be augmented with additional information \
                  which we prepare manually. With new versions of the original data those manually created \
                  data may become insufficient or obsolete. This command updates the data and points to \
                  any further manual updates required.\n\nCurrently this command updates mapping \
                  from Open Food Facts countries to Transpaer regions, \
                  from Open Food Facts categories to Transpaer categories, \
                  from Wikidata countries to Transpaer regions and \
                  from Wikidata classes to Transpaer categories, \
                  "
)]
pub struct UpdatingArgs {
    /// Origin data directory.
    #[arg(long)]
    pub origin: String,

    /// Meta data directory.
    #[arg(long)]
    pub meta: String,

    /// Cache data directory.
    #[arg(long)]
    pub cache: String,

    /// Substrate directory.
    #[arg(long)]
    pub substrate: String,
}

#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
#[clap(rename_all = "kebab_case")]
pub enum CondensationGroup {
    Filtered,
    Immediate,
    All,
}

impl CondensationGroup {
    pub fn use_filtered(self) -> bool {
        match self {
            Self::Filtered | Self::All => true,
            Self::Immediate => false,
        }
    }

    pub fn use_immediate(self) -> bool {
        match self {
            Self::Immediate | Self::All => true,
            Self::Filtered => false,
        }
    }
}

/// Arguments of the `condense` command.
#[derive(Parser, Debug)]
#[command(
    about = "Process big input data sources into substrate files.",
    long_about = "Transpaer projest aims to make as much data as possible available to the consumers. \
                  To make processing of those data easier, we ask producers and certifing organisations \
                  to provide us their data in a unified format, which we call \"substrate files\". \
                  Additionally, we import some chosen, reputable data sources into that format. \
                  This command converts those data sources into substrate files."
)]
pub struct CondensationArgs {
    /// Origin data directory.
    #[arg(long)]
    pub origin: String,

    /// Meta data directory.
    #[arg(long)]
    pub meta: String,

    /// Support data directory.
    #[arg(long)]
    pub support: String,

    /// Cache directory.
    #[arg(long)]
    pub cache: String,

    /// Substrate directory.
    #[arg(long)]
    pub substrate: String,

    /// Uses only the origins from the given group.
    #[clap(long, action)]
    pub group: CondensationGroup,
}

/// Arguments of the `coagulate` command.
#[derive(Parser, Debug)]
#[command(
    about = "Preprocessing step for the `crystalize` command",
    long_about = "Iterates over all IDs of all organisations and products from all substrates to merge
                  them if they refer to the same entity."
)]
pub struct CoagulationArgs {
    /// Substrate data directory.
    #[arg(long)]
    pub substrate: String,

    /// Target data directory.
    #[arg(long)]
    pub coagulate: String,
}

/// Arguments of the `crystalize` command.
#[derive(Parser, Debug)]
#[command(
    about = "Processes all available substrate files to create a new version of Transpaer database",
    long_about = "Substrate files adhere to the schema defined by the Transpaer projest and are provided \
                  by affiliated companies, organisations, reviewers, etc, or are prepared by the Transpaer \
                  from reputable data sources. This command merges all the available substrate files to \
                  create a database used by the Transpaer web service."
)]
pub struct CrystalizationArgs {
    /// Substrate data directory.
    #[arg(long)]
    pub substrate: String,

    /// Coagulation data.
    #[arg(long)]
    pub coagulate: String,

    /// Target data directory.
    #[arg(long)]
    pub target: String,
}

/// Arguments of the `oxidize` command.
#[derive(Parser, Debug)]
#[command(
    about = "Convert backend config files to format storable in the database",
    long_about = "This command converts additional data required by the webservice (e.g. texts, \
                  articles we show on the web page) into a format that can be imported by the database."
)]
pub struct OxidationArgs {
    /// Support data directory.
    #[arg(long)]
    pub support: String,

    /// Library data directory.
    #[arg(long)]
    pub library: String,

    /// Target data directory.
    #[arg(long)]
    pub target: String,
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
    pub meta: String,
}

/// Arguments of the `sample` command.
#[derive(Parser, Debug)]
#[command(
    about = "Verify the result of crystalisation",
    long_about = "Verify the result of crystalisation by checking sample entries."
)]
pub struct SampleArgs {
    #[arg(long)]
    pub target: Option<String>,

    #[arg(long)]
    pub url: Option<String>,
}

/// All arguments of the program.
#[derive(Subcommand, Debug)]
pub enum Commands {
    Absorb(AbsorbingArgs),
    Extract(ExtractingArgs),
    Condense(CondensationArgs),
    Filter(FilteringArgs),
    Coagulate(CoagulationArgs),
    Crystalize(CrystalizationArgs),
    Oxidize(OxidationArgs),
    Update(UpdatingArgs),
    Connect(ConnectionArgs),
    Sample(SampleArgs),
}

/// Program arguments.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// Commands.
    #[command(subcommand)]
    pub command: Commands,
}
