use std::collections::{HashMap, HashSet};

use async_trait::async_trait;

use consumers_wikidata::data::{Entity, Item};

use crate::{
    advisors, config, errors, knowledge,
    processing::{Collectable, Essential, Processor, Sourceable},
    wikidata::{self, ItemExt},
};

/// Represend the numerical part of the Wikidata ID.
type Id = usize;

/// List of classes to be ignored.
#[allow(clippy::unreadable_literal)]
const IGNORED_CLASSES: [Id; 245] = [
    1002954,   // Formula One car
    100349043, // space instrument
    100709275, // combat vehicle family
    100710213, // combat vehicle model
    10316200,  // small patrol boat
    104843,    // cruiser
    1049158,   // air-to-surface missile
    1051067,   // pre-dreadnought battleship
    105999,    // cargo ship
    106179098, // sailboat class
    1069313,   // GPS satellite
    1078334,   // low-floor bus
    10915943,  // diesel railcar
    10929058,  // product model
    109697418, // turbocharged diesel engine
    11012,     // robot
    110944598, // standard circulation coin
    11173,     // chemical compound
    1121471,   // paddle steamer
    11229656,  // tank landing ship
    1130697,   // utility aircraft
    11436,     // aircraft
    1144661,   // amusement ride
    11446,     // ship
    11479409,  // offshore patrol vessel
    1153376,   // flying boat
    115722011, // integrated circuit model
    1184840,   // military vehicle
    11997320,  // sea rescue vessel
    1201871,   // platform supply vessel
    120319,    // Mallet locomotive
    121117,    // supercomputer
    12140,     // medication
    1229765,   // watercraft
    124072,    // rifle
    1245737,   // midibus
    1261534,   // carrier-capable aircraft
    12757,     // internal combustion engine
    127771,    // fighter
    12859788,  // steamship
    12876,     // tank
    1303735,   // survey vessel
    13266730,  // C-segment
    13267846,  // A-segment
    13402959,  // high-speed train
    1361551,   // rescue boat
    1361980,   // guided missile cruiser
    13622547,  // road switcher
    1384417,   // experimental aircraft
    1410980,   // ironclad warship
    1420024,   // fishing vessel
    143872,    // rack locomotive
    1445518,   // airlifter
    148578,    // space telescope
    14928,     // oil tanker
    14970,     // tanker
    14978,     // icebreaker
    149918,    // communications satellite
    1499623,   // destroyer escort
    1504307,   // reefer ship
    15056993,  // aircraft family
    15056995,  // aircraft model
    15057020,  // engine family
    15057021,  // engine model
    15061018,  // proposed aircraft
    15126161,  // prototype aircraft model
    15142889,  // weapon family
    15142894,  // weapon model
    15247,     // LNG carrier
    15254,     // chemical tanker
    15262,     // train ferry
    15276,     // bulk carrier
    1549739,   // large torpedo boat
    15623405,  // Saab 340B
    1567915,   // diesel multiple unit
    15729598,  // coachwork type
    15888,     // hospital ship
    15893266,  // former entity
    161705,    // frigate
    1617851,   // high-speed craft
    16220775,  // torpedo-boat destroyer
    162986,    // replenishment oiler
    167270,    // trademark
    169336,    // mixture
    16936947,  // electric rail motor coach
    170013,    // corvette
    170382,    // revolver
    170877,    // bomber
    171043,    // steam locomotive
    17205,     // aircraft carrier
    17210,     // container ship
    174174,    // diesel engine
    174736,    // destroyer
    1752434,   // mixed cargo ship
    177456,    // assault rifle
    178193,    // steamboat
    178550,    // submachine gun
    180173,    // sailplane
    182531,    // battleship
    184296,    // tapestry
    18433590,  // horizontal axis wind turbine
    18491037,  // fighter biplane
    18491097,  // fighter monoplane with 1 engine
    1872522,   // low-entry bus
    190570,    // sniper rifle
    1917626,   // multi-purpose vessel
    191826,    // tug
    19292005,  // harbor tugboat
    19362195,  // geostationary satellite
    19716577,  // synthesizer model
    197,       // airplane
    19832486,  // locomotive class
    19842071,  // diesel-electric locomotive
    201783,    // moped
    202527,    // minesweeper
    204577,    // schooner
    2055880,   // passenger ship
    20650761,  // tender locomotive
    20741022,  // digital camera model
    207452,    // ship of the line
    207977,    // prototype
    208187,    // attack aircraft
    210932,    // airliner
    2125613,   // land-based aircraft
    214196,    // gunboat
    21505397,  // motor yacht
    216916,    // military aircraft
    22704163,  // firearm model
    2291248,   // semi-automatic rifle
    22999537,  // vehicle family
    23039057,  // bus model
    2312440,   // sportplane
    2389789,   // steel roller coaster
    2424752,   // product
    24294815,  // diesel-hydraulic locomotive
    243249,    // air-to-air missile
    253555,    // escort carrier
    25653,     // ferry ship
    2607934,   // guided missile destroyer
    26251835,  // diesel-mechanical locomotive
    26529,     // space probe
    26540,     // artificial satellite
    2735392,   // utility helicopter
    2811,      // submarine
    281460,    // Pipe organ
    282472,    // anti-tank missile
    2875704,   // fixed-wing aircraft
    28885102,  // pharmaceutical product
    29048322,  // vehicle model
    3041792,   // reconnaissance aircraft
    3119075,   // floatplane
    324233,    // torpedo boat
    331795,    // patrol vessel
    335225,    // rocket engine
    3389302,   // semi-automatic pistol
    3407658,   // tram
    34336,     // diesel locomotive
    34486,     // helicopter
    353699,    // container feeder ship
    3609612,   // rifled breech loader
    376055,    // Garratt locomotive
    391022,    // research vessel
    39495,     // tractor
    3959904,   // multiple unit
    39804,     // cruise ship
    402092,    // motor ship
    40280158,  // bolt-action rifle
    4102249,   // subway car
    41207,     // coin
    41426,     // trainer aircraft
    4167410,   // Wikimedia disambiguation page
    42319471,  // trolleybus model
    428661,    // U-boat
    431289,    // brand
    4407246,   // armored personnel carrier
    44167,     // engine
    44740228,  // wall hanging
    45296117,  // aircraft type
    466421,    // reconnaissance satellite
    466704,    // surface-to-air missile
    473932,    // roll-on/roll-off ship
    4809,      // rocket stage
    4818021,   // attack submarine
    4830453,   // business
    483373,    // electric multiple unit
    486396,    // shotgun
    502048,    // gasoline engine
    509222,    // drillship
    511450,    // naval artillery
    55725952,  // tractor model
    559026,    // ship class
    5638,      // bus
    5810820,   // dredger
    593485,    // liberty ship
    59773381,  // automobile model series
    610398,    // system on a chip
    617792,    // compound locomotive
    625151,    // electric locomotive
    630010,    // reciprocating engine
    640078,    // minelayer
    643532,    // anti-ship missile
    649062,    // armored car
    654051,    // turbofan
    654749,    // articulated bus
    660668,    // training ship
    673687,    // racing automobile
    68072093,  // land-based utility aircraft
    683570,    // ballistic missile submarine
    69129709,  // land-based fighter monoplane
    697175,    // launch vehicle
    697196,    // ocean liner
    7132141,   // railcars
    7325635,   // anti-aircraft gun
    744620,    // grand tourer
    751705,    // autocannon
    752392,    // railcar
    752783,    // human spaceflight
    753224,    // torpedo bomber
    753779,    // passenger railroad car
    757587,    // nuclear-powered attack submarine
    778129,    // light cruiser
    785745,    // tank locomotive
    80831,     // integrated circuit
    811701,    // model series
    811704,    // rolling stock class
    8142,      // currency
    815423,    // supermini
    830335,    // protected cruiser
    847109,    // dreadnought
    847478,    // armored cruiser
    852190,    // shipwreck
    854585,    // double-decker bus
    854845,    // Earth observation satellite
    860861,    // sculpture
    875583,    // light tank
    875600,    // crossover
    892492,    // third-rate
    898771,    // heavy cruiser
    90834785,  // racing automobile model
    920182,    // Main battle tank
    928235,    // sloop-of-war
    946596,    // off-road vehicle
    95945728,  // technology demonstration spacecraft
    972011,    // sports prototype
    973740,    // whaler
];

/// Data related to a products.
#[derive(Clone, Debug)]
pub struct Product {
    /// IDs of te classes this product belongs to.
    classes: HashSet<Id>,
}

/// Data related to a class.
#[derive(Clone, Debug)]
pub struct Class {
    /// Class ID.
    id: Id,

    /// Class label (name).
    label: String,

    /// Number of products belonging to this class.
    amount: usize,
}

impl Class {
    /// Copies this class with diffrent `amount`.
    pub fn clone_with_amount(&self, amount: usize) -> Self {
        Self { amount, ..self.clone() }
    }
}

/// Provides the core data for the processor.
#[derive(Debug)]
pub struct AnalysisEssentials {
    /// Product data loader.
    data: consumers_wikidata::dump::Loader,
}

#[async_trait]
impl Essential for AnalysisEssentials {
    type Config = config::AnalysisConfig;

    fn load(config: &Self::Config) -> Result<Self, errors::ProcessingError> {
        Ok(Self {
            data: consumers_wikidata::dump::Loader::load(&config.wikidata_filtered_dump_path)?,
        })
    }

    async fn run(
        &mut self,
        tx: async_channel::Sender<String>,
    ) -> Result<usize, errors::ProcessingError> {
        Ok(self.data.run_with_channel(tx).await?)
    }
}

/// Holds all the supplementary source data.
pub struct AnalysisSources {
    /// Wikidata data.
    pub wikidata: advisors::WikidataAdvisor,
}

impl Sourceable for AnalysisSources {
    type Config = config::AnalysisConfig;

    /// Constructs a new `AnalysisSources`.
    fn load(config: &Self::Config) -> Result<Self, errors::ProcessingError> {
        let wikidata = advisors::WikidataAdvisor::load(&config.wikidata_cache_path)?;
        Ok(Self { wikidata })
    }
}

/// Data storage for gathered data.
///
/// Allows merging different instances.
#[derive(Debug, Default)]
pub struct AnalysisCollector {
    /// Found products.
    products: Vec<Product>,

    /// Found organisations.
    classes: Vec<Class>,
}

impl AnalysisCollector {
    /// Adds a new product.
    pub fn add_product(&mut self, product: Product) {
        self.products.push(product);
    }

    /// Adds a new organisation.
    pub fn add_class(&mut self, class: Class) {
        self.classes.push(class);
    }
}

impl merge::Merge for AnalysisCollector {
    fn merge(&mut self, other: Self) {
        self.products.extend_from_slice(&other.products);
        self.classes.extend_from_slice(&other.classes);
    }
}

impl Collectable for AnalysisCollector {}

/// Translates the filteres wikidata producern and manufacturers in to the database format.
#[derive(Clone, Debug)]
pub struct AnalysisProcessor;

impl AnalysisProcessor {
    /// Constructs a new `AnalysisProcessor`.
    pub fn new() -> Self {
        Self
    }

    pub fn get_classes(item: &Item) -> HashSet<Id> {
        let mut classes = HashSet::<Id>::new();
        if let Some(superclasses) = item.get_superclasses() {
            for class in &superclasses {
                match Self::to_num_id(class) {
                    Ok(id) => {
                        classes.insert(id);
                    }
                    Err(err) => log::error!("Failed to parse ID: {err}"),
                }
            }
        }
        if let Some(superclasses) = item.get_classes() {
            for class in &superclasses {
                match Self::to_num_id(class) {
                    Ok(id) => {
                        classes.insert(id);
                    }
                    Err(err) => log::error!("Failed to parse ID: {err}"),
                }
            }
        }
        classes
    }

    /// Extracts the numerical part of an Wikidata ID.
    fn to_num_id(id: &knowledge::Id) -> Result<usize, std::num::ParseIntError> {
        id.as_str().split_at(1).1.parse::<usize>()
    }

    /// Extracts the numerical part of an Wikidata ID.
    fn str_to_num_id(id: &str) -> Result<usize, std::num::ParseIntError> {
        id.split_at(1).1.parse::<usize>()
    }

    /// Checks if the passed item represents a class.
    fn is_class(item: &Item, sources: &AnalysisSources) -> bool {
        sources.wikidata.has_class_id(&item.id)
    }

    /// Checks if the passed item represents a product.
    fn is_product(item: &Item) -> bool {
        item.has_manufacturer()
    }
}

impl Processor for AnalysisProcessor {
    type Config = config::AnalysisConfig;
    type Essentials = AnalysisEssentials;
    type Sources = AnalysisSources;
    type Collector = AnalysisCollector;

    /// Handles one Wikidata entity.
    fn handle_entity(
        &self,
        _msg: &str,
        entity: &Entity,
        sources: &Self::Sources,
        collector: &mut Self::Collector,
        _config: &Self::Config,
    ) -> Result<(), errors::ProcessingError> {
        match entity {
            Entity::Item(item) => {
                if let Some(label) = item.get_label(consumers_wikidata::data::Language::En) {
                    if Self::is_class(item, sources) {
                        match Self::to_num_id(&item.id) {
                            Ok(id) => {
                                collector.add_class(Class {
                                    id,
                                    label: label.to_string(),
                                    amount: 1,
                                });
                            }
                            Err(err) => log::error!("Failed to parse ID: {err}"),
                        }
                    }
                    if Self::is_product(item) {
                        let classes = Self::get_classes(item);
                        if !classes.is_empty() {
                            collector.add_product(Product { classes });
                        }
                    }
                }
            }
            Entity::Property(_property) => (),
        }
        Ok(())
    }

    /// Saves the result into files.
    fn finalize(
        &self,
        collector: &Self::Collector,
        _config: &Self::Config,
    ) -> Result<(), errors::ProcessingError> {
        log::info!("Found {} products", collector.products.len());
        log::info!("Found {} classes", collector.classes.len());

        let categories: HashSet<Id> =
            wikidata::items::ALL.iter().filter_map(|s| Self::str_to_num_id(s).ok()).collect();
        let ignored_classes = HashSet::<Id>::from(IGNORED_CLASSES);
        let classes: HashMap<Id, Class> =
            collector.classes.iter().map(|c| (c.id, c.clone())).collect();

        let mut uncategorized_classes = HashMap::<Id, Class>::new();
        for p in &collector.products {
            for c in &p.classes {
                if !categories.contains(c) && classes.contains_key(c) {
                    uncategorized_classes
                        .entry(*c)
                        .and_modify(|e| e.amount += 1)
                        .or_insert_with(|| classes[c].clone_with_amount(1));
                }
            }
        }

        let mut found = false;
        let mut classes: Vec<Class> = uncategorized_classes.values().cloned().collect();
        classes.sort_by(|a, b| a.amount.cmp(&b.amount));
        for c in classes {
            if c.amount > 50 && !ignored_classes.contains(&c.id) {
                println!("  {}, // {} ({})", c.id, c.label, c.amount);
                found = true;
            }
        }
        if !found {
            log::info!("All classes categorized!");
        }

        Ok(())
    }
}
