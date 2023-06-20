use std::collections::{HashMap, HashSet};

use sustainity_wikidata::data::{Entity, Item, Language};

use crate::{
    advisors, config, errors, knowledge,
    processing::{Collectable, Processor, Sourceable},
    runners,
    wikidata::{self, ItemExt},
};

/// List of classes to be ignored.
#[allow(clippy::unreadable_literal)]
const IGNORED_CLASSES: [knowledge::WikiId; 245] = [
    knowledge::WikiId::new(1002954),   // Formula One car
    knowledge::WikiId::new(100349043), // space instrument
    knowledge::WikiId::new(100709275), // combat vehicle family
    knowledge::WikiId::new(100710213), // combat vehicle model
    knowledge::WikiId::new(10316200),  // small patrol boat
    knowledge::WikiId::new(104843),    // cruiser
    knowledge::WikiId::new(1049158),   // air-to-surface missile
    knowledge::WikiId::new(1051067),   // pre-dreadnought battleship
    knowledge::WikiId::new(105999),    // cargo ship
    knowledge::WikiId::new(106179098), // sailboat class
    knowledge::WikiId::new(1069313),   // GPS satellite
    knowledge::WikiId::new(1078334),   // low-floor bus
    knowledge::WikiId::new(10915943),  // diesel railcar
    knowledge::WikiId::new(10929058),  // product model
    knowledge::WikiId::new(109697418), // turbocharged diesel engine
    knowledge::WikiId::new(11012),     // robot
    knowledge::WikiId::new(110944598), // standard circulation coin
    knowledge::WikiId::new(11173),     // chemical compound
    knowledge::WikiId::new(1121471),   // paddle steamer
    knowledge::WikiId::new(11229656),  // tank landing ship
    knowledge::WikiId::new(1130697),   // utility aircraft
    knowledge::WikiId::new(11436),     // aircraft
    knowledge::WikiId::new(1144661),   // amusement ride
    knowledge::WikiId::new(11446),     // ship
    knowledge::WikiId::new(11479409),  // offshore patrol vessel
    knowledge::WikiId::new(1153376),   // flying boat
    knowledge::WikiId::new(115722011), // integrated circuit model
    knowledge::WikiId::new(1184840),   // military vehicle
    knowledge::WikiId::new(11997320),  // sea rescue vessel
    knowledge::WikiId::new(1201871),   // platform supply vessel
    knowledge::WikiId::new(120319),    // Mallet locomotive
    knowledge::WikiId::new(121117),    // supercomputer
    knowledge::WikiId::new(12140),     // medication
    knowledge::WikiId::new(1229765),   // watercraft
    knowledge::WikiId::new(124072),    // rifle
    knowledge::WikiId::new(1245737),   // midibus
    knowledge::WikiId::new(1261534),   // carrier-capable aircraft
    knowledge::WikiId::new(12757),     // internal combustion engine
    knowledge::WikiId::new(127771),    // fighter
    knowledge::WikiId::new(12859788),  // steamship
    knowledge::WikiId::new(12876),     // tank
    knowledge::WikiId::new(1303735),   // survey vessel
    knowledge::WikiId::new(13266730),  // C-segment
    knowledge::WikiId::new(13267846),  // A-segment
    knowledge::WikiId::new(13402959),  // high-speed train
    knowledge::WikiId::new(1361551),   // rescue boat
    knowledge::WikiId::new(1361980),   // guided missile cruiser
    knowledge::WikiId::new(13622547),  // road switcher
    knowledge::WikiId::new(1384417),   // experimental aircraft
    knowledge::WikiId::new(1410980),   // ironclad warship
    knowledge::WikiId::new(1420024),   // fishing vessel
    knowledge::WikiId::new(143872),    // rack locomotive
    knowledge::WikiId::new(1445518),   // airlifter
    knowledge::WikiId::new(148578),    // space telescope
    knowledge::WikiId::new(14928),     // oil tanker
    knowledge::WikiId::new(14970),     // tanker
    knowledge::WikiId::new(14978),     // icebreaker
    knowledge::WikiId::new(149918),    // communications satellite
    knowledge::WikiId::new(1499623),   // destroyer escort
    knowledge::WikiId::new(1504307),   // reefer ship
    knowledge::WikiId::new(15056993),  // aircraft family
    knowledge::WikiId::new(15056995),  // aircraft model
    knowledge::WikiId::new(15057020),  // engine family
    knowledge::WikiId::new(15057021),  // engine model
    knowledge::WikiId::new(15061018),  // proposed aircraft
    knowledge::WikiId::new(15126161),  // prototype aircraft model
    knowledge::WikiId::new(15142889),  // weapon family
    knowledge::WikiId::new(15142894),  // weapon model
    knowledge::WikiId::new(15247),     // LNG carrier
    knowledge::WikiId::new(15254),     // chemical tanker
    knowledge::WikiId::new(15262),     // train ferry
    knowledge::WikiId::new(15276),     // bulk carrier
    knowledge::WikiId::new(1549739),   // large torpedo boat
    knowledge::WikiId::new(15623405),  // Saab 340B
    knowledge::WikiId::new(1567915),   // diesel multiple unit
    knowledge::WikiId::new(15729598),  // coachwork type
    knowledge::WikiId::new(15888),     // hospital ship
    knowledge::WikiId::new(15893266),  // former entity
    knowledge::WikiId::new(161705),    // frigate
    knowledge::WikiId::new(1617851),   // high-speed craft
    knowledge::WikiId::new(16220775),  // torpedo-boat destroyer
    knowledge::WikiId::new(162986),    // replenishment oiler
    knowledge::WikiId::new(167270),    // trademark
    knowledge::WikiId::new(169336),    // mixture
    knowledge::WikiId::new(16936947),  // electric rail motor coach
    knowledge::WikiId::new(170013),    // corvette
    knowledge::WikiId::new(170382),    // revolver
    knowledge::WikiId::new(170877),    // bomber
    knowledge::WikiId::new(171043),    // steam locomotive
    knowledge::WikiId::new(17205),     // aircraft carrier
    knowledge::WikiId::new(17210),     // container ship
    knowledge::WikiId::new(174174),    // diesel engine
    knowledge::WikiId::new(174736),    // destroyer
    knowledge::WikiId::new(1752434),   // mixed cargo ship
    knowledge::WikiId::new(177456),    // assault rifle
    knowledge::WikiId::new(178193),    // steamboat
    knowledge::WikiId::new(178550),    // submachine gun
    knowledge::WikiId::new(180173),    // sailplane
    knowledge::WikiId::new(182531),    // battleship
    knowledge::WikiId::new(184296),    // tapestry
    knowledge::WikiId::new(18433590),  // horizontal axis wind turbine
    knowledge::WikiId::new(18491037),  // fighter biplane
    knowledge::WikiId::new(18491097),  // fighter monoplane with 1 engine
    knowledge::WikiId::new(1872522),   // low-entry bus
    knowledge::WikiId::new(190570),    // sniper rifle
    knowledge::WikiId::new(1917626),   // multi-purpose vessel
    knowledge::WikiId::new(191826),    // tug
    knowledge::WikiId::new(19292005),  // harbor tugboat
    knowledge::WikiId::new(19362195),  // geostationary satellite
    knowledge::WikiId::new(19716577),  // synthesizer model
    knowledge::WikiId::new(197),       // airplane
    knowledge::WikiId::new(19832486),  // locomotive class
    knowledge::WikiId::new(19842071),  // diesel-electric locomotive
    knowledge::WikiId::new(201783),    // moped
    knowledge::WikiId::new(202527),    // minesweeper
    knowledge::WikiId::new(204577),    // schooner
    knowledge::WikiId::new(2055880),   // passenger ship
    knowledge::WikiId::new(20650761),  // tender locomotive
    knowledge::WikiId::new(20741022),  // digital camera model
    knowledge::WikiId::new(207452),    // ship of the line
    knowledge::WikiId::new(207977),    // prototype
    knowledge::WikiId::new(208187),    // attack aircraft
    knowledge::WikiId::new(210932),    // airliner
    knowledge::WikiId::new(2125613),   // land-based aircraft
    knowledge::WikiId::new(214196),    // gunboat
    knowledge::WikiId::new(21505397),  // motor yacht
    knowledge::WikiId::new(216916),    // military aircraft
    knowledge::WikiId::new(22704163),  // firearm model
    knowledge::WikiId::new(2291248),   // semi-automatic rifle
    knowledge::WikiId::new(22999537),  // vehicle family
    knowledge::WikiId::new(23039057),  // bus model
    knowledge::WikiId::new(2312440),   // sportplane
    knowledge::WikiId::new(2389789),   // steel roller coaster
    knowledge::WikiId::new(2424752),   // product
    knowledge::WikiId::new(24294815),  // diesel-hydraulic locomotive
    knowledge::WikiId::new(243249),    // air-to-air missile
    knowledge::WikiId::new(253555),    // escort carrier
    knowledge::WikiId::new(25653),     // ferry ship
    knowledge::WikiId::new(2607934),   // guided missile destroyer
    knowledge::WikiId::new(26251835),  // diesel-mechanical locomotive
    knowledge::WikiId::new(26529),     // space probe
    knowledge::WikiId::new(26540),     // artificial satellite
    knowledge::WikiId::new(2735392),   // utility helicopter
    knowledge::WikiId::new(2811),      // submarine
    knowledge::WikiId::new(281460),    // Pipe organ
    knowledge::WikiId::new(282472),    // anti-tank missile
    knowledge::WikiId::new(2875704),   // fixed-wing aircraft
    knowledge::WikiId::new(28885102),  // pharmaceutical product
    knowledge::WikiId::new(29048322),  // vehicle model
    knowledge::WikiId::new(3041792),   // reconnaissance aircraft
    knowledge::WikiId::new(3119075),   // floatplane
    knowledge::WikiId::new(324233),    // torpedo boat
    knowledge::WikiId::new(331795),    // patrol vessel
    knowledge::WikiId::new(335225),    // rocket engine
    knowledge::WikiId::new(3389302),   // semi-automatic pistol
    knowledge::WikiId::new(3407658),   // tram
    knowledge::WikiId::new(34336),     // diesel locomotive
    knowledge::WikiId::new(34486),     // helicopter
    knowledge::WikiId::new(353699),    // container feeder ship
    knowledge::WikiId::new(3609612),   // rifled breech loader
    knowledge::WikiId::new(376055),    // Garratt locomotive
    knowledge::WikiId::new(391022),    // research vessel
    knowledge::WikiId::new(39495),     // tractor
    knowledge::WikiId::new(3959904),   // multiple unit
    knowledge::WikiId::new(39804),     // cruise ship
    knowledge::WikiId::new(402092),    // motor ship
    knowledge::WikiId::new(40280158),  // bolt-action rifle
    knowledge::WikiId::new(4102249),   // subway car
    knowledge::WikiId::new(41207),     // coin
    knowledge::WikiId::new(41426),     // trainer aircraft
    knowledge::WikiId::new(4167410),   // Wikimedia disambiguation page
    knowledge::WikiId::new(42319471),  // trolleybus model
    knowledge::WikiId::new(428661),    // U-boat
    knowledge::WikiId::new(431289),    // brand
    knowledge::WikiId::new(4407246),   // armored personnel carrier
    knowledge::WikiId::new(44167),     // engine
    knowledge::WikiId::new(44740228),  // wall hanging
    knowledge::WikiId::new(45296117),  // aircraft type
    knowledge::WikiId::new(466421),    // reconnaissance satellite
    knowledge::WikiId::new(466704),    // surface-to-air missile
    knowledge::WikiId::new(473932),    // roll-on/roll-off ship
    knowledge::WikiId::new(4809),      // rocket stage
    knowledge::WikiId::new(4818021),   // attack submarine
    knowledge::WikiId::new(4830453),   // business
    knowledge::WikiId::new(483373),    // electric multiple unit
    knowledge::WikiId::new(486396),    // shotgun
    knowledge::WikiId::new(502048),    // gasoline engine
    knowledge::WikiId::new(509222),    // drillship
    knowledge::WikiId::new(511450),    // naval artillery
    knowledge::WikiId::new(55725952),  // tractor model
    knowledge::WikiId::new(559026),    // ship class
    knowledge::WikiId::new(5638),      // bus
    knowledge::WikiId::new(5810820),   // dredger
    knowledge::WikiId::new(593485),    // liberty ship
    knowledge::WikiId::new(59773381),  // automobile model series
    knowledge::WikiId::new(610398),    // system on a chip
    knowledge::WikiId::new(617792),    // compound locomotive
    knowledge::WikiId::new(625151),    // electric locomotive
    knowledge::WikiId::new(630010),    // reciprocating engine
    knowledge::WikiId::new(640078),    // minelayer
    knowledge::WikiId::new(643532),    // anti-ship missile
    knowledge::WikiId::new(649062),    // armored car
    knowledge::WikiId::new(654051),    // turbofan
    knowledge::WikiId::new(654749),    // articulated bus
    knowledge::WikiId::new(660668),    // training ship
    knowledge::WikiId::new(673687),    // racing automobile
    knowledge::WikiId::new(68072093),  // land-based utility aircraft
    knowledge::WikiId::new(683570),    // ballistic missile submarine
    knowledge::WikiId::new(69129709),  // land-based fighter monoplane
    knowledge::WikiId::new(697175),    // launch vehicle
    knowledge::WikiId::new(697196),    // ocean liner
    knowledge::WikiId::new(7132141),   // railcars
    knowledge::WikiId::new(7325635),   // anti-aircraft gun
    knowledge::WikiId::new(744620),    // grand tourer
    knowledge::WikiId::new(751705),    // autocannon
    knowledge::WikiId::new(752392),    // railcar
    knowledge::WikiId::new(752783),    // human spaceflight
    knowledge::WikiId::new(753224),    // torpedo bomber
    knowledge::WikiId::new(753779),    // passenger railroad car
    knowledge::WikiId::new(757587),    // nuclear-powered attack submarine
    knowledge::WikiId::new(778129),    // light cruiser
    knowledge::WikiId::new(785745),    // tank locomotive
    knowledge::WikiId::new(80831),     // integrated circuit
    knowledge::WikiId::new(811701),    // model series
    knowledge::WikiId::new(811704),    // rolling stock class
    knowledge::WikiId::new(8142),      // currency
    knowledge::WikiId::new(815423),    // supermini
    knowledge::WikiId::new(830335),    // protected cruiser
    knowledge::WikiId::new(847109),    // dreadnought
    knowledge::WikiId::new(847478),    // armored cruiser
    knowledge::WikiId::new(852190),    // shipwreck
    knowledge::WikiId::new(854585),    // double-decker bus
    knowledge::WikiId::new(854845),    // Earth observation satellite
    knowledge::WikiId::new(860861),    // sculpture
    knowledge::WikiId::new(875583),    // light tank
    knowledge::WikiId::new(875600),    // crossover
    knowledge::WikiId::new(892492),    // third-rate
    knowledge::WikiId::new(898771),    // heavy cruiser
    knowledge::WikiId::new(90834785),  // racing automobile model
    knowledge::WikiId::new(920182),    // Main battle tank
    knowledge::WikiId::new(928235),    // sloop-of-war
    knowledge::WikiId::new(946596),    // off-road vehicle
    knowledge::WikiId::new(95945728),  // technology demonstration spacecraft
    knowledge::WikiId::new(972011),    // sports prototype
    knowledge::WikiId::new(973740),    // whaler
];

/// Data related to a products.
#[derive(Clone, Debug)]
pub struct Product {
    /// IDs of te classes this product belongs to.
    classes: HashSet<knowledge::WikiId>,
}

/// Data related to a class.
#[derive(Clone, Debug)]
pub struct Class {
    /// Class ID.
    id: knowledge::WikiId,

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
    pub fn get_classes(item: &Item) -> HashSet<knowledge::WikiId> {
        let mut classes = HashSet::<knowledge::WikiId>::new();
        if let Some(superclasses) = item.get_superclasses() {
            for class in &superclasses {
                match class.to_num_id() {
                    Ok(id) => {
                        classes.insert(id);
                    }
                    Err(err) => log::error!("Failed to parse ID: {err}"),
                }
            }
        }
        if let Some(superclasses) = item.get_classes() {
            for class in &superclasses {
                match class.to_num_id() {
                    Ok(id) => {
                        classes.insert(id);
                    }
                    Err(err) => log::error!("Failed to parse ID: {err}"),
                }
            }
        }
        classes
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

impl Default for AnalysisProcessor {
    fn default() -> Self {
        Self
    }
}

impl Processor for AnalysisProcessor {
    type Config = config::AnalysisConfig;
    type Sources = AnalysisSources;
    type Collector = AnalysisCollector;

    fn initialize(
        &self,
        _collector: &mut Self::Collector,
        _sources: &Self::Sources,
        _config: &Self::Config,
    ) -> Result<(), errors::ProcessingError> {
        Ok(())
    }

    /// Saves the result into files.
    fn finalize(
        &self,
        collector: Self::Collector,
        _sources: &Self::Sources,
        _config: &Self::Config,
    ) -> Result<(), errors::ProcessingError> {
        log::info!("Found {} products", collector.products.len());
        log::info!("Found {} classes", collector.classes.len());

        let categories: HashSet<knowledge::WikiId> = wikidata::items::ALL
            .iter()
            .filter_map(|s| knowledge::WikiId::try_from(*s).ok())
            .collect();
        let ignored_classes = HashSet::<knowledge::WikiId>::from(IGNORED_CLASSES);
        let classes: HashMap<knowledge::WikiId, Class> =
            collector.classes.iter().map(|c| (c.id.clone(), c.clone())).collect();

        let mut uncategorized_classes = HashMap::<knowledge::WikiId, Class>::new();
        for p in &collector.products {
            for c in &p.classes {
                if !categories.contains(c) && classes.contains_key(c) {
                    uncategorized_classes
                        .entry(c.clone())
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
                println!("  {}, // {} ({})", c.id.to_str_id().as_str(), c.label, c.amount);
                found = true;
            }
        }
        if !found {
            log::info!("All classes categorized!");
        }

        Ok(())
    }
}

impl runners::WikidataProcessor for AnalysisProcessor {
    /// Handles one Wikidata entity.
    fn handle_wikidata_entity(
        &self,
        _msg: &str,
        entity: Entity,
        sources: &Self::Sources,
        collector: &mut Self::Collector,
        _config: &Self::Config,
    ) -> Result<(), errors::ProcessingError> {
        match entity {
            Entity::Item(item) => {
                if let Some(label) = item.get_label(Language::En) {
                    if Self::is_class(&item, sources) {
                        match item.id.to_num_id() {
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
                    if Self::is_product(&item) {
                        let classes = Self::get_classes(&item);
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
}

pub type AnalysisRunner = runners::WikidataRunner<AnalysisProcessor>;
