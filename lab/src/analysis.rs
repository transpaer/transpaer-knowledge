use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use async_trait::async_trait;
use merge::Merge;

use sustainity_collecting::data::WikiId;
use sustainity_wikidata::data::{Entity, Item, Language};

use crate::{
    advisors, config, errors, parallel, runners,
    sources::Sourceable,
    wikidata::{self, ItemExt},
};

/// List of classes to be ignored.
#[allow(clippy::unreadable_literal)]
const IGNORED_CLASSES: [WikiId; 245] = [
    WikiId::new(1002954),   // Formula One car
    WikiId::new(100349043), // space instrument
    WikiId::new(100709275), // combat vehicle family
    WikiId::new(100710213), // combat vehicle model
    WikiId::new(10316200),  // small patrol boat
    WikiId::new(104843),    // cruiser
    WikiId::new(1049158),   // air-to-surface missile
    WikiId::new(1051067),   // pre-dreadnought battleship
    WikiId::new(105999),    // cargo ship
    WikiId::new(106179098), // sailboat class
    WikiId::new(1069313),   // GPS satellite
    WikiId::new(1078334),   // low-floor bus
    WikiId::new(10915943),  // diesel railcar
    WikiId::new(10929058),  // product model
    WikiId::new(109697418), // turbocharged diesel engine
    WikiId::new(11012),     // robot
    WikiId::new(110944598), // standard circulation coin
    WikiId::new(11173),     // chemical compound
    WikiId::new(1121471),   // paddle steamer
    WikiId::new(11229656),  // tank landing ship
    WikiId::new(1130697),   // utility aircraft
    WikiId::new(11436),     // aircraft
    WikiId::new(1144661),   // amusement ride
    WikiId::new(11446),     // ship
    WikiId::new(11479409),  // offshore patrol vessel
    WikiId::new(1153376),   // flying boat
    WikiId::new(115722011), // integrated circuit model
    WikiId::new(1184840),   // military vehicle
    WikiId::new(11997320),  // sea rescue vessel
    WikiId::new(1201871),   // platform supply vessel
    WikiId::new(120319),    // Mallet locomotive
    WikiId::new(121117),    // supercomputer
    WikiId::new(12140),     // medication
    WikiId::new(1229765),   // watercraft
    WikiId::new(124072),    // rifle
    WikiId::new(1245737),   // midibus
    WikiId::new(1261534),   // carrier-capable aircraft
    WikiId::new(12757),     // internal combustion engine
    WikiId::new(127771),    // fighter
    WikiId::new(12859788),  // steamship
    WikiId::new(12876),     // tank
    WikiId::new(1303735),   // survey vessel
    WikiId::new(13266730),  // C-segment
    WikiId::new(13267846),  // A-segment
    WikiId::new(13402959),  // high-speed train
    WikiId::new(1361551),   // rescue boat
    WikiId::new(1361980),   // guided missile cruiser
    WikiId::new(13622547),  // road switcher
    WikiId::new(1384417),   // experimental aircraft
    WikiId::new(1410980),   // ironclad warship
    WikiId::new(1420024),   // fishing vessel
    WikiId::new(143872),    // rack locomotive
    WikiId::new(1445518),   // airlifter
    WikiId::new(148578),    // space telescope
    WikiId::new(14928),     // oil tanker
    WikiId::new(14970),     // tanker
    WikiId::new(14978),     // icebreaker
    WikiId::new(149918),    // communications satellite
    WikiId::new(1499623),   // destroyer escort
    WikiId::new(1504307),   // reefer ship
    WikiId::new(15056993),  // aircraft family
    WikiId::new(15056995),  // aircraft model
    WikiId::new(15057020),  // engine family
    WikiId::new(15057021),  // engine model
    WikiId::new(15061018),  // proposed aircraft
    WikiId::new(15126161),  // prototype aircraft model
    WikiId::new(15142889),  // weapon family
    WikiId::new(15142894),  // weapon model
    WikiId::new(15247),     // LNG carrier
    WikiId::new(15254),     // chemical tanker
    WikiId::new(15262),     // train ferry
    WikiId::new(15276),     // bulk carrier
    WikiId::new(1549739),   // large torpedo boat
    WikiId::new(15623405),  // Saab 340B
    WikiId::new(1567915),   // diesel multiple unit
    WikiId::new(15729598),  // coachwork type
    WikiId::new(15888),     // hospital ship
    WikiId::new(15893266),  // former entity
    WikiId::new(161705),    // frigate
    WikiId::new(1617851),   // high-speed craft
    WikiId::new(16220775),  // torpedo-boat destroyer
    WikiId::new(162986),    // replenishment oiler
    WikiId::new(167270),    // trademark
    WikiId::new(169336),    // mixture
    WikiId::new(16936947),  // electric rail motor coach
    WikiId::new(170013),    // corvette
    WikiId::new(170382),    // revolver
    WikiId::new(170877),    // bomber
    WikiId::new(171043),    // steam locomotive
    WikiId::new(17205),     // aircraft carrier
    WikiId::new(17210),     // container ship
    WikiId::new(174174),    // diesel engine
    WikiId::new(174736),    // destroyer
    WikiId::new(1752434),   // mixed cargo ship
    WikiId::new(177456),    // assault rifle
    WikiId::new(178193),    // steamboat
    WikiId::new(178550),    // submachine gun
    WikiId::new(180173),    // sailplane
    WikiId::new(182531),    // battleship
    WikiId::new(184296),    // tapestry
    WikiId::new(18433590),  // horizontal axis wind turbine
    WikiId::new(18491037),  // fighter biplane
    WikiId::new(18491097),  // fighter monoplane with 1 engine
    WikiId::new(1872522),   // low-entry bus
    WikiId::new(190570),    // sniper rifle
    WikiId::new(1917626),   // multi-purpose vessel
    WikiId::new(191826),    // tug
    WikiId::new(19292005),  // harbor tugboat
    WikiId::new(19362195),  // geostationary satellite
    WikiId::new(19716577),  // synthesizer model
    WikiId::new(197),       // airplane
    WikiId::new(19832486),  // locomotive class
    WikiId::new(19842071),  // diesel-electric locomotive
    WikiId::new(201783),    // moped
    WikiId::new(202527),    // minesweeper
    WikiId::new(204577),    // schooner
    WikiId::new(2055880),   // passenger ship
    WikiId::new(20650761),  // tender locomotive
    WikiId::new(20741022),  // digital camera model
    WikiId::new(207452),    // ship of the line
    WikiId::new(207977),    // prototype
    WikiId::new(208187),    // attack aircraft
    WikiId::new(210932),    // airliner
    WikiId::new(2125613),   // land-based aircraft
    WikiId::new(214196),    // gunboat
    WikiId::new(21505397),  // motor yacht
    WikiId::new(216916),    // military aircraft
    WikiId::new(22704163),  // firearm model
    WikiId::new(2291248),   // semi-automatic rifle
    WikiId::new(22999537),  // vehicle family
    WikiId::new(23039057),  // bus model
    WikiId::new(2312440),   // sportplane
    WikiId::new(2389789),   // steel roller coaster
    WikiId::new(2424752),   // product
    WikiId::new(24294815),  // diesel-hydraulic locomotive
    WikiId::new(243249),    // air-to-air missile
    WikiId::new(253555),    // escort carrier
    WikiId::new(25653),     // ferry ship
    WikiId::new(2607934),   // guided missile destroyer
    WikiId::new(26251835),  // diesel-mechanical locomotive
    WikiId::new(26529),     // space probe
    WikiId::new(26540),     // artificial satellite
    WikiId::new(2735392),   // utility helicopter
    WikiId::new(2811),      // submarine
    WikiId::new(281460),    // Pipe organ
    WikiId::new(282472),    // anti-tank missile
    WikiId::new(2875704),   // fixed-wing aircraft
    WikiId::new(28885102),  // pharmaceutical product
    WikiId::new(29048322),  // vehicle model
    WikiId::new(3041792),   // reconnaissance aircraft
    WikiId::new(3119075),   // floatplane
    WikiId::new(324233),    // torpedo boat
    WikiId::new(331795),    // patrol vessel
    WikiId::new(335225),    // rocket engine
    WikiId::new(3389302),   // semi-automatic pistol
    WikiId::new(3407658),   // tram
    WikiId::new(34336),     // diesel locomotive
    WikiId::new(34486),     // helicopter
    WikiId::new(353699),    // container feeder ship
    WikiId::new(3609612),   // rifled breech loader
    WikiId::new(376055),    // Garratt locomotive
    WikiId::new(391022),    // research vessel
    WikiId::new(39495),     // tractor
    WikiId::new(3959904),   // multiple unit
    WikiId::new(39804),     // cruise ship
    WikiId::new(402092),    // motor ship
    WikiId::new(40280158),  // bolt-action rifle
    WikiId::new(4102249),   // subway car
    WikiId::new(41207),     // coin
    WikiId::new(41426),     // trainer aircraft
    WikiId::new(4167410),   // Wikimedia disambiguation page
    WikiId::new(42319471),  // trolleybus model
    WikiId::new(428661),    // U-boat
    WikiId::new(431289),    // brand
    WikiId::new(4407246),   // armored personnel carrier
    WikiId::new(44167),     // engine
    WikiId::new(44740228),  // wall hanging
    WikiId::new(45296117),  // aircraft type
    WikiId::new(466421),    // reconnaissance satellite
    WikiId::new(466704),    // surface-to-air missile
    WikiId::new(473932),    // roll-on/roll-off ship
    WikiId::new(4809),      // rocket stage
    WikiId::new(4818021),   // attack submarine
    WikiId::new(4830453),   // business
    WikiId::new(483373),    // electric multiple unit
    WikiId::new(486396),    // shotgun
    WikiId::new(502048),    // gasoline engine
    WikiId::new(509222),    // drillship
    WikiId::new(511450),    // naval artillery
    WikiId::new(55725952),  // tractor model
    WikiId::new(559026),    // ship class
    WikiId::new(5638),      // bus
    WikiId::new(5810820),   // dredger
    WikiId::new(593485),    // liberty ship
    WikiId::new(59773381),  // automobile model series
    WikiId::new(610398),    // system on a chip
    WikiId::new(617792),    // compound locomotive
    WikiId::new(625151),    // electric locomotive
    WikiId::new(630010),    // reciprocating engine
    WikiId::new(640078),    // minelayer
    WikiId::new(643532),    // anti-ship missile
    WikiId::new(649062),    // armored car
    WikiId::new(654051),    // turbofan
    WikiId::new(654749),    // articulated bus
    WikiId::new(660668),    // training ship
    WikiId::new(673687),    // racing automobile
    WikiId::new(68072093),  // land-based utility aircraft
    WikiId::new(683570),    // ballistic missile submarine
    WikiId::new(69129709),  // land-based fighter monoplane
    WikiId::new(697175),    // launch vehicle
    WikiId::new(697196),    // ocean liner
    WikiId::new(7132141),   // railcars
    WikiId::new(7325635),   // anti-aircraft gun
    WikiId::new(744620),    // grand tourer
    WikiId::new(751705),    // autocannon
    WikiId::new(752392),    // railcar
    WikiId::new(752783),    // human spaceflight
    WikiId::new(753224),    // torpedo bomber
    WikiId::new(753779),    // passenger railroad car
    WikiId::new(757587),    // nuclear-powered attack submarine
    WikiId::new(778129),    // light cruiser
    WikiId::new(785745),    // tank locomotive
    WikiId::new(80831),     // integrated circuit
    WikiId::new(811701),    // model series
    WikiId::new(811704),    // rolling stock class
    WikiId::new(8142),      // currency
    WikiId::new(815423),    // supermini
    WikiId::new(830335),    // protected cruiser
    WikiId::new(847109),    // dreadnought
    WikiId::new(847478),    // armored cruiser
    WikiId::new(852190),    // shipwreck
    WikiId::new(854585),    // double-decker bus
    WikiId::new(854845),    // Earth observation satellite
    WikiId::new(860861),    // sculpture
    WikiId::new(875583),    // light tank
    WikiId::new(875600),    // crossover
    WikiId::new(892492),    // third-rate
    WikiId::new(898771),    // heavy cruiser
    WikiId::new(90834785),  // racing automobile model
    WikiId::new(920182),    // Main battle tank
    WikiId::new(928235),    // sloop-of-war
    WikiId::new(946596),    // off-road vehicle
    WikiId::new(95945728),  // technology demonstration spacecraft
    WikiId::new(972011),    // sports prototype
    WikiId::new(973740),    // whaler
];

/// Data related to a products.
#[derive(Clone, Debug)]
pub struct Product {
    /// IDs of te classes this product belongs to.
    classes: HashSet<WikiId>,
}

/// Data related to a class.
#[derive(Clone, Debug)]
pub struct Class {
    /// Class ID.
    id: WikiId,

    /// Class label (name).
    label: String,

    /// Number of products belonging to this class.
    amount: usize,
}

impl Class {
    /// Copies this class with diffrent `amount`.
    #[must_use]
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
#[derive(Debug, Default, Clone)]
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

#[derive(Clone)]
pub struct AnalysisWorker {
    collector: AnalysisCollector,
    sources: Arc<AnalysisSources>,
}

impl AnalysisWorker {
    #[must_use]
    pub fn new(sources: Arc<AnalysisSources>) -> Self {
        Self { collector: AnalysisCollector::default(), sources }
    }

    pub fn get_classes(item: &Item) -> Result<HashSet<WikiId>, errors::ProcessingError> {
        let mut classes = HashSet::<WikiId>::new();
        if let Some(superclasses) = item.get_superclasses()? {
            classes.extend(superclasses);
        }
        if let Some(categories) = item.get_classes()? {
            classes.extend(categories);
        }
        Ok(classes)
    }

    /// Checks if the passed item represents a class.
    fn is_class(&self, item: &Item) -> bool {
        self.sources.wikidata.has_class_id(&item.id)
    }

    /// Checks if the passed item represents a product.
    #[allow(clippy::unused_self)]
    fn is_product(&self, item: &Item) -> bool {
        item.has_manufacturer()
    }
}

#[async_trait]
impl runners::WikidataWorker for AnalysisWorker {
    type Output = AnalysisCollector;

    async fn process(
        &mut self,
        _msg: &str,
        entity: Entity,
        _tx: parallel::Sender<Self::Output>,
    ) -> Result<(), errors::ProcessingError> {
        match entity {
            Entity::Item(item) => {
                if let Some(label) = item.get_label(Language::En) {
                    if self.is_class(&item) {
                        self.collector.add_class(Class {
                            id: item.id,
                            label: label.to_string(),
                            amount: 1,
                        });
                    }
                    if self.is_product(&item) {
                        let classes = Self::get_classes(&item)?;
                        if !classes.is_empty() {
                            self.collector.add_product(Product { classes });
                        }
                    }
                }
            }
            Entity::Property(_property) => (),
        }
        Ok(())
    }

    async fn finish(
        self,
        tx: parallel::Sender<Self::Output>,
    ) -> Result<(), errors::ProcessingError> {
        tx.send(self.collector).await;
        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
pub struct AnalysisStash {
    /// Collected data.
    collector: AnalysisCollector,
}

#[async_trait]
impl runners::Stash for AnalysisStash {
    type Input = AnalysisCollector;

    fn stash(&mut self, input: Self::Input) -> Result<(), errors::ProcessingError> {
        self.collector.merge(input);
        Ok(())
    }

    fn finish(self) -> Result<(), errors::ProcessingError> {
        log::info!("Found {} products", self.collector.products.len());
        log::info!("Found {} classes", self.collector.classes.len());

        let categories: HashSet<WikiId> =
            wikidata::items::ALL.iter().filter_map(|s| WikiId::try_from(*s).ok()).collect();
        let ignored_classes = HashSet::<WikiId>::from(IGNORED_CLASSES);
        let classes: HashMap<WikiId, Class> =
            self.collector.classes.iter().map(|c| (c.id, c.clone())).collect();

        let mut uncategorized_classes = HashMap::<WikiId, Class>::new();
        for p in &self.collector.products {
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

pub struct AnalysisRunner;

impl AnalysisRunner {
    pub fn run(config: &config::AnalysisConfig) -> Result<(), errors::ProcessingError> {
        let sources = Arc::new(AnalysisSources::load(config)?);

        let worker = AnalysisWorker::new(sources);
        let stash = AnalysisStash::default();

        let flow = parallel::Flow::new();
        runners::WikidataRunner::flow(flow, config, worker, stash)?.join();

        Ok(())
    }
}
