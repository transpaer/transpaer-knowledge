//! Extensions for types from `sustainity_wikidata` crate.

use std::collections::HashSet;

use sustainity_wikidata::{data, errors, properties};

pub use sustainity_wikidata::data::Id as WikiId;

use crate::utils;

pub mod items {
    pub const ACTION_FIGURE: &str = "Q343566";
    pub const ALCOHOL_BRAND: &str = "Q80359036";
    pub const AUTOMOBILE_MODEL: &str = "Q3231690";
    pub const CALCULATOR: &str = "Q31087";
    pub const CAMERA_LENS: &str = "Q192234";
    pub const CAMERA_MODEL: &str = "Q20888659";
    pub const CATAMARAN: &str = "Q190403";
    pub const CELL_PHONE_MODEL: &str = "Q19723444";
    pub const CENTRAL_PROCESSING_UNIT: &str = "Q5300";
    pub const CITY_CAR: &str = "Q504154";
    pub const COMPACT_CAR: &str = "Q946808";
    pub const COMPACT_DIGITAL_CAMERA: &str = "Q106646709";
    pub const COMPUTER_MODEL: &str = "Q55990535";
    pub const COMPUTER: &str = "Q68";
    pub const CONCEPT_CAR: &str = "Q850270";
    pub const DIGITAL_CAMERA: &str = "Q62927";
    pub const DIGITAL_SINGLE_LENS_REFLEX_CAMERA: &str = "Q196342";
    pub const DRINK: &str = "Q40050";
    pub const ELECTRIC_CAR: &str = "Q193692";
    pub const ELECTRIC_GUITAR: &str = "Q78987";
    pub const EXECUTIVE_CAR: &str = "Q1357619";
    pub const FAMILY_CAR: &str = "Q1940287";
    pub const FOOD_BRAND: &str = "Q16323605";
    pub const GAME_CONTROLLER: &str = "Q865422";
    pub const GRAPHICS_PROCESSING_UNIT: &str = "Q183484";
    pub const GUITAR: &str = "Q6607";
    pub const HANDHELD_GAME_CONSOLE: &str = "Q941818";
    pub const HOME_COMPUTER: &str = "Q473708";
    pub const HOME_VIDEO_GAME_CONSOLE: &str = "Q17589470";
    pub const KEI_CAR: &str = "Q1059437";
    pub const LAPTOP: &str = "Q3962";
    pub const LUXURY_VEHICLE: &str = "Q5581707";
    pub const MICROCONTROLLER: &str = "Q165678";
    pub const MICROPROCESSOR: &str = "Q5297";
    pub const MID_SIZE_CAR: &str = "Q4010528";
    pub const MINIVAN: &str = "Q223189";
    pub const MIRRORLESS_INTERCHANGEABLE_LENS_CAMERA: &str = "Q209918";
    pub const MOBILE_PHONE: &str = "Q17517";
    pub const MONOHULL: &str = "Q1999103";
    pub const MOTOR_CAR: &str = "Q1420";
    pub const MOTORCYCLE_MODEL: &str = "Q23866334";
    pub const MOTORCYCLE: &str = "Q34493";
    pub const MOTOR_SCOOTER: &str = "Q193234";
    pub const MUSICAL_INSTRUMENT_MODEL: &str = "Q29982117";
    pub const PERSONAL_COMPUTER: &str = "Q16338";
    pub const PHABLET: &str = "Q521097";
    pub const PICKUP_TRUCK: &str = "Q215601";
    pub const PRIME_LENS: &str = "Q631962";
    pub const SAILING_SHIP: &str = "Q170483";
    pub const SINGLE_LENS_REFLEX_CAMERA: &str = "Q196353";
    pub const SMARTPHONE_MODEL: &str = "Q19723451";
    pub const SMARTPHONE_MODEL_SERIES: &str = "Q71266741";
    pub const SMARTPHONE: &str = "Q22645";
    pub const SMARTWATCH_MODEL: &str = "Q19799938";
    pub const SOFT_DRINK: &str = "Q147538";
    pub const SPORTS_CAR: &str = "Q274586";
    pub const SPORT_UTILITY_VEHICLE: &str = "Q192152";
    pub const SUBCOMPACT_CAR: &str = "Q2704381";
    pub const SUPERCAR: &str = "Q815679";
    pub const TABLET_COMPUTER: &str = "Q155972";
    pub const TELEPHOTO_LENS: &str = "Q516461";
    pub const TOY: &str = "Q11422";
    pub const TRUCK: &str = "Q43193";
    pub const UNMANNED_AERIAL_VEHICLE: &str = "Q484000";
    pub const VAN: &str = "Q193468";
    pub const VIDEO_GAME_CONSOLE: &str = "Q8076";
    pub const WASHING_MACHINE: &str = "Q124441";
    pub const WASHING_MACHINE_MODEL: &str = "Q109736715";
    pub const WIDE_ANGLE_LENS: &str = "Q632867";
    pub const YACHT: &str = "Q170173";
    pub const ZOOM_LENS: &str = "Q220310";

    pub const ALL: &[&str] = &[
        ACTION_FIGURE,
        ALCOHOL_BRAND,
        AUTOMOBILE_MODEL,
        CALCULATOR,
        CAMERA_LENS,
        CAMERA_MODEL,
        CATAMARAN,
        CELL_PHONE_MODEL,
        CENTRAL_PROCESSING_UNIT,
        CITY_CAR,
        COMPACT_CAR,
        COMPACT_DIGITAL_CAMERA,
        COMPUTER,
        COMPUTER_MODEL,
        CONCEPT_CAR,
        DIGITAL_CAMERA,
        DIGITAL_SINGLE_LENS_REFLEX_CAMERA,
        DRINK,
        ELECTRIC_CAR,
        ELECTRIC_GUITAR,
        EXECUTIVE_CAR,
        FAMILY_CAR,
        FOOD_BRAND,
        GAME_CONTROLLER,
        GRAPHICS_PROCESSING_UNIT,
        GUITAR,
        HANDHELD_GAME_CONSOLE,
        HOME_COMPUTER,
        HOME_VIDEO_GAME_CONSOLE,
        KEI_CAR,
        LAPTOP,
        LUXURY_VEHICLE,
        MICROCONTROLLER,
        MICROPROCESSOR,
        MID_SIZE_CAR,
        MINIVAN,
        MIRRORLESS_INTERCHANGEABLE_LENS_CAMERA,
        MOBILE_PHONE,
        MONOHULL,
        MOTOR_CAR,
        MOTORCYCLE,
        MOTORCYCLE_MODEL,
        MOTOR_SCOOTER,
        MUSICAL_INSTRUMENT_MODEL,
        PERSONAL_COMPUTER,
        PHABLET,
        PICKUP_TRUCK,
        PRIME_LENS,
        SAILING_SHIP,
        SINGLE_LENS_REFLEX_CAMERA,
        SMARTPHONE,
        SMARTPHONE_MODEL,
        SMARTPHONE_MODEL_SERIES,
        SMARTWATCH_MODEL,
        SOFT_DRINK,
        SPORTS_CAR,
        SPORT_UTILITY_VEHICLE,
        SUBCOMPACT_CAR,
        SUPERCAR,
        TABLET_COMPUTER,
        TELEPHOTO_LENS,
        TOY,
        TRUCK,
        UNMANNED_AERIAL_VEHICLE,
        VAN,
        VIDEO_GAME_CONSOLE,
        WASHING_MACHINE,
        WASHING_MACHINE_MODEL,
        WIDE_ANGLE_LENS,
        YACHT,
        ZOOM_LENS,
    ];
}

pub mod ignored {
    pub const AIRCRAFT_CARRIER: &str = "Q17205";
    pub const AIRCRAFT: &str = "Q11436";
    pub const AIRLIFTER: &str = "Q1445518";
    pub const AIRLINER: &str = "Q210932";
    pub const AIRPLANE: &str = "Q197";
    pub const AIR_TO_AIR_MISSILE: &str = "Q243249";
    pub const AIR_TO_SURFACE_MISSILE: &str = "Q1049158";
    pub const AMUSEMENT_RIDE: &str = "Q1144661";
    pub const ANTI_AIRCRAFT_GUN: &str = "Q7325635";
    pub const ANTI_SHIP_MISSILE: &str = "Q643532";
    pub const ANTI_TANK_MISSILE: &str = "Q282472";
    pub const ARMORED_CAR: &str = "Q649062";
    pub const ARMORED_CRUISER: &str = "Q847478";
    pub const ARMORED_PERSONNEL_CARRIER: &str = "Q4407246";
    pub const ARTICULATED_BUS: &str = "Q654749";
    pub const ARTIFICIAL_SATELLITE: &str = "Q26540";
    pub const A_SEGMENT: &str = "Q13267846";
    pub const ASSAULT_RIFLE: &str = "Q177456";
    pub const ATTACK_AIRCRAFT: &str = "Q208187";
    pub const ATTACK_SUBMARINE: &str = "Q4818021";
    pub const AUTOCANNON: &str = "Q751705";
    pub const AUTOMOBILE_MODEL_SERIES: &str = "Q59773381";
    pub const BALLISTIC_MISSILE_SUBMARINE: &str = "Q683570";
    pub const BATTLESHIP: &str = "Q182531";
    pub const BOLT_ACTION_RIFLE: &str = "Q40280158";
    pub const BOMBER: &str = "Q170877";
    pub const BULK_CARRIER: &str = "Q15276";
    pub const BUS_MODEL: &str = "Q23039057";
    pub const BUS: &str = "Q5638";
    pub const CARGO_SHIP: &str = "Q105999";
    pub const CARRIER_CAPABLE_AIRCRAFT: &str = "Q1261534";
    pub const CHEMICAL_TANKER: &str = "Q15254";
    pub const COACHWORK_TYPE: &str = "Q15729598";
    pub const COMBAT_VEHICLE_FAMILY: &str = "Q100709275";
    pub const COMBAT_VEHICLE_MODEL: &str = "Q100710213";
    pub const COMMUNICATIONS_SATELLITE: &str = "Q149918";
    pub const COMPOUND_LOCOMOTIVE: &str = "Q617792";
    pub const CONTAINER_FEEDER_SHIP: &str = "Q353699";
    pub const CONTAINER_SHIP: &str = "Q17210";
    pub const CORVETTE: &str = "Q170013";
    pub const CROSSOVER: &str = "Q875600";
    pub const CRUISER: &str = "Q104843";
    pub const CRUISE_SHIP: &str = "Q39804";
    pub const C_SEGMENT: &str = "Q13266730";
    pub const DESTROYER_ESCORT: &str = "Q1499623";
    pub const DESTROYER: &str = "Q174736";
    pub const DIESEL_ELECTRIC_LOCOMOTIVE: &str = "Q19842071";
    pub const DIESEL_HYDRAULIC_LOCOMOTIVE: &str = "Q24294815";
    pub const DIESEL_LOCOMOTIVE: &str = "Q34336";
    pub const DIESEL_MECHANICAL_LOCOMOTIVE: &str = "Q26251835";
    pub const DIESEL_MULTIPLE_UNIT: &str = "Q1567915";
    pub const DIESEL_RAILCAR: &str = "Q10915943";
    pub const DIGITAL_CAMERA_MODEL: &str = "Q20741022";
    pub const DOUBLE_DECKER_BUS: &str = "Q854585";
    pub const DREADNOUGHT: &str = "Q847109";
    pub const DREDGER: &str = "Q5810820";
    pub const DRILLSHIP: &str = "Q509222";
    pub const EARTH_OBSERVATION_SATELLITE: &str = "Q854845";
    pub const ELECTRIC_LOCOMOTIVE: &str = "Q625151";
    pub const ELECTRIC_MULTIPLE_UNIT: &str = "Q483373";
    pub const ELECTRIC_RAIL_MOTOR_COACH: &str = "Q16936947";
    pub const ESCORT_CARRIER: &str = "Q253555";
    pub const EXPERIMENTAL_AIRCRAFT: &str = "Q1384417";
    pub const FERRY_SHIP: &str = "Q25653";
    pub const FIGHTER_BIPLANE: &str = "Q18491037";
    pub const FIGHTER_MONOPLANE_WITH_1_ENGINE: &str = "Q18491097";
    pub const FIGHTER: &str = "Q127771";
    pub const FIREARM_MODEL: &str = "Q22704163";
    pub const FISHING_VESSEL: &str = "Q1420024";
    pub const FIXED_WING_AIRCRAFT: &str = "Q2875704";
    pub const FLOATPLANE: &str = "Q3119075";
    pub const FLYING_BOAT: &str = "Q1153376";
    pub const FORMER_ENTITY: &str = "Q15893266";
    pub const FORMULA_ONE_CAR: &str = "Q1002954";
    pub const FRIGATE: &str = "Q161705";
    pub const GARRATT_LOCOMOTIVE: &str = "Q376055";
    pub const GEOSTATIONARY_SATELLITE: &str = "Q19362195";
    pub const GPS_SATELLITE: &str = "Q1069313";
    pub const GRAND_TOURER: &str = "Q744620";
    pub const GUIDED_MISSILE_CRUISER: &str = "Q1361980";
    pub const GUIDED_MISSILE_DESTROYER: &str = "Q2607934";
    pub const GUNBOAT: &str = "Q214196";
    pub const HARBOR_TUGBOAT: &str = "Q19292005";
    pub const HEAVY_CRUISER: &str = "Q898771";
    pub const HELICOPTER: &str = "Q34486";
    pub const HIGH_SPEED_CRAFT: &str = "Q1617851";
    pub const HIGH_SPEED_TRAIN: &str = "Q13402959";
    pub const HORIZONTAL_AXIS_WIND_TURBINE: &str = "Q18433590";
    pub const HOSPITAL_SHIP: &str = "Q15888";
    pub const HUMAN_SPACEFLIGHT: &str = "Q752783";
    pub const ICEBREAKER: &str = "Q14978";
    pub const INTERNAL_COMBUSTION_ENGINE: &str = "Q12757";
    pub const IRONCLAD_WARSHIP: &str = "Q1410980";
    pub const LAND_BASED_AIRCRAFT: &str = "Q2125613";
    pub const LAND_BASED_FIGHTER_MONOPLANE: &str = "Q69129709";
    pub const LAND_BASED_UTILITY_AIRCRAFT: &str = "Q68072093";
    pub const LARGE_TORPEDO_BOAT: &str = "Q1549739";
    pub const LAUNCH_VEHICLE: &str = "Q697175";
    pub const LIBERTY_SHIP: &str = "Q593485";
    pub const LIGHT_CRUISER: &str = "Q778129";
    pub const LIGHT_TANK: &str = "Q875583";
    pub const LNG_CARRIER: &str = "Q15247";
    pub const LOCOMOTIVE_CLASS: &str = "Q19832486";
    pub const LOW_ENTRY_BUS: &str = "Q1872522";
    pub const LOW_FLOOR_BUS: &str = "Q1078334";
    pub const MAIN_BATTLE_TANK: &str = "Q920182";
    pub const MALLET_LOCOMOTIVE: &str = "Q120319";
    pub const MEDICATION: &str = "Q12140";
    pub const MIDIBUS: &str = "Q1245737";
    pub const MILITARY_AIRCRAFT: &str = "Q216916";
    pub const MILITARY_VEHICLE: &str = "Q1184840";
    pub const MINELAYER: &str = "Q640078";
    pub const MINESWEEPER: &str = "Q202527";
    pub const MIXED_CARGO_SHIP: &str = "Q1752434";
    pub const MOPED: &str = "Q201783";
    pub const MULTIPLE_UNIT: &str = "Q3959904";
    pub const MULTI_PURPOSE_VESSEL: &str = "Q1917626";
    pub const NAVAL_ARTILLERY: &str = "Q511450";
    pub const NUCLEAR_POWERED_ATTACK_SUBMARINE: &str = "Q757587";
    pub const OCEAN_LINER: &str = "Q697196";
    pub const OFF_ROAD_VEHICLE: &str = "Q946596";
    pub const OFFSHORE_PATROL_VESSEL: &str = "Q11479409";
    pub const OIL_TANKER: &str = "Q14928";
    pub const PADDLE_STEAMER: &str = "Q1121471";
    pub const PASSENGER_RAILROAD_CAR: &str = "Q753779";
    pub const PASSENGER_SHIP: &str = "Q2055880";
    pub const PATROL_VESSEL: &str = "Q331795";
    pub const PHARMACEUTICAL_PRODUCT: &str = "Q28885102";
    pub const PLATFORM_SUPPLY_VESSEL: &str = "Q1201871";
    pub const PRE_DREADNOUGHT_BATTLESHIP: &str = "Q1051067";
    pub const PROPOSED_AIRCRAFT: &str = "Q15061018";
    pub const PROTECTED_CRUISER: &str = "Q830335";
    pub const PROTOTYPE_AIRCRAFT_MODEL: &str = "Q15126161";
    pub const RACING_AUTOMOBILE_MODEL: &str = "Q90834785";
    pub const RACING_AUTOMOBILE: &str = "Q673687";
    pub const RACK_LOCOMOTIVE: &str = "Q143872";
    pub const RAILCARS: &str = "Q7132141";
    pub const RAILCAR: &str = "Q752392";
    pub const RECIPROCATING_ENGINE: &str = "Q630010";
    pub const RECONNAISSANCE_AIRCRAFT: &str = "Q3041792";
    pub const RECONNAISSANCE_SATELLITE: &str = "Q466421";
    pub const REEFER_SHIP: &str = "Q1504307";
    pub const REPLENISHMENT_OILER: &str = "Q162986";
    pub const RESCUE_BOAT: &str = "Q1361551";
    pub const RESEARCH_VESSEL: &str = "Q391022";
    pub const REVOLVER: &str = "Q170382";
    pub const RIFLED_BREECH_LOADER: &str = "Q3609612";
    pub const RIFLE: &str = "Q124072";
    pub const ROAD_SWITCHER: &str = "Q13622547";
    pub const ROCKET_ENGINE: &str = "Q335225";
    pub const ROCKET_STAGE: &str = "Q4809";
    pub const ROLLING_STOCK_CLASS: &str = "Q811704";
    pub const ROLL_ON_ROLL_OFF_SHIP: &str = "Q473932";
    pub const SAAB_340B: &str = "Q15623405";
    pub const SAILPLANE: &str = "Q180173";
    pub const SCHOONER: &str = "Q204577";
    pub const SEA_RESCUE_VESSEL: &str = "Q11997320";
    pub const SEMI_AUTOMATIC_PISTOL: &str = "Q3389302";
    pub const SEMI_AUTOMATIC_RIFLE: &str = "Q2291248";
    pub const SHIP_CLASS: &str = "Q559026";
    pub const SHIP_OF_THE_LINE: &str = "Q207452";
    pub const SHIPWRECK: &str = "Q852190";
    pub const SHOTGUN: &str = "Q486396";
    pub const SLOOP_OF_WAR: &str = "Q928235";
    pub const SMALL_PATROL_BOAT: &str = "Q10316200";
    pub const SNIPER_RIFLE: &str = "Q190570";
    pub const SPACE_INSTRUMENT: &str = "Q100349043";
    pub const SPACE_PROBE: &str = "Q26529";
    pub const SPACE_TELESCOPE: &str = "Q148578";
    pub const SPORTPLANE: &str = "Q2312440";
    pub const SPORTS_PROTOTYPE: &str = "Q972011";
    pub const STANDARD_CIRCULATION_COIN: &str = "Q110944598";
    pub const STEAMBOAT: &str = "Q178193";
    pub const STEAM_LOCOMOTIVE: &str = "Q171043";
    pub const STEAMSHIP: &str = "Q12859788";
    pub const STEEL_ROLLER_COASTER: &str = "Q2389789";
    pub const SUBMACHINE_GUN: &str = "Q178550";
    pub const SUBMARINE: &str = "Q2811";
    pub const SUBWAY_CAR: &str = "Q4102249";
    pub const SUPERCOMPUTER: &str = "Q121117";
    pub const SUPERMINI: &str = "Q815423";
    pub const SURFACE_TO_AIR_MISSILE: &str = "Q466704";
    pub const SURVEY_VESSEL: &str = "Q1303735";
    pub const SYNTHESIZER_MODEL: &str = "Q19716577";
    pub const TANKER: &str = "Q14970";
    pub const TANK_LANDING_SHIP: &str = "Q11229656";
    pub const TANK_LOCOMOTIVE: &str = "Q785745";
    pub const TANK: &str = "Q12876";
    pub const TAPESTRY: &str = "Q184296";
    pub const TECHNOLOGY_DEMONSTRATION_SPACECRAFT: &str = "Q95945728";
    pub const TENDER_LOCOMOTIVE: &str = "Q20650761";
    pub const THIRD_RATE: &str = "Q892492";
    pub const TORPEDO_BOAT_DESTROYER: &str = "Q16220775";
    pub const TORPEDO_BOAT: &str = "Q324233";
    pub const TORPEDO_BOMBER: &str = "Q753224";
    pub const TRAINER_AIRCRAFT: &str = "Q41426";
    pub const TRAIN_FERRY: &str = "Q15262";
    pub const TRAINING_SHIP: &str = "Q660668";
    pub const TRAM: &str = "Q3407658";
    pub const TROLLEYBUS_MODEL: &str = "Q42319471";
    pub const TUG: &str = "Q191826";
    pub const TURBOCHARGED_DIESEL_ENGINE: &str = "Q109697418";
    pub const TURBOFAN: &str = "Q654051";
    pub const U_BOAT: &str = "Q428661";
    pub const UTILITY_AIRCRAFT: &str = "Q1130697";
    pub const UTILITY_HELICOPTER: &str = "Q2735392";
    pub const VEHICLE_FAMILY: &str = "Q22999537";
    pub const VEHICLE_MODEL: &str = "Q29048322";
    pub const WALL_HANGING: &str = "Q44740228";
    pub const WATERCRAFT: &str = "Q1229765";
    pub const WEAPON_FAMILY: &str = "Q15142889";
    pub const WEAPON_MODEL: &str = "Q15142894";
    pub const WHALER: &str = "Q973740";
    pub const WIKIMEDIA_DISAMBIGUATION_PAGE: &str = "Q4167410";

    pub const ALL: &[&str] = &[
        AIRCRAFT,
        AIRCRAFT_CARRIER,
        AIRLIFTER,
        AIRLINER,
        AIRPLANE,
        AIR_TO_AIR_MISSILE,
        AIR_TO_SURFACE_MISSILE,
        AMUSEMENT_RIDE,
        ANTI_AIRCRAFT_GUN,
        ANTI_SHIP_MISSILE,
        ANTI_TANK_MISSILE,
        ARMORED_CAR,
        ARMORED_CRUISER,
        ARMORED_PERSONNEL_CARRIER,
        ARTICULATED_BUS,
        ARTIFICIAL_SATELLITE,
        A_SEGMENT,
        ASSAULT_RIFLE,
        ATTACK_AIRCRAFT,
        ATTACK_SUBMARINE,
        AUTOCANNON,
        AUTOMOBILE_MODEL_SERIES,
        BALLISTIC_MISSILE_SUBMARINE,
        BATTLESHIP,
        BOLT_ACTION_RIFLE,
        BOMBER,
        BULK_CARRIER,
        BUS,
        BUS_MODEL,
        CARGO_SHIP,
        CARRIER_CAPABLE_AIRCRAFT,
        CHEMICAL_TANKER,
        COACHWORK_TYPE,
        COMBAT_VEHICLE_FAMILY,
        COMBAT_VEHICLE_MODEL,
        COMMUNICATIONS_SATELLITE,
        COMPOUND_LOCOMOTIVE,
        CONTAINER_FEEDER_SHIP,
        CONTAINER_SHIP,
        CORVETTE,
        CROSSOVER,
        CRUISER,
        CRUISE_SHIP,
        C_SEGMENT,
        DESTROYER,
        DESTROYER_ESCORT,
        DIESEL_ELECTRIC_LOCOMOTIVE,
        DIESEL_HYDRAULIC_LOCOMOTIVE,
        DIESEL_LOCOMOTIVE,
        DIESEL_MECHANICAL_LOCOMOTIVE,
        DIESEL_MULTIPLE_UNIT,
        DIESEL_RAILCAR,
        DIGITAL_CAMERA_MODEL,
        DOUBLE_DECKER_BUS,
        DREADNOUGHT,
        DREDGER,
        DRILLSHIP,
        EARTH_OBSERVATION_SATELLITE,
        ELECTRIC_LOCOMOTIVE,
        ELECTRIC_MULTIPLE_UNIT,
        ELECTRIC_RAIL_MOTOR_COACH,
        ESCORT_CARRIER,
        EXPERIMENTAL_AIRCRAFT,
        FERRY_SHIP,
        FIGHTER,
        FIGHTER_BIPLANE,
        FIGHTER_MONOPLANE_WITH_1_ENGINE,
        FIREARM_MODEL,
        FISHING_VESSEL,
        FIXED_WING_AIRCRAFT,
        FLOATPLANE,
        FLYING_BOAT,
        FORMER_ENTITY,
        FORMULA_ONE_CAR,
        FRIGATE,
        GARRATT_LOCOMOTIVE,
        GEOSTATIONARY_SATELLITE,
        GPS_SATELLITE,
        GRAND_TOURER,
        GUIDED_MISSILE_CRUISER,
        GUIDED_MISSILE_DESTROYER,
        GUNBOAT,
        HARBOR_TUGBOAT,
        HEAVY_CRUISER,
        HELICOPTER,
        HIGH_SPEED_CRAFT,
        HIGH_SPEED_TRAIN,
        HORIZONTAL_AXIS_WIND_TURBINE,
        HOSPITAL_SHIP,
        HUMAN_SPACEFLIGHT,
        ICEBREAKER,
        INTERNAL_COMBUSTION_ENGINE,
        IRONCLAD_WARSHIP,
        LAND_BASED_AIRCRAFT,
        LAND_BASED_FIGHTER_MONOPLANE,
        LAND_BASED_UTILITY_AIRCRAFT,
        LARGE_TORPEDO_BOAT,
        LAUNCH_VEHICLE,
        LIBERTY_SHIP,
        LIGHT_CRUISER,
        LIGHT_TANK,
        LNG_CARRIER,
        LOCOMOTIVE_CLASS,
        LOW_ENTRY_BUS,
        LOW_FLOOR_BUS,
        MAIN_BATTLE_TANK,
        MALLET_LOCOMOTIVE,
        MEDICATION,
        MIDIBUS,
        MILITARY_AIRCRAFT,
        MILITARY_VEHICLE,
        MINELAYER,
        MINESWEEPER,
        MIXED_CARGO_SHIP,
        MOPED,
        MULTIPLE_UNIT,
        MULTI_PURPOSE_VESSEL,
        NAVAL_ARTILLERY,
        NUCLEAR_POWERED_ATTACK_SUBMARINE,
        OCEAN_LINER,
        OFF_ROAD_VEHICLE,
        OFFSHORE_PATROL_VESSEL,
        OIL_TANKER,
        PADDLE_STEAMER,
        PASSENGER_RAILROAD_CAR,
        PASSENGER_SHIP,
        PATROL_VESSEL,
        PHARMACEUTICAL_PRODUCT,
        PLATFORM_SUPPLY_VESSEL,
        PRE_DREADNOUGHT_BATTLESHIP,
        PROPOSED_AIRCRAFT,
        PROTECTED_CRUISER,
        PROTOTYPE_AIRCRAFT_MODEL,
        RACING_AUTOMOBILE,
        RACING_AUTOMOBILE_MODEL,
        RACK_LOCOMOTIVE,
        RAILCAR,
        RAILCARS,
        RECIPROCATING_ENGINE,
        RECONNAISSANCE_AIRCRAFT,
        RECONNAISSANCE_SATELLITE,
        REEFER_SHIP,
        REPLENISHMENT_OILER,
        RESCUE_BOAT,
        RESEARCH_VESSEL,
        REVOLVER,
        RIFLE,
        RIFLED_BREECH_LOADER,
        ROAD_SWITCHER,
        ROCKET_ENGINE,
        ROCKET_STAGE,
        ROLLING_STOCK_CLASS,
        ROLL_ON_ROLL_OFF_SHIP,
        SAAB_340B,
        SAILPLANE,
        SCHOONER,
        SEA_RESCUE_VESSEL,
        SEMI_AUTOMATIC_PISTOL,
        SEMI_AUTOMATIC_RIFLE,
        SHIP_CLASS,
        SHIP_OF_THE_LINE,
        SHIPWRECK,
        SHOTGUN,
        SLOOP_OF_WAR,
        SMALL_PATROL_BOAT,
        SNIPER_RIFLE,
        SPACE_INSTRUMENT,
        SPACE_PROBE,
        SPACE_TELESCOPE,
        SPORTPLANE,
        SPORTS_PROTOTYPE,
        STANDARD_CIRCULATION_COIN,
        STEAMBOAT,
        STEAM_LOCOMOTIVE,
        STEAMSHIP,
        STEEL_ROLLER_COASTER,
        SUBMACHINE_GUN,
        SUBMARINE,
        SUBWAY_CAR,
        SUPERCOMPUTER,
        SUPERMINI,
        SURFACE_TO_AIR_MISSILE,
        SURVEY_VESSEL,
        SYNTHESIZER_MODEL,
        TANK,
        TANKER,
        TANK_LANDING_SHIP,
        TANK_LOCOMOTIVE,
        TAPESTRY,
        TECHNOLOGY_DEMONSTRATION_SPACECRAFT,
        TENDER_LOCOMOTIVE,
        THIRD_RATE,
        TORPEDO_BOAT,
        TORPEDO_BOAT_DESTROYER,
        TORPEDO_BOMBER,
        TRAINER_AIRCRAFT,
        TRAIN_FERRY,
        TRAINING_SHIP,
        TRAM,
        TROLLEYBUS_MODEL,
        TUG,
        TURBOCHARGED_DIESEL_ENGINE,
        TURBOFAN,
        U_BOAT,
        UTILITY_AIRCRAFT,
        UTILITY_HELICOPTER,
        VEHICLE_FAMILY,
        VEHICLE_MODEL,
        WALL_HANGING,
        WATERCRAFT,
        WEAPON_FAMILY,
        WEAPON_MODEL,
        WHALER,
        WIKIMEDIA_DISAMBIGUATION_PAGE,
    ];
}

pub mod organisations {
    pub const BUSSINESS: u64 = 4_830_453;
    pub const PUBLIC_COMPANY: u64 = 891_723;
    pub const BRAND: u64 = 431_289;
    pub const RETAIL_CHAIN: u64 = 507_619;
    pub const FASHION_HOUSE: u64 = 3_661_311;
    pub const ENTERPRISE: u64 = 6_881_511;
    pub const ONLINE_SHOP: u64 = 4_382_945;
    pub const SUPERMARKET_CHAIN: u64 = 18_043_413;
    pub const CONCERN: u64 = 206_361;
    pub const CONSUMER_COOPERATIVE: u64 = 614_084;
    pub const BRICK_AND_MORTAR: u64 = 726_870;
    pub const COMPANY: u64 = 783_794;
    pub const SUBSIDIARY: u64 = 658_255;
    pub const DEPARTMENT_STORE_CHAIN: u64 = 2_549_179;
    pub const DEPARTMENT_STORE: u64 = 216_107;
    pub const FOOD_MANUFACTURER: u64 = 1_252_971;

    pub const ALL: &[u64] = &[
        BUSSINESS,
        PUBLIC_COMPANY,
        BRAND,
        RETAIL_CHAIN,
        FASHION_HOUSE,
        ENTERPRISE,
        ONLINE_SHOP,
        SUPERMARKET_CHAIN,
        CONCERN,
        CONSUMER_COOPERATIVE,
        BRICK_AND_MORTAR,
        COMPANY,
        SUBSIDIARY,
        DEPARTMENT_STORE_CHAIN,
        DEPARTMENT_STORE,
        FOOD_MANUFACTURER,
    ];
}

pub trait ItemExt {
    /// Returns items label in the speified language.
    fn get_label(&self, lang: data::Language) -> Option<&str>;

    /// Returns all labels proritizing English.
    fn get_labels(&self) -> Vec<&str>;

    /// Returns all labels and aliases.
    fn get_all_labels_and_aliases(&self) -> HashSet<&str>;

    /// Returns ID associated with the passed property.
    fn get_entity_ids(
        &self,
        property_id: &str,
    ) -> Result<Option<Vec<data::Id>>, errors::ParseIdError>;

    /// Returns strings associated with the passed property.
    fn get_strings(&self, property_id: &str) -> Option<Vec<String>>;

    /// Checks if has at least one passed property.
    fn has_property(&self, property_id: &str) -> bool;

    /// Checks if this item is related to another via a specified property.
    fn relates(&self, property_id: &str, class_id: &str) -> bool;

    /// Returns IDs of entities linked with "follows" property.
    fn get_follows(&self) -> Result<Option<Vec<data::Id>>, errors::ParseIdError>;

    /// Returns IDs of entities linked with "followed by" property.
    fn get_followed_by(&self) -> Result<Option<Vec<data::Id>>, errors::ParseIdError>;

    /// Returns IDs of entities linked with "manufacturer" property.
    fn get_manufacturer_ids(&self) -> Result<Option<Vec<data::Id>>, errors::ParseIdError>;

    /// Checks if has any entities linked with "manufacturer" property.
    #[must_use]
    fn has_manufacturer(&self) -> bool;

    /// Returns IDs of entities linked with "official website" property.
    #[must_use]
    fn get_official_websites(&self) -> Option<Vec<String>>;

    /// Checks if has entities linked with "official website" property.
    #[must_use]
    fn has_official_website(&self) -> bool;

    /// Returns strings associated with the "image" property.
    #[must_use]
    fn get_images(&self) -> Option<Vec<String>>;

    /// Checks if has associated images.
    #[must_use]
    fn has_image(&self) -> bool;

    /// Returns strings associated with the "logo image" property.
    #[must_use]
    fn get_logo_images(&self) -> Option<Vec<String>>;

    /// Checks if has associated logo images.
    #[must_use]
    fn has_logo_image(&self) -> bool;

    /// Checks if this items is linked to the passed entity with `instalce of` property.
    ///
    /// In simpler words: chacks if this item is an instance of the passed class.
    #[must_use]
    fn is_instance_of(&self, class: &str) -> bool;

    /// Returns IDs of classes this item is an instance of.
    fn get_classes(&self) -> Result<Option<Vec<data::Id>>, errors::ParseIdError>;

    /// Checks if this item is a subclass of the given class.
    #[must_use]
    fn is_subclass_of(&self, class: &str) -> bool;

    /// Returns all superclasses of this item.
    fn get_superclasses(&self) -> Result<Option<Vec<data::Id>>, errors::ParseIdError>;

    /// Returns strings associated with the "GTIN" property.
    #[must_use]
    fn get_gtins(&self) -> Option<Vec<String>>;

    /// Checks if has associated "GTIN" values.
    #[must_use]
    fn has_gtin(&self) -> bool;

    /// Returns strings associated with the "EU VAT" property.
    #[must_use]
    fn get_eu_vat_numbers(&self) -> Option<Vec<String>>;

    /// Checks if has associated "EU VAT" values.
    #[must_use]
    fn has_eu_vat_number(&self) -> bool;

    /// Checks if this item can be clasified as an organisation.
    #[must_use]
    fn is_organisation(&self) -> bool;

    /// Extracts internet domains from website addresses.
    #[must_use]
    fn extract_domains(&self) -> Option<HashSet<String>>;
}

impl ItemExt for data::Item {
    fn get_label(&self, lang: data::Language) -> Option<&str> {
        self.labels.get(lang.as_str()).map(|label| label.value.as_str())
    }

    fn get_labels(&self) -> Vec<&str> {
        if let Some(en_label) = self.labels.get(data::Language::En.as_str()) {
            vec![en_label.value.as_str()]
        } else {
            let mut labels = Vec::new();
            for (lang, label) in &self.labels {
                if lang != data::Language::En.as_str() {
                    labels.push(label.value.as_str());
                }
            }
            labels
        }
    }

    fn get_all_labels_and_aliases(&self) -> HashSet<&str> {
        let mut result = HashSet::new();
        for label in self.labels.values() {
            result.insert(label.value.as_str());
        }
        for aliases in self.aliases.values() {
            for alias in aliases {
                result.insert(alias.value.as_str());
            }
        }
        result
    }

    fn get_entity_ids(
        &self,
        property_id: &str,
    ) -> Result<Option<Vec<data::Id>>, errors::ParseIdError> {
        if let Some(claims) = self.claims.get(property_id) {
            let mut result = Vec::<data::Id>::new();
            for claim in claims {
                let data::Claim::Statement(statement) = claim;
                if let data::Snak::Value(value) = &statement.mainsnak {
                    if let data::DataValue::WikibaseEntityId(data::EntityIdDataValue::Item(
                        entity_info,
                    )) = &value.datavalue
                    {
                        result.push(entity_info.id.to_num_id()?);
                    }
                }
            }
            Ok(Some(result))
        } else {
            Ok(None)
        }
    }

    fn relates(&self, property: &str, class: &str) -> bool {
        if let Some(claims) = self.claims.get(property) {
            for claim in claims {
                let data::Claim::Statement(statement) = claim;
                if let data::Snak::Value(value) = &statement.mainsnak {
                    if let data::DataValue::WikibaseEntityId(data::EntityIdDataValue::Item(
                        entity_info,
                    )) = &value.datavalue
                    {
                        if entity_info.id == class {
                            return true;
                        }
                    }
                }
            }
        }
        false
    }

    fn get_strings(&self, property_id: &str) -> Option<Vec<String>> {
        if let Some(claims) = self.claims.get(property_id) {
            let mut result = Vec::new();
            for claim in claims {
                let data::Claim::Statement(statement) = claim;
                if let data::Snak::Value(value) = &statement.mainsnak {
                    if let data::DataValue::String(website) = &value.datavalue {
                        result.push(website.clone());
                    } else {
                        log::warn!(
                            "Item {:?} has properties {} which are not strings: {:?}",
                            self.id,
                            property_id,
                            value
                        );
                    }
                }
            }
            Some(result)
        } else {
            None
        }
    }

    fn has_property(&self, property_id: &str) -> bool {
        if let Some(claims) = self.claims.get(property_id) {
            !claims.is_empty()
        } else {
            false
        }
    }

    fn get_follows(&self) -> Result<Option<Vec<data::Id>>, errors::ParseIdError> {
        self.get_entity_ids(properties::FOLLOWS)
    }

    fn get_followed_by(&self) -> Result<Option<Vec<data::Id>>, errors::ParseIdError> {
        self.get_entity_ids(properties::FOLLOWED_BY)
    }

    fn get_manufacturer_ids(&self) -> Result<Option<Vec<data::Id>>, errors::ParseIdError> {
        self.get_entity_ids(properties::MANUFACTURER)
    }

    #[must_use]
    fn has_manufacturer(&self) -> bool {
        self.has_property(properties::MANUFACTURER)
    }

    #[must_use]
    fn get_official_websites(&self) -> Option<Vec<String>> {
        self.get_strings(properties::OFFICIAL_WEBSITE)
    }

    #[must_use]
    fn has_official_website(&self) -> bool {
        self.has_property(properties::OFFICIAL_WEBSITE)
    }

    #[must_use]
    fn get_images(&self) -> Option<Vec<String>> {
        self.get_strings(properties::IMAGE)
    }

    #[must_use]
    fn has_image(&self) -> bool {
        self.has_property(properties::IMAGE)
    }

    #[must_use]
    fn get_logo_images(&self) -> Option<Vec<String>> {
        self.get_strings(properties::LOGO_IMAGE)
    }

    #[must_use]
    fn has_logo_image(&self) -> bool {
        self.has_property(properties::LOGO_IMAGE)
    }

    #[must_use]
    fn is_instance_of(&self, class: &str) -> bool {
        self.relates(properties::INSTANCE_OF, class)
    }

    fn get_classes(&self) -> Result<Option<Vec<data::Id>>, errors::ParseIdError> {
        self.get_entity_ids(properties::INSTANCE_OF)
    }

    #[must_use]
    fn is_subclass_of(&self, class: &str) -> bool {
        self.relates(properties::SUBCLASS_OF, class)
    }

    fn get_superclasses(&self) -> Result<Option<Vec<data::Id>>, errors::ParseIdError> {
        self.get_entity_ids(properties::SUBCLASS_OF)
    }

    #[must_use]
    fn get_gtins(&self) -> Option<Vec<String>> {
        self.get_strings(properties::GTIN)
    }

    #[must_use]
    fn has_gtin(&self) -> bool {
        self.has_property(properties::GTIN)
    }

    #[must_use]
    fn get_eu_vat_numbers(&self) -> Option<Vec<String>> {
        self.get_strings(properties::EU_VAT_NUMBER)
    }

    #[must_use]
    fn has_eu_vat_number(&self) -> bool {
        self.has_property(properties::EU_VAT_NUMBER)
    }

    #[must_use]
    fn is_organisation(&self) -> bool {
        if self.has_eu_vat_number() {
            return true;
        }

        if self.has_manufacturer() {
            return false;
        }

        if let Ok(Some(ids)) = self.get_classes() {
            for id in ids {
                if organisations::ALL.contains(&id.get_value()) {
                    return true;
                }
            }
        }

        false
    }

    #[must_use]
    fn extract_domains(&self) -> Option<HashSet<String>> {
        self.get_official_websites().map(|u| utils::extract_domains_from_urls(&u))
    }
}
