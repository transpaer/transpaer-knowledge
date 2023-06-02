use crate::wikidata::items;

pub const SMARTPHONE: &[&str] = &[
    items::SMARTPHONE_MODEL,
    items::MOBILE_PHONE,
    items::SMARTPHONE,
    items::PHABLET,
    items::CELL_PHONE_MODEL,
];

pub const SMARTWATCH: &[&str] = &[items::SMARTWATCH_MODEL];

pub const TABLET: &[&str] = &[items::TABLET_COMPUTER];

pub const LAPTOP: &[&str] = &[items::LAPTOP];

pub const COMPUTER: &[&str] =
    &[items::COMPUTER, items::COMPUTER_MODEL, items::HOME_COMPUTER, items::PERSONAL_COMPUTER];

pub const GAME_CONSOLE: &[&str] =
    &[items::HOME_VIDEO_GAME_CONSOLE, items::HANDHELD_GAME_CONSOLE, items::VIDEO_GAME_CONSOLE];

pub const GAME_CONTROLLER: &[&str] = &[items::GAME_CONTROLLER];

pub const CAMERA: &[&str] = &[
    items::CAMERA_MODEL,
    items::COMPACT_DIGITAL_CAMERA,
    items::DIGITAL_CAMERA,
    items::DIGITAL_SINGLE_LENS_REFLEX_CAMERA,
    items::MIRRORLESS_INTERCHANGEABLE_LENS_CAMERA,
    items::SINGLE_LENS_REFLEX_CAMERA,
];

pub const CAMERA_LENS: &[&str] = &[
    items::CAMERA_LENS,
    items::PRIME_LENS,
    items::TELEPHOTO_LENS,
    items::WIDE_ANGLE_LENS,
    items::ZOOM_LENS,
];

pub const MICROPROCESSOR: &[&str] = &[
    items::CENTRAL_PROCESSING_UNIT,
    items::GRAPHICS_PROCESSING_UNIT,
    items::MICROCONTROLLER,
    items::MICROPROCESSOR,
];

pub const CALCULATOR: &[&str] = &[items::CALCULATOR];

pub const MUSICAL_INSTRUMENT: &[&str] =
    &[items::ELECTRIC_GUITAR, items::GUITAR, items::MUSICAL_INSTRUMENT_MODEL];

pub const CAR: &[&str] = &[
    items::AUTOMOBILE_MODEL,
    items::CITY_CAR,
    items::COMPACT_CAR,
    items::CONCEPT_CAR,
    items::ELECTRIC_CAR,
    items::EXECUTIVE_CAR,
    items::FAMILY_CAR,
    items::KEI_CAR,
    items::LUXURY_VEHICLE,
    items::MID_SIZE_CAR,
    items::MINIVAN,
    items::MOTOR_CAR,
    items::PICKUP_TRUCK,
    items::SPORTS_CAR,
    items::SPORT_UTILITY_VEHICLE,
    items::SUBCOMPACT_CAR,
    items::SUPERCAR,
    items::TRUCK,
    items::VAN,
];

pub const MOTORCYCLE: &[&str] = &[items::MOTORCYCLE, items::MOTORCYCLE_MODEL, items::MOTOR_SCOOTER];

pub const BOAT: &[&str] = &[items::CATAMARAN, items::MONOHULL, items::SAILING_SHIP, items::YACHT];

pub const DRONE: &[&str] = &[items::UNMANNED_AERIAL_VEHICLE];

pub const DRINK: &[&str] = &[items::DRINK, items::SOFT_DRINK];

pub const FOOD: &[&str] = &[items::FOOD_BRAND];

pub const TOY: &[&str] = &[items::TOY, items::ACTION_FIGURE];
