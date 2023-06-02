//! Extensions for types from `consumers_wikidata` crate.

use consumers_wikidata::{data, properties};

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
        WIDE_ANGLE_LENS,
        YACHT,
        ZOOM_LENS,
    ];
}

pub trait ItemExt {
    /// Returns items label in the speified language.
    fn get_label(&self, lang: data::Language) -> Option<&str>;

    /// Returns ID associated with the passed property.
    fn get_entity_ids(&self, property_id: &str) -> Option<Vec<data::Id>>;

    /// Returns strings associated with the passed property.
    fn get_strings(&self, property_id: &str) -> Option<Vec<String>>;

    /// Checks if has at least one passed property.
    fn has_property(&self, property_id: &str) -> bool;

    /// Checks if this item is related to another via a specified property.
    fn relates(&self, property_id: &str, class_id: &str) -> bool;

    /// Returns IDs of entities linked with "follows" property.
    #[must_use]
    fn get_follows(&self) -> Option<Vec<data::Id>>;

    /// Returns IDs of entities linked with "followed by" property.
    #[must_use]
    fn get_followed_by(&self) -> Option<Vec<data::Id>>;

    /// Returns IDs of entities linked with "manufacturer" property.
    #[must_use]
    fn get_manufacturer_ids(&self) -> Option<Vec<data::Id>>;

    /// Checks if has any entities linked with "manufacturer" property.
    #[must_use]
    fn has_manufacturer(&self) -> bool;

    /// Returns IDs of entities linked with "official website" property.
    #[must_use]
    fn get_official_websites(&self) -> Option<Vec<String>>;

    /// Checks if has entities linked with "official website" property.
    #[must_use]
    fn has_official_website(&self) -> bool;

    /// Checks if this items is linked to the passed entity with `instalce of` property.
    ///
    /// In simpler words: chacks if this item is an instance of the passed class.
    #[must_use]
    fn is_instance_of(&self, class: &str) -> bool;

    /// Returns IDs of classes this item is an instance of.
    #[must_use]
    fn get_classes(&self) -> Option<Vec<data::Id>>;

    /// Checks if this item is a subclass of the given class.
    #[must_use]
    fn is_subclass_of(&self, class: &str) -> bool;

    /// Returns all superclasses of this item.
    #[must_use]
    fn get_superclasses(&self) -> Option<Vec<data::Id>>;
}

impl ItemExt for data::Item {
    fn get_label(&self, lang: data::Language) -> Option<&str> {
        self.labels.get(lang.as_str()).map(|label| label.value.as_str())
    }

    fn get_entity_ids(&self, property_id: &str) -> Option<Vec<data::Id>> {
        if let Some(claims) = self.claims.get(property_id) {
            let mut result = Vec::<data::Id>::new();
            for claim in claims {
                let data::Claim::Statement(statement) = claim;
                if let data::Snak::Value(value) = &statement.mainsnak {
                    if let data::DataValue::WikibaseEntityId(data::EntityIdDataValue::Item(
                        entity_info,
                    )) = &value.datavalue
                    {
                        result.push(entity_info.id.clone());
                    }
                }
            }
            Some(result)
        } else {
            None
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

    #[must_use]
    fn get_follows(&self) -> Option<Vec<data::Id>> {
        self.get_entity_ids(properties::FOLLOWS)
    }

    #[must_use]
    fn get_followed_by(&self) -> Option<Vec<data::Id>> {
        self.get_entity_ids(properties::FOLLOWED_BY)
    }

    #[must_use]
    fn get_manufacturer_ids(&self) -> Option<Vec<data::Id>> {
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
    fn is_instance_of(&self, class: &str) -> bool {
        self.relates(properties::INSTANCE_OF, class)
    }

    #[must_use]
    fn get_classes(&self) -> Option<Vec<data::Id>> {
        self.get_entity_ids(properties::INSTANCE_OF)
    }

    #[must_use]
    fn is_subclass_of(&self, class: &str) -> bool {
        self.relates(properties::SUBCLASS_OF, class)
    }

    #[must_use]
    fn get_superclasses(&self) -> Option<Vec<data::Id>> {
        self.get_entity_ids(properties::SUBCLASS_OF)
    }
}
