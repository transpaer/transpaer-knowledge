// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use serde::{de::Deserializer, Deserialize, Serialize};

// NOTE: In the API separators are dots `.`.
pub const SEPARATOR: char = '/';

#[derive(thiserror::Error, Debug)]
pub enum CategoryError {
    #[error("`{value}` is not a valid category")]
    Invalid { value: String },
}

/// Represents a category as a string with subcategories divided by `/`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct Category {
    string: String,
}

impl Category {
    /// Constructs a new `Category` from the given string.
    ///
    /// # Errors
    ///
    /// Returns an error if the string is not a valid category.
    pub fn new(string: String) -> Result<Self, CategoryError> {
        if Self::is_valid_category(&string) {
            Ok(Self { string })
        } else {
            Err(CategoryError::Invalid { value: string })
        }
    }

    #[must_use]
    pub fn is_valid_category(string: &str) -> bool {
        let mut node: &Node = &CATEGORIES;
        if node.name == string {
            return true;
        }
        for cat in string.split(SEPARATOR) {
            if let Some(find) = node.sub.iter().find(|e| e.name == cat) {
                node = find;
            } else {
                return false;
            }
        }
        true
    }

    #[must_use]
    pub fn get_info(&self) -> Option<Info> {
        let mut node: &Node = &CATEGORIES;
        if node.name == self.string {
            return Some(node.to_info());
        }
        for cat in self.string.split(SEPARATOR) {
            if let Some(find) = node.sub.iter().find(|e| e.name == cat) {
                node = find;
            } else {
                return None;
            }
        }
        Some(node.to_info())
    }

    #[must_use]
    pub fn get_string(&self) -> String {
        self.string.clone()
    }
}

impl<'de> Deserialize<'de> for Category {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let value = String::deserialize(d)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

/// Defines how well prepared the category is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Exploratory,
    Incomplete,
    Satisfactory,
    Complete,
    Broad,
}

impl Status {
    #[must_use]
    pub fn are_products_comparable(&self) -> bool {
        match self {
            Self::Exploratory | Self::Incomplete | Self::Satisfactory | Self::Complete => true,
            Self::Broad => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Info {
    pub status: Status,
    pub subcategories: Vec<String>,
}

struct Node {
    /// Google Product Category associated with this (sub)category.
    #[allow(dead_code)]
    id: Option<usize>,

    /// Name for this category.
    name: &'static str,

    /// Human-readable name of this (sub)category.
    #[allow(dead_code)]
    title: &'static str,

    status: Status,

    /// Subcategories.
    sub: &'static [Node],
}

impl Node {
    fn to_info(&self) -> Info {
        Info {
            status: self.status,
            subcategories: self.sub.iter().map(|n| n.name.to_string()).collect(),
        }
    }
}

/// Defines all valid categories.
const CATEGORIES: Node = Node {
    id: None,
    name: "",
    title: "",
    status: Status::Broad,
    sub: &[
        Node {
            id: Some(141),
            name: "cameras_and_optics",
            title: "Cameras & Optics",
            status: Status::Broad,
            sub: &[
                Node {
                    id: Some(142),
                    name: "cameras",
                    title: "Cameras",
                    status: Status::Exploratory,
                    sub: &[Node {
                        id: Some(152),
                        name: "digital_cameras",
                        title: "Digital Cameras",
                        status: Status::Exploratory,
                        sub: &[],
                    }],
                },
                Node {
                    id: Some(2096),
                    name: "camera_and_optic_accessories",
                    title: "Camera & Optic Accessories",
                    status: Status::Exploratory,
                    sub: &[Node {
                        id: Some(149),
                        name: "camera_and_video_camera_lenses",
                        title: "Camera & Video Camera Lenses",
                        status: Status::Exploratory,
                        sub: &[Node {
                            id: Some(4432),
                            name: "camera_lenses",
                            title: "Camera Lenses",
                            status: Status::Exploratory,
                            sub: &[],
                        }],
                    }],
                },
            ],
        },
        Node {
            id: Some(412),
            name: "food_beverages_and_tobacco",
            title: "Food, Beverages & Tobacco",
            status: Status::Broad,
            sub: &[
                Node {
                    id: Some(413),
                    name: "beverages",
                    title: "Beverages",
                    status: Status::Exploratory,
                    sub: &[Node {
                        id: None,
                        name: "alcoholic_beverages",
                        title: "Alcoholic Beverages",
                        status: Status::Exploratory,
                        sub: &[],
                    }],
                },
                Node {
                    id: Some(422),
                    name: "food",
                    title: "Food items",
                    status: Status::Exploratory,
                    sub: &[
                        Node {
                            id: Some(136),
                            name: "food_gift_baskets",
                            title: "Food Gift Baskets",
                            status: Status::Exploratory,
                            sub: &[],
                        },
                        Node {
                            id: Some(423),
                            name: "snack_foods",
                            title: "Snack Foods",
                            status: Status::Exploratory,
                            sub: &[],
                        },
                        Node {
                            id: Some(427),
                            name: "condiments_and_sauces",
                            title: "Condiments & Sauces",
                            status: Status::Exploratory,
                            sub: &[],
                        },
                        Node {
                            id: Some(428),
                            name: "dairy_products",
                            title: "Dairy Products",
                            status: Status::Exploratory,
                            sub: &[],
                        },
                        Node {
                            id: Some(430),
                            name: "fruits_and_vegetables",
                            title: "Fruits & Vegetables",
                            status: Status::Exploratory,
                            sub: &[],
                        },
                        Node {
                            id: Some(433),
                            name: "nuts_and_seeds",
                            title: "Nuts & Seeds",
                            status: Status::Exploratory,
                            sub: &[],
                        },
                        Node {
                            id: Some(434),
                            name: "pasta_and_noodles",
                            title: "Pasta & Noodles",
                            status: Status::Exploratory,
                            sub: &[],
                        },
                        Node {
                            id: Some(1876),
                            name: "bakery",
                            title: "Bakery",
                            status: Status::Exploratory,
                            sub: &[],
                        },
                        Node {
                            id: Some(2423),
                            name: "soups_and_broths",
                            title: "Soups & Broths",
                            status: Status::Exploratory,
                            sub: &[],
                        },
                        Node {
                            id: Some(2660),
                            name: "cooking_and_baking_ingredients",
                            title: "Cooking & Baking Ingredients",
                            status: Status::Exploratory,
                            sub: &[],
                        },
                    ],
                },
                Node {
                    id: Some(435),
                    name: "tobacco",
                    title: "Tobacco Products",
                    status: Status::Exploratory,
                    sub: &[],
                },
            ],
        },
        Node {
            id: Some(1239),
            name: "toys_and_games",
            title: "Toys & Games",
            status: Status::Exploratory,
            sub: &[Node {
                id: Some(1253),
                name: "toys",
                title: "Toys",
                status: Status::Exploratory,
                sub: &[Node {
                    id: Some(2546),
                    name: "remote_control_toys",
                    title: "Remote Control Toys",
                    status: Status::Exploratory,
                    sub: &[Node {
                        id: Some(7090),
                        name: "remote_control_airships_and_blimps",
                        title: "Remote Control Airships & Blimps",
                        status: Status::Exploratory,
                        sub: &[],
                    }],
                }],
            }],
        },
        Node {
            id: Some(222),
            name: "electronics",
            title: "Electronics",
            status: Status::Broad,
            sub: &[
                Node {
                    id: Some(262),
                    name: "communications",
                    title: "Communications",
                    status: Status::Broad,
                    sub: &[Node {
                        id: Some(270),
                        name: "telephony",
                        title: "Telephony",
                        status: Status::Exploratory,
                        sub: &[Node {
                            id: Some(267),
                            name: "mobile_phones",
                            title: "Mobile Phones",
                            status: Status::Incomplete,
                            sub: &[],
                        }],
                    }],
                },
                Node {
                    id: Some(278),
                    name: "computers",
                    title: "Computers",
                    status: Status::Exploratory,
                    sub: &[
                        Node {
                            id: Some(325),
                            name: "desktop_computers",
                            title: "Desktop Computers",
                            status: Status::Exploratory,
                            sub: &[],
                        },
                        Node {
                            id: Some(328),
                            name: "laptops",
                            title: "Laptops",
                            status: Status::Exploratory,
                            sub: &[],
                        },
                        Node {
                            id: Some(4745),
                            name: "tablet_computers",
                            title: "Tablet Computers",
                            status: Status::Exploratory,
                            sub: &[],
                        },
                        Node {
                            id: None,
                            name: "smartwatches",
                            title: "Smartwatches",
                            status: Status::Exploratory,
                            sub: &[],
                        },
                    ],
                },
                Node {
                    id: Some(1294),
                    name: "video_game_consoles",
                    title: "Video Game Consoles",
                    status: Status::Exploratory,
                    sub: &[],
                },
                Node {
                    id: Some(1270),
                    name: "video_game_console_accessories",
                    title: "Video Game Console Accessories",
                    status: Status::Exploratory,
                    sub: &[Node {
                        id: Some(1505),
                        name: "home_game_console_accessories",
                        title: "Home Game Console Accessories",
                        status: Status::Exploratory,
                        sub: &[],
                    }],
                },
            ],
        },
        Node {
            id: Some(888),
            name: "vehicles_and_parts",
            title: "Vehicles & Parts",
            status: Status::Broad,
            sub: &[Node {
                id: Some(5614),
                name: "vehicles",
                title: "Vehicles",
                status: Status::Broad,
                sub: &[
                    Node {
                        id: Some(1267),
                        name: "motor_vehicles",
                        title: "Motor Vehicles",
                        status: Status::Exploratory,
                        sub: &[],
                    },
                    Node {
                        id: Some(3540),
                        name: "watercraft",
                        title: "Watercraft",
                        status: Status::Exploratory,
                        sub: &[
                            Node {
                                id: Some(3087),
                                name: "sailboats",
                                title: "Sailboats",
                                status: Status::Exploratory,
                                sub: &[],
                            },
                            Node {
                                id: Some(5644),
                                name: "yachts",
                                title: "Yachts",
                                status: Status::Exploratory,
                                sub: &[],
                            },
                        ],
                    },
                ],
            }],
        },
        Node {
            id: Some(8),
            name: "arts_and_entertainment",
            title: "Arts & Entertainment",
            status: Status::Broad,
            sub: &[Node {
                id: Some(5710),
                name: "hobbies_and_creative_arts",
                title: "Hobbies & Creative Arts",
                status: Status::Broad,
                sub: &[Node {
                    id: Some(54),
                    name: "musical_instruments",
                    title: "Musical Instruments",
                    status: Status::Exploratory,
                    sub: &[Node {
                        id: Some(77),
                        name: "string_instruments",
                        title: "String Instruments",
                        status: Status::Exploratory,
                        sub: &[Node {
                            id: Some(80),
                            name: "guitars",
                            title: "Guitars",
                            status: Status::Exploratory,
                            sub: &[],
                        }],
                    }],
                }],
            }],
        },
        Node {
            id: Some(922),
            name: "office_supplies",
            title: "Office Supplies",
            status: Status::Broad,
            sub: &[Node {
                id: Some(950),
                name: "office_equipment",
                title: "Office Equipment",
                status: Status::Broad,
                sub: &[Node {
                    id: Some(333),
                    name: "calculators",
                    title: "Calculators",
                    status: Status::Exploratory,
                    sub: &[],
                }],
            }],
        },
        Node {
            id: Some(536),
            name: "home_and_garden",
            title: "Home & Garden",
            status: Status::Broad,
            sub: &[Node {
                id: Some(604),
                name: "household_appliances",
                title: "Household Appliances",
                status: Status::Broad,
                sub: &[Node {
                    id: Some(2706),
                    name: "laundry_appliances",
                    title: "Laundry Appliances",
                    status: Status::Exploratory,
                    sub: &[Node {
                        id: Some(2549),
                        name: "washing_machines",
                        title: "Washing Machines",
                        status: Status::Exploratory,
                        sub: &[],
                    }],
                }],
            }],
        },
    ],
};

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct Document {
        category: Category,
    }

    #[test]
    fn serde_json_leaf() {
        let document = Document {
            category: Category::new("food_beverages_and_tobacco/food/snack_foods".into()).unwrap(),
        };
        let string = "{\"category\":\"food_beverages_and_tobacco/food/snack_foods\"}".to_string();

        let serialized = serde_json::to_string(&document).unwrap();
        let deserialized: Document = serde_json::from_str(&string).unwrap();

        assert_eq!(serialized, string);
        assert_eq!(deserialized, document);
    }

    #[test]
    fn serde_json_branch() {
        let document =
            Document { category: Category::new("food_beverages_and_tobacco/food".into()).unwrap() };
        let string = "{\"category\":\"food_beverages_and_tobacco/food\"}".to_string();

        let serialized = serde_json::to_string(&document).unwrap();
        let deserialized: Document = serde_json::from_str(&string).unwrap();

        assert_eq!(serialized, string);
        assert_eq!(deserialized, document);
    }

    #[test]
    fn existing_categories_valid() {
        Category::new("food_beverages_and_tobacco".into()).unwrap();
        Category::new("food_beverages_and_tobacco/food".into()).unwrap();
        Category::new("food_beverages_and_tobacco/food/snack_foods".into()).unwrap();
    }

    #[test]
    fn not_existing_categories_not_valid() {
        assert!(Category::new("food_beverages_tobacco/food/snack_foods".into()).is_err());
        assert!(Category::new("food_beverages_and_tobacco/drinks".into()).is_err());
        assert!(Category::new("food_beverages_and_tobacco/snack_foods".into()).is_err());
    }

    #[test]
    fn root_category_info() {
        let cat = Category::new("".into()).unwrap();
        let info = cat.get_info().unwrap();
        assert_eq!(info.status, Status::Broad);
        assert_eq!(
            info.subcategories,
            vec![
                "cameras_and_optics".to_string(),
                "food_beverages_and_tobacco".to_string(),
                "toys_and_games".to_string(),
                "electronics".to_string(),
                "vehicles_and_parts".to_string(),
                "arts_and_entertainment".to_string(),
                "office_supplies".to_string(),
                "home_and_garden".to_string()
            ]
        );
    }
}
