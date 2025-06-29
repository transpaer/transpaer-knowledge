use serde::{de::Deserializer, Deserialize, Serialize};

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
        for cat in string.split('/') {
            if let Some(find) = node.sub.iter().find(|e| e.name == cat) {
                node = find;
            } else {
                return false;
            }
        }
        true
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

struct Node {
    /// Google Product Category associated with this (sub)category.
    #[allow(dead_code)]
    id: Option<usize>,

    /// Name for this category.
    name: &'static str,

    /// Human-readable name of this (sub)category.
    #[allow(dead_code)]
    title: &'static str,

    /// Subcategories.
    sub: &'static [Node],
}

/// Defines all valid categories.
const CATEGORIES: Node = Node {
    id: None,
    name: "",
    title: "",
    sub: &[
        Node {
            id: Some(141),
            name: "cameras_and_optics",
            title: "Cameras & Optics",
            sub: &[
                Node {
                    id: Some(142),
                    name: "cameras",
                    title: "Cameras",
                    sub: &[Node {
                        id: Some(152),
                        name: "digital_cameras",
                        title: "Digital Cameras",
                        sub: &[],
                    }],
                },
                Node {
                    id: Some(2096),
                    name: "camera_and_optic_accessories",
                    title: "Camera & Optic Accessories",
                    sub: &[Node {
                        id: Some(149),
                        name: "camera_and_video_camera_lenses",
                        title: "Camera & Video Camera Lenses",
                        sub: &[Node {
                            id: Some(4432),
                            name: "camera_lenses",
                            title: "Camera Lenses",
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
            sub: &[
                Node {
                    id: Some(413),
                    name: "beverages",
                    title: "Beverages",
                    sub: &[Node {
                        id: None,
                        name: "alcoholic_beverages",
                        title: "Alcoholic Beverages",
                        sub: &[],
                    }],
                },
                Node {
                    id: Some(422),
                    name: "food",
                    title: "Food items",
                    sub: &[
                        Node {
                            id: Some(136),
                            name: "food_gift_baskets",
                            title: "Food Gift Baskets",
                            sub: &[],
                        },
                        Node { id: Some(423), name: "snack_foods", title: "Snack Foods", sub: &[] },
                        Node {
                            id: Some(427),
                            name: "condiments_and_sauces",
                            title: "Condiments & Sauces",
                            sub: &[],
                        },
                        Node {
                            id: Some(428),
                            name: "dairy_products",
                            title: "Dairy Products",
                            sub: &[],
                        },
                        Node {
                            id: Some(430),
                            name: "fruits_and_vegetables",
                            title: "Fruits & Vegetables",
                            sub: &[],
                        },
                        Node {
                            id: Some(433),
                            name: "nuts_and_seeds",
                            title: "Nuts & Seeds",
                            sub: &[],
                        },
                        Node {
                            id: Some(434),
                            name: "pasta_and_noodles",
                            title: "Pasta & Noodles",
                            sub: &[],
                        },
                        Node { id: Some(1876), name: "bakery", title: "Bakery", sub: &[] },
                        Node {
                            id: Some(2423),
                            name: "soups_and_broths",
                            title: "Soups & Broths",
                            sub: &[],
                        },
                        Node {
                            id: Some(2660),
                            name: "cooking_and_baking_ingredients",
                            title: "Cooking & Baking Ingredients",
                            sub: &[],
                        },
                    ],
                },
                Node { id: Some(435), name: "tobacco", title: "Tobacco Products", sub: &[] },
            ],
        },
        Node {
            id: Some(1239),
            name: "toys_and_games",
            title: "Toys & Games",
            sub: &[Node {
                id: Some(1253),
                name: "toys",
                title: "Toys",
                sub: &[Node {
                    id: Some(2546),
                    name: "remote_control_toys",
                    title: "Remote Control Toys",
                    sub: &[Node {
                        id: Some(7090),
                        name: "remote_control_airships_and_blimps",
                        title: "Remote Control Airships & Blimps",
                        sub: &[],
                    }],
                }],
            }],
        },
        Node {
            id: Some(222),
            name: "electronics",
            title: "Electronics",
            sub: &[
                Node {
                    id: Some(262),
                    name: "communications",
                    title: "Communications",
                    sub: &[Node {
                        id: Some(270),
                        name: "telephony",
                        title: "Telephony",
                        sub: &[Node {
                            id: Some(267),
                            name: "mobile_phones",
                            title: "Mobile Phones",
                            sub: &[],
                        }],
                    }],
                },
                Node {
                    id: Some(278),
                    name: "computers",
                    title: "Computers",
                    sub: &[
                        Node {
                            id: Some(325),
                            name: "desktop_computers",
                            title: "Desktop Computers",
                            sub: &[],
                        },
                        Node { id: Some(328), name: "laptops", title: "Laptops", sub: &[] },
                        Node {
                            id: Some(4745),
                            name: "tablet_computers",
                            title: "Tablet Computers",
                            sub: &[],
                        },
                        Node { id: None, name: "smartwatches", title: "Smartwatches", sub: &[] },
                    ],
                },
                Node {
                    id: Some(1294),
                    name: "video_game_consoles",
                    title: "Video Game Consoles",
                    sub: &[],
                },
                Node {
                    id: Some(1270),
                    name: "video_game_console_accessories",
                    title: "Video Game Console Accessories",
                    sub: &[Node {
                        id: Some(1505),
                        name: "home_game_console_accessories",
                        title: "Home Game Console Accessories",
                        sub: &[],
                    }],
                },
            ],
        },
        Node {
            id: Some(888),
            name: "vehicles_and_parts",
            title: "Vehicles & Parts",
            sub: &[Node {
                id: Some(5614),
                name: "vehicles",
                title: "Vehicles",
                sub: &[
                    Node {
                        id: Some(1267),
                        name: "motor_vehicles",
                        title: "Motor Vehicles",
                        sub: &[],
                    },
                    Node {
                        id: Some(3540),
                        name: "watercraft",
                        title: "Watercraft",
                        sub: &[
                            Node {
                                id: Some(3087),
                                name: "sailboats",
                                title: "Sailboats",
                                sub: &[],
                            },
                            Node { id: Some(5644), name: "yachts", title: "Yachts", sub: &[] },
                        ],
                    },
                ],
            }],
        },
        Node {
            id: Some(8),
            name: "arts_and_entertainment",
            title: "Arts & Entertainment",
            sub: &[Node {
                id: Some(5710),
                name: "hobbies_and_creative_arts",
                title: "Hobbies & Creative Arts",
                sub: &[Node {
                    id: Some(54),
                    name: "musical_instruments",
                    title: "Musical Instruments",
                    sub: &[Node {
                        id: Some(77),
                        name: "string_instruments",
                        title: "String Instruments",
                        sub: &[Node { id: Some(80), name: "guitars", title: "Guitars", sub: &[] }],
                    }],
                }],
            }],
        },
        Node {
            id: Some(922),
            name: "office_supplies",
            title: "Office Supplies",
            sub: &[Node {
                id: Some(950),
                name: "office_equipment",
                title: "Office Equipment",
                sub: &[Node { id: Some(333), name: "calculators", title: "Calculators", sub: &[] }],
            }],
        },
        Node {
            id: Some(536),
            name: "home_and_garden",
            title: "Home & Garden",
            sub: &[Node {
                id: Some(604),
                name: "household_appliances",
                title: "Household Appliances",
                sub: &[Node {
                    id: Some(2706),
                    name: "laundry_appliances",
                    title: "Laundry Appliances",
                    sub: &[Node {
                        id: Some(2549),
                        name: "washing_machines",
                        title: "Washing Machines",
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
}
