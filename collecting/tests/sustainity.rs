#[test]
fn country_entry_serde() {
    use sustainity_collecting::sustainity::data::{CountryEntry, Regions};

    let original_string_none = "tag: tag\ncount: 1\n".to_string();
    let original_string_world = "tag: tag\nregions: all\ncount: 2\n".to_string();
    let original_string_unknown = "tag: tag\nregions: unknown\ncount: 3\n".to_string();
    let original_string_list = "tag: tag\nregions: !list\n- FRA\n- USA\ncount: 4\n".to_string();

    let original_entry_none =
        CountryEntry { tag: "tag".to_string(), description: None, regions: None, count: 1 };
    let original_entry_world = CountryEntry {
        tag: "tag".to_string(),
        description: None,
        regions: Some(Regions::World),
        count: 2,
    };
    let original_entry_unknown = CountryEntry {
        tag: "tag".to_string(),
        description: None,
        regions: Some(Regions::Unknown),
        count: 3,
    };
    let original_entry_list = CountryEntry {
        tag: "tag".to_string(),
        description: None,
        regions: Some(Regions::List(vec!["FRA".to_string(), "USA".to_string()])),
        count: 4,
    };

    let received_string_none = serde_yaml::to_string(&original_entry_none).unwrap();
    let received_string_world = serde_yaml::to_string(&original_entry_world).unwrap();
    let received_string_unknown = serde_yaml::to_string(&original_entry_unknown).unwrap();
    let received_string_list = serde_yaml::to_string(&original_entry_list).unwrap();

    assert_eq!(original_string_none, received_string_none);
    assert_eq!(original_string_world, received_string_world);
    assert_eq!(original_string_unknown, received_string_unknown);
    assert_eq!(original_string_list, received_string_list);

    let received_entry_none: CountryEntry = serde_yaml::from_str(&original_string_none).unwrap();
    let received_entry_world: CountryEntry = serde_yaml::from_str(&original_string_world).unwrap();
    let received_entry_unknown: CountryEntry =
        serde_yaml::from_str(&original_string_unknown).unwrap();
    let received_entry_list: CountryEntry = serde_yaml::from_str(&original_string_list).unwrap();

    assert_eq!(original_entry_none, received_entry_none);
    assert_eq!(original_entry_world, received_entry_world);
    assert_eq!(original_entry_unknown, received_entry_unknown);
    assert_eq!(original_entry_list, received_entry_list);
}
