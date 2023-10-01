use std::collections::HashSet;

#[test]
fn regions_merge() {
    use isocountry::CountryCode::{DEU, ESP, FRA, ITA, POL, SWE};
    use merge::Merge;
    use sustainity_condensing::knowledge::Regions;

    fn merge(r1: &Regions, r2: &Regions) -> Regions {
        let mut r = r1.clone();
        r.merge(r2.clone());
        r
    }

    let world = Regions::World;
    let unknown = Regions::Unknown;
    let list_1 = Regions::List(vec![DEU, ESP, FRA, ITA]);
    let list_2 = Regions::List(vec![DEU, POL, SWE, ITA]);
    let list_3 = Regions::List(vec![FRA, DEU, ITA, POL, ESP, SWE]);

    assert!(merge(&world, &unknown) == world);
    assert!(merge(&unknown, &world) == world);
    assert!(merge(&world, &list_1) == world);
    assert!(merge(&list_2, &world) == world);
    assert!(merge(&unknown, &list_1) == list_1);
    assert!(merge(&list_2, &unknown) == list_2);
    assert!(merge(&list_1, &list_2) == list_3);
    assert!(merge(&list_2, &list_1) == list_3);
}

#[test]
fn serde_product_1() {
    use sustainity_condensing::knowledge::{
        Certifications, Gtin, Product, ProductId, Regions, SustainityScore,
    };

    let original_product = Product {
        db_id: "id".to_string(),
        id: ProductId::Gtin(Gtin::new(123)),
        gtins: HashSet::default(),
        names: Vec::default(),
        descriptions: Vec::default(),
        images: Vec::default(),
        certifications: Certifications::default(),
        follows: HashSet::default(),
        followed_by: HashSet::default(),
        regions: Regions::World,
        sustainity_score: SustainityScore::default(),
    };

    let expected_string = indoc::indoc!(
        r#"{
          "_id": "id",
          "id": "G00000000000123",
          "gtins": [],
          "names": [],
          "descriptions": [],
          "images": [],
          "certifications": {
            "bcorp": null,
            "eu_ecolabel": null,
            "fti": null,
            "tco": null
          },
          "follows": [],
          "followed_by": [],
          "regions": {
            "variant": "all"
          },
          "sustainity_score": {
            "tree": [],
            "total": 0.0
          }
        }"#
    );

    let received_string = serde_json::to_string_pretty(&original_product).unwrap();

    assert_eq!(expected_string, received_string);
}

#[test]
fn serde_product_2() {
    use sustainity_condensing::knowledge::{
        Certifications, Gtin, Product, ProductId, Regions, SustainityScore,
    };

    let original_product = Product {
        db_id: "id".to_string(),
        id: ProductId::Gtin(Gtin::new(123)),
        gtins: HashSet::default(),
        names: Vec::default(),
        descriptions: Vec::default(),
        images: Vec::default(),
        certifications: Certifications::default(),
        follows: HashSet::default(),
        followed_by: HashSet::default(),
        regions: Regions::List(vec![isocountry::CountryCode::FRA, isocountry::CountryCode::NLD]),
        sustainity_score: SustainityScore::default(),
    };

    let expected_string = indoc::indoc!(
        r#"{
          "_id": "id",
          "id": "G00000000000123",
          "gtins": [],
          "names": [],
          "descriptions": [],
          "images": [],
          "certifications": {
            "bcorp": null,
            "eu_ecolabel": null,
            "fti": null,
            "tco": null
          },
          "follows": [],
          "followed_by": [],
          "regions": {
            "variant": "list",
            "content": [
              "FR",
              "NL"
            ]
          },
          "sustainity_score": {
            "tree": [],
            "total": 0.0
          }
        }"#
    );

    let received_string = serde_json::to_string_pretty(&original_product).unwrap();

    assert_eq!(expected_string, received_string);
}
