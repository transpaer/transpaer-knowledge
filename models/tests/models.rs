use sustainity_api::models as api;
use sustainity_models::ids;

/// This test makes sure that Sustainity scores in the database and in the API can be mapped "1 to 1".
///
/// In the code there is already mapping from the database to the API.
#[test]
fn score_category_to_api() {
    use sustainity_models::models::SustainityScoreCategory;

    #[allow(dead_code)]
    fn convert(cat: api::SustainityScoreCategory) -> SustainityScoreCategory {
        match cat {
            api::SustainityScoreCategory::DataAvailability => {
                SustainityScoreCategory::DataAvailability
            }
            api::SustainityScoreCategory::ProducerKnown => SustainityScoreCategory::ProducerKnown,
            api::SustainityScoreCategory::ProductionPlaceKnown => {
                SustainityScoreCategory::ProductionPlaceKnown
            }
            api::SustainityScoreCategory::IdKnown => SustainityScoreCategory::IdKnown,
            api::SustainityScoreCategory::CategoryAssigned => {
                SustainityScoreCategory::CategoryAssigned
            }
            api::SustainityScoreCategory::Category => SustainityScoreCategory::Category,
            api::SustainityScoreCategory::WarrantyLength => SustainityScoreCategory::WarrantyLength,
            api::SustainityScoreCategory::NumCerts => SustainityScoreCategory::NumCerts,
            api::SustainityScoreCategory::AtLeastOneCert => SustainityScoreCategory::AtLeastOneCert,
            api::SustainityScoreCategory::AtLeastTwoCerts => {
                SustainityScoreCategory::AtLeastTwoCerts
            }
        }
    }
}

#[test]
fn regions_merge() {
    use isocountry::CountryCode::{DEU, ESP, FRA, ITA, POL, SWE};
    use merge::Merge;
    use sustainity_models::models::Regions;

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
fn serde_product_defaults() {
    use sustainity_models::store::{Certifications, Product, ProductIds, Regions, SustainityScore};

    let original_product = Product {
        ids: ProductIds { eans: vec![], gtins: vec![], wiki: vec![] },
        names: Vec::default(),
        descriptions: Vec::default(),
        images: Vec::default(),
        categories: Vec::default(),
        regions: Regions::World,
        origins: Vec::new(),
        certifications: Certifications::default(),
        manufacturers: Vec::new(),
        follows: Vec::default(),
        followed_by: Vec::default(),
        sustainity_score: SustainityScore::default(),
    };

    let expected_string = indoc::indoc!(
        r#"{
          "ids": {
            "eans": [],
            "gtins": [],
            "wiki": []
          },
          "names": [],
          "descriptions": [],
          "images": [],
          "categories": [],
          "regions": "World",
          "origins": [],
          "certifications": {
            "bcorp": null,
            "eu_ecolabel": null,
            "fti": null,
            "tco": null
          },
          "manufacturers": [],
          "follows": [],
          "followed_by": [],
          "sustainity_score": {
            "tree": [],
            "total": 0.0
          }
        }"#
    );

    let received_string = serde_json::to_string_pretty(&original_product).unwrap();

    pretty_assertions::assert_eq!(expected_string, received_string);
}

#[test]
fn serde_product_filled() {
    use sustainity_models::store::{Certifications, Product, ProductIds, Regions, SustainityScore};

    let original_product = Product {
        ids: ProductIds {
            eans: vec![ids::Ean::new(34)],
            gtins: vec![ids::Gtin::new(56)],
            wiki: vec![ids::WikiId::new(78)],
        },
        names: Vec::default(),
        descriptions: Vec::default(),
        images: Vec::default(),
        categories: Vec::default(),
        regions: Regions::List(vec![isocountry::CountryCode::FRA, isocountry::CountryCode::NLD]),
        origins: Vec::default(),
        certifications: Certifications::default(),
        manufacturers: Vec::default(),
        follows: Vec::default(),
        followed_by: Vec::default(),
        sustainity_score: SustainityScore::default(),
    };

    let expected_string = indoc::indoc!(
        r#"{
          "ids": {
            "eans": [
              34
            ],
            "gtins": [
              56
            ],
            "wiki": [
              78
            ]
          },
          "names": [],
          "descriptions": [],
          "images": [],
          "categories": [],
          "regions": {
            "List": [
              "FR",
              "NL"
            ]
          },
          "origins": [],
          "certifications": {
            "bcorp": null,
            "eu_ecolabel": null,
            "fti": null,
            "tco": null
          },
          "manufacturers": [],
          "follows": [],
          "followed_by": [],
          "sustainity_score": {
            "tree": [],
            "total": 0.0
          }
        }"#
    );

    let received_string = serde_json::to_string_pretty(&original_product).unwrap();

    pretty_assertions::assert_eq!(expected_string, received_string);
}
