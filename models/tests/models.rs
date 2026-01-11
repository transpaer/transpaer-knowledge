// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use transpaer_api::models as api;
use transpaer_models::ids;

/// This test makes sure that transpaer scores in the database and in the API can be mapped "1 to 1".
///
/// In the code there is already mapping from the database to the API.
#[test]
fn score_category_to_api() {
    use transpaer_models::models::TranspaerScoreCategory;

    #[allow(dead_code)]
    fn convert(cat: api::TranspaerScoreCategory) -> TranspaerScoreCategory {
        match cat {
            api::TranspaerScoreCategory::DataAvailability => {
                TranspaerScoreCategory::DataAvailability
            }
            api::TranspaerScoreCategory::ProducerKnown => TranspaerScoreCategory::ProducerKnown,
            api::TranspaerScoreCategory::ProductionPlaceKnown => {
                TranspaerScoreCategory::ProductionPlaceKnown
            }
            api::TranspaerScoreCategory::IdKnown => TranspaerScoreCategory::IdKnown,
            api::TranspaerScoreCategory::CategoryAssigned => {
                TranspaerScoreCategory::CategoryAssigned
            }
            api::TranspaerScoreCategory::Category => TranspaerScoreCategory::Category,
            api::TranspaerScoreCategory::WarrantyLength => TranspaerScoreCategory::WarrantyLength,
            api::TranspaerScoreCategory::NumCerts => TranspaerScoreCategory::NumCerts,
            api::TranspaerScoreCategory::AtLeastOneCert => TranspaerScoreCategory::AtLeastOneCert,
            api::TranspaerScoreCategory::AtLeastTwoCerts => TranspaerScoreCategory::AtLeastTwoCerts,
        }
    }
}

#[test]
fn regions_merge() {
    use isocountry::CountryCode::{DEU, ESP, FRA, ITA, POL, SWE};
    use transpaer_models::{combine::Combine, models::Regions};

    fn combine(r1: &Regions, r2: &Regions) -> Regions {
        Combine::combine(r1.clone(), r2.clone())
    }

    let world = Regions::World;
    let unknown = Regions::Unknown;
    let list_1 = Regions::List(vec![DEU, ESP, FRA, ITA]);
    let list_2 = Regions::List(vec![DEU, POL, SWE, ITA]);
    let list_3 = Regions::List(vec![FRA, DEU, ITA, POL, ESP, SWE]);

    assert!(combine(&world, &unknown) == world);
    assert!(combine(&unknown, &world) == world);
    assert!(combine(&world, &list_1) == world);
    assert!(combine(&list_2, &world) == world);
    assert!(combine(&unknown, &list_1) == list_1);
    assert!(combine(&list_2, &unknown) == list_2);
    assert!(combine(&list_1, &list_2) == list_3);
    assert!(combine(&list_2, &list_1) == list_3);
}

#[test]
fn serde_product_defaults() {
    use transpaer_models::store::{
        Availability, Certifications, Product, ProductIds, TranspaerProductData,
    };

    let original_product = Product {
        ids: ProductIds { eans: vec![], gtins: vec![], wiki: vec![] },
        names: Vec::default(),
        descriptions: Vec::default(),
        images: Vec::default(),
        categories: Vec::default(),
        availability: Availability::default(),
        origins: Vec::new(),
        certifications: Certifications::default(),
        manufacturers: Vec::default(),
        shopping: Vec::default(),
        media: Vec::default(),
        follows: Vec::default(),
        followed_by: Vec::default(),
        transpaer: TranspaerProductData::default(),
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
          "availability": {
            "regions": "Unknown",
            "sources": []
          },
          "origins": [],
          "certifications": {
            "bcorp": null,
            "eu_ecolabel": null,
            "fti": null,
            "tco": null
          },
          "manufacturers": [],
          "shopping": [],
          "media": [],
          "follows": [],
          "followed_by": [],
          "transpaer": {
            "score": {
              "tree": [],
              "total": 0.0
            },
            "significance": {}
          }
        }"#
    );

    let received_string = serde_json::to_string_pretty(&original_product).unwrap();

    pretty_assertions::assert_eq!(expected_string, received_string);
}

#[test]
fn serde_product_filled() {
    use transpaer_models::store::{
        Availability, Certifications, Product, ProductIds, Regions, Source, SourcedEan,
        SourcedGtin, SourcedWikiId, TranspaerProductData,
    };

    let original_product = Product {
        ids: ProductIds {
            eans: vec![SourcedEan { id: ids::Ean::new(34), sources: vec![Source::Wikidata] }],
            gtins: vec![SourcedGtin { id: ids::Gtin::new(56), sources: vec![Source::BCorp] }],
            wiki: vec![SourcedWikiId {
                id: ids::WikiId::new(78),
                sources: vec![Source::Transpaer],
            }],
        },
        names: Vec::default(),
        descriptions: Vec::default(),
        images: Vec::default(),
        categories: Vec::default(),
        availability: Availability {
            regions: Regions::List(vec![
                isocountry::CountryCode::FRA,
                isocountry::CountryCode::NLD,
            ]),
            sources: maplit::btreeset! { Source::Wikidata },
        },
        origins: Vec::default(),
        certifications: Certifications::default(),
        manufacturers: Vec::default(),
        shopping: Vec::default(),
        media: Vec::default(),
        follows: Vec::default(),
        followed_by: Vec::default(),
        transpaer: TranspaerProductData::default(),
    };

    let expected_string = indoc::indoc!(
        r#"{
          "ids": {
            "eans": [
              {
                "id": 34,
                "sources": [
                  "Wikidata"
                ]
              }
            ],
            "gtins": [
              {
                "id": 56,
                "sources": [
                  "BCorp"
                ]
              }
            ],
            "wiki": [
              {
                "id": 78,
                "sources": [
                  "Transpaer"
                ]
              }
            ]
          },
          "names": [],
          "descriptions": [],
          "images": [],
          "categories": [],
          "availability": {
            "regions": {
              "List": [
                "FR",
                "NL"
              ]
            },
            "sources": [
              "Wikidata"
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
          "shopping": [],
          "media": [],
          "follows": [],
          "followed_by": [],
          "transpaer": {
            "score": {
              "tree": [],
              "total": 0.0
            },
            "significance": {}
          }
        }"#
    );

    let received_string = serde_json::to_string_pretty(&original_product).unwrap();

    pretty_assertions::assert_eq!(expected_string, received_string);
}
