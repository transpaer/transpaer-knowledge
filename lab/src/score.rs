// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use transpaer_models::gather as models;

enum ScoreBranch {
    Leaf(models::TranspaerScoreBranch),
    Branch(SubscoreCalculator),
}

struct SubscoreCalculator {
    category: models::TranspaerScoreCategory,
    weight: i32,
    branches: Vec<ScoreBranch>,
}

impl SubscoreCalculator {
    fn calculate(self) -> models::TranspaerScoreBranch {
        let mut branches = Vec::<models::TranspaerScoreBranch>::with_capacity(self.branches.len());
        for branch in self.branches {
            match branch {
                ScoreBranch::Leaf(leaf) => branches.push(leaf),
                ScoreBranch::Branch(branch) => branches.push(branch.calculate()),
            }
        }

        let total_weight: i32 = branches.iter().map(|b| b.weight).sum();
        let total_score: f64 =
            branches.iter().fold(0.0, |acc, branch| acc + branch.score * f64::from(branch.weight));
        let score = if total_weight == 0 { 0.0 } else { total_score / f64::from(total_weight) };

        models::TranspaerScoreBranch {
            category: self.category,
            weight: self.weight,
            score,
            branches,
        }
    }
}

#[must_use]
pub fn calculate(product: &models::Product) -> models::TranspaerScore {
    let has_producer = !product.manufacturers.is_empty();
    let has_categories = !product.categories.is_empty();
    let num_certs = product.certifications.get_num();

    let mut category_contributions = Vec::new();
    if product.categories.contains("smartphone") {
        category_contributions.push(ScoreBranch::Leaf(models::TranspaerScoreBranch {
            category: models::TranspaerScoreCategory::WarrantyLength,
            weight: 1,
            score: 0.5,
            branches: vec![],
        }));
    }

    let tree = SubscoreCalculator {
        category: models::TranspaerScoreCategory::Root,
        weight: 1,
        branches: vec![
            ScoreBranch::Branch(SubscoreCalculator {
                category: models::TranspaerScoreCategory::DataAvailability,
                weight: 1,
                branches: vec![
                    ScoreBranch::Leaf(models::TranspaerScoreBranch {
                        category: models::TranspaerScoreCategory::ProducerKnown,
                        weight: 1,
                        score: if has_producer { 1.0 } else { 0.5 },
                        branches: vec![],
                    }),
                    ScoreBranch::Leaf(models::TranspaerScoreBranch {
                        category: models::TranspaerScoreCategory::CategoryAssigned,
                        weight: 1,
                        score: if has_categories { 1.0 } else { 0.5 },
                        branches: vec![],
                    }),
                    ScoreBranch::Leaf(models::TranspaerScoreBranch {
                        category: models::TranspaerScoreCategory::ProductionPlaceKnown,
                        weight: 1,
                        score: 0.5, // TODO
                        branches: vec![],
                    }),
                    ScoreBranch::Leaf(models::TranspaerScoreBranch {
                        category: models::TranspaerScoreCategory::IdKnown,
                        weight: 1,
                        score: if product.ids.is_empty() { 0.5 } else { 1.0 },
                        branches: vec![],
                    }),
                ],
            }),
            ScoreBranch::Branch(SubscoreCalculator {
                category: models::TranspaerScoreCategory::Category,
                weight: 2,
                branches: category_contributions,
            }),
            ScoreBranch::Branch(SubscoreCalculator {
                category: models::TranspaerScoreCategory::NumCerts,
                weight: 2,
                branches: vec![
                    ScoreBranch::Leaf(models::TranspaerScoreBranch {
                        category: models::TranspaerScoreCategory::AtLeastOneCert,
                        weight: 1,
                        score: if num_certs > 0 { 1.0 } else { 0.0 },
                        branches: vec![],
                    }),
                    ScoreBranch::Leaf(models::TranspaerScoreBranch {
                        category: models::TranspaerScoreCategory::AtLeastTwoCerts,
                        weight: 2,
                        score: if num_certs > 1 { 1.0 } else { 0.0 },
                        branches: vec![],
                    }),
                ],
            }),
        ],
    }
    .calculate();

    models::TranspaerScore { tree: tree.branches, total: tree.score }
}
