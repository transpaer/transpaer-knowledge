use std::collections::HashSet;

use sustainity_models::write as models;

enum ScoreBranch {
    Leaf(models::SustainityScoreBranch),
    Branch(SubscoreCalculator),
}

struct SubscoreCalculator {
    category: models::SustainityScoreCategory,
    weight: u32,
    branches: Vec<ScoreBranch>,
}

impl SubscoreCalculator {
    fn calculate(self) -> models::SustainityScoreBranch {
        let mut branches = Vec::<models::SustainityScoreBranch>::with_capacity(self.branches.len());
        for branch in self.branches {
            match branch {
                ScoreBranch::Leaf(leaf) => branches.push(leaf),
                ScoreBranch::Branch(branch) => branches.push(branch.calculate()),
            }
        }

        let total_weight: u32 = branches.iter().map(|b| b.weight).sum();
        let total_score: f64 =
            branches.iter().fold(0.0, |acc, branch| acc + branch.score * f64::from(branch.weight));
        let score = if total_weight == 0 { 0.0 } else { total_score / f64::from(total_weight) };

        models::SustainityScoreBranch {
            category: self.category,
            weight: self.weight,
            score,
            branches,
        }
    }
}

#[must_use]
pub fn calculate<S: std::hash::BuildHasher>(
    product: &models::Product,
    has_producer: bool,
    categories: Option<&HashSet<String, S>>,
) -> models::SustainityScore {
    let num_certs = product.certifications.get_num();

    let mut category_contributions = Vec::new();
    let has_categories = if let Some(categories) = categories {
        if categories.contains("smartphone") {
            category_contributions.push(ScoreBranch::Leaf(models::SustainityScoreBranch {
                category: models::SustainityScoreCategory::WarrantyLength,
                weight: 1,
                score: 0.5,
                branches: vec![],
            }));
        }
        !categories.is_empty()
    } else {
        false
    };

    let tree = SubscoreCalculator {
        category: models::SustainityScoreCategory::Root,
        weight: 1,
        branches: vec![
            ScoreBranch::Branch(SubscoreCalculator {
                category: models::SustainityScoreCategory::DataAvailability,
                weight: 1,
                branches: vec![
                    ScoreBranch::Leaf(models::SustainityScoreBranch {
                        category: models::SustainityScoreCategory::ProducerKnown,
                        weight: 1,
                        score: if has_producer { 1.0 } else { 0.5 },
                        branches: vec![],
                    }),
                    ScoreBranch::Leaf(models::SustainityScoreBranch {
                        category: models::SustainityScoreCategory::CategoryAssigned,
                        weight: 1,
                        score: if has_categories { 1.0 } else { 0.5 },
                        branches: vec![],
                    }),
                    ScoreBranch::Leaf(models::SustainityScoreBranch {
                        category: models::SustainityScoreCategory::ProductionPlaceKnown,
                        weight: 1,
                        score: 0.5, // TODO
                        branches: vec![],
                    }),
                    ScoreBranch::Leaf(models::SustainityScoreBranch {
                        category: models::SustainityScoreCategory::IdKnown,
                        weight: 1,
                        score: if product.gtins.is_empty() { 0.5 } else { 1.0 },
                        branches: vec![],
                    }),
                ],
            }),
            ScoreBranch::Branch(SubscoreCalculator {
                category: models::SustainityScoreCategory::Category,
                weight: 2,
                branches: category_contributions,
            }),
            ScoreBranch::Branch(SubscoreCalculator {
                category: models::SustainityScoreCategory::NumCerts,
                weight: 2,
                branches: vec![
                    ScoreBranch::Leaf(models::SustainityScoreBranch {
                        category: models::SustainityScoreCategory::AtLeastOneCert,
                        weight: 1,
                        score: if num_certs > 0 { 1.0 } else { 0.0 },
                        branches: vec![],
                    }),
                    ScoreBranch::Leaf(models::SustainityScoreBranch {
                        category: models::SustainityScoreCategory::AtLeastTwoCerts,
                        weight: 2,
                        score: if num_certs > 1 { 1.0 } else { 0.0 },
                        branches: vec![],
                    }),
                ],
            }),
        ],
    }
    .calculate();

    models::SustainityScore { tree: tree.branches, total: tree.score }
}
