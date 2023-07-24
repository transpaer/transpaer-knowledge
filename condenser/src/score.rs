use std::collections::HashSet;

use crate::knowledge;

enum ScoreBranch {
    Leaf(knowledge::SustainityScoreBranch),
    Branch(Subscore),
}

struct Subscore {
    symbol: char,
    weight: u32,
    branches: Vec<ScoreBranch>,
}

impl Subscore {
    fn calculate(self) -> knowledge::SustainityScoreBranch {
        let mut branches =
            Vec::<knowledge::SustainityScoreBranch>::with_capacity(self.branches.len());
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

        knowledge::SustainityScoreBranch {
            symbol: self.symbol,
            weight: self.weight,
            score,
            branches,
        }
    }
}

pub fn calculate(
    product: &knowledge::Product,
    has_producer: bool,
    categories: Option<&HashSet<String>>,
) -> knowledge::SustainityScore {
    let num_certs = product.certifications.get_num();

    let mut category_contributions = Vec::new();
    let has_categories = if let Some(categories) = categories {
        if categories.contains("smartphone") {
            category_contributions.push(ScoreBranch::Leaf(knowledge::SustainityScoreBranch {
                symbol: '👮', // Warranty
                weight: 1,
                score: 0.5,
                branches: vec![],
            }));
        }
        !categories.is_empty()
    } else {
        false
    };

    let tree = Subscore {
        symbol: 'A',
        weight: 1,
        branches: vec![
            ScoreBranch::Branch(Subscore {
                symbol: '💁', // data available
                weight: 1,
                branches: vec![
                    ScoreBranch::Leaf(knowledge::SustainityScoreBranch {
                        symbol: '🏭', // has producer
                        weight: 1,
                        score: if has_producer { 1.0 } else { 0.5 },
                        branches: vec![],
                    }),
                    ScoreBranch::Leaf(knowledge::SustainityScoreBranch {
                        symbol: '📥', // has category
                        weight: 1,
                        score: if has_categories { 1.0 } else { 0.5 },
                        branches: vec![],
                    }),
                    ScoreBranch::Leaf(knowledge::SustainityScoreBranch {
                        symbol: '🌐', // has place of production
                        weight: 1,
                        score: 0.5,
                        branches: vec![],
                    }),
                    ScoreBranch::Leaf(knowledge::SustainityScoreBranch {
                        symbol: '👈', // has ID
                        weight: 1,
                        score: if product.gtins.is_empty() { 0.5 } else { 1.0 },
                        branches: vec![],
                    }),
                ],
            }),
            ScoreBranch::Branch(Subscore {
                symbol: '📂', // Categories
                weight: 2,
                branches: category_contributions,
            }),
            ScoreBranch::Branch(Subscore {
                symbol: '📜', // Certificates
                weight: 2,
                branches: vec![
                    ScoreBranch::Leaf(knowledge::SustainityScoreBranch {
                        symbol: '🙋',
                        weight: 1,
                        score: if num_certs > 0 { 1.0 } else { 0.0 },
                        branches: vec![],
                    }),
                    ScoreBranch::Leaf(knowledge::SustainityScoreBranch {
                        symbol: '🙌',
                        weight: 2,
                        score: if num_certs > 1 { 1.0 } else { 0.0 },
                        branches: vec![],
                    }),
                ],
            }),
        ],
    }
    .calculate();

    knowledge::SustainityScore { tree: tree.branches, total: tree.score }
}
