// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/// Data structures for parsing `BCorp` data.
pub mod data {
    use serde::{Deserialize, Serialize};

    /// Status of a `BCorp`.
    #[derive(Serialize, Deserialize, Debug)]
    pub enum Status {
        #[serde(rename = "certified")]
        Certified,

        #[serde(rename = "de-certified")]
        Decertified,
    }

    impl Status {
        #[must_use]
        pub fn is_certified(&self) -> bool {
            match self {
                Self::Certified => true,
                Self::Decertified => false,
            }
        }
    }

    /// Record in a `BCorp` data.
    #[derive(Serialize, Deserialize, Debug)]
    pub struct Record {
        /// Company ID.
        pub company_id: String,

        /// Company name.
        pub company_name: String,

        /// Data of last certification.
        // #[serde(with = "super::serde")]
        pub date_first_certified: String,

        /// Data of last certification.
        ///
        /// The data contains multiple records for the same company. Only the latest one is relevant.
        pub date_certified: String,

        /// Current status.
        pub current_status: Status,

        /// Description.
        pub description: String,

        /// Industry
        ///
        /// TODO: Make it an enum?.
        pub industry: String,

        /// Industry category
        pub industry_category: String,

        pub products_and_services: String,

        /// Company country of origin.
        pub country: String,
        pub state: String,
        pub city: String,
        pub other_countries_of_operation: String,

        pub sector: String,
        pub size: String,

        pub b_corp_profile: String,

        /// Official website URL.
        pub website: String,

        pub ownership: String,
        pub vat_id: String,

        pub assessment_year: String,
        pub overall_score: String,

        pub impact_area_community: String,
        pub impact_area_customers: String,
        pub impact_area_environment: String,
        pub impact_area_governance: String,
        pub impact_area_workers: String,

        pub impact_area_community_na_score: String,
        pub impact_area_customers_na_score: String,
        pub impact_area_environment_na_score: String,
        pub impact_area_governance_na_score: String,
        pub impact_area_workers_na_score: String,

        pub ia_community_it_civic_engagement_giving: String,
        pub ia_community_it_designed_for_charitable_giving: String,
        pub ia_community_it_designed_to_give: String,
        pub ia_community_it_diversity_equity_inclusion: String,
        pub ia_community_it_diversity_inclusion: String,
        pub ia_community_it_economic_impact: String,
        pub ia_community_it_job_creation: String,
        pub ia_community_it_local_economic_development: String,
        pub ia_community_it_local_involvement: String,
        pub ia_community_it_microdistribution_poverty_alleviation: String,
        pub ia_community_it_microfranchise_poverty_alleviation: String,
        pub ia_community_it_national_economic_development: String,
        pub ia_community_it_producer_cooperative: String,
        pub ia_community_it_suppliers_distributors: String,
        pub ia_community_it_suppliers_distributors_product: String,
        pub ia_community_it_supply_chain_management: String,
        pub ia_community_it_supply_chain_poverty_alleviation: String,
        pub ia_customers_it_arts_media_culture: String,
        pub ia_customers_it_basic_services_for_the_underserved: String,
        pub ia_customers_it_business_model_and_engagement: String,
        pub ia_customers_it_capacity_building: String,
        pub ia_customers_it_current_fund: String,
        pub ia_customers_it_customer_stewardship: String,
        pub ia_customers_it_economic_empowerment_for_the_underserved: String,
        pub ia_customers_it_education: String,
        pub ia_customers_it_educational_models_and_engagement: String,
        pub ia_customers_it_educational_outcomes: String,
        pub ia_customers_it_fund_governance: String,
        pub ia_customers_it_health: String,
        pub ia_customers_it_health_wellness_improvement: String,
        pub ia_customers_it_impact_improvement: String,
        pub ia_customers_it_improved_impact: String,
        pub ia_customers_it_infrastructure_market_access: String,
        pub ia_customers_it_infrastructure_market_access_building: String,
        pub ia_customers_it_investment_criteria: String,
        pub ia_customers_it_leadership_outreach: String,
        pub ia_customers_it_marketing_recruiting_and_transparency: String,
        pub ia_customers_it_mission_aligned_exit: String,
        pub ia_customers_it_mission_lock: String,
        pub ia_customers_it_past_performance: String,
        pub ia_customers_it_portfolio_management: String,
        pub ia_customers_it_portfolio_reporting: String,
        pub ia_customers_it_positive_impact: String,
        pub ia_customers_it_privacy_and_consumer_protection: String,
        pub ia_customers_it_quality_and_continuous_improvement: String,
        pub ia_customers_it_serving_in_need_populations: String,
        pub ia_customers_it_serving_underserved_populations_direct_: String,
        pub ia_customers_it_student_experience: String,
        pub ia_customers_it_student_outcomes: String,
        pub ia_customers_it_student_outcomes_special_interest_students_: String,
        pub ia_customers_it_student_outcomes_traditional_students_: String,
        pub ia_customers_it_support_for_underserved_purpose_driven_enterprises: String,
        pub ia_customers_it_targeted_for_investment: String,
        pub ia_environment_it_air_climate: String,
        pub ia_environment_it_certification: String,
        pub ia_environment_it_community: String,
        pub ia_environment_it_construction_practices: String,
        pub ia_environment_it_designed_to_conserve_agriculture_process: String,
        pub ia_environment_it_designed_to_conserve_manufacturing_process: String,
        pub ia_environment_it_designed_to_conserve_wholesale_process: String,
        pub ia_environment_it_energy_water_efficiency: String,
        pub ia_environment_it_environment_products_services_introduction: String,
        pub ia_environment_it_environmental_education_information: String,
        pub ia_environment_it_environmental_management: String,
        pub ia_environment_it_environmentally_innovative_agricultural_process: String,
        pub ia_environment_it_environmentally_innovative_manufacturing_process: String,
        pub ia_environment_it_environmentally_innovative_wholesale_process: String,
        pub ia_environment_it_green_investing: String,
        pub ia_environment_it_green_lending: String,
        pub ia_environment_it_inputs: String,
        pub ia_environment_it_land_life: String,
        pub ia_environment_it_land_office_plant: String,
        pub ia_environment_it_land_wildlife_conservation: String,
        pub ia_environment_it_material_energy_use: String,
        pub ia_environment_it_materials_codes: String,
        pub ia_environment_it_outputs: String,
        pub ia_environment_it_renewable_or_cleaner_burning_energy: String,
        pub ia_environment_it_resource_conservation: String,
        pub ia_environment_it_safety: String,
        pub ia_environment_it_toxin_reduction_remediation: String,
        pub ia_environment_it_training_collaboration: String,
        pub ia_environment_it_transportation_distribution_suppliers: String,
        pub ia_environment_it_water: String,
        pub ia_governance_it_corporate_accountability: String,
        pub ia_governance_it_ethics: String,
        pub ia_governance_it_ethics_transparency: String,
        pub ia_governance_it_governance: String,
        pub ia_governance_it_mission_engagement: String,
        pub ia_governance_it_mission_locked: String,
        pub ia_governance_it_transparency: String,
        pub ia_workers_it_benefits: String,
        pub ia_workers_it_career_development: String,
        pub ia_workers_it_compensation_wages: String,
        pub ia_workers_it_engagement_satisfaction: String,
        pub ia_workers_it_financial_security: String,
        pub ia_workers_it_health_wellness_safety: String,
        pub ia_workers_it_human_rights_labor_policy: String,
        pub ia_workers_it_job_flexibility_corporate_culture: String,
        pub ia_workers_it_management_worker_communication: String,
        pub ia_workers_it_occupational_health_safety: String,
        pub ia_workers_it_training_education: String,
        pub ia_workers_it_worker_benefits: String,
        pub ia_workers_it_worker_owned: String,
        pub ia_workers_it_worker_ownership: String,
        pub ia_workers_it_workforce_development: String,
        pub certification_cycle: String,
    }
}

/// Reader to loading `BCorp` data.
pub mod reader {
    use super::data::Record;
    use crate::errors::{IoOrSerdeError, MapSerde};

    /// Loads the `BCorp` data from a file.
    ///
    /// # Errors
    ///
    /// Returns `Err` if fails to read from `path` or parse the contents.
    pub fn parse(path: &std::path::Path) -> Result<Vec<Record>, IoOrSerdeError> {
        let mut parsed = Vec::<Record>::new();
        let mut reader = csv::ReaderBuilder::new()
            .delimiter(b',')
            .quote(b'"')
            .flexible(true)
            .from_path(path)
            .map_with_path(path)?;
        let _headers = reader.headers().map_with_path(path)?;
        for result in reader.deserialize() {
            parsed.push(result.map_with_path(path)?);
        }
        Ok(parsed)
    }
}
