use sustainity_models::gather as models;

/// Converts Open Food Facts to model regions.
pub fn to_model_regions(
    regions: &sustainity_collecting::open_food_facts::data::Regions,
) -> Result<models::Regions, isocountry::CountryCodeParseErr> {
    use sustainity_collecting::open_food_facts::data::Regions;
    Ok(match regions {
        Regions::World => models::Regions::World,
        Regions::Unknown => models::Regions::Unknown,
        Regions::List(list) => {
            let regions = list
                .iter()
                .map(|c| isocountry::CountryCode::for_alpha3(c))
                .collect::<Result<Vec<isocountry::CountryCode>, _>>()?;
            models::Regions::List(regions)
        }
    })
}

/// Converts model to Open Food Facts rgions.
#[must_use]
pub fn to_off_regions(
    regions: &models::Regions,
) -> sustainity_collecting::open_food_facts::data::Regions {
    use sustainity_collecting::open_food_facts::data::Regions;
    match regions {
        models::Regions::World => Regions::World,
        models::Regions::Unknown => Regions::Unknown,
        models::Regions::List(list) => {
            let regions = list.iter().map(|c| c.alpha3().to_owned()).collect::<Vec<String>>();
            Regions::List(regions)
        }
    }
}
