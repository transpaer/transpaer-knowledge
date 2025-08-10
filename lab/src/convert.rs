use transpaer_models::gather as models;

/// Converts Open Food Facts to model regions.
pub fn to_model_regions(
    regions: &transpaer_collecting::transpaer::data::Regions,
) -> Result<models::Regions, isocountry::CountryCodeParseErr> {
    use transpaer_collecting::transpaer::data::Regions;
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
