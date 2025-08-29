// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

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
