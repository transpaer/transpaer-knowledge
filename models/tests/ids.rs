#[test]
fn gtin_to_string() {
    use sustainity_models::ids::Gtin;

    assert_eq!(Gtin::new(2345).to_string(), "00000000002345");
}

#[test]
fn gtin_from_string() {
    use sustainity_models::ids::{Gtin, ParseIdError};

    assert_eq!(Gtin::try_from("012345678"), Ok(Gtin::new(12345678)));
    assert_eq!(Gtin::try_from("123456789"), Ok(Gtin::new(123456789)));
    assert_eq!(
        Gtin::try_from("123456789012345"),
        Err(ParseIdError::length("123456789012345".to_string()))
    );
    assert_eq!(
        Gtin::try_from("123A5678"),
        Err(ParseIdError::num("123A5678".to_string(), "123A5678".parse::<usize>().err().unwrap()))
    );
}

#[test]
fn vat_id_from_string() {
    use sustainity_models::ids::{ParseIdError, VatId};

    assert_eq!(VatId::try_from("NL12345678"), Ok(VatId::new("NL12345678".to_string())));
    assert_eq!(VatId::try_from("NL123-45 67.8"), Ok(VatId::new("NL12345678".to_string())));
    assert_eq!(
        VatId::try_from("1"),
        Result::<VatId, ParseIdError>::Err(ParseIdError::length("1".to_string()))
    );
}

#[test]
fn organisation_id_to_string() {
    use sustainity_models::ids::{NumId, OrganisationId, VatId};

    assert_eq!(&OrganisationId::Wiki(NumId::new(1234)).to_string(), "Q1234");
    assert_eq!(&OrganisationId::Vat(VatId::new("1234".to_string())).to_string(), "V1234");
}

#[test]
fn organisation_id_from_string() {
    use sustainity_models::ids::{NumId, OrganisationId, ParseIdError, VatId};

    assert_eq!(
        OrganisationId::try_from("Q12345678"),
        Ok(OrganisationId::Wiki(NumId::new(12345678)))
    );
    assert_eq!(
        OrganisationId::try_from("V12345678"),
        Ok(OrganisationId::Vat(VatId::new("12345678".to_string())))
    );
    assert_eq!(
        OrganisationId::try_from("A12345678"),
        Result::<OrganisationId, ParseIdError>::Err(ParseIdError::prefix("A12345678".to_string()))
    );
}

#[test]
fn product_id_to_string() {
    use sustainity_models::ids::{Gtin, NumId, ProductId};

    assert_eq!(&ProductId::Wiki(NumId::new(1234)).to_string(), "Q1234");
    assert_eq!(&ProductId::Gtin(Gtin::new(1234)).to_string(), "G00000000001234");
}

#[test]
fn product_id_from_string() {
    use sustainity_models::ids::{Gtin, NumId, ParseIdError, ProductId};

    assert_eq!(ProductId::try_from("Q12345678"), Ok(ProductId::Wiki(NumId::new(12345678))));
    assert_eq!(ProductId::try_from("G12345678"), Ok(ProductId::Gtin(Gtin::new(12345678))));
    assert_eq!(
        ProductId::try_from("A12345678"),
        Result::<ProductId, ParseIdError>::Err(ParseIdError::prefix("A12345678".to_string()))
    );
}
