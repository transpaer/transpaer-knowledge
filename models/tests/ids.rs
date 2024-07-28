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

    assert_eq!(VatId::try_from("NL12345678"), Ok(VatId::new("NL12345678")));
    assert_eq!(VatId::try_from("NL123-45 67.8"), Ok(VatId::new("NL12345678")));
    assert_eq!(
        VatId::try_from("1"),
        Result::<VatId, ParseIdError>::Err(ParseIdError::length("1".to_string()))
    );
}

#[test]
fn organisation_id_to_string() {
    use sustainity_models::ids::OrganisationId;

    assert_eq!(&OrganisationId::from_value(1234).to_string(), "1234");
}

#[test]
fn product_id_to_string() {
    use sustainity_models::ids::ProductId;

    assert_eq!(&ProductId::from_value(1234).to_string(), "1234");
}
