#[test]
fn gtin_to_string() {
    use sustainity_collecting::data::Gtin;

    assert_eq!(Gtin::new(2345).to_string(), "00000000002345");
}

#[test]
fn gtin_from_string() {
    use sustainity_collecting::{data::Gtin, errors::ParseIdError};

    assert_eq!(Gtin::try_from("12345678"), Ok(Gtin::new(12345678)));
    assert_eq!(Gtin::try_from("123456789"), Err(ParseIdError::Length("123456789".to_string())));
    assert_eq!(
        Gtin::try_from("123A5678"),
        Err(ParseIdError::Num("123A5678".to_string(), "123A5678".parse::<usize>().err().unwrap()))
    );
}

#[test]
fn vat_id_from_string() {
    use sustainity_collecting::{data::VatId, errors::ParseIdError};

    assert_eq!(VatId::try_from("NL12345678"), Ok(VatId::new("NL12345678".to_string())));
    assert_eq!(VatId::try_from("NL123-45 67.8"), Ok(VatId::new("NL12345678".to_string())));
    assert_eq!(
        VatId::try_from("123"),
        Result::<VatId, ParseIdError>::Err(ParseIdError::Length("123".to_string()))
    );
}

#[test]
fn organisation_id_to_string() {
    use sustainity_collecting::data::{OrganisationId, VatId, WikiId};

    assert_eq!(&OrganisationId::Wiki(WikiId::new(1234)).to_string(), "Q1234");
    assert_eq!(&OrganisationId::Vat(VatId::new("1234".to_string())).to_string(), "V1234");
}

#[test]
fn organisation_id_from_string() {
    use sustainity_collecting::{
        data::{OrganisationId, VatId, WikiId},
        errors::ParseIdError,
    };

    assert_eq!(
        OrganisationId::try_from("Q12345678"),
        Ok(OrganisationId::Wiki(WikiId::new(12345678)))
    );
    assert_eq!(
        OrganisationId::try_from("V12345678"),
        Ok(OrganisationId::Vat(VatId::new("12345678".to_string())))
    );
    assert_eq!(
        OrganisationId::try_from("A12345678"),
        Result::<OrganisationId, ParseIdError>::Err(ParseIdError::Prefix("A12345678".to_string()))
    );
}

#[test]
fn product_id_to_string() {
    use sustainity_collecting::data::{Gtin, ProductId, WikiId};

    assert_eq!(&ProductId::Wiki(WikiId::new(1234)).to_string(), "Q1234");
    assert_eq!(&ProductId::Gtin(Gtin::new(1234)).to_string(), "G00000000001234");
}

#[test]
fn product_id_from_string() {
    use sustainity_collecting::{
        data::{Gtin, ProductId, WikiId},
        errors::ParseIdError,
    };

    assert_eq!(ProductId::try_from("Q12345678"), Ok(ProductId::Wiki(WikiId::new(12345678))));
    assert_eq!(ProductId::try_from("G12345678"), Ok(ProductId::Gtin(Gtin::new(12345678))));
    assert_eq!(
        ProductId::try_from("A12345678"),
        Result::<ProductId, ParseIdError>::Err(ParseIdError::Prefix("A12345678".to_string()))
    );
}
