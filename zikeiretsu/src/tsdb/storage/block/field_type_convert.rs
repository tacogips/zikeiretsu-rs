use crate::tsdb::field::*;
pub fn type_to_val(typ: &FieldType) -> u8 {
    match typ {
        FieldType::Float64 => 2,
    }
}

pub fn val_to_type(v: u8) -> FieldType {
    match v {
        2u8 => FieldType::Float64,
        v @ _ => panic!("invalid field type value {}", v),
    }
}
