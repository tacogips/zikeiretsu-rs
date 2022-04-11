use crate::tsdb::field::*;
pub fn type_to_val(typ: &FieldType) -> u8 {
    match typ {
        FieldType::Float64 => 2,
        FieldType::String => 3,
        FieldType::TimestampNano => 4,
        FieldType::Bool => 5,
        FieldType::UInt64 => 6,
        FieldType::TimestampSec => 7,
        FieldType::Vacant => 255,
    }
}

pub fn val_to_type(v: u8) -> FieldType {
    match v {
        2u8 => FieldType::Float64,
        3u8 => FieldType::String,
        4u8 => FieldType::TimestampNano,
        5u8 => FieldType::Bool,
        6u8 => FieldType::UInt64,
        7u8 => FieldType::TimestampSec,
        255u8 => FieldType::Vacant,
        v => panic!("invalid field type value {}", v),
    }
}
