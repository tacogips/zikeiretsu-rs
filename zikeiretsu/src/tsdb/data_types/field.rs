use super::DataPoint;
use crate::datetime::TimestampNano;
use serde::{Deserialize, Serialize};
use std::fmt;
use strum::AsRefStr;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum FieldError {
    #[error("field has different type expected :{0} acrtual:{1}")]
    DifferentType(String, String),

    #[error("all field size must be same but actually not. {0}")]
    DifferentFieldSize(usize),

    #[error("invalid field type. expected {0} but actual is {1}")]
    InvalidFieldType(String, String),

    //TODO(tacogips) ned to check max field value somewhere...
    #[error("muxiumu field number exeed. max:{0} actual {1}")]
    TooManyField(usize, usize),
}

const MAX_FIELD_SIZE: usize = 255;

type Result<T> = std::result::Result<T, FieldError>;

#[derive(AsRefStr, Debug, PartialEq, Clone, Deserialize, Serialize)]
pub enum FieldValue {
    TimestampNano(TimestampNano),
    Float64(f64),
    UInt64(u64),
    String(String),
    Bool(bool),
}

impl FieldValue {
    pub fn as_f64(&self) -> Result<f64> {
        match self {
            Self::Float64(v) => Ok(*v),
            _ => Err(FieldError::InvalidFieldType(
                "float64".to_string(),
                format!("{self}"),
            )),
        }
    }

    pub fn as_bool(&self) -> Result<bool> {
        match self {
            Self::Bool(v) => Ok(*v),
            _ => Err(FieldError::InvalidFieldType(
                "bool".to_string(),
                format!("{self}"),
            )),
        }
    }

    pub fn as_type(&self) -> FieldType {
        match self {
            Self::Float64(_) => FieldType::Float64,
            Self::UInt64(_) => FieldType::UInt64,
            Self::TimestampNano(_) => FieldType::TimestampNano,
            Self::Bool(_) => FieldType::Bool,
            Self::String(_) => FieldType::String,
        }
    }
}

impl fmt::Display for FieldValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FieldValue::Float64(v) => write!(f, "{v:?}"),
            FieldValue::UInt64(v) => write!(f, "{v:?}"),
            FieldValue::TimestampNano(v) => write!(f, "{v:?}"),
            FieldValue::Bool(v) => write!(f, "{v:?}"),
            FieldValue::String(v) => write!(f, "{v:?}"),
        }
    }
}

pub fn same_field_types(types: &Vec<FieldType>, values: &Vec<FieldValue>) -> bool {
    if types.len() == values.len() {
        (0..types.len())
            .all(|i| unsafe { *types.get_unchecked(i) == values.get_unchecked(i).as_type() })
    } else {
        false
    }
}

#[derive(PartialEq, Eq, Debug, Clone)]
pub enum FieldType {
    Float64,
    UInt64,
    Bool,
    TimestampNano,
    String,
}

impl fmt::Display for FieldType {
    fn fmt(&self, f: &mut fmt::Formatter) -> std::fmt::Result {
        let name = match self {
            FieldType::Float64 => "Float64",
            FieldType::UInt64 => "UInt64",
            FieldType::String => "String",
            FieldType::TimestampNano => "TimestampNano",
            FieldType::Bool => "Bool",
        };

        write!(f, "{name}")
    }
}

pub(crate) trait AsFieldValuesRefIterator<'data> {
    fn values_iter(&'data self) -> FieldValuesIter<'data>;
}

impl<'data> AsFieldValuesRefIterator<'data> for &'data [DataPoint] {
    fn values_iter(&'data self) -> FieldValuesIter<'data> {
        match self.get(0) {
            Some(head) => FieldValuesIter {
                current_idx: 0,
                field_num: head.filed_num(),
                datapoints: self,
            },

            None => FieldValuesIter {
                current_idx: 0,
                field_num: 0,
                datapoints: &[],
            },
        }
    }
}

pub(crate) struct FieldValuesIter<'data> {
    current_idx: usize,
    field_num: usize,
    datapoints: &'data [DataPoint],
}

impl<'data> Iterator for FieldValuesIter<'data> {
    type Item = FieldValuesIterElem<'data>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.datapoints.is_empty() || self.current_idx >= self.field_num {
            None
        } else {
            let field_values: Option<Vec<&'data FieldValue>> = self
                .datapoints
                .iter()
                .map(|datapoint| datapoint.field_values.get(self.current_idx))
                .collect();

            match field_values {
                Some(field_values) => {
                    self.current_idx += 1;
                    Some(FieldValuesIterElem {
                        field_type: field_values.get(0).unwrap().as_type(),
                        values: field_values,
                    })
                }

                None => {
                    //TODO(tacogips) more reasonable error handling needed?
                    panic!(
                        "invalid field value num in datapoint field point num :{current_idx}",
                        current_idx = self.current_idx
                    )
                }
            }
        }
    }
}

pub(crate) fn check_fields_are_valid(datapoints: &[DataPoint]) -> Result<()> {
    if datapoints.is_empty() {
        Ok(())
    } else {
        let head = unsafe { datapoints.get_unchecked(0) };

        let head_field_size = head.field_values.len();
        if head_field_size > MAX_FIELD_SIZE {
            return Err(FieldError::TooManyField(
                MAX_FIELD_SIZE,
                head.field_values.len(),
            ));
        }
        if datapoints[1..]
            .iter()
            .map(|each| each.field_values.len())
            .all(|each_field_size| each_field_size == head_field_size)
        {
            Ok(())
        } else {
            Err(FieldError::DifferentFieldSize(head_field_size))
        }
    }
}

pub(crate) struct FieldValuesIterElem<'data> {
    pub field_type: FieldType,
    pub values: Vec<&'data FieldValue>,
}

#[cfg(test)]
mod test {

    use crate::tsdb::*;
    macro_rules! float_data_points {
        ($({$timestamp:expr,$values:expr}),*) => {
            vec![
            $(DataPoint::new(TimestampNano::new($timestamp), $values.into_iter().map(|each| FieldValue::Float64(each as f64)).collect())),*
            ]
        };
    }

    use super::*;

    #[test]
    pub fn field_value_iterator_test_1() {
        let datapoints = float_data_points!(
            {10, vec![2,3,4]},
            {12, vec![20,30,40]}
        );
        let datapoints = datapoints.as_slice();
        let mut itr = datapoints.values_iter();

        {
            let elem = itr.next();
            assert!(elem.is_some());
            let elem = elem.unwrap();
            assert_eq!(elem.values.len(), 2);

            assert_eq!(
                elem.values,
                vec![&FieldValue::Float64(2f64), &FieldValue::Float64(20f64)]
            );
        }

        {
            let elem = itr.next();
            assert!(elem.is_some());
            let elem = elem.unwrap();
            assert_eq!(elem.values.len(), 2);

            assert_eq!(
                elem.values,
                vec![&FieldValue::Float64(3f64), &FieldValue::Float64(30f64)]
            );
        }

        {
            let elem = itr.next();
            assert!(elem.is_some());
            let elem = elem.unwrap();
            assert_eq!(elem.values.len(), 2);

            assert_eq!(
                elem.values,
                vec![&FieldValue::Float64(4f64), &FieldValue::Float64(40f64)]
            );
        }

        {
            let elem = itr.next();
            assert!(elem.is_none());
        }
    }
}
