use super::compress::bools;
use super::{field_type_convert, BlockError, Result, TimestampDeltas};
use crate::tsdb::*;
use bits_ope::*;
use std::collections::HashMap;

pub(crate) fn read_from_block(block_data: &[u8]) -> Result<DataFrame> {
    read_from_block_with_specific_fields(block_data, None)
}

pub(crate) fn read_from_block_with_specific_fields(
    block_data: &[u8],
    field_selectors: Option<&[usize]>,
) -> Result<DataFrame> {
    // 1. number  of data
    let (number_of_data, mut block_idx): (u64, usize) =
        base_128_variants::decompress_u64(&block_data)?;
    let number_of_datapoints = number_of_data as usize;

    // 2. data field num
    let number_of_field: u8 = match block_data.get(block_idx) {
        Some(b) => *b,
        None => {
            return Err(BlockError::InvalidBlockfileError(
                "no `number of field` data".to_string(),
                block_idx,
            ))
        }
    };
    block_idx += 1;

    // validate field number and field_selectors
    let field_selectors_map = match field_selectors {
        None => HashMap::new(),
        Some(field_selectors) => {
            let mut field_selectors_set = HashMap::new();
            if field_selectors.len() == 0 {
                return Err(BlockError::InvalidFieldSelector(
                    "empty field selector".to_string(),
                ));
            } else {
                for (idx, each_selector) in field_selectors.iter().enumerate() {
                    if *each_selector >= number_of_field as usize {
                        return Err(BlockError::InvalidFieldSelector(
                            "empty field selector".to_string(),
                        ));
                    }
                    field_selectors_set.insert(*each_selector, idx);
                }
                field_selectors_set
            }
        }
    };

    // 3.  field types
    let mut field_types = Vec::<FieldType>::new();

    for field_idx in 0..number_of_field as usize {
        match block_data.get(block_idx + field_idx) {
            Some(b) => field_types.push(field_type_convert::val_to_type(*b)),
            None => {
                return Err(BlockError::InvalidBlockfileError(
                    format!("no {field_idx}th `field type` data"),
                    block_idx,
                ))
            }
        };
    }
    block_idx += number_of_field as usize;

    // 4. head timestamp
    let (head_timestamp, consumed_idx): (TimestampNano, usize) = {
        let mut reader = RefBitsReader::new(&block_data[block_idx..]);
        match reader.chomp_as_u64(64)? {
            Some(head_timestamp) => (
                TimestampNano::new(head_timestamp),
                reader.current_byte_index() + 1,
            ),
            None => {
                return Err(BlockError::InvalidBlockfileError(
                    "no `head timestamp` data".to_string(),
                    block_idx,
                ))
            }
        }
    };
    block_idx += consumed_idx;

    // parse timestamps
    let timestamps: Vec<TimestampNano> = {
        let number_of_timestamp_deltas = number_of_datapoints - 1;
        if number_of_timestamp_deltas == 0 {
            vec![head_timestamp]
        } else {
            // (5)timestamp deltas seconds
            let mut timestamps_deltas_second = Vec::<u64>::new();

            let consumed_idx = simple8b_rle::decompress(
                &block_data[block_idx..],
                &mut timestamps_deltas_second,
                Some(number_of_timestamp_deltas),
            )?;
            block_idx += consumed_idx;

            // (6) common trailing zero num of timestamp nano
            let (common_trailing_zero_bits, consumed_idx): (u8, usize) = {
                let mut reader = RefBitsReader::new(&block_data[block_idx..]);
                match reader.chomp_as_u8(8)? {
                    Some(common_trailing_zero_num) => {
                        (common_trailing_zero_num, reader.current_byte_index() + 1)
                    }
                    None => {
                        return Err(BlockError::InvalidBlockfileError(
                            "no `common trailing zero` data".to_string(),
                            block_idx,
                        ))
                    }
                }
            };
            block_idx += consumed_idx;

            // (7) timestamp sub nano sec (n bytes)
            let mut timestamps_nanoseconds = Vec::<u64>::new();
            let consumed_idx = simple8b_rle::decompress(
                &block_data[block_idx..],
                &mut timestamps_nanoseconds,
                Some(number_of_timestamp_deltas),
            )?;

            block_idx += consumed_idx;

            let timestamp_deltas = TimestampDeltas {
                head_timestamp,
                timestamps_deltas_second,
                common_trailing_zero_bits,
                timestamps_nanoseconds,
            };
            timestamp_deltas.as_timestamps()
        }
    };

    let data_field_size = if field_selectors_map.is_empty() {
        number_of_field as usize
    } else {
        field_selectors_map.len()
    };
    let mut block_field_values = Vec::<SeriesValues>::with_capacity(data_field_size);
    for _ in 0..data_field_size {
        block_field_values.push(SeriesValues::Vacant(number_of_datapoints));
    }

    let is_field_to_select = |idx: usize| {
        if field_selectors_map.is_empty() {
            Some(idx)
        } else {
            field_selectors_map.get(&idx).map(|v| *v)
        }
    };

    for (field_idx, each_field_type) in field_types.iter().enumerate() {
        match each_field_type {
            FieldType::Float64 => {
                let mut float_values = Vec::<f64>::new();
                let read_idx = xor_encoding::decompress_f64(
                    &&block_data[block_idx..],
                    number_of_datapoints,
                    &mut float_values,
                )?;
                block_idx += read_idx;

                if let Some(data_series_idx) = is_field_to_select(field_idx) {
                    let _ = std::mem::replace(
                        &mut block_field_values[data_series_idx],
                        SeriesValues::Float64(float_values),
                    );
                }
            }

            FieldType::Bool => {
                let mut bool_values = Vec::<bool>::new();
                let read_idx = bools::decompress(
                    &&block_data[block_idx..],
                    &mut bool_values,
                    number_of_datapoints,
                )?;
                block_idx += read_idx;

                if let Some(data_series_idx) = is_field_to_select(field_idx) {
                    let _ = std::mem::replace(
                        &mut block_field_values[data_series_idx],
                        SeriesValues::Bool(bool_values),
                    );
                }
            }
        }
    }

    let dataframe = DataFrame::new(
        timestamps,
        block_field_values
            .into_iter()
            .map(|field_values| DataSeries::new(field_values))
            .collect(),
    );
    Ok(dataframe)
}
