use super::compress::bools;
use super::{field_type_convert, BlockError, Result, TimestampDeltas};
use crate::tsdb::*;
use bits_ope::*;

pub(crate) fn read_from_block(block_data: &[u8]) -> Result<Vec<DataPoint>> {
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

    let mut block_field_values = Vec::<Vec<FieldValue>>::new();

    for each_field_type in field_types {
        match each_field_type {
            FieldType::Float64 => {
                let mut float_values = Vec::<f64>::new();
                let read_idx = xor_encoding::decompress_f64(
                    &&block_data[block_idx..],
                    number_of_datapoints,
                    &mut float_values,
                )?;
                block_idx += read_idx;

                block_field_values.push(
                    float_values
                        .into_iter()
                        .map(|v| FieldValue::Float64(v))
                        .collect(),
                )
            }

            FieldType::Bool => {
                let mut bool_values = Vec::<bool>::new();
                let read_idx = bools::decompress(
                    &&block_data[block_idx..],
                    &mut bool_values,
                    number_of_datapoints,
                )?;
                block_idx += read_idx;

                block_field_values.push(
                    bool_values
                        .into_iter()
                        .map(|v| FieldValue::Bool(v))
                        .collect(),
                )
            }
        }
    }

    let mut datapoints = Vec::<DataPoint>::new();
    for data_idx in 0..number_of_datapoints {
        let mut field_values = Vec::<FieldValue>::new();
        for field_idx in 0..number_of_field as usize {
            let block_values = unsafe { block_field_values.get_unchecked(field_idx) };
            let block_field_value = unsafe { block_values.get_unchecked(data_idx) };
            field_values.push(block_field_value.clone());
        }

        let datapoint = DataPoint {
            timestamp_nano: unsafe { timestamps.get_unchecked(data_idx) }.clone(),
            field_values,
        };
        datapoints.push(datapoint);
    }
    Ok(datapoints)
}
