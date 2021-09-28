use super::Result;

use super::compress::bools;
use super::{field_type_convert, BlockError, TimestampDeltas};
use crate::tsdb::*;
use base_128_variants;
use bits_ope::*;
use simple8b_rle;

use std::io::Write;
use std::iter::Iterator;
use xor_encoding;

pub(crate) fn write_to_block<W>(mut block_file: W, datapoints: &[DataPoint]) -> Result<()>
where
    W: Write,
{
    if datapoints.is_empty() {
        return Err(BlockError::EmptyDatapoints);
    }

    #[cfg(feature = "validate")]
    DataPoint::check_datapoints_is_sorted(&datapoints).map_err(|s| BlockError::UnKnownError(s))?;

    #[cfg(feature = "validate")]
    check_fields_are_valid(&datapoints)?;

    let head_datapoint = datapoints.get(0).unwrap();
    let data_field_num = head_datapoint.field_values.len();

    // (1). number of datapoints
    base_128_variants::compress_u64(datapoints.len() as u64, &mut block_file)?;

    // (2). data field num
    block_file.write(&[data_field_num as u8])?;

    // (3). write field types
    let field_types: Vec<FieldType> = head_datapoint
        .field_values
        .iter()
        .map(|field_value| field_value.as_type())
        .collect();
    write_type_of_fields(field_types, &mut block_file)?;

    // (4). head timestamp
    let TimestampDeltas {
        head_timestamp,
        timestamps_deltas_second,
        common_trailing_zero_bits,
        timestamps_nanoseconds,
    } = TimestampDeltas::from(datapoints);
    {
        let mut bits_writer = BitsWriter::new();
        bits_writer.append(u64_bits_reader!(*head_timestamp, 64)?, 64)?;
        bits_writer.flush(&mut block_file)?;
    }

    if !timestamps_deltas_second.is_empty() {
        // (5)timestamp deltas seconds
        simple8b_rle::compress(&timestamps_deltas_second, &mut block_file)?;

        // (6) common trailing zero num of timestamp nano
        block_file.write(&[common_trailing_zero_bits])?;

        // (7) timestamp nano sec(n bytes)
        simple8b_rle::compress(&timestamps_nanoseconds, &mut block_file)?;
    }

    // (8) datas of fields
    for FieldValuesIterElem { field_type, values } in datapoints.values_iter() {
        match field_type {
            FieldType::Float64 => {
                let float_values = values
                    .into_iter()
                    .map(|v| v.as_f64())
                    .collect::<std::result::Result<Vec<f64>, FieldError>>()?;
                xor_encoding::compress_f64(&float_values, &mut block_file)?;
            }

            FieldType::Bool => {
                let bool_values = values
                    .into_iter()
                    .map(|v| v.as_bool())
                    .collect::<std::result::Result<Vec<bool>, FieldError>>()?;

                bools::compress(&bool_values, &mut block_file)?;
            }
        }
    }

    Ok(())
}

fn write_type_of_fields<W>(field_types: Vec<FieldType>, w: &mut W) -> Result<()>
where
    W: Write,
{
    for each_field_type in field_types.iter() {
        let field_type_val = field_type_convert::type_to_val(each_field_type);
        w.write(&[field_type_val])?;
    }
    Ok(())
}
