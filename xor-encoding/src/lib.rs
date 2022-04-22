use std::mem;
use thiserror::Error;

use bits_ope::Error as BitsError;
use bits_ope::*;
use std::io::Write;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("value {0} is out of bound ")]
    ValueOutOfBound(u64),

    #[error("bits operation error. {0}")]
    BitsOperationError(#[from] BitsError),

    #[error("invalid xor encoding. {0} {1}")]
    InvalidXorEncoding(usize, String),

    #[error("writer error. {0}")]
    WriterError(#[from] std::io::Error),
}

const LEADING_ZERO_LENGTH_BITS_SIZE: usize = 6; // number of leading zeros: 0-63
const DATA_LENGTH_BITS_SIZE: usize = 6; // data bits length value may be between 1-64, we will minus 1 from the value to make the value range 0-63 to fit into 6 bits

pub(crate) fn f64_to_u64(v: f64) -> u64 {
    unsafe { mem::transmute::<f64, u64>(v) }
}

pub(crate) fn u64_to_f64(v: u64) -> f64 {
    unsafe { mem::transmute::<u64, f64>(v) }
}

pub fn compress_f64<W>(src: &[f64], dst: &mut W) -> Result<()>
where
    W: Write,
{
    if src.is_empty() {
        return Ok(());
    }

    let mut writer = BitsWriter::default();
    let head = f64_to_u64(src[0]);
    writer.append(u64_bits_reader!(head, 64)?, 64)?;

    let mut prev_val: u64 = head;

    let mut prev_leading_zeros: u32 = u32::MAX;
    let mut prev_trailing_zeros: u32 = u32::MAX;

    for each_value in src[1..].iter() {
        let each_value = f64_to_u64(*each_value);
        let xor = prev_val ^ each_value;

        if xor == 0 {
            writer.append(bits_reader!(Zero)?, 1)?;

            prev_leading_zeros = u32::MAX;
            prev_trailing_zeros = u32::MAX;
        } else {
            writer.append(bits_reader!(One)?, 1)?;

            let mut leading_zeros = xor.leading_zeros();
            let mut trailing_zeros = xor.trailing_zeros();

            if leading_zeros >= prev_leading_zeros && trailing_zeros >= prev_trailing_zeros {
                leading_zeros = prev_leading_zeros;
                trailing_zeros = prev_trailing_zeros;

                writer.append(bits_reader!(Zero)?, 1)?;
                let data_length = 64 - leading_zeros - trailing_zeros;
                let xor = xor >> prev_trailing_zeros;

                writer.append(
                    u64_bits_reader!(xor, data_length as usize)?,
                    data_length as usize,
                )?;
            } else {
                writer.append(bits_reader!(One)?, 1)?;

                debug_assert!(leading_zeros < 64);

                //put leading zero nums
                writer.append(
                    u32_bits_reader!(leading_zeros, LEADING_ZERO_LENGTH_BITS_SIZE)?,
                    LEADING_ZERO_LENGTH_BITS_SIZE,
                )?;

                // data_bits_size range is 1-64
                let data_bits_size = 64 - leading_zeros - trailing_zeros;

                writer.append(
                    u32_bits_reader!(data_bits_size - 1, DATA_LENGTH_BITS_SIZE)?,
                    DATA_LENGTH_BITS_SIZE,
                )?;

                let xor = xor >> trailing_zeros;
                writer.append(
                    u64_bits_reader!(xor, data_bits_size as usize)?,
                    data_bits_size as usize,
                )?;
            }

            prev_leading_zeros = leading_zeros;
            prev_trailing_zeros = trailing_zeros;
        }

        prev_val = each_value;
    }
    writer.flush(dst)?;

    Ok(())
}

pub fn decompress_f64(src: &[u8], num: usize, dst: &mut Vec<f64>) -> Result<usize> {
    if num == 0 {
        return Ok(0);
    }

    let mut reader = RefBitsReader::new(src);
    let head_value = match reader.chomp_as_u64(64)? {
        Some(v) => v,
        None => return Err(Error::InvalidXorEncoding(0, "no head value".to_string())),
    };

    let mut added_value_num = 0;
    dst.push(u64_to_f64(head_value));
    added_value_num += 1;

    let mut prev_value = head_value;
    let mut prev_leading_zeros: u8 = u8::MAX;
    let mut prev_data_length: u8 = u8::MAX;

    loop {
        if num <= added_value_num {
            break;
        }
        match reader.chomp_as_bit()? {
            Some(first_bit) => match first_bit {
                Bit::Zero => {
                    dst.push(u64_to_f64(prev_value));
                    added_value_num += 1;

                    continue;
                }
                Bit::One => match reader.chomp_as_bit()? {
                    Some(second_value) => match second_value {
                        Bit::Zero => {
                            let data = reader.chomp_as_u64(prev_data_length as usize)?;
                            let xor = match data {
                                Some(v) => v,
                                None => {
                                    return Err(Error::InvalidXorEncoding(
                                        reader.current_byte_index(),
                                        "no xor data bits ".to_string(),
                                    ))
                                }
                            };

                            let trailing_zero_size = 64 - (prev_leading_zeros + prev_data_length);

                            let xor = xor << trailing_zero_size;

                            let current_value = prev_value ^ xor;

                            dst.push(u64_to_f64(current_value));
                            added_value_num += 1;

                            prev_value = current_value;
                        }
                        Bit::One => {
                            let leading_zero_num =
                                reader.chomp_as_u8(LEADING_ZERO_LENGTH_BITS_SIZE)?;
                            let leading_zero_num = match leading_zero_num {
                                Some(v) => v,
                                None => {
                                    return Err(Error::InvalidXorEncoding(
                                        reader.current_byte_index(),
                                        "no leading zero bits".to_string(),
                                    ))
                                }
                            };

                            let data_bits_size = reader.chomp_as_u8(DATA_LENGTH_BITS_SIZE)?;
                            let data_bits_size = match data_bits_size {
                                Some(v) => v + 1, // add 1 cause we decreased the value to make it fit to 6 bits length on compressing
                                None => {
                                    return Err(Error::InvalidXorEncoding(
                                        reader.current_byte_index(),
                                        "no data bits length data".to_string(),
                                    ))
                                }
                            };

                            let data = reader.chomp_as_u64(data_bits_size as usize)?;
                            let xor = match data {
                                Some(v) => v,
                                None => {
                                    return Err(Error::InvalidXorEncoding(
                                        reader.current_byte_index(),
                                        "no xor data bits ".to_string(),
                                    ))
                                }
                            };

                            let trailing_zero_size = 64 - (leading_zero_num + data_bits_size);

                            let xor = xor << trailing_zero_size;

                            let current_value = prev_value ^ xor;

                            dst.push(u64_to_f64(current_value));
                            added_value_num += 1;

                            prev_value = current_value;
                            prev_leading_zeros = leading_zero_num;
                            prev_data_length = data_bits_size;
                        }
                    },
                    None => {
                        return Err(Error::InvalidXorEncoding(
                            reader.current_byte_index(),
                            "no second control bit".to_string(),
                        ))
                    }
                },
            },
            None => break,
        }
    }

    Ok(reader.current_byte_index() + 1)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_compress_1() {
        let mut dst = Vec::<u8>::new();
        let src = vec![12f64, 12f64];
        let result = compress_f64(&src, &mut dst);
        assert!(result.is_ok());
        assert_eq!(dst.len(), 9);

        {
            let mut reader = RefBitsReader::new(&dst);
            let result = reader.chomp_as_u64(64);
            assert!(result.is_ok());
            let result = result.unwrap();
            assert!(result.is_some());
            let v = u64_to_f64(result.unwrap());
            assert_eq!(v, src[0]);

            let result = reader.chomp_as_bit();
            assert!(result.is_ok());
            let result = result.unwrap();
            assert!(result.is_some());
            assert_eq!(Bit::Zero, result.unwrap());
        }

        {
            let mut decomp = Vec::<f64>::new();
            let result = decompress_f64(&dst, 2, &mut decomp);

            assert!(result.is_ok());
            let result = result.unwrap();
            assert_eq!(result, 9);
            assert_eq!(decomp, src);
        }
    }

    #[test]
    fn test_compress_2() {
        let mut dst = Vec::<u8>::new();
        let src = vec![12f64, 12f64, 24f64];
        let result = compress_f64(&src, &mut dst);
        assert!(result.is_ok());
        assert_eq!(dst.len(), 10);

        {
            let mut reader = RefBitsReader::new(&dst);
            let result = reader.chomp_as_u64(64);
            assert!(result.is_ok());
            let result = result.unwrap();
            assert!(result.is_some());
            let v = u64_to_f64(result.unwrap());
            assert_eq!(v, src[0]);

            let result = reader.chomp_as_bit();
            assert!(result.is_ok());
            let result = result.unwrap();
            assert!(result.is_some());
            assert_eq!(Bit::Zero, result.unwrap());
        }

        {
            let mut decomp = Vec::<f64>::new();
            let result = decompress_f64(&dst, 3, &mut decomp);

            assert!(result.is_ok());
            let result = result.unwrap();
            assert_eq!(result, 10);
            assert_eq!(decomp, src);
        }
    }

    #[test]
    fn test_compress_3() {
        let mut dst = Vec::<u8>::new();
        let src = vec![15.5, 14.0625, 3.25, 8.625, 13.1];
        let result = compress_f64(&src, &mut dst);
        assert!(result.is_ok());
        assert_eq!(dst.len(), 23);

        {
            let mut reader = RefBitsReader::new(&dst);
            let result = reader.chomp_as_u64(64);
            assert!(result.is_ok());
            let result = result.unwrap();
            assert!(result.is_some());
            let v = u64_to_f64(result.unwrap());
            assert_eq!(v, src[0]);

            let result = reader.chomp_as_bit();
            assert!(result.is_ok());
            let result = result.unwrap();
            assert!(result.is_some());
            assert_eq!(Bit::One, result.unwrap());
        }

        {
            let mut decomp = Vec::<f64>::new();
            let result = decompress_f64(&dst, 5, &mut decomp);

            assert!(result.is_ok());
            let result = result.unwrap();
            assert_eq!(result, 23);
            assert_eq!(decomp, src);
        }
    }

    #[test]
    fn test_compress_4() {
        let mut dst = Vec::<u8>::new();

        let src: Vec<f64> = vec![
            5012606f64, 5012999f64, 5012999f64, 5013135f64, 5013266f64, 5013999f64, 5013999f64,
            5013999f64, 5013952f64, 5013999f64, 5014006f64, 5014006f64, 5014006f64, 5014067f64,
            5014227f64, 5014412f64, 5014409f64, 5014518f64, 5014518f64, 5014518f64, 5014263f64,
            5014260f64, 5013650f64, 5013445f64, 5013274f64, 5013021f64, 5012603f64, 5012346f64,
            5012646f64, 5012346f64, 5012860f64, 5012555f64, 5012334f64, 5012334f64, 5012143f64,
            5011864f64, 5011829f64, 5011028f64, 5011027f64, 5011027f64, 5010999f64, 5010999f64,
        ];

        let result = compress_f64(&src, &mut dst);
        assert!(result.is_ok());
        assert_eq!(dst.len(), 81);

        {
            let mut reader = RefBitsReader::new(&dst);
            let result = reader.chomp_as_u64(64);
            assert!(result.is_ok());
            let result = result.unwrap();
            assert!(result.is_some());
            let v = u64_to_f64(result.unwrap());
            assert_eq!(v, src[0]);

            let result = reader.chomp_as_bit();
            assert!(result.is_ok());
            let result = result.unwrap();
            assert!(result.is_some());
            assert_eq!(Bit::One, result.unwrap());
        }

        {
            let mut decomp = Vec::<f64>::new();
            let result = decompress_f64(&dst, src.len(), &mut decomp);

            assert!(result.is_ok());
            let result = result.unwrap();
            assert_eq!(result, 81);
            assert_eq!(decomp, src);
        }
    }

    #[test]
    fn test_compress_5() {
        let mut dst = Vec::<u8>::new();
        let src = vec![15.5, 14.0625, 3.25, 8.625, 13.1];
        let result = compress_f64(&src, &mut dst);
        assert!(result.is_ok());
        assert_eq!(dst.len(), 23);

        {
            let mut reader = RefBitsReader::new(&dst);
            let result = reader.chomp_as_u64(64);
            assert!(result.is_ok());
            let result = result.unwrap();
            assert!(result.is_some());
            let v = u64_to_f64(result.unwrap());
            assert_eq!(v, src[0]);

            let result = reader.chomp_as_bit();
            assert!(result.is_ok());
            let result = result.unwrap();
            assert!(result.is_some());
            assert_eq!(Bit::One, result.unwrap());
        }

        {
            let mut decomp = Vec::<f64>::new();
            let result = decompress_f64(&dst, 6, &mut decomp);

            assert!(result.is_ok());
            let result = result.unwrap();
            let expected = vec![15.5, 14.0625, 3.25, 8.625, 13.1, 13.1];

            assert_eq!(result, 23);
            assert_eq!(decomp, expected);
        }
    }
}
