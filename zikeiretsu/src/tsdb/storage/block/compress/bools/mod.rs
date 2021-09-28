use super::Result;
use bits_ope::*;
use std::io::Write;

pub fn compress<W>(src: &[bool], dst: &mut W) -> Result<()>
where
    W: Write,
{
    let len = src.len();
    let mut writer = BitsWriter::new();
    let bits: Vec<Bit> = src
        .into_iter()
        .map(|b| if *b { Bit::One } else { Bit::Zero })
        .collect();

    writer.append(bits_reader_from_vec!(bits)?, len)?;
    writer.flush(dst)?;
    Ok(())
}

pub fn decompress(src: &[u8], dst: &mut Vec<bool>, num_of_value: usize) -> Result<usize> {
    if num_of_value == 0 {
        return Ok(0);
    }
    let bytes_size = match (num_of_value / 8, num_of_value % 8) {
        (bytes_size, 0) => bytes_size,
        (bytes_size, _) => bytes_size + 1,
    };

    let mut writed_num = 0;
    let reader = ref_bits_reader!(src);

    for each_bit in reader {
        let bool_value = match each_bit {
            Bit::One => true,
            Bit::Zero => false,
        };
        dst.push(bool_value);
        writed_num += 1;
        if writed_num >= num_of_value {
            return Ok(bytes_size);
        }
    }
    Ok(bytes_size)
}

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn compress_decompress_test_1() {
        let input = vec![true, false, true, false, false, true];
        let mut dst: Vec<u8> = Vec::new();
        let result = compress(&input, &mut dst);

        assert!(result.is_ok());

        let mut dec = Vec::<bool>::new();
        let result = decompress(&dst, &mut dec, input.len());

        assert!(result.is_ok());
        let result = result.unwrap();

        assert_eq!(result, 1);
        assert_eq!(dec, input);
    }

    #[test]
    fn compress_decompress_test_2() {
        let input = vec![
            true, false, true, false, false, true, true, false, true, false, false, false, true,
        ];
        let mut dst: Vec<u8> = Vec::new();
        let result = compress(&input, &mut dst);

        assert!(result.is_ok());

        let mut dec = Vec::<bool>::new();
        let result = decompress(&dst, &mut dec, input.len());

        assert!(result.is_ok());
        let result = result.unwrap();

        assert_eq!(result, 2);
        assert_eq!(dec, input);
    }

    #[test]
    fn compress_decompress_test_3() {
        let input = vec![true, false, true, false, false, true, true, false];
        let mut dst: Vec<u8> = Vec::new();
        let result = compress(&input, &mut dst);

        assert!(result.is_ok());

        let mut dec = Vec::<bool>::new();
        let result = decompress(&dst, &mut dec, input.len());

        assert!(result.is_ok());
        let result = result.unwrap();

        assert_eq!(result, 1);
        assert_eq!(dec, input);
    }
}
