use std::io::Write;
use thiserror::Error;

pub(crate) type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("value out of bound: {0}")]
    ValueOutOfBound(String),

    #[error("writer error. {0}")]
    WriterError(#[from] std::io::Error),
}

const CONTINUE_CONTROL_BIT: u8 = 1 << 7;
const VALUE_MASK: u8 = (1 << 7) - 1;

pub fn compress_u64<W>(src: u64, dst: &mut W) -> Result<()>
where
    W: Write,
{
    let meaningful_bits = 64 - src.leading_zeros();

    let byte_size = match (meaningful_bits / 7, meaningful_bits % 7) {
        (byte_size, 0) => byte_size,
        (byte_size, _) => byte_size + 1,
    };

    for i in 0..byte_size {
        let is_last = i == byte_size - 1;
        if is_last {
            dst.write(&[(src >> i * 7) as u8; 1])?;
        } else {
            dst.write(&[((src >> i * 7) as u8) | CONTINUE_CONTROL_BIT])?;
        }
    }

    Ok(())
}

pub fn decompress_u64(src: &[u8]) -> Result<(u64, usize)> {
    let mut result: u64 = 0;
    let mut variants_size = 0;
    let mut variants = Vec::<u8>::new();

    for v in src.iter() {
        let value = v & VALUE_MASK;

        variants.push(value);
        variants_size += 1;

        if CONTINUE_CONTROL_BIT != v & CONTINUE_CONTROL_BIT {
            break;
        }
    }

    if variants_size >= 11 {
        return Err(Error::ValueOutOfBound(format!(
            "variants size out of bound of u64 size:{variants_size}"
        )));
    } else if variants_size >= 10 {
        if variants.last().unwrap().leading_zeros() <= 6 {
            return Err(Error::ValueOutOfBound(format!(
                "variants size out of bound of u64 size:{variants_size}"
            )));
        }
    }

    for i in (0..variants_size).rev() {
        result |= (variants[i] as u64) << i * 7;
    }

    Ok((result, variants_size))
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_compress_1() {
        let src = 0b01100;
        let mut dst = Vec::<u8>::new();
        let result = compress_u64(src, &mut dst);
        assert!(result.is_ok());

        assert_eq!(dst, vec![0b1100]);
        let (decomp_val, _idx) = decompress_u64(&dst).unwrap();

        assert_eq!(decomp_val, src);
    }

    #[test]
    fn test_compress_2() {
        let src = 0b111_0101100;
        let mut dst = Vec::<u8>::new();
        let result = compress_u64(src, &mut dst);
        assert!(result.is_ok());

        assert_eq!(dst, vec![0b10101100, 0b111]);
        let (decomp_val, _idx) = decompress_u64(&dst).unwrap();

        assert_eq!(decomp_val, src);
    }

    #[test]
    fn test_compress_3() {
        let src = 0b1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111;
        let mut dst = Vec::<u8>::new();
        let result = compress_u64(src, &mut dst);
        assert!(result.is_ok());

        assert_eq!(
            dst,
            vec![
                0b1111_1111,
                0b1111_1111,
                0b1111_1111,
                0b1111_1111,
                0b1111_1111,
                0b1111_1111,
                0b1111_1111,
                0b1111_1111,
                0b1111_1111,
                0b000000001,
            ]
        );
        let (decomp_val, _idx) = decompress_u64(&dst).unwrap();

        assert_eq!(decomp_val, src);
    }

    #[test]
    fn test_compress_4() {
        let src = vec![
            0b1111_1111,
            0b1111_1111,
            0b1111_1111,
            0b1111_1111,
            0b1111_1111,
            0b1111_1111,
            0b1111_1111,
            0b1111_1111,
            0b1111_1111,
            0b000000011,
        ];
        let result = decompress_u64(&src);
        assert!(result.is_err())
    }

    #[test]
    fn test_compress_5() {
        let mut dst = Vec::<u8>::new();
        let result = compress_u64(11, &mut dst);
        assert!(result.is_ok());

        let result = compress_u64(100, &mut dst);
        assert!(result.is_ok());

        let (decomp_val, idx) = decompress_u64(&dst).unwrap();
        assert_eq!(decomp_val, 11);

        let (decomp_val, _idx) = decompress_u64(&dst.as_slice()[idx..]).unwrap();
        assert_eq!(decomp_val, 100);
    }
}
