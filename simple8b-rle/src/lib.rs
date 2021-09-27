use lazy_static::lazy_static;
use std::io::Write;
use std::iter;
use thiserror::Error;

pub(crate) type Result<T> = std::result::Result<T, Error>;
type DataNum = usize;
type Index = usize;

#[derive(Error, Debug)]
pub enum Error {
    #[error("value {0} is out of bound ")]
    ValueOutOfBound(u64),

    #[error("simple 8b compression failed. index:{0}")]
    Simple8bCompressionFailed(Index),

    #[error("no fitting compression set. index:{0} bitsize:{1}")]
    NoFittingCompressionSet(Index, usize),

    #[error("Broken simple 8b data. index:{0}")]
    InvalidSimple8bBrokenData(Index),

    #[error("invalid rle selector. selector:{0}")]
    InvalidRLESelectorValue(u64),

    #[error("invalid selector. selector:{0}")]
    InvalidSelectorValue(u64),

    #[error("writer error. {0}")]
    WriterError(#[from] std::io::Error),
}

/// Compress src values and put it into dst with simple-8b-rle algorithm.
/// The each selector and corresponding bitsizes are below.
///
/// ```markdown
/// ┌──────────────────┬───────────────────────────────────────────┬────────────────────────┐
/// │ Selector(4bits)  │  1  2  3  4  5  6  7  8  9 10 11 12 13 14 │ 15(RLE)                │
/// ├──────────────────┼───────────────────────────────────────────┼────────────────────────┤
/// │ Bitsize of value │  1  2  3  4  5  6  7  8 10 12 15 20 30 60 │ 32bits (value)         │
/// ├──────────────────┼───────────────────────────────────────────┼────────────────────────┤
/// │ Number of val    │ 60 30 20 15 12 10  8  7  6  5  4  3  2  1 │ up to 2^28 (repeat num)│
/// ├──────────────────┼───────────────────────────────────────────┼────────────────────────┤
/// │ Wasted Bits      │             12     4  4                   │                        │
/// └──────────────────┴───────────────────────────────────────────┴────────────────────────┘
/// ```
pub fn compress<W>(src: &[u64], dst: &mut W) -> Result<()>
where
    W: Write,
{
    if src.len() == 0 {
        return Ok(());
    }

    let mut current_idx: Index = 0;
    let src_len = src.len();
    loop {
        if current_idx >= src_len {
            return Ok(());
        }

        if let Some((rle_compression, rle_bound_idx)) = should_rle_compression(src, current_idx)? {
            let compressed_bytes = rle_compression.to_u64().to_be_bytes();
            dst.write(compressed_bytes.as_ref())?;
            current_idx = rle_bound_idx;
        } else {
            if let Some((compression_set, bound_idx)) =
                search_simple_8b_compress_set(src, current_idx)?
            {
                debug_assert!(current_idx < bound_idx);
                debug_assert!(bound_idx <= src.len());
                compress_simple_8b(&src[current_idx..bound_idx], dst, compression_set)?;
                current_idx = bound_idx;
            } else {
                unreachable!("current idx out of bounds. (it should be a bug)")
            }
        }
    }
}

pub fn decompress(src: &[u8], dst: &mut Vec<u64>, num_of_value: Option<usize>) -> Result<usize> {
    if num_of_value == Some(0) {
        return Ok(0);
    }

    let max_num = num_of_value.unwrap_or_else(|| std::usize::MAX);
    let mut current_index = 0;
    let mut decompressed_num: usize = 0;

    loop {
        if current_index >= src.len() {
            return Ok(current_index);
        }

        let bound_index = current_index + 8;
        if bound_index > src.len() {
            return Err(Error::InvalidSimple8bBrokenData(current_index));
        }

        let mut compressed_data_partition: [u8; 8] = Default::default();
        compressed_data_partition.copy_from_slice(&src[current_index..bound_index]);
        let compressed_data: u64 = u64::from_be_bytes(compressed_data_partition);

        let decompressed_datas = decompress_single_compressed_data(compressed_data)?;
        for each_data in decompressed_datas {
            dst.push(each_data);
            decompressed_num += 1;
            if decompressed_num >= max_num {
                return Ok(bound_index);
            }
        }

        current_index = bound_index;
    }
}

const DATA_AREA_BITS: usize = 60;
const MAX_MEANINGFUL_BIT_SIZE: usize = DATA_AREA_BITS;
const DATA_AREA_BIT_MASK: u64 = (1 << DATA_AREA_BITS) - 1;

const SELECTOR_FOR_RLE: u64 = 15;
const RLE_VALUE_BITS: usize = 32;
const RLE_VALUE_BITS_MASK: u64 = (1 << RLE_VALUE_BITS) - 1;
const MAX_RLE_REPEATABLE_NUMBER: DataNum = (1 << 28) - 1;

#[derive(Debug, Eq, PartialEq)]
enum Simple8BOrRLESelector {
    Simple8B(&'static CompressionSet),
    RLE,
}

impl Simple8BOrRLESelector {
    fn from_u64(compressed_data: u64) -> Result<Self> {
        let selector = compressed_data >> DATA_AREA_BITS;
        if selector == SELECTOR_FOR_RLE {
            Ok(Self::RLE)
        } else {
            for each in COMPRESSION_SETS.iter() {
                if each.selector.val == selector {
                    return Ok(Self::Simple8B(each));
                }
            }
            Err(Error::InvalidSelectorValue(selector))
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct CompressionSet {
    selector: Simple8bSelector,
    meaningful_bitsize: usize,
    contain_num: DataNum,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct Simple8bSelector {
    val: u64,
}

macro_rules! compression_set {
    ($({$selector:expr, $bits:expr, $num_of_val:expr}),*,) => {
        lazy_static! {
            static ref COMPRESSION_SETS: Vec<CompressionSet> = vec![
                $(CompressionSet {
                selector:  Simple8bSelector {val: $selector},
                meaningful_bitsize: $bits,
                contain_num: $num_of_val
            }) , *];
        }
    };
}

// should be sorted ascending by bitszize(2nd value in the col)
compression_set! {
    {1,  1,  60},
    {2,  2,  30},
    {3,  3,  20},
    {4,  4,  15},
    {5,  5,  12},
    {6,  6,  10},
    {7,  7,  8},
    {8,  8,  7},
    {9,  10, 6},
    {10, 12, 5},
    {11, 15, 4},
    {12, 20, 3},
    {13, 30, 2},
    {14, 60, 1},
}

#[derive(Eq, PartialEq, Debug)]
pub(crate) struct RLECompression {
    value: u64,
    repeat_num: DataNum,
}

/// format
/// |selector(4bit)| repeat num (28bit) | repeated value (32bit)|
impl RLECompression {
    fn to_u64(&self) -> u64 {
        SELECTOR_FOR_RLE << DATA_AREA_BITS as u64
            | (self.repeat_num << RLE_VALUE_BITS) as u64
            | self.value
    }

    fn from_u64(compressed_value: u64) -> Result<RLECompression> {
        debug_assert!(compressed_value >> DATA_AREA_BITS == 15);

        let compressed_value = compressed_value & DATA_AREA_BIT_MASK;
        let value = compressed_value & RLE_VALUE_BITS_MASK;
        let repeat_num: DataNum = (compressed_value >> RLE_VALUE_BITS) as usize;

        Ok(RLECompression { value, repeat_num })
    }
}

//TODO(tacogips) think about leading zero 0111_1110
fn meaningful_bitsize(n: u64) -> usize {
    let mbs = (64u32 - n.leading_zeros()) as usize;
    if mbs == 0 {
        // `0` can represented by 1 bit
        1
    } else {
        mbs
    }
}

pub(crate) fn decompress_rle(compressed_data: u64) -> Result<Vec<u64>> {
    let RLECompression { value, repeat_num } = RLECompression::from_u64(compressed_data)?;
    Ok(iter::repeat(value).take(repeat_num).collect())
}

pub(crate) fn decompress_simple_8b(
    compressed_data: u64,
    compression_set: &CompressionSet,
) -> Result<Vec<u64>> {
    let mut result = Vec::<u64>::new();
    let CompressionSet {
        meaningful_bitsize,
        contain_num,
        ..
    } = compression_set;

    let value_mask = (1 << meaningful_bitsize) - 1;
    let mut shift_bits = DATA_AREA_BITS - meaningful_bitsize;
    let mut value_num: usize = 0;
    loop {
        let value = (compressed_data >> shift_bits) & value_mask;
        result.push(value);
        value_num += 1;
        if value_num >= *contain_num {
            break;
        }
        shift_bits -= meaningful_bitsize;
    }
    Ok(result)
}

pub(crate) fn decompress_single_compressed_data(compressed_data: u64) -> Result<Vec<u64>> {
    match Simple8BOrRLESelector::from_u64(compressed_data)? {
        Simple8BOrRLESelector::Simple8B(compress_set) => {
            decompress_simple_8b(compressed_data, compress_set)
        }
        Simple8BOrRLESelector::RLE => decompress_rle(compressed_data),
    }
}

pub(crate) fn search_fitting_compress_set_by_bitsize(
    src: &[u64],
    index: Index,
) -> Result<&CompressionSet> {
    let bitsize_of_val = meaningful_bitsize(src[index]);

    for each_set in COMPRESSION_SETS.iter() {
        if bitsize_of_val >= each_set.meaningful_bitsize {
            return Ok(each_set);
        }
    }

    Err(Error::NoFittingCompressionSet(index, bitsize_of_val))
}

/// returns (compression_set, bound_index)
/// bound_index is the index of u64 vec that is not containd to current compression.
/// this means data to compress is `[start_idx..bound_index]`
pub(crate) fn search_simple_8b_compress_set(
    src: &[u64],
    start_idx: Index,
) -> Result<Option<(&CompressionSet, Index)>> {
    if start_idx >= src.len() {
        return Ok(None);
    }
    'compression_set_loop: for each_compression_set in COMPRESSION_SETS.iter() {
        let mut bound_idx = start_idx + each_compression_set.contain_num;
        if bound_idx > src.len() {
            bound_idx = src.len()
        }

        for val in src[start_idx..bound_idx].iter() {
            //TODO(tacogips) think about leading zero 0111_1110
            let bitsize_of_val = meaningful_bitsize(*val);

            if bitsize_of_val > MAX_MEANINGFUL_BIT_SIZE {
                return Err(Error::ValueOutOfBound(*val));
            } else if bitsize_of_val > each_compression_set.meaningful_bitsize {
                continue 'compression_set_loop;
            }
        }

        return Ok(Some((each_compression_set, bound_idx)));
    }

    Err(Error::Simple8bCompressionFailed(start_idx))
}

pub(crate) fn compress_simple_8b<W>(
    src: &[u64],
    dst: &mut W,
    compression_set: &CompressionSet,
) -> Result<()>
where
    W: Write,
{
    debug_assert!(src.len() > 0);
    let mut result: u64 = src[0];
    for each_val in src[1..].iter() {
        result <<= compression_set.meaningful_bitsize;
        result |= each_val;
    }

    let meaningful_data_bits = src.len() * compression_set.meaningful_bitsize;
    let right_packing_bits_size = DATA_AREA_BITS - meaningful_data_bits;

    if right_packing_bits_size > 0 {
        result <<= right_packing_bits_size;
    }
    result |= compression_set.selector.val << DATA_AREA_BITS;

    dst.write(result.to_be_bytes().as_ref())?;
    Ok(())
}

pub(crate) fn should_rle_compression(
    values: &[u64],
    start_idx: usize,
) -> Result<Option<(RLECompression, Index)>> {
    debug_assert!(start_idx < values.len());
    let value = values[start_idx];

    // when repeat_num := 1
    //   [value, other_value]
    // repeat_num := 2 means
    //   [value, value, other_value]
    let mut repeat_num: usize = 1;

    for each in values[start_idx + 1..values.len()].iter() {
        if *each == value {
            repeat_num += 1;
        } else {
            break;
        }
    }

    let result = if repeat_num == 0 {
        None
    } else {
        //TODO(tacogips) think about leading zero 0111_1110
        let bitsize_of_val = meaningful_bitsize(value);
        if bitsize_of_val > RLE_VALUE_BITS {
            None
        } else if repeat_num > MAX_RLE_REPEATABLE_NUMBER {
            None
        } else {
            let compress_set = search_fitting_compress_set_by_bitsize(values, start_idx)?;
            if repeat_num > compress_set.contain_num {
                Some((RLECompression { value, repeat_num }, start_idx + repeat_num))
            } else {
                // using simple 8b is more efficient
                None
            }
        }
    };
    Ok(result)
}

#[cfg(test)]
mod test {

    use super::*;
    use std::iter;

    #[test]
    fn test_meaningful_bitsize_1() {
        assert_eq!(meaningful_bitsize(0b1101), 4);
        assert_eq!(meaningful_bitsize(0b01101), 4);
        assert_eq!(meaningful_bitsize(0b0), 1);
        assert_eq!(
            meaningful_bitsize(
                0b_1000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000
            ),
            64
        );
    }

    #[test]
    fn test_rle_compression_1() {
        let comp = RLECompression {
            value: 4294967295,
            repeat_num: 10,
        };

        let comped_val = comp.to_u64();
        let expected =
            0b_1111_0000_0000_0000_0000_0000_0000_1010_1111_1111_1111_1111_1111_1111_1111_1111;

        assert_eq!(comped_val, expected);
        assert_eq!(RLECompression::from_u64(expected).unwrap(), comp);
    }

    #[test]
    fn test_rle_compression_2() {
        let comp = RLECompression {
            value: 4294967295,
            repeat_num: 268435455,
        };

        let comped_val = comp.to_u64();
        let expected =
            0b_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111;

        assert_eq!(comped_val, expected);
        assert_eq!(RLECompression::from_u64(expected).unwrap(), comp);
    }

    #[test]
    fn test_rle_compression_3() {
        let comp = RLECompression {
            value: 0,
            repeat_num: 268435455,
        };

        let comped_val = comp.to_u64();
        let expected =
            0b_1111_1111_1111_1111_1111_1111_1111_1111_0000_0000_0000_0000_0000_0000_0000_0000;

        assert_eq!(comped_val, expected);
        assert_eq!(RLECompression::from_u64(expected).unwrap(), comp);
    }

    #[test]
    fn test_should_rle_compression_1() {
        {
            let mut src: Vec<u64> = iter::repeat(1).take(60).collect();
            src.push(2);
            let result = should_rle_compression(src.as_ref(), 0);
            assert!(result.is_ok());
            let result = result.unwrap();

            assert!(result.is_none());
        }

        {
            let mut src: Vec<u64> = iter::repeat(1).take(61).collect();
            src.push(2);
            let result = should_rle_compression(src.as_ref(), 0);
            assert!(result.is_ok());
            let result = result.unwrap();

            assert!(result.is_some());

            let (compression, idx) = result.unwrap();

            assert_eq!(
                compression,
                RLECompression {
                    value: 1,
                    repeat_num: 61,
                }
            );

            assert_eq!(61, idx);
        }

        {
            let src: Vec<u64> = iter::repeat(1).take(61).collect();
            let result = should_rle_compression(src.as_ref(), 1);
            assert!(result.is_ok());
            let result = result.unwrap();

            assert!(result.is_none());
        }
    }

    #[test]
    fn test_should_rle_compression_2() {
        {
            let mut src: Vec<u64> = iter::repeat(60).take(60).collect();
            src.push(2);
            let result = should_rle_compression(src.as_ref(), 0);
            assert!(result.is_ok());
            let result = result.unwrap();

            assert!(result.is_none());
        }
    }

    #[test]
    fn test_search_simple_8b_compress_set_1() {
        let src = [0b101, 0b1101, 0b11];
        {
            let result = search_simple_8b_compress_set(&src, 1);
            assert!(result.is_ok());
            let (compression_set, idx) = result.unwrap().unwrap();
            assert_eq!(compression_set.meaningful_bitsize, 4);
            assert_eq!(idx, 3);
        }

        {
            let result = search_simple_8b_compress_set(&src, 2);
            assert!(result.is_ok());
            let (compression_set, idx) = result.unwrap().unwrap();
            assert_eq!(compression_set.meaningful_bitsize, 2);
            assert_eq!(idx, 3);
        }
    }

    #[test]
    fn test_search_simple_8b_compress_set_2() {
        let src = [
            0b11101,
            0b101,
            0b1101,
            0b_0000_1000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000,
            0b11,
        ];
        {
            let result = search_simple_8b_compress_set(&src, 0);
            assert!(result.is_ok());
            let (compression_set, idx) = result.unwrap().unwrap();
            assert_eq!(compression_set.meaningful_bitsize, 20);
            assert_eq!(idx, 3);
        }

        {
            let result = search_simple_8b_compress_set(&src, 2);
            assert!(result.is_ok());
            let (compression_set, idx) = result.unwrap().unwrap();
            //  0b1101  at third of src supposed to be put using 60 bits,
            //  cause  the consequence value  spend 60 bits to store
            //  so this value can't be stored with other value
            assert_eq!(compression_set.meaningful_bitsize, 60);
            assert_eq!(idx, 3);
        }

        {
            let result = search_simple_8b_compress_set(&src, 3);
            assert!(result.is_ok());
            let (compression_set, idx) = result.unwrap().unwrap();
            assert_eq!(compression_set.meaningful_bitsize, 60);
            assert_eq!(idx, 4);
        }

        {
            let result = search_simple_8b_compress_set(&src, 4);
            assert!(result.is_ok());
            let (compression_set, idx) = result.unwrap().unwrap();
            assert_eq!(compression_set.meaningful_bitsize, 2);
            assert_eq!(idx, 5);
        }
    }

    #[test]
    fn test_search_simple_8b_compress_set_3() {
        let src = [
            0b101,
            0b1101,
            0b_0001_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000,
            0b11,
        ];
        let result = search_simple_8b_compress_set(&src, 0);
        assert!(result.is_err()); // value out of bounds > 60 bits
    }

    #[test]
    fn test_search_simple_8b_compress_set_4() {
        let src = [
            0b101,
            0b1101,
            0b_0001_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000,
            0b11,
        ];
        let result = search_simple_8b_compress_set(&src, 4);
        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_compress_simple_8b_1() {
        let src = [0b101, 0b1101, 0b11];
        let mut dst = Vec::<u8>::new();

        let c = CompressionSet {
            selector: Simple8bSelector { val: 4 },
            meaningful_bitsize: 4,
            contain_num: 15,
        };

        let result = compress_simple_8b(&src, &mut dst, &c);
        assert!(result.is_ok());
        assert_eq!(
            dst,
            vec![
                0b0100_0101,
                0b1101_0011,
                0b0000_0000,
                0b0000_0000,
                0b0000_0000,
                0b0000_0000,
                0b0000_0000,
                0b0000_0000
            ]
        );
    }

    #[test]
    fn test_simple8b_or_rle_selector_1() {
        let b = 0b0011_1010_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000;

        let result = Simple8BOrRLESelector::from_u64(b);
        assert!(result.is_ok());
        let result = result.unwrap();

        assert_eq!(
            result,
            Simple8BOrRLESelector::Simple8B(&COMPRESSION_SETS[2])
        );
    }

    #[test]
    fn test_decompress_single_compressed_data_1() {
        let b = 0b0011_1010_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000;

        let result = decompress_single_compressed_data(b);
        assert!(result.is_ok());
        let result = result.unwrap();
        let mut trailing_zeros = iter::repeat(0u64).take(19).collect(); // contians 20 3-bits-value
        let mut exptected = vec![0b101];
        exptected.append(&mut trailing_zeros);
        assert_eq!(exptected, result);
    }

    #[test]
    fn test_compress_1() {
        let src = [0b101];
        let mut dst = Vec::<u8>::new();
        let result = compress(&src, &mut dst);
        assert!(result.is_ok());

        assert_eq!(
            dst,
            vec![
                0b0011_1010,
                0b0000_0000,
                0b0000_0000,
                0b0000_0000,
                0b0000_0000,
                0b0000_0000,
                0b0000_0000,
                0b0000_0000
            ]
        );

        let mut dec_dest = Vec::<u64>::new();
        let result = decompress(&dst, &mut dec_dest, Some(1));
        assert!(result.is_ok());
        assert_eq!(dec_dest, src);
    }

    #[test]
    fn test_compress_2() {
        let src = [0b101, 0b1101, 0b11];
        let mut dst = Vec::<u8>::new();
        let result = compress(&src, &mut dst);
        assert!(result.is_ok());

        assert_eq!(
            dst,
            vec![
                0b0100_0101,
                0b1101_0011,
                0b0000_0000,
                0b0000_0000,
                0b0000_0000,
                0b0000_0000,
                0b0000_0000,
                0b0000_0000
            ]
        );

        let mut dec_dest = Vec::<u64>::new();
        let result = decompress(&dst, &mut dec_dest, Some(3));
        assert!(result.is_ok());
        assert_eq!(dec_dest, src);
    }

    #[test]
    fn test_compress_3() {
        let mut src: Vec<u64> = vec![0b1001, 0b11101, 0b01];

        let mut repeat_src: Vec<u64> = iter::repeat(3).take(60).collect();
        src.append(&mut repeat_src);
        src.push(0b1001_1111);

        let mut dst = Vec::<u8>::new();
        let result = compress(&src, &mut dst);
        assert!(result.is_ok());

        let mut dec_dest = Vec::<u64>::new();
        let result = decompress(&dst, &mut dec_dest, Some(64));
        assert!(result.is_ok());
        assert_eq!(dec_dest, src);
    }

    #[test]
    fn test_compress_4() {
        let dst = Vec::<u8>::new();
        let mut dec_dest = Vec::<u64>::new();
        let result = decompress(&dst, &mut dec_dest, Some(0));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0);
    }

    #[test]
    fn test_compress_5() {
        let mut dst = Vec::<u8>::new();
        let mut dec_dest = Vec::<u64>::new();

        dst.push(10);
        let result = decompress(&dst, &mut dec_dest, Some(0));

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0);
    }

    #[test]
    fn test_compress_6() {
        let src: Vec<u64> = vec![0];

        let mut dst = Vec::<u8>::new();
        let result = compress(&src, &mut dst);
        assert!(result.is_ok());

        let mut dec_dest = Vec::<u64>::new();
        let result = decompress(&dst, &mut dec_dest, Some(1));
        assert!(result.is_ok());
        assert_eq!(dec_dest, src);
    }

    #[test]
    fn test_compress_7() {
        let src: Vec<u64> = vec![1, 0, 255];

        let mut dst = Vec::<u8>::new();
        let result = compress(&src, &mut dst);
        assert!(result.is_ok());

        let mut dec_dest = Vec::<u64>::new();
        let result = decompress(&dst, &mut dec_dest, Some(3));
        assert!(result.is_ok());
        assert_eq!(dec_dest, src);
    }
}
