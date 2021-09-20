mod macros;

pub use macros::*;
use std::cmp::min;
use std::io::Write;
use thiserror::Error;

pub type BytesIndex = usize;
pub type BytesSize = usize;
pub type BitsIndex = usize;
pub type BitsSize = usize;

pub(crate) type Result<S> = std::result::Result<S, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("bit range out of bound")]
    BitRangeOutOfBound(usize),

    #[error("bytes out of bound")]
    BytesOutOfBound(usize),

    #[error("flush bits error")]
    FlushBitsError(String),

    #[error("writer error. {0}")]
    WriterError(#[from] std::io::Error),
}

#[derive(Debug, Eq, PartialEq)]
pub enum Bit {
    Zero,
    One,
}

pub mod bytes_converter {
    use super::BitsSize;
    use super::Result;

    use super::Bit;
    /// ```markdown
    /// [ 1 0 1 0 1 0 1 0 1 1 1 ]
    /// to
    /// [10101010u8, 11100000u8]
    /// ```
    pub fn from_bits(src: &[Bit], dst: &mut Vec<u8>) -> Result<()> {
        for chunk_bits in src.chunks(8) {
            let mut each_byte: u8 = 0;
            for (idx, bit) in chunk_bits.iter().enumerate() {
                match bit {
                    Bit::One => {
                        let lshift_bits = 8 - idx - 1;
                        each_byte = each_byte | (1 << lshift_bits)
                    }
                    _ => { /*do nothing*/ }
                }
            }
            dst.push(each_byte);
        }
        Ok(())
    }

    pub fn from_bits_to_vec(src: &[Bit]) -> Result<Vec<u8>> {
        let mut dst = Vec::<u8>::new();

        from_bits(src, &mut dst)?;
        Ok(dst)
    }

    pub fn from_u8(src: u8, meaningful_bit_size: BitsSize) -> Result<u8> {
        debug_assert!(meaningful_bit_size <= 8);
        let left_shift_bits = 8 - meaningful_bit_size;
        let src = src << left_shift_bits;

        Ok(src)
    }

    pub fn from_u64(src: u64, meaningful_bit_size: BitsSize, dst: &mut Vec<u8>) -> Result<()> {
        debug_assert!(meaningful_bit_size <= 64);
        let left_shift_bits = 64 - meaningful_bit_size;
        let src = src << left_shift_bits;
        let src = src.to_be_bytes();
        let needed_head_byte_size =
            meaningful_bit_size / 8 + (if meaningful_bit_size % 8 != 0 { 1 } else { 0 });

        for each_byte in src.iter().take(needed_head_byte_size) {
            dst.push(*each_byte);
        }

        Ok(())
    }

    pub fn from_u64_to_vec(src: u64, meaningful_bit_size: BitsSize) -> Result<Vec<u8>> {
        let mut dst = Vec::<u8>::new();

        from_u64(src, meaningful_bit_size, &mut dst)?;
        Ok(dst)
    }

    pub fn from_u32(src: u32, meaningful_bit_size: BitsSize, dst: &mut Vec<u8>) -> Result<()> {
        debug_assert!(meaningful_bit_size <= 32);
        let left_shift_bits = 32 - meaningful_bit_size;
        let src = src << left_shift_bits;
        let src = src.to_be_bytes();
        let needed_head_byte_size =
            meaningful_bit_size / 8 + (if meaningful_bit_size % 8 != 0 { 1 } else { 0 });

        for each_byte in src.iter().take(needed_head_byte_size) {
            dst.push(*each_byte);
        }

        Ok(())
    }

    pub fn from_u32_to_vec(src: u32, meaningful_bit_size: BitsSize) -> Result<Vec<u8>> {
        let mut dst = Vec::<u8>::new();

        from_u32(src, meaningful_bit_size, &mut dst)?;
        Ok(dst)
    }
}

pub trait BitsReader {
    //TODO(tacogips) rename to more apropriate name
    fn bits_size(&self) -> BitsSize;
    fn chomp_as_bits_unit(&mut self, chomp_bit_size: BitsSize) -> Result<Option<BitsUnit>>;

    fn chomp_as_u64(&mut self, chomp_bit_size: BitsSize) -> Result<Option<u64>> {
        if chomp_bit_size == 0 || chomp_bit_size > 64 {
            return Err(Error::BitRangeOutOfBound(chomp_bit_size));
        }

        let division = chomp_bit_size / 8;
        let mut result: u64 = 0;

        for _ in 0..division {
            match self.chomp_as_bits_unit(8)? {
                Some(v) => result = result << 8 | v.bits_buffer as u64,
                None => return Ok(None),
            }
        }

        let reminder = chomp_bit_size % 8;
        if reminder > 0 {
            match self.chomp_as_bits_unit(reminder)? {
                Some(v) => result = result << reminder | v.bits_buffer as u64,
                None => return Ok(None),
            }
        }

        Ok(Some(result))
    }

    fn chomp_as_u8(&mut self, chomp_bit_size: BitsSize) -> Result<Option<u8>> {
        if chomp_bit_size == 0 || chomp_bit_size > 8 {
            return Err(Error::BitRangeOutOfBound(chomp_bit_size));
        }
        let val = self.chomp_as_bits_unit(chomp_bit_size)?;
        Ok(val.map(|v| v.inner_value()))
    }

    fn chomp_as_bytes(&mut self, mut chomp_bit_size: BitsSize) -> Result<Vec<BitsUnit>> {
        let mut result = Vec::<BitsUnit>::new();
        loop {
            if chomp_bit_size == 0 {
                break;
            }
            let each_chomp_size = min(chomp_bit_size, 8);
            match self.chomp_as_bits_unit(each_chomp_size)? {
                Some(each_bits) => result.push(each_bits),
                None => break,
            }

            chomp_bit_size -= each_chomp_size;
        }
        Ok(result)
    }

    fn chomp_as_bit(&mut self) -> Result<Option<Bit>> {
        let val = self.chomp_as_bits_unit(1)?;
        Ok(val.map(|v| match v.inner_value() {
            0 => Bit::Zero,
            1 => Bit::One,
            v @ _ => unreachable!("chomp as bits returns with invalid value :{}", v),
        }))
    }
}

///
///
/// Current_bits_pos_in_current_byte stands for current bits index of the current byte. Takes the values of [0-8].
/// `8` means it have written full of the bytes.
/// ```markdown
/// e.g.
///
/// At First the current byte is empty.
///
/// | 0 0 0 0 0 0 0 0|
///  ^ current_bit:0
///
///  write 3 bits `110`
/// | 1 1 0 0 0 0 0 0 |
///        ^ current_bit:3
///
///  write 5 bits `11111`
/// | 1 1 0 1 1 1 1 1 |
///                  ^ current_bit:8(full)
///
/// ```
///
pub struct BitsWriter {
    buffer: Vec<u8>,
    current_bits_offset_in_current_byte: BitsIndex,
}

impl BitsWriter {
    pub fn new() -> Self {
        Self {
            buffer: Vec::new(),
            current_bits_offset_in_current_byte: 8,
        }
    }

    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    pub fn inner_value(&self) -> &[u8] {
        self.buffer.as_slice()
    }

    pub fn append<T>(&mut self, mut bits: T, mut append_bits_size: BitsSize) -> Result<()>
    where
        T: BitsReader,
    {
        debug_assert!(append_bits_size <= bits.bits_size());
        loop {
            if append_bits_size == 0 {
                break;
            }

            if self.current_bits_offset_in_current_byte == 8 {
                self.buffer.push(0);
                self.current_bits_offset_in_current_byte = 0
            }

            let blank_size_in_current_byte = 8 - self.current_bits_offset_in_current_byte;
            let chomp_bits_size = min(append_bits_size, blank_size_in_current_byte);

            match bits.chomp_as_bits_unit(chomp_bits_size)? {
                None => break,
                Some(chomped_byte) => {
                    debug_assert!(self.current_bits_offset_in_current_byte <= 8);

                    let current_byte = self.buffer.pop().unwrap();
                    let actual_bits_size = chomped_byte.meaningful_bits_len();
                    let left_shit_bits = blank_size_in_current_byte - actual_bits_size;
                    let new_val = current_byte | (chomped_byte.inner_value() << left_shit_bits);
                    self.buffer.push(new_val);
                    append_bits_size -= chomp_bits_size;
                    self.current_bits_offset_in_current_byte += actual_bits_size;
                }
            }
        }

        Ok(())
    }

    pub fn flush<W>(self, dst: &mut W) -> Result<()>
    where
        W: Write,
    {
        dst.write(&self.buffer)?;
        Ok(())
    }
}

/// If bits value is "11" (3 in decimal)
///  bits buffer will be
/// ```markdown
///        | 0 0 0 0 0 0 1 1 | as u8
/// offset   0 1 2 3 4 5 6 7
/// ```
///  the most significant bit offset is 6
///
pub struct BitsUnit {
    bits_buffer: u8,
    most_sig_bit_offset: BitsIndex,
}

impl BitsUnit {
    pub fn inner_value(&self) -> u8 {
        self.bits_buffer
    }
    pub fn meaningful_bits_len(&self) -> usize {
        8 - self.most_sig_bit_offset
    }
}

pub trait ByteArrayBitsReader {
    fn bytes_buffer_len(&self) -> usize;
    fn current_byte_index(&self) -> BytesIndex;
    fn current_bits_offset_in_current_byte(&self) -> BytesIndex;

    fn src(&self, i: BytesIndex) -> u8;
    fn set_current_byte_index(&mut self, i: BytesIndex);
    fn set_current_bits_offset_in_current_byte(&mut self, i: BytesIndex);

    fn at_tail(&self) -> bool {
        if self.current_byte_index() == self.bytes_buffer_len() - 1
            && self.current_bits_offset_in_current_byte() >= 8
        {
            true
        } else {
            false
        }
    }

    /// retaining bits size
    /// ``` markdown
    ///  if src data like below
    ///
    ///          |<- retaining ->|
    ///  [ff ff ff fa fb ba 01 10]
    ///         ^ current index (2) .current bits index = 4
    ///
    ///  the retaining bits sie =  36
    ///
    /// ```
    fn retaining_bits_size(&self) -> BitsSize {
        (self.bytes_buffer_len() - self.current_byte_index()) * 8
            - self.current_bits_offset_in_current_byte()
    }

    ///
    /// ## case 1 before
    /// ```markdown
    ///   src : [[ 1 0 1 0 1 0 1 1 ]  [ 1 0 1 0 1 0 1 0 ]]
    ///   idx      0 1 2 3 4 5 6 7      0 1 2 3 4 5 6 7
    ///                        ^ current bit offset
    ///   current_byte_index = 0;
    ///   current_bits_offset_in_current_byte  = 6
    ///
    ///   chomp_bit_size = 3
    /// ```
    ///
    /// ## case 1 after
    /// ```markdown
    ///   chompd value = [1 1 1]
    ///
    ///   src : [[ 1 0 1 0 1 0 1 1 ]  [ 1 0 1 0 1 0 1 0 ]]
    ///   idx      0 1 2 3 4 5 6 7      0 1 2 3 4 5 6 7
    ///                                   ^ current bit offset
    ///   current_byte_index = 1;
    ///   current_bits_offset_in_current_byte  = 1
    /// ```
    ///
    ///
    fn chomp_as_bits_unit_byte_array(
        &mut self,
        chomp_bit_size: BitsSize,
    ) -> Result<Option<BitsUnit>> {
        if chomp_bit_size == 0 || chomp_bit_size > 8 {
            return Err(Error::BitRangeOutOfBound(chomp_bit_size));
        }

        if self.at_tail() {
            return Ok(None);
            //TODO(taocgips) should be out of bounds error?
            //return Err(out of boundsk);
        }

        let mut bits_buffer: u8 = 0;
        let mut actual_chomped_bit_size: BitsSize = 0;
        let mut chomp_bit_size_in_iter = chomp_bit_size;
        loop {
            if chomp_bit_size_in_iter == 0 {
                break;
            }

            if self.at_tail() {
                break;
                //TODO(taocgips) should return error of out of bounds?
                //return Err(out of boundsk);
            }

            debug_assert!(self.current_bits_offset_in_current_byte() <= 8);
            if self.current_bits_offset_in_current_byte() == 8 {
                self.set_current_byte_index(self.current_byte_index() + 1);
                self.set_current_bits_offset_in_current_byte(0);
            }

            let mut chomp_bit_size_in_next_iter = 0;
            let meaningful_bitsize = 8 - self.current_bits_offset_in_current_byte();
            if chomp_bit_size_in_iter > meaningful_bitsize {
                chomp_bit_size_in_next_iter = chomp_bit_size_in_iter - meaningful_bitsize;
                chomp_bit_size_in_iter = meaningful_bitsize;
            }

            let current_byte = self.src(self.current_byte_index());
            let mask: u8 = ((1 << chomp_bit_size_in_iter) - 1) as u8;

            let right_shift_size =
                8 - chomp_bit_size_in_iter - self.current_bits_offset_in_current_byte();

            let new_val = (current_byte >> right_shift_size) & mask;
            if bits_buffer == 0 {
                bits_buffer = new_val
            } else {
                bits_buffer = (bits_buffer << chomp_bit_size_in_iter) | new_val;
            }

            self.set_current_bits_offset_in_current_byte(
                self.current_bits_offset_in_current_byte() + chomp_bit_size_in_iter,
            );

            actual_chomped_bit_size += chomp_bit_size_in_iter;
            chomp_bit_size_in_iter = chomp_bit_size_in_next_iter
        }

        Ok(Some(BitsUnit {
            bits_buffer,
            most_sig_bit_offset: 8 - actual_chomped_bit_size,
        }))
    }
}

pub struct RefBitsReader<'a> {
    src: &'a [u8],
    current_byte_index: BytesIndex,
    current_bits_offset_in_current_byte: BitsIndex,
}

impl<'a> RefBitsReader<'a> {
    pub fn new(src: &'a [u8]) -> Self {
        Self {
            src,
            current_byte_index: 0,
            current_bits_offset_in_current_byte: 0,
        }
    }

    pub fn new_with_offset(src: &'a [u8], current_bits_offset_in_current_byte: BitsIndex) -> Self {
        Self {
            src,
            current_byte_index: 0,
            current_bits_offset_in_current_byte,
        }
    }
    pub fn current_byte_index(&self) -> BytesIndex {
        self.current_byte_index
    }
}

impl<'a> ByteArrayBitsReader for RefBitsReader<'a> {
    fn src(&self, i: BytesIndex) -> u8 {
        self.src[i]
    }
    fn set_current_byte_index(&mut self, i: BytesIndex) {
        self.current_byte_index = i
    }
    fn set_current_bits_offset_in_current_byte(&mut self, i: BytesIndex) {
        self.current_bits_offset_in_current_byte = i
    }

    fn bytes_buffer_len(&self) -> usize {
        self.src.len()
    }
    fn current_byte_index(&self) -> BytesIndex {
        self.current_byte_index
    }
    fn current_bits_offset_in_current_byte(&self) -> BytesIndex {
        self.current_bits_offset_in_current_byte
    }
}

impl<'a> BitsReader for RefBitsReader<'a> {
    fn bits_size(&self) -> BitsSize {
        self.src.len() * 8
    }

    fn chomp_as_bits_unit(&mut self, chomp_bit_size: BitsSize) -> Result<Option<BitsUnit>> {
        self.chomp_as_bits_unit_byte_array(chomp_bit_size)
    }
}

pub struct ValBitsReader {
    src: Vec<u8>,
    current_byte_index: BytesIndex,
    current_bits_offset_in_current_byte: BitsIndex,
}

impl ValBitsReader {
    pub fn new(src: Vec<u8>) -> Self {
        Self {
            src,
            current_byte_index: 0,
            current_bits_offset_in_current_byte: 0,
        }
    }

    pub fn new_with_offset(src: Vec<u8>, current_bits_offset_in_current_byte: BitsIndex) -> Self {
        Self {
            src,
            current_byte_index: 0,
            current_bits_offset_in_current_byte,
        }
    }
}

impl ByteArrayBitsReader for ValBitsReader {
    fn src(&self, i: BytesIndex) -> u8 {
        self.src[i]
    }
    fn set_current_byte_index(&mut self, i: BytesIndex) {
        self.current_byte_index = i
    }
    fn set_current_bits_offset_in_current_byte(&mut self, i: BytesIndex) {
        self.current_bits_offset_in_current_byte = i
    }

    fn bytes_buffer_len(&self) -> usize {
        self.src.len()
    }
    fn current_byte_index(&self) -> BytesIndex {
        self.current_byte_index
    }
    fn current_bits_offset_in_current_byte(&self) -> BytesIndex {
        self.current_bits_offset_in_current_byte
    }
}

impl BitsReader for ValBitsReader {
    fn bits_size(&self) -> BitsSize {
        self.src.len() * 8
    }

    fn chomp_as_bits_unit(&mut self, chomp_bit_size: BitsSize) -> Result<Option<BitsUnit>> {
        self.chomp_as_bits_unit_byte_array(chomp_bit_size)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    pub fn test_bits_reader_bits_1() {
        let src = [0b10111001u8, 0b11111111u8];
        let mut reader = RefBitsReader::new(&src);
        {
            let invalid = reader.chomp_as_bits_unit(0);
            assert!(invalid.is_err());
            let invalid = reader.chomp_as_bits_unit(9);
            assert!(invalid.is_err());
        }

        let chomped = reader.chomp_as_bits_unit(3).unwrap();
        assert!(chomped.is_some());
        let chomped = chomped.unwrap();

        assert_eq!(chomped.bits_buffer, 0b00000101u8);
        assert_eq!(chomped.most_sig_bit_offset, 5);
        assert_eq!(chomped.meaningful_bits_len(), 3);

        assert_eq!(reader.current_byte_index, 0);
        assert_eq!(reader.current_bits_offset_in_current_byte, 3);
    }

    #[test]
    pub fn test_bits_reader_bits_2() {
        let src = [0b10111001u8, 0b11111111u8];
        let mut reader = RefBitsReader::new(&src);

        reader.current_bits_offset_in_current_byte = 3;

        let chomped = reader.chomp_as_bits_unit(6).unwrap();
        assert!(chomped.is_some());
        let chomped = chomped.unwrap();

        assert_eq!(chomped.bits_buffer, 0b110011u8);
        assert_eq!(chomped.most_sig_bit_offset, 2);
        assert_eq!(chomped.meaningful_bits_len(), 6);

        assert_eq!(reader.current_byte_index, 1);
        assert_eq!(reader.current_bits_offset_in_current_byte, 1);
    }

    #[test]
    pub fn test_bits_reader_bits_3() {
        let src = [0b10111001u8, 0b11111111u8];
        let mut reader = RefBitsReader::new(&src);

        reader.current_byte_index = 1;
        reader.current_bits_offset_in_current_byte = 1;

        let chomped = reader.chomp_as_bits_unit(6).unwrap();
        assert!(chomped.is_some());
        let chomped = chomped.unwrap();

        assert_eq!(chomped.bits_buffer, 0b111111u8);
        assert_eq!(chomped.most_sig_bit_offset, 2);
        assert_eq!(chomped.meaningful_bits_len(), 6);

        assert_eq!(reader.current_byte_index, 1);
        assert_eq!(reader.current_bits_offset_in_current_byte, 7);
    }

    #[test]
    pub fn test_bits_reader_bits_4() {
        let src = [0b10111001u8, 0b11111111u8];
        let mut reader = RefBitsReader::new(&src);

        reader.current_byte_index = 1;
        reader.current_bits_offset_in_current_byte = 7;

        let chomped = reader.chomp_as_bits_unit(2).unwrap();
        assert!(chomped.is_some());
        let chomped = chomped.unwrap();

        assert_eq!(chomped.bits_buffer, 0b1u8);
        assert_eq!(chomped.most_sig_bit_offset, 7);
        assert_eq!(chomped.meaningful_bits_len(), 1);

        assert_eq!(reader.current_byte_index, 1);
        assert_eq!(reader.current_bits_offset_in_current_byte, 8);
    }

    #[test]
    pub fn test_bits_reader_bits_5() {
        let src = [0b10111001u8, 0b11111111u8];
        let mut reader = RefBitsReader::new(&src);

        reader.current_byte_index = 0;
        reader.current_bits_offset_in_current_byte = 0;

        let chomped = reader.chomp_as_bits_unit(8).unwrap();
        assert!(chomped.is_some());
        let chomped = chomped.unwrap();
        assert_eq!(chomped.bits_buffer, 0b10111001u8);
        assert_eq!(reader.current_byte_index, 0);
        assert_eq!(reader.current_bits_offset_in_current_byte, 8);

        let chomped = reader.chomp_as_bits_unit(8).unwrap();
        assert!(chomped.is_some());
        let chomped = chomped.unwrap();
        assert_eq!(chomped.bits_buffer, 0b11111111u8);
        assert_eq!(reader.current_byte_index, 1);
        assert_eq!(reader.current_bits_offset_in_current_byte, 8);

        let chomped = reader.chomp_as_bits_unit(8).unwrap();
        assert!(chomped.is_none());
    }

    #[test]
    pub fn test_bits_reader_bits_6() {
        let src = [0b10111001u8, 0b11111111u8];
        let mut reader = RefBitsReader::new(&src);

        reader.current_byte_index = 1;
        reader.current_bits_offset_in_current_byte = 8;

        let chomped = reader.chomp_as_bits_unit(2).unwrap();
        assert!(chomped.is_none());
    }

    #[test]
    pub fn test_bits_reader_bits_7() {
        let src = [0b10111001u8, 0b00000000];
        let mut reader = RefBitsReader::new(&src);

        reader.current_byte_index = 0;
        reader.current_bits_offset_in_current_byte = 0;

        let chomped = reader.chomp_as_bits_unit(8).unwrap();
        assert!(chomped.is_some());
        let chomped = chomped.unwrap();
        assert_eq!(chomped.bits_buffer, 0b10111001u8);
        assert_eq!(reader.current_byte_index, 0);
        assert_eq!(reader.current_bits_offset_in_current_byte, 8);

        let chomped = reader.chomp_as_bits_unit(2).unwrap();
        assert!(chomped.is_some());
        let chomped = chomped.unwrap();
        assert_eq!(chomped.bits_buffer, 0b00);
        assert_eq!(reader.current_byte_index, 1);
        assert_eq!(reader.current_bits_offset_in_current_byte, 2);

        let chomped = reader.chomp_as_bits_unit(8).unwrap();
        assert!(chomped.is_some());
    }

    #[test]
    pub fn test_bits_reader_bits_8() {
        let src = [0b10111001u8, 0b00000000];
        let mut reader = RefBitsReader::new(&src);

        reader.current_byte_index = 0;
        reader.current_bits_offset_in_current_byte = 2;

        let chomped = reader.chomp_as_bits_unit(3).unwrap();
        assert!(chomped.is_some());
        let chomped = chomped.unwrap();
        assert_eq!(chomped.bits_buffer, 0b111u8);
        assert_eq!(reader.current_byte_index, 0);
        assert_eq!(reader.current_bits_offset_in_current_byte, 5);
    }

    #[test]
    pub fn test_bits_reader_bits_9() {
        let src = [0b10111001u8, 0b00000000];
        let mut reader = RefBitsReader::new(&src);

        reader.current_byte_index = 1;
        reader.current_bits_offset_in_current_byte = 8;

        let chomped = reader.chomp_as_bits_unit(3).unwrap();
        assert!(chomped.is_none());
    }

    #[test]
    pub fn test_bits_reader_bits_10() {
        let src = [0b10111001u8, 0b00000001];
        let mut reader = RefBitsReader::new(&src);

        reader.current_byte_index = 1;
        reader.current_bits_offset_in_current_byte = 7;

        let chomped = reader.chomp_as_bits_unit(4).unwrap();
        assert!(chomped.is_some());

        let chomped = chomped.unwrap();
        assert_eq!(chomped.bits_buffer, 0b1u8);
        assert_eq!(reader.current_byte_index, 1);
        assert_eq!(reader.current_bits_offset_in_current_byte, 8);
    }

    #[test]
    pub fn test_bits_reader_bits_u64_1() {
        let src = [
            0b10111001u8,
            0b00000001,
            0b10111u8,
            0b10111001u8,
            0b10011001u8,
            0b1001u8,
            0b111u8,
            0b1111u8,
        ];
        let mut reader = RefBitsReader::new(&src);

        reader.current_byte_index = 0;
        reader.current_bits_offset_in_current_byte = 0;

        let chomped = reader.chomp_as_u64(64).unwrap();
        assert!(chomped.is_some());

        let chomped = chomped.unwrap();

        let expected: u64 = u64::from_be_bytes(src);
        assert_eq!(chomped, expected);
    }

    #[test]
    pub fn test_bits_reader_bits_u64_2() {
        let src = [
            0b10111001u8,
            0b00000001,
            0b10111u8,
            0b10111001u8,
            0b10011001u8,
            0b1001u8,
            0b111u8,
            0b1111u8,
        ];
        let mut reader = RefBitsReader::new(&src);

        reader.current_byte_index = 0;
        reader.current_bits_offset_in_current_byte = 0;

        let chomped = reader.chomp_as_u64(4).unwrap();
        assert!(chomped.is_some());

        let chomped = chomped.unwrap();

        let expected: u64 = 0b1011;
        assert_eq!(chomped, expected);
    }

    #[test]
    pub fn test_bits_retainint_bits_1() {
        let src = [0b10111001u8, 0b00000001, 0b00000001];

        {
            let mut reader = RefBitsReader::new(&src);
            reader.current_byte_index = 0;
            reader.current_bits_offset_in_current_byte = 0;

            assert_eq!(reader.retaining_bits_size(), 24);
        }

        {
            let mut reader = RefBitsReader::new(&src);
            reader.current_byte_index = 0;
            reader.current_bits_offset_in_current_byte = 4;

            assert_eq!(reader.retaining_bits_size(), 20);
        }

        {
            let mut reader = RefBitsReader::new(&src);
            reader.current_byte_index = 1;
            reader.current_bits_offset_in_current_byte = 4;

            assert_eq!(reader.retaining_bits_size(), 12);
        }

        {
            let mut reader = RefBitsReader::new(&src);
            reader.current_byte_index = 2;
            reader.current_bits_offset_in_current_byte = 7;

            assert_eq!(reader.retaining_bits_size(), 1);
        }

        {
            let mut reader = RefBitsReader::new(&src);
            reader.current_byte_index = 2;
            reader.current_bits_offset_in_current_byte = 8;

            assert_eq!(reader.retaining_bits_size(), 0);
        }
    }

    #[test]
    pub fn test_bit_from_bits_1() {
        let input = [
            Bit::One,
            Bit::Zero,
            Bit::One,
            Bit::Zero,
            Bit::One,
            Bit::Zero,
            Bit::One,
            Bit::Zero,
            Bit::One,
            Bit::One,
        ];

        let mut dst = Vec::<u8>::new();
        assert!(bytes_converter::from_bits(&input, &mut dst).is_ok());
        assert_eq!(dst.len(), 2);

        assert_eq!(dst[0], 0b10101010);
        assert_eq!(dst[1], 0b11000000);
    }

    #[test]
    pub fn test_bit_from_bits_2() {
        let input = [Bit::Zero, Bit::One, Bit::One];
        let mut dst = Vec::<u8>::new();
        assert!(bytes_converter::from_bits(&input, &mut dst).is_ok());
        assert_eq!(dst.len(), 1);

        assert_eq!(dst[0], 0b01100000);
    }

    #[test]
    pub fn test_bit_from_u64_1() {
        let mut dst = Vec::<u8>::new();
        let input: u64 = 0b0000001101;
        assert!(bytes_converter::from_u64(input, 4, &mut dst).is_ok());

        assert_eq!(dst.len(), 1);

        assert_eq!(dst[0], 0b11010000);
    }

    #[test]
    pub fn test_bit_from_u64_2() {
        let mut dst = Vec::<u8>::new();
        let input: u64 = 0b000000000_000000010_10011010_10101101;
        assert!(bytes_converter::from_u64(input, 18, &mut dst).is_ok());

        assert_eq!(dst.len(), 3);

        assert_eq!(dst[0], 0b10100110);
        assert_eq!(dst[1], 0b10101011);
        assert_eq!(dst[2], 0b01000000);
    }

    #[test]
    pub fn test_bit_from_u64_3() {
        let mut dst = Vec::<u8>::new();
        let input: u64 = 0b10_10101100;
        assert!(bytes_converter::from_u64(input, 10, &mut dst).is_ok());

        assert_eq!(dst.len(), 2);

        assert_eq!(dst[0], 0b10101011);
        assert_eq!(dst[1], 0b00);
    }

    #[test]
    pub fn test_bits_writer_from_bits_1() {
        let mut v = Vec::new();
        assert!(bytes_converter::from_u64(0b1010101100, 10, &mut v).is_ok());
        let input = RefBitsReader::new(v.as_slice());

        let mut writer = BitsWriter::new();
        assert!(writer.append(input, 10).is_ok());
        let inner_value = writer.inner_value();
        assert_eq!(inner_value.len(), 2);
        assert_eq!(inner_value[0], 0b10101011);
        assert_eq!(inner_value[1], 0b00);
    }

    #[test]
    pub fn test_bits_writer_from_bits_2() {
        let mut v = Vec::new();
        assert!(bytes_converter::from_u64(0b1010101111, 10, &mut v).is_ok());
        let input = RefBitsReader::new(v.as_slice());

        let mut writer = BitsWriter::new();
        assert!(writer.append(input, 10).is_ok());
        let inner_value = writer.inner_value();
        assert_eq!(inner_value.len(), 2);
        assert_eq!(inner_value[0], 0b10101011);
        assert_eq!(inner_value[1], 0b11000000);
    }

    #[test]
    pub fn test_bits_writer_from_bits_3() {
        let mut v = Vec::new();
        assert!(bytes_converter::from_u64(0b10101011, 8, &mut v).is_ok());
        let input = RefBitsReader::new(v.as_slice());

        let mut writer = BitsWriter::new();
        assert!(writer.append(input, 8).is_ok());
        let inner_value = writer.inner_value();
        assert_eq!(inner_value.len(), 1);
        assert_eq!(inner_value[0], 0b10101011);
    }

    #[test]
    pub fn test_bits_macro_1() {
        let bits_reader = bits_reader!(One, Zero, One);
        assert!(bits_reader.is_ok());
        let mut bits_reader = bits_reader.unwrap();
        assert_eq!(bits_reader.chomp_as_u8(3).unwrap().unwrap(), 0b00000101);
    }

    #[test]
    pub fn test_bits_macro_2() {
        let bits_reader = u64_bits_reader!(5, 3);
        assert!(bits_reader.is_ok());
        let mut bits_reader = bits_reader.unwrap();
        assert_eq!(bits_reader.chomp_as_u8(3).unwrap().unwrap(), 0b00000101);
    }

    #[test]
    pub fn test_bits_macro_3() {
        let bits_reader = u64_bits_reader!(0b10111, 3);
        assert!(bits_reader.is_ok());
        let mut bits_reader = bits_reader.unwrap();
        assert_eq!(bits_reader.chomp_as_u8(3).unwrap().unwrap(), 0b00000111);
    }

    #[test]
    pub fn test_bits_macro_4() {
        let bits_reader = u8_bits_reader!(5, 3);
        assert!(bits_reader.is_ok());
        let mut bits_reader = bits_reader.unwrap();
        assert_eq!(bits_reader.chomp_as_u8(3).unwrap().unwrap(), 0b101);
    }

    #[test]
    pub fn test_bits_macro_5() {
        let bits_reader = u32_bits_reader!(5, 3);
        assert!(bits_reader.is_ok());
        let mut bits_reader = bits_reader.unwrap();
        assert_eq!(bits_reader.chomp_as_u8(3).unwrap().unwrap(), 0b101);
    }
}
