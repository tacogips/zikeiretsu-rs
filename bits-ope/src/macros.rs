#[macro_export]
macro_rules! bits_reader {
    ($($val:ident),*) => {
        match bytes_converter::from_bits_to_vec(&[$(Bit::$val),*]){
            Ok(bytes) => Ok(ValBitsReader::new(bytes)),
            Err(e) => Err(e)
        }
    };
}

#[macro_export]
///
///
/// example
/// if meaning_full_bit_size = 16
///
/// ```markdown
/// |0b0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_1001_0100|
///                                                                | meaningful bits  |
///
/// ```
///
macro_rules! u64_bits_reader {
    ($val:expr, $meaningful_bit_size:expr) => {
        match bytes_converter::from_u64_to_vec($val, $meaningful_bit_size) {
            Ok(bytes) => Ok(ValBitsReader::new(bytes)),
            Err(e) => Err(e),
        }
    };
}

#[macro_export]
macro_rules! u32_bits_reader {
    ($val:expr, $meaningful_bit_size:expr) => {
        match bytes_converter::from_u32_to_vec($val, $meaningful_bit_size) {
            Ok(bytes) => Ok(ValBitsReader::new(bytes)),
            Err(e) => Err(e),
        }
    };
}

#[macro_export]
macro_rules! u8_bits_reader {
    ($val:expr, $meaningful_bit_size:expr) => {
        match bytes_converter::from_u8($val, $meaningful_bit_size) {
            Ok(b) => Ok(ValBitsReader::new(vec![b])),
            Err(e) => Err(e),
        }
    };
}
