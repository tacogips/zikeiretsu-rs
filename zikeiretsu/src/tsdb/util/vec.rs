use std::ptr;
use thiserror::*;

type Result<T> = std::result::Result<T, VecOpeError>;

#[derive(Error, Debug)]
pub enum VecOpeError {
    #[error("vec out of range: {0}")]
    OutOfRange(usize),

    #[error("invalid Range: {0} {1}")]
    InvalidRange(usize, usize),
}

// remove_range([2,3,4,5,6],(0,1)) => [4,5,6] retuens:Ok([2,3])
pub fn remove_range<T>(datas: &mut Vec<T>, range: (usize, usize)) -> Result<Vec<T>> {
    let drained = datas.drain(range.0..range.1 + 1);
    Ok(drained.collect())
    // TODO same code as below causes memory leak somehow..
    //let orig_len = datapoints.len();
    //let (start, end) = range;
    //assert!(
    //    start <= end,
    //    "invalid purge index start:{} > end:{}",
    //    start,
    //    end
    //);

    //assert!(
    //    end < orig_len,
    //    "invalid purge end index  end:{}, len:{}",
    //    end,
    //    orig_len
    //);

    //let purge_len = end - start + 1;

    //let remaining_len = orig_len - purge_len;
    //let shift_elem_len = orig_len - end - 1;
    //unsafe {
    //    let purge_start_ptr = datapoints.as_mut_ptr().add(start);
    //    ptr::copy(
    //        purge_start_ptr.offset(purge_len as isize),
    //        purge_start_ptr,
    //        shift_elem_len,
    //    );

    //    datapoints.set_len(remaining_len);
    //}
}

pub fn prepend<T>(datas: &mut Vec<T>, new_datas: &mut Vec<T>) {
    let orig_len = datas.len();
    let new_data_len = new_datas.len();
    datas.reserve(new_data_len);

    unsafe {
        ptr::copy(
            datas.as_ptr(),
            datas.as_mut_ptr().offset((new_data_len) as isize),
            orig_len,
        );
        ptr::copy(new_datas.as_ptr(), datas.as_mut_ptr(), new_data_len);
        datas.set_len(orig_len + new_data_len);
    }
}

/// [0,1,2,3,4].trim(1,2) -> [1]
/// [0,1,2,3,4].trim(1,3) -> [1,2]
pub fn trim_values<V>(
    values: &mut Vec<V>,
    retain_start_index: usize,
    cut_off_suffix_start_idx: usize,
) -> Result<(Vec<V>, Vec<V>)> {
    if retain_start_index > cut_off_suffix_start_idx {
        return Err(VecOpeError::InvalidRange(
            retain_start_index,
            cut_off_suffix_start_idx,
        ));
    }

    if retain_start_index > values.len() || cut_off_suffix_start_idx > values.len() {
        return Err(VecOpeError::OutOfRange(retain_start_index));
    }

    if retain_start_index == values.len() {
        let removed = remove_range(values, (0, values.len() - 1))?;
        return Ok((removed, vec![]));
    }

    let prefix_remove_until_index = if retain_start_index == 0 {
        None
    } else {
        Some(retain_start_index - 1)
    };

    let remaining_size = cut_off_suffix_start_idx - retain_start_index;
    let mut removed_prefix = vec![];
    if let Some(end) = prefix_remove_until_index {
        removed_prefix = remove_range(values, (0, end))?;
    }

    //values.truncate(remaining_size);
    let removed_suffix = remove_range(values, (remaining_size, values.len() - 1))?;
    Ok((removed_prefix, removed_suffix))
}

#[cfg(test)]
mod test {

    use super::*;
    use crate::tsdb::{DataPoint, FieldValue, TimestampNano};
    macro_rules! float_data_points {
        ($({$timestamp:expr,$values:expr}),*) => {
            vec![
            $(DataPoint::new(ts!($timestamp), $values.into_iter().map(|each| FieldValue::Float64(each as f64)).collect())),*
            ]
        };
    }

    macro_rules! ts {
        ($v:expr) => {
            TimestampNano::new($v)
        };
    }

    #[tokio::test]
    async fn remove_range_1() {
        let mut datapoints = float_data_points!(
            {1629745451_715062000, vec![100f64,12f64]},
            {1629745451_715063000, vec![200f64,36f64]},
            {1629745451_715064000, vec![200f64,36f64]},
            {1629745451_715065000, vec![300f64,36f64]},
            {1629745451_715066000, vec![300f64,36f64]}
        );

        let removed = remove_range(&mut datapoints, (2, 3)).unwrap();

        let expected_datapoints = float_data_points!(
            {1629745451_715062000, vec![100f64,12f64]},
            {1629745451_715063000, vec![200f64,36f64]},
            {1629745451_715066000, vec![300f64,36f64]}
        );

        let expected_removed = float_data_points!(
            {1629745451_715064000, vec![200f64,36f64]},
            {1629745451_715065000, vec![300f64,36f64]}
        );

        assert_eq!(*datapoints, expected_datapoints);
        assert_eq!(removed, expected_removed);
    }

    #[tokio::test]
    async fn remove_range_2() {
        let mut datapoints = float_data_points!(
            {1629745451_715062000, vec![100f64,12f64]},
            {1629745451_715063000, vec![200f64,36f64]},
            {1629745451_715064000, vec![200f64,36f64]},
            {1629745451_715065000, vec![300f64,36f64]},
            {1629745451_715066000, vec![300f64,36f64]}
        );

        remove_range(&mut datapoints, (0, 4)).unwrap();

        let expected_datapoints = float_data_points!();

        assert_eq!(*datapoints, expected_datapoints);
    }

    #[tokio::test]
    async fn remove_range_3() {
        let mut datapoints = float_data_points!(
            {1629745451_715062000, vec![100f64,12f64]},
            {1629745451_715063000, vec![200f64,36f64]},
            {1629745451_715064000, vec![200f64,36f64]},
            {1629745451_715065000, vec![300f64,36f64]},
            {1629745451_715066000, vec![300f64,36f64]}
        );

        remove_range(&mut datapoints, (4, 4)).unwrap();

        let expected_datapoints = float_data_points!(
            {1629745451_715062000, vec![100f64,12f64]},
            {1629745451_715063000, vec![200f64,36f64]},
            {1629745451_715064000, vec![200f64,36f64]},
            {1629745451_715065000, vec![300f64,36f64]}
        );

        assert_eq!(*datapoints, expected_datapoints);
    }

    #[tokio::test]
    async fn remove_range_4() {
        let mut datapoints = float_data_points!(
            {1629745451_715062000, vec![100f64,12f64]}
        );

        remove_range(&mut datapoints, (0, 0)).unwrap();

        let expected_datapoints = float_data_points!();

        assert_eq!(*datapoints, expected_datapoints);
    }

    #[tokio::test]
    async fn remove_range_5() {
        let mut datapoints = float_data_points!(
            {1629745451_715061000, vec![100f64,12f64]},
            {1629745451_715062000, vec![100f64,12f64]},
            {1629745451_715063000, vec![200f64,36f64]},
            {1629745451_715064000, vec![200f64,36f64]},
            {1629745451_715065000, vec![300f64,36f64]},
            {1629745451_715066000, vec![300f64,36f64]},
            {1629745451_715067000, vec![300f64,36f64]}
        );

        remove_range(&mut datapoints, (2, 4)).unwrap();

        let expected_datapoints = float_data_points!(
            {1629745451_715061000, vec![100f64,12f64]},
            {1629745451_715062000, vec![100f64,12f64]},
            {1629745451_715066000, vec![300f64,36f64]},
            {1629745451_715067000, vec![300f64,36f64]}
        );

        assert_eq!(*datapoints, expected_datapoints);
    }

    #[tokio::test]
    async fn remove_range_6() {
        let mut datapoints = float_data_points!(
            {1629745451_715061000, vec![100f64,12f64]},
            {1629745451_715062000, vec![100f64,12f64]},
            {1629745451_715063000, vec![200f64,36f64]},
            {1629745451_715064000, vec![200f64,36f64]},
            {1629745451_715065000, vec![300f64,36f64]},
            {1629745451_715066000, vec![300f64,36f64]},
            {1629745451_715067000, vec![300f64,36f64]}
        );

        remove_range(&mut datapoints, (0, 6)).unwrap();

        let expected_datapoints = float_data_points!();

        assert_eq!(*datapoints, expected_datapoints);
    }

    #[tokio::test]
    async fn trim_value_1() {
        let mut data = vec![1, 2, 3, 4, 5, 6];

        let (head, tail) = trim_values(&mut data, 0, 0).unwrap();

        assert_eq!(Vec::<i32>::new(), data);
        assert_eq!(head, Vec::<i32>::new());
        assert_eq!(tail, vec![1, 2, 3, 4, 5, 6]);
    }

    #[tokio::test]
    async fn trim_value_2() {
        let mut data = vec![1, 2, 3, 4, 5, 6];

        let (head, tail) = trim_values(&mut data, 1, 3).unwrap();

        assert_eq!(vec![2, 3], data);
        assert_eq!(vec![1], head);
        assert_eq!(vec![4, 5, 6], tail);
    }

    #[tokio::test]
    async fn trim_value_3() {
        let mut data = vec![1, 2, 3, 4, 5, 6];

        let (head, tail) = trim_values(&mut data, 0, 6).unwrap();

        assert_eq!(vec![1, 2, 3, 4, 5, 6], data);
        assert_eq!(Vec::<i32>::new(), head);
        assert_eq!(Vec::<i32>::new(), tail);
    }

    #[tokio::test]
    async fn trim_value_4() {
        let mut data = vec![1, 2, 3, 4, 5, 6];

        let (head, tail) = trim_values(&mut data, 5, 6).unwrap();

        assert_eq!(vec![6], data);

        assert_eq!(vec![1, 2, 3, 4, 5], head);
        assert_eq!(Vec::<i32>::new(), tail);
    }

    #[tokio::test]
    async fn trim_value_5() {
        let mut data = vec![1, 2, 3, 4, 5, 6];

        let (head, tail) = trim_values(&mut data, 5, 5).unwrap();

        assert_eq!(Vec::<i32>::new(), data);

        assert_eq!(vec![1, 2, 3, 4, 5], head);
        assert_eq!(vec![6], tail);
    }

    #[tokio::test]
    async fn trim_value_6() {
        let mut data = vec![1, 2, 3, 4, 5, 6];

        let (head, tail) = trim_values(&mut data, 6, 6).unwrap();

        assert_eq!(Vec::<i32>::new(), data);

        assert_eq!(vec![1, 2, 3, 4, 5, 6], head);
        assert_eq!(Vec::<i32>::new(), tail);
    }

    #[tokio::test]
    async fn trim_value_7() {
        let mut data = vec![1, 2, 3, 4, 5, 6];

        let (head, tail) = trim_values(&mut data, 0, 6).unwrap();

        assert_eq!(vec![1, 2, 3, 4, 5, 6], data);
        assert_eq!(head, Vec::<i32>::new());
        assert_eq!(tail, Vec::<i32>::new());
    }

    #[tokio::test]
    async fn trim_value_8() {
        let mut data = vec![1, 2, 3, 4, 5, 6];

        let (head, tail) = trim_values(&mut data, 5, 6).unwrap();

        assert_eq!(vec![6], data);

        assert_eq!(vec![1, 2, 3, 4, 5], head);
        assert_eq!(Vec::<i32>::new(), tail);
    }

    #[tokio::test]
    async fn trim_value_9() {
        let mut data = vec![1, 2, 3, 4, 5, 6];

        assert!(trim_values(&mut data, 7, 7).is_err());
        assert!(trim_values(&mut data, 0, 7).is_err());
    }

    #[tokio::test]
    async fn prepend_1() {
        let mut datas = vec![7, 8];
        let mut new_datas = vec![1, 2, 3, 4, 5, 6];

        prepend(&mut datas, &mut new_datas);

        assert_eq!(vec![1, 2, 3, 4, 5, 6, 7, 8], datas);
    }

    fn to_s(v: i32) -> String {
        v.to_string()
    }

    #[tokio::test]
    async fn prepend_2() {
        let mut datas: Vec<String> = vec![7, 8].into_iter().map(to_s).collect();
        let mut new_datas: Vec<String> = vec![1, 2, 3, 4, 5, 6].into_iter().map(to_s).collect();

        prepend(&mut datas, &mut new_datas);

        assert_eq!(
            vec![1, 2, 3, 4, 5, 6, 7, 8]
                .into_iter()
                .map(to_s)
                .collect::<Vec<String>>(),
            datas
        );
    }
}
