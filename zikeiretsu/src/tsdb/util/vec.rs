use thiserror::*;
type Result<T> = std::result::Result<T, VecOpeError>;

#[derive(Error, Debug)]
pub enum VecOpeError {
    #[error("vec out of range: {0}")]
    OutOfRange(usize),
}

pub fn remove_range<T>(datapoints: &mut Vec<T>, range: (usize, usize)) -> Result<()> {
    datapoints.drain(range.0..range.1 + 1);
    Ok(())
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

/// [0,1,2,3,4].trim(1,2) -> [1]
/// [0,1,2,3,4].trim(1,3) -> [1,2]
pub fn trim_values<V>(
    values: &mut Vec<V>,
    retain_start_index: usize,
    cut_off_suffix_start_idx: usize,
) -> Result<()> {
    let prefix_remove_range = if retain_start_index == 0 {
        None
    } else {
        Some((0 as usize, retain_start_index))
    };

    let suffix_remove_range = if cut_off_suffix_start_idx >= values.len() - 1 {
        None
    } else {
        if retain_start_index != 0 {
            let prefix_trimmed_num = retain_start_index + 1;
            let len_after_prefix_trimmed = values.len() - prefix_trimmed_num;
            Some((
                cut_off_suffix_start_idx - prefix_trimmed_num,
                len_after_prefix_trimmed,
            ))
        } else {
            Some((cut_off_suffix_start_idx, values.len() - 1))
        }
    };

    if let Some((start, end)) = prefix_remove_range {
        remove_range(values, (start, end))?;
    }

    if let Some((start, end)) = suffix_remove_range {
        remove_range(values, (start, end))?;
    }

    Ok(())
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

        remove_range(&mut datapoints, (2, 3));

        let expected_datapoints = float_data_points!(
            {1629745451_715062000, vec![100f64,12f64]},
            {1629745451_715063000, vec![200f64,36f64]},
            {1629745451_715066000, vec![300f64,36f64]}
        );

        assert_eq!(*datapoints, expected_datapoints);
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

        remove_range(&mut datapoints, (0, 4));

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

        remove_range(&mut datapoints, (4, 4));

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

        remove_range(&mut datapoints, (0, 0));

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

        remove_range(&mut datapoints, (2, 4));

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

        remove_range(&mut datapoints, (0, 6));

        let expected_datapoints = float_data_points!();

        assert_eq!(*datapoints, expected_datapoints);
    }
}
