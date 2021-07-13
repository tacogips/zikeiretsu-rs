use std::cmp::Ordering;

#[derive(Eq, PartialEq)]
pub enum BinaryRangeSearchType {
    AtLeast,
    AtMost,
}

/// - search at most 3 from [4,6,10] => None
/// - search at most 5 from [4,6,10] => 4
/// - search at least 3 from [4,6,10] => 4
/// - search at least 5 from [4,6,10] => 10
///
pub fn binary_search_by<T, F>(
    datas: &[T],
    cond: F,
    condition_order: BinaryRangeSearchType,
) -> Option<usize>
where
    F: Fn(&T) -> Ordering,
{
    let mut left = 0;
    let mut right = datas.len();
    let mut latest_hit_idx = None;
    while left < right {
        let curr_idx = (left + right) / 2;

        let curr_val = unsafe { datas.get_unchecked(curr_idx) };
        let cmp = cond(curr_val);

        if cmp == Ordering::Less {
            left = curr_idx + 1;
            if condition_order == BinaryRangeSearchType::AtMost {
                latest_hit_idx.replace(curr_idx);
            }
        } else if cmp == Ordering::Greater {
            right = curr_idx;
            if condition_order == BinaryRangeSearchType::AtLeast {
                latest_hit_idx.replace(curr_idx);
            }
        } else {
            latest_hit_idx.replace(curr_idx);
            break;
        }
    }

    if let Some(latest_choice_idx) = latest_hit_idx {
        if condition_order == BinaryRangeSearchType::AtLeast && latest_choice_idx > 0 {
            if let Some(new_idx) = linear_search_same_timestamp(
                datas,
                latest_choice_idx - 1,
                |data| match cond(data) {
                    Ordering::Equal | Ordering::Greater => true,
                    _ => false,
                },
                LinearSearchDirection::Desc,
            ) {
                latest_hit_idx.replace(new_idx);
            }
        } else if condition_order == BinaryRangeSearchType::AtMost
            && latest_choice_idx < datas.len()
        {
            if let Some(new_idx) = linear_search_same_timestamp(
                datas,
                latest_choice_idx + 1,
                |data| match cond(data) {
                    Ordering::Equal | Ordering::Less => true,
                    _ => false,
                },
                LinearSearchDirection::Asc,
            ) {
                latest_hit_idx.replace(new_idx);
            }
        }
    }

    latest_hit_idx
}

pub fn binary_search_range_by<T, F1, F2>(
    datas: &[T],
    condition_at_least: Option<F1>,
    condition_at_most: Option<F2>,
) -> Option<&[T]>
where
    F1: Fn(&T) -> Ordering,
    F2: Fn(&T) -> Ordering,
{
    let start_idx = if let Some(condition_at_least) = condition_at_least {
        match binary_search_by(datas, condition_at_least, BinaryRangeSearchType::AtLeast) {
            Some(idx) => idx,
            None => return None,
        }
    } else {
        0
    };

    let end_idx = if let Some(condition_at_most) = condition_at_most {
        match binary_search_by(datas, condition_at_most, BinaryRangeSearchType::AtMost) {
            Some(idx) => idx,
            None => return None,
        }
    } else {
        datas.len() - 1
    };

    Some(&datas[start_idx..end_idx + 1])
}

#[derive(Eq, PartialEq)]
pub(crate) enum LinearSearchDirection {
    Asc,
    Desc,
}

pub(crate) fn linear_search_same_timestamp<F, T>(
    datas: &[T],
    start_idx: usize,
    cond: F,
    search_direction: LinearSearchDirection,
) -> Option<usize>
where
    F: Fn(&T) -> bool,
{
    if start_idx >= datas.len() {
        return None;
    }

    let indices: Vec<usize> = if search_direction == LinearSearchDirection::Asc {
        (start_idx..datas.len()).collect()
    } else {
        (0..(start_idx + 1)).rev().collect()
    };
    let mut latest_found_idx = None;

    for idx in indices {
        let curr_val = unsafe { datas.get_unchecked(idx) };
        if cond(curr_val) {
            latest_found_idx.replace(idx);
        } else {
            break;
        }
    }
    latest_found_idx
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::tsdb::*;

    macro_rules! empty_data_points {
        ($($timestamp:expr),*) => {
            vec![
            $(DataPoint::new(TimestampNano::new($timestamp),vec![])),*
            ]
        };
    }

    #[test]
    fn binsearch_test_1() {
        let data_points: Vec<DataPoint> = empty_data_points!(10, 20, 20, 20, 30);

        let result = binary_search_by(
            &data_points,
            |datapoint| datapoint.timestamp_nano.cmp(&20),
            BinaryRangeSearchType::AtLeast,
        );
        assert!(result.is_some());
        let result = result.unwrap();
        assert_eq!(result, 1);
    }

    #[test]
    fn binsearch_test_2() {
        let data_points: Vec<DataPoint> = empty_data_points!(10, 20, 20, 20, 30);

        let result = binary_search_by(
            &data_points,
            |datapoint| datapoint.timestamp_nano.cmp(&20),
            BinaryRangeSearchType::AtMost,
        );
        assert!(result.is_some());
        let result = result.unwrap();
        assert_eq!(result, 3);
    }

    #[test]
    fn binsearch_test_3() {
        let data_points: Vec<DataPoint> = empty_data_points!(10, 12, 12, 21, 40);

        let result = binary_search_by(
            &data_points,
            |datapoint| datapoint.timestamp_nano.cmp(&20),
            BinaryRangeSearchType::AtLeast,
        );
        assert!(result.is_some());
        let result = result.unwrap();
        assert_eq!(result, 3);
    }

    #[test]
    fn binsearch_test_4() {
        let data_points: Vec<DataPoint> = empty_data_points!(10, 12, 12, 21, 40);

        let result = binary_search_by(
            &data_points,
            |datapoint| datapoint.timestamp_nano.cmp(&20),
            BinaryRangeSearchType::AtMost,
        );
        assert!(result.is_some());
        let result = result.unwrap();
        assert_eq!(result, 2);
    }

    #[test]
    fn binsearch_test_5() {
        let data_points: Vec<DataPoint> = empty_data_points!(10, 12, 12, 21, 40);

        let result = binary_search_by(
            &data_points,
            |datapoint| datapoint.timestamp_nano.cmp(&1),
            BinaryRangeSearchType::AtMost,
        );
        assert!(result.is_none());
    }

    #[test]
    fn binsearch_test_6() {
        let data_points: Vec<DataPoint> = empty_data_points!(10, 12, 12, 21, 40);

        let result = binary_search_by(
            &data_points,
            |datapoint| datapoint.timestamp_nano.cmp(&41),
            BinaryRangeSearchType::AtLeast,
        );
        assert!(result.is_none());
    }

    #[test]
    fn binsearch_test_7() {
        let data_points: Vec<DataPoint> = empty_data_points!(10, 12, 12, 13, 21, 40);

        let result = binary_search_by(
            &data_points,
            |datapoint| datapoint.timestamp_nano.cmp(&13),
            BinaryRangeSearchType::AtLeast,
        );
        assert!(result.is_some());
        let result = result.unwrap();
        assert_eq!(result, 3);
    }

    #[test]
    fn binsearch_test_8() {
        let data_points: Vec<DataPoint> = empty_data_points!(10, 12, 12, 13, 13, 13, 21, 40);

        let result = binary_search_by(
            &data_points,
            |datapoint| datapoint.timestamp_nano.cmp(&13),
            BinaryRangeSearchType::AtMost,
        );
        assert!(result.is_some());
        let result = result.unwrap();
        assert_eq!(result, 5);
    }

    #[test]
    fn linear_search_same_timestamp_1() {
        let datapoints: Vec<DataPoint> = empty_data_points!(10, 20, 20, 20, 30);

        {
            let result = linear_search_same_timestamp(
                &datapoints,
                2,
                |datapoint| datapoint.timestamp_nano == TimestampNano::new(20),
                LinearSearchDirection::Asc,
            );
            assert!(result.is_some());
            let result = result.unwrap();

            assert_eq!(result, 3);
        }

        {
            let result = linear_search_same_timestamp(
                &datapoints,
                2,
                |datapoint| datapoint.timestamp_nano == TimestampNano::new(20),
                LinearSearchDirection::Desc,
            );
            assert!(result.is_some());
            let result = result.unwrap();

            assert_eq!(result, 1);
        }
    }

    #[test]
    fn linear_search_same_timestamp_2() {
        let datapoints: Vec<DataPoint> = empty_data_points!(10, 20, 20, 20, 30);

        {
            let result = linear_search_same_timestamp(
                &datapoints,
                2,
                |datapoint| datapoint.timestamp_nano == TimestampNano::new(19),
                LinearSearchDirection::Asc,
            );
            assert!(result.is_none());
        }

        {
            let result = linear_search_same_timestamp(
                &datapoints,
                2,
                |datapoint| datapoint.timestamp_nano == TimestampNano::new(19),
                LinearSearchDirection::Desc,
            );
            assert!(result.is_none());
        }
    }

    #[test]
    fn linear_search_same_timestamp_3() {
        let datapoints: Vec<DataPoint> = empty_data_points!(10, 20, 20, 20, 30, 30);

        {
            let result = linear_search_same_timestamp(
                &datapoints,
                datapoints.len() - 1,
                |datapoint| datapoint.timestamp_nano == TimestampNano::new(30),
                LinearSearchDirection::Asc,
            );
            assert!(result.is_some());
            assert_eq!(result.unwrap(), datapoints.len() - 1);
        }

        {
            let result = linear_search_same_timestamp(
                &datapoints,
                datapoints.len() - 1,
                |datapoint| datapoint.timestamp_nano == TimestampNano::new(30),
                LinearSearchDirection::Desc,
            );
            assert!(result.is_some());
            assert_eq!(result.unwrap(), datapoints.len() - 2);
        }

        {
            let result = linear_search_same_timestamp(
                &datapoints,
                datapoints.len(),
                |datapoint| datapoint.timestamp_nano == TimestampNano::new(30),
                LinearSearchDirection::Asc,
            );
            assert!(result.is_none());
        }

        {
            let result = linear_search_same_timestamp(
                &datapoints,
                datapoints.len(),
                |datapoint| datapoint.timestamp_nano == TimestampNano::new(30),
                LinearSearchDirection::Desc,
            );
            assert!(result.is_none());
        }
    }
}
