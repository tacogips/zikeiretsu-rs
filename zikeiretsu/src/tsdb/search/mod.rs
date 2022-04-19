use std::cmp::Ordering;

#[derive(Eq, PartialEq)]
pub enum BinaryRangeSearchType {
    AtLeastEq,
    AtMostEq,
    AtMostNeq,
}

/// - search at most eq 3 from [4,6,10] => None
/// - search at most eq 5 from [4,6,10] => 4
/// - search at most neq 4 from [4,5,6,10] => None
/// - search at most neq 6 from [4,5,6,10] => 5
/// - search at least eq 3 from [4,6,10] => 4
/// - search at least eq 5 from [4,6,10] => 6
/// - search at least eq 5 from [4,5,6,10] => 5
/// - search at least eq 11 from [4,5,6,10] => None
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
            if condition_order == BinaryRangeSearchType::AtMostEq
                || condition_order == BinaryRangeSearchType::AtMostNeq
            {
                latest_hit_idx.replace(curr_idx);
            }
        } else if cmp == Ordering::Greater {
            right = curr_idx;
            if condition_order == BinaryRangeSearchType::AtLeastEq {
                latest_hit_idx.replace(curr_idx);
            }
        } else {
            latest_hit_idx.replace(curr_idx);
            break;
        }
    }

    if let Some(latest_choice_idx) = latest_hit_idx {
        if condition_order == BinaryRangeSearchType::AtLeastEq && latest_choice_idx > 0 {
            if let Some(new_idx) = linear_search_last_index_which_match_rule(
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
        } else if condition_order == BinaryRangeSearchType::AtMostEq
            && latest_choice_idx < datas.len()
        {
            if let Some(new_idx) = linear_search_last_index_which_match_rule(
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
        } else if condition_order == BinaryRangeSearchType::AtMostNeq {
            match linear_search_first_index_which_match_rule(
                datas,
                latest_choice_idx,
                |data| match cond(data) {
                    Ordering::Less => true,
                    _ => false,
                },
                LinearSearchDirection::Desc,
            ) {
                Some(new_idx) => {
                    latest_hit_idx.replace(new_idx);
                }
                None => {
                    latest_hit_idx = None;
                }
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
    let result = binary_search_range_with_idx_by(datas, condition_at_least, condition_at_most);
    result.map(|(datapoints, _indicex)| datapoints)
}

pub fn binary_search_range_with_idx_by<T, F1, F2>(
    datas: &[T],
    condition_at_least_eq: Option<F1>,
    condition_at_most_exclusive: Option<F2>,
) -> Option<(&[T], (usize, usize))>
where
    F1: Fn(&T) -> Ordering,
    F2: Fn(&T) -> Ordering,
{
    let start_idx = if let Some(condition_at_least) = condition_at_least_eq {
        match binary_search_by(datas, condition_at_least, BinaryRangeSearchType::AtLeastEq) {
            Some(idx) => idx,
            None => return None,
        }
    } else {
        0
    };

    let last_index = datas.len() - 1;
    let end_idx = if let Some(condition_at_most) = condition_at_most_exclusive {
        match binary_search_by(datas, condition_at_most, BinaryRangeSearchType::AtMostNeq) {
            Some(idx) => idx,
            None => return None,
        }
    } else {
        last_index
    };

    Some((&datas[start_idx..=end_idx], (start_idx, end_idx)))
}

#[derive(Eq, PartialEq)]
pub enum LinearSearchDirection {
    Asc,
    Desc,
}

///search and return the index of element which match to condition.
pub fn linear_search_last_index_which_match_rule<F, T>(
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

pub fn linear_search_first_index_which_match_rule<F, T>(
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
        (0..=start_idx).rev().collect()
    };

    for idx in indices {
        let curr_val = unsafe { datas.get_unchecked(idx) };
        if cond(curr_val) {
            return Some(idx);
        }
    }
    None
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

    macro_rules! tss {
        ($($timestamp:expr),*) => {
            vec![
            $(TimestampNano::new($timestamp)),*
            ]
        };
    }

    macro_rules! ts {
        ($v:expr) => {
            TimestampNano::new($v)
        };
    }

    #[test]
    fn binsearch_test_1() {
        let data_points: Vec<DataPoint> = empty_data_points!(10, 20, 20, 20, 30);

        let result = binary_search_by(
            &data_points,
            |datapoint| datapoint.timestamp_nano.cmp(&20),
            BinaryRangeSearchType::AtLeastEq,
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
            BinaryRangeSearchType::AtMostEq,
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
            BinaryRangeSearchType::AtLeastEq,
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
            BinaryRangeSearchType::AtMostEq,
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
            BinaryRangeSearchType::AtMostEq,
        );
        assert!(result.is_none());
    }

    #[test]
    fn binsearch_test_6() {
        let data_points: Vec<DataPoint> = empty_data_points!(10, 12, 12, 21, 40);

        let result = binary_search_by(
            &data_points,
            |datapoint| datapoint.timestamp_nano.cmp(&41),
            BinaryRangeSearchType::AtLeastEq,
        );
        assert!(result.is_none());
    }

    #[test]
    fn binsearch_test_7() {
        let data_points: Vec<DataPoint> = empty_data_points!(10, 12, 12, 13, 21, 40);

        let result = binary_search_by(
            &data_points,
            |datapoint| datapoint.timestamp_nano.cmp(&13),
            BinaryRangeSearchType::AtLeastEq,
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
            BinaryRangeSearchType::AtMostEq,
        );
        assert!(result.is_some());
        let result = result.unwrap();
        assert_eq!(result, 5);
    }

    #[test]
    fn binsearch_test_9() {
        let data_points: Vec<DataPoint> = empty_data_points!(10, 12, 12, 13, 13, 13, 21, 40);

        let result = binary_search_by(
            &data_points,
            |datapoint| datapoint.timestamp_nano.cmp(&13),
            BinaryRangeSearchType::AtMostNeq,
        );
        assert!(result.is_some());
        let result = result.unwrap();
        assert_eq!(result, 2);
    }

    #[test]
    fn binsearch_test_10() {
        let data_points: Vec<DataPoint> = empty_data_points!(10, 12, 12, 13, 13, 13, 21, 40);

        let result = binary_search_by(
            &data_points,
            |datapoint| datapoint.timestamp_nano.cmp(&10),
            BinaryRangeSearchType::AtMostNeq,
        );
        assert!(result.is_none());
    }

    #[test]
    fn binsearch_test_11() {
        let data_points: Vec<DataPoint> = empty_data_points!(10, 11, 12, 12, 13, 13, 13, 21, 40);

        let result = binary_search_by(
            &data_points,
            |datapoint| datapoint.timestamp_nano.cmp(&11),
            BinaryRangeSearchType::AtMostNeq,
        );
        assert!(result.is_some());
        let result = result.unwrap();
        assert_eq!(result, 0);
    }

    #[test]
    fn binsearch_test_12() {
        let data_points: Vec<DataPoint> = empty_data_points!(10, 11, 12, 12, 13, 13, 13, 21, 40);

        let result = binary_search_by(
            &data_points,
            |datapoint| datapoint.timestamp_nano.cmp(&41),
            BinaryRangeSearchType::AtMostNeq,
        );
        assert!(result.is_some());
        let result = result.unwrap();
        assert_eq!(result, data_points.len() - 1);
    }

    #[test]
    fn binsearch_test_13() {
        let data_points: Vec<DataPoint> = empty_data_points!(4, 6, 10);

        let result = binary_search_by(
            &data_points,
            |datapoint| datapoint.timestamp_nano.cmp(&5),
            BinaryRangeSearchType::AtLeastEq,
        );
        assert!(result.is_some());
        let result = result.unwrap();
        assert_eq!(result, 1);
    }

    #[test]
    fn binsearch_test_14() {
        let data_points: Vec<DataPoint> = empty_data_points!(4, 5, 6, 10);

        let result = binary_search_by(
            &data_points,
            |datapoint| datapoint.timestamp_nano.cmp(&5),
            BinaryRangeSearchType::AtLeastEq,
        );
        assert!(result.is_some());
        let result = result.unwrap();
        assert_eq!(result, 1);
    }

    #[test]
    fn binsearch_test_15() {
        let data_points: Vec<DataPoint> = empty_data_points!(4, 6, 10);

        let result = binary_search_by(
            &data_points,
            |datapoint| datapoint.timestamp_nano.cmp(&3),
            BinaryRangeSearchType::AtLeastEq,
        );
        assert!(result.is_some());
        let result = result.unwrap();
        assert_eq!(result, 0);
    }

    #[test]
    fn binsearch_test_16() {
        let data_points: Vec<DataPoint> = empty_data_points!(4, 5, 6, 10);

        let result = binary_search_by(
            &data_points,
            |datapoint| datapoint.timestamp_nano.cmp(&11),
            BinaryRangeSearchType::AtLeastEq,
        );
        assert!(result.is_none());
    }

    #[test]
    fn binsearch_test_17() {
        let data_points: Vec<DataPoint> = empty_data_points!(2, 3, 4, 5, 6, 7, 8, 10);

        let result = binary_search_by(
            &data_points,
            |datapoint| datapoint.timestamp_nano.cmp(&3),
            BinaryRangeSearchType::AtMostNeq,
        );
        assert!(result.is_some());
        let result = result.unwrap();
        assert_eq!(result, 0);
    }

    #[test]
    fn linear_search_same_timestamp_1() {
        let datapoints: Vec<DataPoint> = empty_data_points!(10, 20, 20, 20, 30);

        {
            let result = linear_search_last_index_which_match_rule(
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
            let result = linear_search_last_index_which_match_rule(
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
            let result = linear_search_last_index_which_match_rule(
                &datapoints,
                2,
                |datapoint| datapoint.timestamp_nano == TimestampNano::new(19),
                LinearSearchDirection::Asc,
            );
            assert!(result.is_none());
        }

        {
            let result = linear_search_last_index_which_match_rule(
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
            let result = linear_search_last_index_which_match_rule(
                &datapoints,
                datapoints.len() - 1,
                |datapoint| datapoint.timestamp_nano == TimestampNano::new(30),
                LinearSearchDirection::Asc,
            );
            assert!(result.is_some());
            assert_eq!(result.unwrap(), datapoints.len() - 1);
        }

        {
            let result = linear_search_last_index_which_match_rule(
                &datapoints,
                datapoints.len() - 1,
                |datapoint| datapoint.timestamp_nano == TimestampNano::new(30),
                LinearSearchDirection::Desc,
            );
            assert!(result.is_some());
            assert_eq!(result.unwrap(), datapoints.len() - 2);
        }

        {
            let result = linear_search_last_index_which_match_rule(
                &datapoints,
                datapoints.len(),
                |datapoint| datapoint.timestamp_nano == TimestampNano::new(30),
                LinearSearchDirection::Asc,
            );
            assert!(result.is_none());
        }

        {
            let result = linear_search_last_index_which_match_rule(
                &datapoints,
                datapoints.len(),
                |datapoint| datapoint.timestamp_nano == TimestampNano::new(30),
                LinearSearchDirection::Desc,
            );
            assert!(result.is_none());
        }
    }

    #[test]
    fn linear_search_first_index_which_match_rule_timestamp_1() {
        let datapoints: Vec<DataPoint> = empty_data_points!(10, 20, 20, 20, 30, 30);

        {
            let result = linear_search_first_index_which_match_rule(
                &datapoints,
                datapoints.len() - 1,
                |datapoint| datapoint.timestamp_nano.cmp(&TimestampNano::new(30)) == Ordering::Less,
                LinearSearchDirection::Desc,
            );
            assert!(result.is_some());
            assert_eq!(result.unwrap(), 3);
        }

        {
            let result = linear_search_first_index_which_match_rule(
                &datapoints,
                datapoints.len() - 1,
                |datapoint| datapoint.timestamp_nano == TimestampNano::new(30),
                LinearSearchDirection::Desc,
            );
            assert!(result.is_some());
            assert_eq!(result.unwrap(), datapoints.len() - 1);
        }

        {
            let result = linear_search_first_index_which_match_rule(
                &datapoints,
                datapoints.len(),
                |datapoint| datapoint.timestamp_nano == TimestampNano::new(30),
                LinearSearchDirection::Asc,
            );
            assert!(result.is_none());
        }

        {
            let result = linear_search_first_index_which_match_rule(
                &datapoints,
                0,
                |datapoint| datapoint.timestamp_nano == TimestampNano::new(30),
                LinearSearchDirection::Desc,
            );
            assert!(result.is_none());
        }

        {
            let result = linear_search_first_index_which_match_rule(
                &datapoints,
                datapoints.len(),
                |datapoint| datapoint.timestamp_nano == TimestampNano::new(30),
                LinearSearchDirection::Desc,
            );
            assert!(result.is_none());
        }
    }

    #[test]
    fn binary_search_range_1() {
        let since_inclusive_cond = Some(|ts: &TimestampNano| ts.cmp(&ts!(20)));
        let until_exclusive_cond = Some(|ts: &TimestampNano| ts.cmp(&ts!(40)));

        let datapoints = tss!(10, 20, 20, 20, 30, 30, 40);
        let result = binary_search_range_by(&datapoints, since_inclusive_cond, until_exclusive_cond);

        assert!(result.is_some());
        let result = result.unwrap();

        assert_eq!(result, tss!(20, 20, 20, 30, 30));
    }

    #[test]
    fn binary_search_range_2() {
        let datapoints = tss!(10, 20, 20, 20, 30, 30, 40);
        let empty_cond1: Option<Box<dyn Fn(&TimestampNano) -> Ordering>> = None;
        let empty_cond2: Option<Box<dyn Fn(&TimestampNano) -> Ordering>> = None;
        let result = binary_search_range_by(&datapoints, empty_cond1, empty_cond2);

        assert!(result.is_some());
        let result = result.unwrap();

        assert_eq!(result, tss!(10, 20, 20, 20, 30, 30, 40));
    }

    #[test]
    fn binary_search_range_3() {
        let datapoints = tss!(10, 20, 20, 20, 30, 30, 40);
        let since_inclusive_cond = Some(|ts: &TimestampNano| ts.cmp(&ts!(30)));
        let until_exclusive_cond = Some(|ts: &TimestampNano| ts.cmp(&ts!(60)));
        let result = binary_search_range_by(&datapoints, since_inclusive_cond, until_exclusive_cond);

        assert!(result.is_some());
        let result = result.unwrap();

        assert_eq!(result, tss!(30, 30, 40));
    }

    #[test]
    fn binary_search_range_4() {
        let datapoints = tss!(10, 20, 20, 20, 30, 30, 40);
        let since_inclusive_cond = Some(|ts: &TimestampNano| ts.cmp(&ts!(0)));
        let until_exclusive_cond = Some(|ts: &TimestampNano| ts.cmp(&ts!(20)));
        let result = binary_search_range_by(&datapoints, since_inclusive_cond, until_exclusive_cond);

        assert!(result.is_some());
        let result = result.unwrap();

        assert_eq!(result, tss!(10));
    }

    #[test]
    fn binary_search_range_5() {
        let datapoints = tss!(10, 20, 20, 20, 30, 30, 40);
        let empty_cond1: Option<Box<dyn Fn(&TimestampNano) -> Ordering>> = None;
        let until_exclusive_cond = Some(|ts: &TimestampNano| ts.cmp(&ts!(30)));
        let result = binary_search_range_by(&datapoints, empty_cond1, until_exclusive_cond);

        assert!(result.is_some());
        let result = result.unwrap();

        assert_eq!(result, tss!(10, 20, 20, 20));
    }

    #[test]
    fn binary_search_range_6() {
        let datapoints = tss!(10, 20, 20, 20, 30, 30, 40);
        let since_inclusive_cond = Some(|ts: &TimestampNano| ts.cmp(&ts!(20)));
        let empty_cond2: Option<Box<dyn Fn(&TimestampNano) -> Ordering>> = None;
        let result = binary_search_range_by(&datapoints, since_inclusive_cond, empty_cond2);

        assert!(result.is_some());
        let result = result.unwrap();

        assert_eq!(result, tss!(20, 20, 20, 30, 30, 40));
    }

    #[test]
    fn binary_search_range_7() {
        let datapoints = tss!(10, 20, 20, 20, 30, 30, 40);
        let since_inclusive_cond = Some(|ts: &TimestampNano| ts.cmp(&ts!(2)));
        let until_exclusive_cond = Some(|ts: &TimestampNano| ts.cmp(&ts!(10)));
        let result = binary_search_range_by(&datapoints, since_inclusive_cond, until_exclusive_cond);

        assert!(result.is_none());
    }

    #[test]
    fn binary_search_range_8() {
        let datapoints = tss!(10, 20, 20, 20, 30, 30, 40);
        let empty_cond1: Option<Box<dyn Fn(&TimestampNano) -> Ordering>> = None;
        let until_exclusive_cond = Some(|ts: &TimestampNano| ts.cmp(&ts!(10)));
        let result = binary_search_range_by(&datapoints, empty_cond1, until_exclusive_cond);

        assert!(result.is_none());
    }

    #[test]
    fn binary_search_range_9() {
        let datapoints = tss!(10, 20, 20, 20, 30, 30, 40);
        let since_inclusive_cond = Some(|ts: &TimestampNano| ts.cmp(&ts!(41)));
        let until_exclusive_cond = Some(|ts: &TimestampNano| ts.cmp(&ts!(42)));
        let result = binary_search_range_by(&datapoints, since_inclusive_cond, until_exclusive_cond);

        assert!(result.is_none());
    }

    #[test]
    fn binary_search_range_10() {
        let datapoints = tss!(10, 20, 20, 20, 30, 30, 40);
        let since_inclusive_cond = Some(|ts: &TimestampNano| ts.cmp(&ts!(41)));
        let empty_cond1: Option<Box<dyn Fn(&TimestampNano) -> Ordering>> = None;
        let result = binary_search_range_by(&datapoints, since_inclusive_cond, empty_cond1);

        assert!(result.is_none());
    }

    #[test]
    fn binary_search_range_11() {
        let datapoints = tss!(2, 3, 4, 5, 6, 7, 8, 10);

        let since_inclusive_cond = Some(|ts: &TimestampNano| ts.cmp(&ts!(0)));
        let until_exclusive_cond = Some(|ts: &TimestampNano| ts.cmp(&ts!(3)));
        let result = binary_search_range_by(&datapoints, since_inclusive_cond, until_exclusive_cond);

        assert!(result.is_some());
        let result = result.unwrap();

        assert_eq!(result, tss!(2));
    }
}
