use super::linear::*;
use std::cmp::Ordering;

#[derive(Eq, PartialEq)]
pub enum BinaryRangeSearchType {
    AtLeastInclusive,
    AtLeastExclusive,
    AtMostInclusive,
    AtMostExclusive,
}

/// - search at most inclusive 3 from [4,5,6,10] => None
/// - search at most inclusive 5 from [4,5,6,10] => 5
/// - search at most inclusive 6 from [4,5,6,10] => 6
///
/// - search at most exclusive 4 from [4,5,6,10] => None
/// - search at most exclusive 5 from [4,5,6,10] => 4
/// - search at most exclusive 6 from [4,5,6,10] => 5
///
/// - search at least inclusive 3 from [4,6,10] => 4
/// - search at least inclusive 5 from [4,6,10] => 6
/// - search at least inclusive 5 from [4,5,6,10] => 5
/// - search at least inclusive 11 from [4,5,6,10] => None
///
/// - search at least exclusive 3 from [4,6,10] => 4
/// - search at least exclusive 5 from [4,6,10] => 6
/// - search at least exclusive 5 from [4,5,6,10] => 6
/// - search at least exclusive 11 from [4,5,6,10] => None
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
        let cmp_current_value_is = cond(curr_val);

        if cmp_current_value_is == Ordering::Less {
            left = curr_idx + 1;
            if condition_order == BinaryRangeSearchType::AtMostInclusive
                || condition_order == BinaryRangeSearchType::AtMostExclusive
            {
                latest_hit_idx.replace(curr_idx);
            }
        } else if cmp_current_value_is == Ordering::Greater {
            // means curr_val is Greater than condition value
            // | 1,2,3,4,5,6 |
            //   l   ^     r
            //
            //  to below in next loop
            //
            // | 1,2,3,4,5,6 |
            //   l ^ r
            //
            right = curr_idx;
            if condition_order == BinaryRangeSearchType::AtLeastInclusive
                || condition_order == BinaryRangeSearchType::AtLeastExclusive
            {
                latest_hit_idx.replace(curr_idx);
            }
        } else {
            latest_hit_idx.replace(curr_idx);
            break;
        }
    }
    // latest_hit_idx supposed tobe
    // nearest value greater than, or equal to the base value when the ccondition is AtLeast{Inclusive|Exclusive}
    // nearest value smaller than, or equal to the base value when the ccondition is AtMost{Inclusive|Exclusive}

    if let Some(latest_choice_idx) = latest_hit_idx {
        if condition_order == BinaryRangeSearchType::AtLeastInclusive && latest_choice_idx > 0 {
            // finding
            // [1,2,3,3,3,4,5,5,5,6]
            //      ^
            // from
            // [1,2,3,3,3,4,5,5,5,6]
            //          ^
            if let Some(new_idx) = linear_search_last_index_which_match_rule(
                datas,
                latest_choice_idx - 1,
                |data| matches!(cond(data), Ordering::Equal | Ordering::Greater),
                LinearSearchDirection::Desc,
            ) {
                latest_hit_idx.replace(new_idx);
            }
        } else if condition_order == BinaryRangeSearchType::AtLeastExclusive {
            // finding
            // [1,2,3,3,3,4,5,5,5,6]
            //            ^
            // from
            // [1,2,3,3,3,4,5,5,5,6]
            //        ^
            // return none if search at least excusive 4 from
            // [1,2,3,3,3,4]
            //            ^
            match linear_search_first_index_which_match_rule(
                datas,
                latest_choice_idx,
                |data| matches!(cond(data), Ordering::Greater),
                LinearSearchDirection::Asc,
            ) {
                Some(new_idx) => {
                    latest_hit_idx.replace(new_idx);
                }
                None => {
                    latest_hit_idx = None;
                }
            }
        } else if condition_order == BinaryRangeSearchType::AtMostInclusive
            && latest_choice_idx < datas.len()
        {
            // finding
            // [1,2,3,3,3,4,5,5,5,6]
            //          ^
            // from
            // [1,2,3,3,3,4,5,5,5,6]
            //        ^
            //
            if let Some(new_idx) = linear_search_last_index_which_match_rule(
                datas,
                latest_choice_idx + 1,
                |data| matches!(cond(data), Ordering::Equal | Ordering::Less),
                LinearSearchDirection::Asc,
            ) {
                latest_hit_idx.replace(new_idx);
            }
        } else if condition_order == BinaryRangeSearchType::AtMostExclusive {
            // finding
            // [1,2,2,3,3,4,5,5,5,6]
            //      ^
            // from
            // [1,2,2,3,3,4,5,5,5,6]
            //        ^
            // return none if search at most excusive 2 from
            // [2,3,3,3,4]
            //  ^
            match linear_search_first_index_which_match_rule(
                datas,
                latest_choice_idx,
                |data| matches!(cond(data), Ordering::Less),
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
    result.map(|(datapoints, _indices)| datapoints)
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
        match binary_search_by(
            datas,
            condition_at_least,
            BinaryRangeSearchType::AtLeastInclusive,
        ) {
            Some(idx) => idx,
            None => return None,
        }
    } else {
        0
    };

    let last_index = datas.len() - 1;
    let end_idx = if let Some(condition_at_most) = condition_at_most_exclusive {
        match binary_search_by(
            datas,
            condition_at_most,
            BinaryRangeSearchType::AtMostExclusive,
        ) {
            Some(idx) => idx,
            None => return None,
        }
    } else {
        last_index
    };

    Some((&datas[start_idx..=end_idx], (start_idx, end_idx)))
}

#[cfg(test)]
mod test {
    use super::*;
    use zikeiretsu::*;

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
            |datapoint| datapoint.timestamp_nano.cmp(&ts!(20)),
            BinaryRangeSearchType::AtLeastInclusive,
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
            |datapoint| datapoint.timestamp_nano.cmp(&ts!(20)),
            BinaryRangeSearchType::AtMostInclusive,
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
            |datapoint| datapoint.timestamp_nano.cmp(&ts!(20)),
            BinaryRangeSearchType::AtLeastInclusive,
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
            |datapoint| datapoint.timestamp_nano.cmp(&ts!(20)),
            BinaryRangeSearchType::AtMostInclusive,
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
            |datapoint| datapoint.timestamp_nano.cmp(&ts!(1)),
            BinaryRangeSearchType::AtMostInclusive,
        );
        assert!(result.is_none());
    }

    #[test]
    fn binsearch_test_6() {
        let data_points: Vec<DataPoint> = empty_data_points!(10, 12, 12, 21, 40);

        let result = binary_search_by(
            &data_points,
            |datapoint| datapoint.timestamp_nano.cmp(&ts!(41)),
            BinaryRangeSearchType::AtLeastInclusive,
        );
        assert!(result.is_none());
    }

    #[test]
    fn binsearch_test_7() {
        let data_points: Vec<DataPoint> = empty_data_points!(10, 12, 12, 13, 21, 40);

        let result = binary_search_by(
            &data_points,
            |datapoint| datapoint.timestamp_nano.cmp(&ts!(13)),
            BinaryRangeSearchType::AtLeastInclusive,
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
            |datapoint| datapoint.timestamp_nano.cmp(&ts!(13)),
            BinaryRangeSearchType::AtMostInclusive,
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
            |datapoint| datapoint.timestamp_nano.cmp(&ts!(13)),
            BinaryRangeSearchType::AtMostExclusive,
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
            |datapoint| datapoint.timestamp_nano.cmp(&ts!(10)),
            BinaryRangeSearchType::AtMostExclusive,
        );
        assert!(result.is_none());
    }

    #[test]
    fn binsearch_test_11() {
        let data_points: Vec<DataPoint> = empty_data_points!(10, 11, 12, 12, 13, 13, 13, 21, 40);

        let result = binary_search_by(
            &data_points,
            |datapoint| datapoint.timestamp_nano.cmp(&ts!(11)),
            BinaryRangeSearchType::AtMostExclusive,
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
            |datapoint| datapoint.timestamp_nano.cmp(&ts!(41)),
            BinaryRangeSearchType::AtMostExclusive,
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
            |datapoint| datapoint.timestamp_nano.cmp(&ts!(5)),
            BinaryRangeSearchType::AtLeastInclusive,
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
            |datapoint| datapoint.timestamp_nano.cmp(&ts!(5)),
            BinaryRangeSearchType::AtLeastInclusive,
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
            |datapoint| datapoint.timestamp_nano.cmp(&ts!(3)),
            BinaryRangeSearchType::AtLeastInclusive,
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
            |datapoint| datapoint.timestamp_nano.cmp(&ts!(11)),
            BinaryRangeSearchType::AtLeastInclusive,
        );
        assert!(result.is_none());
    }

    #[test]
    fn binsearch_test_17() {
        let data_points: Vec<DataPoint> = empty_data_points!(2, 3, 4, 5, 6, 7, 8, 10);

        let result = binary_search_by(
            &data_points,
            |datapoint| datapoint.timestamp_nano.cmp(&ts!(3)),
            BinaryRangeSearchType::AtMostExclusive,
        );
        assert!(result.is_some());
        let result = result.unwrap();
        assert_eq!(result, 0);
    }

    #[test]
    fn binsearch_test_18() {
        let data_points: Vec<DataPoint> = empty_data_points!(2, 3, 4, 5, 6, 7, 8, 10);

        let result = binary_search_by(
            &data_points,
            |datapoint| datapoint.timestamp_nano.cmp(&ts!(3)),
            BinaryRangeSearchType::AtLeastExclusive,
        );
        assert!(result.is_some());
        let result = result.unwrap();
        assert_eq!(result, 2);
    }

    #[test]
    fn binsearch_test_19() {
        let data_points: Vec<DataPoint> = empty_data_points!(2, 3, 4, 5, 6, 7, 8, 10);

        let result = binary_search_by(
            &data_points,
            |datapoint| datapoint.timestamp_nano.cmp(&ts!(10)),
            BinaryRangeSearchType::AtLeastExclusive,
        );
        assert!(result.is_none());
    }

    #[test]
    fn binsearch_test_20() {
        let data_points: Vec<DataPoint> = empty_data_points!(2, 3, 4, 5, 6, 7, 8, 10);

        let result = binary_search_by(
            &data_points,
            |datapoint| datapoint.timestamp_nano.cmp(&ts!(1)),
            BinaryRangeSearchType::AtLeastExclusive,
        );
        assert!(result.is_some());
        let result = result.unwrap();
        assert_eq!(result, 0);
    }

    #[test]
    fn binsearch_test_21() {
        let data_points: Vec<DataPoint> = empty_data_points!(2, 3, 4, 5, 6, 7, 8, 10);

        let result = binary_search_by(
            &data_points,
            |datapoint| datapoint.timestamp_nano.cmp(&ts!(2)),
            BinaryRangeSearchType::AtLeastExclusive,
        );
        assert!(result.is_some());
        let result = result.unwrap();
        assert_eq!(result, 1);
    }

    #[test]
    fn binary_search_range_1() {
        let since_inclusive_cond = Some(|ts: &TimestampNano| ts.cmp(&ts!(20)));
        let until_exclusive_cond = Some(|ts: &TimestampNano| ts.cmp(&ts!(40)));

        let datapoints = tss!(10, 20, 20, 20, 30, 30, 40);
        let result =
            binary_search_range_by(&datapoints, since_inclusive_cond, until_exclusive_cond);

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
        let result =
            binary_search_range_by(&datapoints, since_inclusive_cond, until_exclusive_cond);

        assert!(result.is_some());
        let result = result.unwrap();

        assert_eq!(result, tss!(30, 30, 40));
    }

    #[test]
    fn binary_search_range_4() {
        let datapoints = tss!(10, 20, 20, 20, 30, 30, 40);
        let since_inclusive_cond = Some(|ts: &TimestampNano| ts.cmp(&ts!(0)));
        let until_exclusive_cond = Some(|ts: &TimestampNano| ts.cmp(&ts!(20)));
        let result =
            binary_search_range_by(&datapoints, since_inclusive_cond, until_exclusive_cond);

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
        let result =
            binary_search_range_by(&datapoints, since_inclusive_cond, until_exclusive_cond);

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
        let result =
            binary_search_range_by(&datapoints, since_inclusive_cond, until_exclusive_cond);

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
        let result =
            binary_search_range_by(&datapoints, since_inclusive_cond, until_exclusive_cond);

        assert!(result.is_some());
        let result = result.unwrap();

        assert_eq!(result, tss!(2));
    }
}
