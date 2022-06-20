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

// return n values  from left or right, with same values count as one.
// return the index which matches slice literal
//
//  when search_direction: LinearSearchDirection::Asc
//  retain target values by [..found_idx]
//
//  when search_direction: LinearSearchDirection::Desc
//  retain target values by [found_idx..]
pub fn linear_search_grouped_n_datas<T>(
    datas: &[T],
    limit: usize,
    search_direction: LinearSearchDirection,
) -> Option<usize>
where
    T: PartialEq + std::fmt::Debug,
{
    if limit == 0 {
        None
    } else {
        let mut counter: usize = 0;
        let found_intermediate_index = linear_search_by_condition(
            datas,
            &mut counter,
            |prev, current, each_counter| {
                println!(
                    "=== prev:{:?} current:{:?} each_counter{:?}",
                    prev, current, each_counter
                );
                match prev {
                    None => *each_counter = 1,
                    Some(prev) => {
                        if prev != current {
                            *each_counter += 1
                        }
                    }
                }
                println!("--- after each counter: {}", *each_counter);
                //  use '>' rather than '>=' to count through same values at the tail
                *each_counter > limit
            },
            &search_direction,
        );
        match found_intermediate_index {
            Some(idx) => {
                debug_assert!(idx <= datas.len());
                if search_direction == LinearSearchDirection::Asc {
                    Some(idx)
                } else {
                    Some(idx + 1)
                }
            }
            None => {
                if search_direction == LinearSearchDirection::Asc {
                    Some(datas.len())
                } else {
                    Some(0)
                }
            }
        }
    }
}

pub fn linear_search_by_condition<F, T, A>(
    datas: &[T],
    accumulate: &mut A,
    cond: F,
    search_direction: &LinearSearchDirection,
) -> Option<usize>
where
    F: Fn(Option<&T>, &T, &mut A) -> bool,
{
    let indices: Vec<usize> = if *search_direction == LinearSearchDirection::Asc {
        (0..datas.len()).collect()
    } else {
        (0..=datas.len() - 1).rev().collect()
    };

    let mut prev = None;
    for idx in indices {
        let curr_val = unsafe { datas.get_unchecked(idx) };

        if cond(prev, curr_val, accumulate) {
            return Some(idx);
        }

        prev.replace(unsafe { datas.get_unchecked(idx) });
    }
    None
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::tsdb::*;
    use std::cmp::Ordering;

    macro_rules! empty_data_points {
        ($($timestamp:expr),*) => {
            vec![
            $(DataPoint::new(TimestampNano::new($timestamp),vec![])),*
            ]
        };
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
    fn test_linear_search_grouped_n_datas_1_asc() {
        let datapoints: Vec<DataPoint> = empty_data_points!(10, 20, 20, 20, 30, 30);

        {
            let result = linear_search_grouped_n_datas(&datapoints, 1, LinearSearchDirection::Asc);
            assert_eq!(result, Some(1))
        }

        {
            let result = linear_search_grouped_n_datas(&datapoints, 2, LinearSearchDirection::Asc);
            assert_eq!(result, Some(4))
        }

        {
            let result = linear_search_grouped_n_datas(&datapoints, 3, LinearSearchDirection::Asc);
            assert_eq!(result, Some(datapoints.len()))
        }

        {
            let result = linear_search_grouped_n_datas(&datapoints, 4, LinearSearchDirection::Asc);
            assert_eq!(result, Some(datapoints.len()))
        }
    }

    #[test]
    fn test_linear_search_grouped_n_datas_2_desc() {
        let datapoints: Vec<DataPoint> = empty_data_points!(10, 20, 20, 20, 30, 30);

        {
            let result = linear_search_grouped_n_datas(&datapoints, 1, LinearSearchDirection::Desc);
            assert_eq!(result, Some(4))
        }

        {
            let result = linear_search_grouped_n_datas(&datapoints, 2, LinearSearchDirection::Desc);
            assert_eq!(result, Some(1))
        }

        {
            let result = linear_search_grouped_n_datas(&datapoints, 3, LinearSearchDirection::Desc);
            assert_eq!(result, Some(0))
        }

        {
            let result = linear_search_grouped_n_datas(&datapoints, 4, LinearSearchDirection::Desc);
            assert_eq!(result, Some(0))
        }

        {
            let result = linear_search_grouped_n_datas(&datapoints, 1, LinearSearchDirection::Desc);
            assert_eq!(result, Some(4))
        }
    }

    #[test]
    fn test_linear_search_grouped_n_datas_3_asc() {
        let datapoints: Vec<DataPoint> = empty_data_points!(10, 10, 20, 20, 20, 30, 30);

        {
            let result = linear_search_grouped_n_datas(&datapoints, 0, LinearSearchDirection::Asc);
            assert_eq!(result, None)
        }

        {
            let result = linear_search_grouped_n_datas(&datapoints, 1, LinearSearchDirection::Asc);
            assert_eq!(result, Some(2))
        }
    }
}
