use crate::tsdb::datapoint::*;

pub struct DatapointSearcher<'a> {
    datapoints: &'a [DataPoint],
}

impl<'a> DatapointSearcher<'a> {
    pub fn new(datapoints: &'a [DataPoint]) -> Self {
        Self { datapoints }
    }

    pub async fn search(&self, cond: &DatapointSearchCondition) -> Option<&[DataPoint]> {
        DataPoint::search(self.datapoints, cond).await
    }

    pub async fn search_with_indices(
        &self,
        cond: DatapointSearchCondition,
    ) -> Option<(&[DataPoint], (usize, usize))> {
        DataPoint::search_with_indices(self.datapoints, &cond).await
    }
}

#[cfg(test)]
mod test {

    use super::*;
    use crate::tsdb::datapoint::DataPoint;
    use crate::tsdb::timestamp_nano::*;

    macro_rules! empty_data_points {
        ($($timestamp:expr),*) => {
            vec![
            $(DataPoint::new(TimestampNano::new($timestamp),vec![])),*
            ]
        };
    }

    macro_rules! ts {
        ($v:expr) => {
            TimestampNano::new($v)
        };
    }

    #[tokio::test]
    async fn binsearch_test_1() {
        let datas = empty_data_points!(9, 10, 19, 20, 20, 20, 30, 40, 50, 50, 51);
        let store = DatapointSearcher::new(&datas);
        let condition = DatapointSearchCondition::since(ts!(20)).with_until(ts!(51));
        let result = store.search(&condition).await;
        assert!(result.is_some());
        let result = result.unwrap();

        assert_eq!(result, empty_data_points!(20, 20, 20, 30, 40, 50, 50));
    }

    #[tokio::test]
    async fn binsearch_test_2() {
        let datas = empty_data_points!(9, 10, 19, 20, 20, 20, 30, 40, 50, 50, 51);
        let store = DatapointSearcher::new(&datas);

        let condition = DatapointSearchCondition::since(ts!(20));
        let result = store.search(&condition).await;
        assert!(result.is_some());
        let result = result.unwrap();

        assert_eq!(result, empty_data_points!(20, 20, 20, 30, 40, 50, 50, 51));
    }

    #[tokio::test]
    async fn binsearch_test_3() {
        let datas = empty_data_points!(9, 10, 19, 20, 20, 20, 30, 40, 50, 50, 51);
        let store = DatapointSearcher::new(&datas);

        let condition = DatapointSearchCondition::until(ts!(41));
        let result = store.search(&condition).await;
        assert!(result.is_some());
        let result = result.unwrap();

        assert_eq!(result, empty_data_points!(9, 10, 19, 20, 20, 20, 30, 40));
    }
}
