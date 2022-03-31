use super::field::*;
use crate::tsdb::datetime::*;
use async_trait::async_trait;
use chrono::FixedOffset;
use polars::prelude::{DataFrame as PDataFrame, Series as PSeries, *};
use serde::Serialize;

#[derive(Debug, PartialEq, Clone, Serialize)]
pub enum SeriesValuesRef<'a> {
    Vacant(usize),
    Float64(&'a [f64]),
    Bool(&'a [bool]),
    String(&'a [String]),
    TimestampNano(&'a [TimestampNano]),
}

#[derive(Debug, PartialEq, Clone, Serialize)]
pub struct DataSeriesRef<'a> {
    pub values: SeriesValuesRef<'a>,
}

impl<'a> DataSeriesRef<'a> {
    pub fn new(values: SeriesValuesRef<'a>) -> Self {
        Self { values }
    }

    pub fn get(&self, index: usize) -> Option<FieldValue> {
        match &self.values {
            SeriesValuesRef::Float64(vs) => vs.get(index).map(|v| FieldValue::Float64(*v)),
            SeriesValuesRef::Bool(vs) => vs.get(index).map(|v| FieldValue::Bool(*v)),
            _ => None,
        }
    }

    pub async fn as_polar_series(&self, field_name: &str, tz: Option<&FixedOffset>) -> PSeries {
        match &self.values {
            SeriesValuesRef::Float64(vs) => PSeries::new(field_name, vs),
            SeriesValuesRef::Bool(vs) => PSeries::new(field_name, vs),
            SeriesValuesRef::Vacant(_) => PSeries::new_empty(field_name, &DataType::Null),
            SeriesValuesRef::String(vs) => PSeries::new(field_name, vs),
            SeriesValuesRef::TimestampNano(timestamps) => PSeries::new(
                field_name,
                timestamps
                    .into_iter()
                    .map(|ts| ts.as_formated_datetime(tz))
                    .collect::<Vec<String>>(),
            ),
        }
    }
}

#[async_trait]
pub trait DataSeriesRefs {
    fn as_data_serieses_ref_vec<'a>(&'a self) -> Vec<DataSeriesRef<'a>>;

    async fn as_polar_dataframes(
        &self,
        column_names: Option<&[&str]>,
        timezone: Option<&FixedOffset>,
    ) -> Result<PDataFrame> {
        unimplemented!()
        //let field_names: Vec<String> = match column_names {
        //    Some(column_names) => {
        //        if data_frame.fields_len() != column_names.len() {
        //            return Err(DataframeError::UnmatchedColumnNameNumber(
        //                data_frame.fields_len(),
        //                column_names.len(),
        //            ));
        //        }
        //        column_names.into_iter().map(|s| s.to_string()).collect()
        //    }
        //    None => (0..data_frame.fields_len())
        //        .into_iter()
        //        .map(|e| e.to_string())
        //        .collect(),
        //};

        //let serieses = field_names.iter().zip(data_frame.data_serieses.iter()).map(
        //    |(field_name, each_series)| zdata_series_to_series(field_name, &each_series, timezone),
        //);

        //let serieses: Vec<PSeries> = join_all(serieses)
        //    .await
        //    .into_iter()
        //    .collect::<DataFrameResult<Vec<PSeries>>>()?;
        //Ok(PDataFrame::new(serieses)?)
    }
}

impl<'a> DataSeriesRefs for Vec<&'a Vec<String>> {
    fn as_data_serieses_ref_vec(&self) -> Vec<DataSeriesRef<'a>> {
        let vs: Vec<DataSeriesRef<'_>> = self
            .iter()
            .map(|strs| DataSeriesRef::new(SeriesValuesRef::String(strs)))
            .collect();

        vs
    }
}

//pub async fn zdata_frame_to_dataframe(
//    data_frame: &ZDataFrame,
//    column_names: Option<&[&str]>,
//    timezone: &FixedOffset,
//) -> DataFrameResult<PDataFrame> {
//    let field_names: Vec<String> = match column_names {
//        Some(column_names) => {
//            if data_frame.fields_len() != column_names.len() {
//                return Err(DataframeError::UnmatchedColumnNameNumber(
//                    data_frame.fields_len(),
//                    column_names.len(),
//                ));
//            }
//            column_names.into_iter().map(|s| s.to_string()).collect()
//        }
//        None => (0..data_frame.fields_len())
//            .into_iter()
//            .map(|e| e.to_string())
//            .collect(),
//    };
//
//    let serieses =
//        field_names
//            .iter()
//            .zip(data_frame.data_serieses.iter())
//            .map(|(field_name, each_series)| {
//                zdata_series_to_series(field_name, &each_series, timezone)
//            });
//
//    let serieses: Vec<PSeries> = join_all(serieses)
//        .await
//        .into_iter()
//        .collect::<DataFrameResult<Vec<PSeries>>>()?;
//    Ok(PDataFrame::new(serieses)?)
//}
//
//pub async fn zdata_series_to_series(
//    field_name: &str,
//    series: &ZDataSeries,
//    tz: &FixedOffset,
//) -> DataFrameResult<PSeries> {
//    match &series.values {
//        SeriesValues::Float64(vs) => Ok(PSeries::new(field_name, vs)),
//        SeriesValues::Bool(vs) => Ok(PSeries::new(field_name, vs)),
//        SeriesValues::Vacant(_) => Ok(PSeries::new_empty(field_name, &DataType::Null)),
//        SeriesValues::String(vs) => Ok(PSeries::new(field_name, vs)),
//        SeriesValues::TimestampNano(timestamps) => Ok(PSeries::new(
//            field_name,
//            timestamps
//                .into_iter()
//                .map(|ts| ts.as_formated_datetime(tz))
//                .collect::<Vec<String>>(),
//        )),
//    }
//}