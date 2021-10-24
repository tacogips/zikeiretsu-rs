mod cloud;
pub(crate) mod output;
use ::zikeiretsu::*;

pub enum Operation {
    ListMetrics(ListMetricsCondition),
    FetchMetics(FetchMetricsCondition),
    Describe(DescribeDatabaseCondition),
}

pub struct ListMetricsCondition {
    pub db_dir: Option<String>,
    pub setting: SearchSettings,
    pub output_setting: output::OutputSetting,
}

pub struct FetchMetricsCondition {
    pub db_dir: String,
    pub metrics: String,
    pub condition: DatapointSearchCondition,
    pub setting: SearchSettings,
    pub output_setting: output::OutputSetting,
}

pub struct DescribeDatabaseCondition {
    pub db_dir: String,
    pub setting: SearchSettings,
    pub output_setting: output::OutputSetting,
}
