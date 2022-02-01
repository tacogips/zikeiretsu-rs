use super::output;
use crate::*;

pub struct FetchMetricsCondition {
    pub db_dir: String,
    pub metrics: String,
    pub condition: DatapointSearchCondition,
    pub setting: SearchSettings,
    pub output_setting: output::OutputSetting,
}
