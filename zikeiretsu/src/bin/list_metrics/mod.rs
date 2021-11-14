use super::{
    operation::{output, ListMetricsCondition},
    Result,
};
use serde_json;

use ::zikeiretsu::*;

pub async fn execute(list_metrics: ListMetricsCondition) -> Result<()> {
    let metrics =
        Zikeiretsu::list_metrics(list_metrics.db_dir.as_ref(), &list_metrics.setting).await?;

    match list_metrics.output_setting.format {
        output::OutputFormat::Json => {
            let json_str = serde_json::to_string(&metrics)
                .map_err(|e| output::OutputError::SerdeJsonError(e))?;
            list_metrics
                .output_setting
                .destination
                .write(vec![json_str])?
        }
        output::OutputFormat::Tsv => list_metrics
            .output_setting
            .destination
            .write(metrics.into_iter().map(|e| e.to_string()))?,
    };
    Ok(())
}
