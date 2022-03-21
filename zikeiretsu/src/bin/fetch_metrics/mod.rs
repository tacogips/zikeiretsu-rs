use super::{operation::*, Result};

use ::zikeiretsu::*;

pub async fn execute(fetch_metrics: FetchMetricsCondition) -> Result<()> {
    let store = Zikeiretsu::readonly_store(
        &fetch_metrics.db_dir,
        fetch_metrics.metrics.as_str(),
        None,
        &fetch_metrics.condition,
        &fetch_metrics.setting,
    )
    .await?;

    match fetch_metrics.output_setting.format {
        output::OutputFormat::Json => {
            let json_str = serde_json::to_string(&store.all_datapoints())
                .map_err(|e| output::OutputError::SerdeJsonError(e))?;
            fetch_metrics
                .output_setting
                .destination
                .write(vec![json_str])?
        }
        output::OutputFormat::Tsv => fetch_metrics.output_setting.destination.write(
            store
                .all_datapoints()
                .iter()
                .map(|datapoint| datapoint_as_tsv(&datapoint)),
        )?,
    };
    Ok(())
}

//TODO(tacogip) To be more sophisticatged at output
fn datapoint_as_tsv(datapoint: &DataPoint) -> String {
    format!(
        "{timestamp_nano}\t{tsv_record}",
        timestamp_nano = datapoint.timestamp_nano,
        tsv_record = datapoint
            .field_values
            .iter()
            .map(|field_value| field_value.to_string())
            .collect::<Vec<String>>()
            .join("\t")
    )
}
