# Monitoring

Run the ingestion pipeline before using these notebooks. See the [Jupyter setup guide](../setup/README.md) to connect to the local environment.

| Notebook | Purpose |
| --- | --- |
| [Pipeline executions](inspect_pipeline_monitoring.ipynb) | Inspect execution metadata and recent runs. |
| [Monitoring queries](platform_monitoring_queries.ipynb) | Compare processing times, validation failures, and rejection counts. |
| [Rejected records](inspect_rejected_records.ipynb) | Inspect quarantined records and rejection reasons. |
| [Data products](inspect_data_products.ipynb) | Inspect analytical tables and refresh history. |

Execution metrics are stored in `s3a://delta-data/metadata/pipeline_executions`; quarantined records are stored in `s3a://delta-data/metadata/rejected_records`.
