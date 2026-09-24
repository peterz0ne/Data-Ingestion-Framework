# Task 3: Platform Monitoring System

The platform records one monitoring row for every bronze, silver, gold,
and analytical-refresh execution.

## Monitoring data

Monitoring records are stored as a Delta table:

```text
s3a://delta-data/metadata/pipeline_executions
```

Each record contains:

- pipeline and execution IDs
- stage and dataset
- schema version
- start/end timestamps and duration
- processed, inserted, and rejected record counts
- validation-failure count

## Run monitoring

Run the complete pipeline:

```sh
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/ingest.py
```

Inspect all monitoring fields in:

```text
inspect_pipeline_monitoring.ipynb
```

Run the required Spark SQL monitoring queries in:

```text
platform_monitoring_queries.ipynb
```

The queries answer these:
1. Which dataset fails validation most frequently?
2. Which dataset requires the longest processing time?
3. How many records were rejected during each execution?
4. How has processing time changed over multiple executions?

## Discuss

### Which operational metrics are most useful?
The most useful metrics are:
- duration_seconds:
    - Duration exposes performance regressions
- processed_records, inserted_records:
    - Record counts reveal unexpected changes in data volume
- rejected_records and validation_failure_count:
    - Rejected records and validation failures identify data-quality problems
- schema_version, execution_id, pipeline_run_id:
    - Provide context for comparing runs

### How could this information support debugging and system maintenance?
- A spike in rejected records or validation failures points to a data-quality problem in a specific dataset.
- Longer execution times identify stages that may need optimization.
- Pipeline and execution IDs trace a problem across bronze, silver, gold, and analytical-refresh stages.
- Schema version helps connect issues to schema changes.

### What additional monitoring information would be valuable in a production system?
- Source-file name and arrival time to detect delayed or missing inputs.
- Resource usage, such as memory and CPU, to identify infrastructure bottlenecks.
- Alerts when validation failures or processing time exceed defined thresholds.