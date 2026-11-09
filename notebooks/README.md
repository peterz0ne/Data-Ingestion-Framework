# Notebooks

Start the Docker services and run ingestion before exploring pipeline outputs. See the [Jupyter setup guide](setup/README.md) to connect.

| Folder | Purpose |
| --- | --- |
| [benchmarks](benchmarks/) | Compare storage layouts and pipeline performance; exported query results are in `results/`. |
| [monitoring](monitoring/) | Inspect executions, rejected records, and analytical products. |
| [optimizations](optimizations/) | Evaluate broadcast joins, caching, partition pruning, and adaptive query execution. |
| [predictions](predictions/) | Inspect ML features and compare raw-versus-platform training datasets. |
| [run_queries](run_queries/) | Explore taxi demand, weather, and air-quality queries. |
| [setup](setup/) | Connect notebooks to the local Jupyter server. |
| [storage_metrics](storage_metrics/) | Measure Delta table storage overhead. |
| [update_evaluation](update_evaluation/) | Measure incremental updates, product refreshes, and validation and monitoring overhead. |

Experiment-specific prerequisites are described inside each notebook.
