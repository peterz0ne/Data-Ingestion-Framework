# Task 3 Optimization Experiments

Each notebook in this directory runs a controlled Task 3 experiment. It keeps
the query result constant, records the implementation-specific overhead, and
compares execution time and Spark physical plans.

Run only one notebook Spark session at a time. Finish each notebook by running
its cleanup cell so its executors and cached data are released.

## Caching: Queries 3 and 5

Notebook: `caching_queries_03_05.ipynb`

### Experiment

- Compare uncached and cached Query 3 and Query 5.
- Keep SQL and results unchanged.
- Cache `pickup_datetime`, `pu_datetime_hour`, and `average_value_pu`.
- Materialize with `count()`; record its cost.
- Use one warm-up and three timed runs; compare medians.
- Clear the cache and stop Spark.

The full Gold table exceeded executor memory. The narrow view keeps all needed
columns and rows.

### Evidence to save

- Timings, medians, and materialization time.
- Result-check message.
- Uncached plan: `Scan parquet`.
- Cached plan: `Scan In-memory table` and `InMemoryRelation`.

### Report notes

- Cache a shared input, not final results.
- Report medians, speedup, and materialization time.
- Compare `Scan parquet` with `Scan In-memory table`.
- Explain that shuffles and aggregation remain.
- Mention the narrow view and memory limit.

Task 3: caching, timing, same results, and plan comparison.

## Partition Pruning: Queries 3 and 5

Notebook: `partition_pruning_queries_03_5.ipynb`

### Experiment

- Filter both layouts to February 2024.
- Run the filtered Query 3 and Query 5 variants.
- Compare date-partitioned Gold with an unpartitioned Delta copy.
- Keep SQL and results unchanged.
- Use one warm-up and three timed runs; compare medians.

### Evidence to save

- Equal-result check and timing summary.
- Partitioned plan: `PartitionFilters` with the February range.
- Unpartitioned plan: no date `PartitionFilters`.

### Report notes

- Do not compare a filtered query with an all-data query.
- State that both layouts use the same February filter.
- Explain that only the partitioned layout can skip January and March.

Task 3: same result, timing comparison, and pruning evidence.

## Broadcast Join: Query 3 Source Tables

Notebook: `broadcast_join_query_03.ipynb`

Query 5 has no required small-table join, so it is not included in this experiment.

### Experiment

- Join Silver taxi trips with hourly Silver air quality.
- Compare a normal join with a forced broadcast join.
- Disable AQE and automatic broadcasting in both versions.
- Use one warm-up and three timed runs; compare medians.

### Evidence to save

- Equal-result check and timing summary.
- Normal plan: shuffle join, such as `SortMergeJoin`.
- Broadcast plan: `BroadcastHashJoin` and `BroadcastExchange`.

### Report notes

- Gold is not used because its joins already happened during ingestion.
- The air-quality side is small enough to broadcast.
- Explain reduced shuffle work, if shown in the plan.

Task 3: normal versus broadcast join, same result, timing, and plans.

## Adaptive Query Execution: Queries 3 and 5

Notebook: `aqe_queries_03_05.ipynb`

### Experiment

- Compare AQE disabled and enabled.
- Change only `spark.sql.adaptive.enabled`.
- Keep inputs and SQL unchanged.
- Use one warm-up and three timed runs; compare medians.

### Evidence to save

- Equal-result check and timing summary.
- AQE-enabled final plan after executing the query.
- Plan changes such as coalesced shuffle partitions or a different join strategy.

### Report notes

- AQE uses runtime statistics to re-optimize a plan.
- Compare the final AQE plan with the non-adaptive plan.
- Query 3 shows coalesced adaptive shuffle reads.
- Its one-row cross join stays broadcast in both plans.
- Report no improvement if Spark makes no useful change for this workload.

Task 3: AQE enabled versus disabled, same result, timing, and final plans.
