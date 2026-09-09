# Task 2. Design Your Storage Architecture

## Design

### Directory structure
`raw_data`, `cleaned_data`, and `final_data` are all Delta Lake layers

#### Locally:

```text
project/
├── configs/
│   ├── taxi_trips.json
│   ├── weather.json
│   ├── air_quality.json
│   └── taxi_zone_lookup.json
├── landing/
│   ├── taxi_trips/
│   ├── weather_hourly/
│   ├── air_quality_hourly/
│   └── taxi_zones/
└── jobs/
    └── *.py
```
* <b><u>configs/</u></b> stores the configuration files for each data (metadata + dataset-specific transformation rules)
* <b><u>landing/</u></b> stores the the actual data source files (the landing layer)
* <b><u>jobs/</u></b> the Python scripts responsible for connecting to Spark, passing to Spark the transformation rules to execute (also defining these rules), and invoking write/read operations

#### On the Delta Lake Storage:
```text
data_lake/
├── bronze/
│   ├── taxi_trips/
│   ├── weather/
│   ├── air_quality/
│   └── taxi_zone_lookup/
├── silver/
│   ├── taxi_trips/
│   ├── weather_hourly/
│   ├── air_quality_hourly/
│   └── taxi_zones/
└── gold/
    └── enriched_taxi_trips/
```
* Explanation [here](https://medium.com/@divyanshgoyal8989/understanding-the-medallion-architecture-in-a-data-lakehouse-40d03bb28761)

Each dataset directory represents one Delta table containing its own _delta_log and Parquet data files. Monthly source files for taxi trips are ingested into the same dataset table.

### Delta table organization

Each dataset is stored as a separate Delta table. This preserves the original datasets independently and allows them to be validated and transformed before
they are integrated.

| Layer | Delta Table | Row Granularity | Purpose |
|---|---|---|---|
| Bronze | `bronze_taxi_trips` | One raw taxi-trip record | Stores taxi records as ingested, with source and ingestion metadata. |
| Bronze | `bronze_weather` | One raw hourly weather record | Stores weather records as ingested from the source CSV, with source and ingestion metadata. |
| Bronze | `bronze_air_quality` | One raw air-quality measurement | Stores air-quality records as ingested from the source CSV, with source and ingestion metadata. |
| Bronze | `bronze_taxi_zone_lookup` | One taxi-zone lookup record | Stores the zone lookup data as ingested, with source and ingestion metadata. |
| Silver | `silver_taxi_trips` | One validated taxi trip | Stores standardized taxi records with formatted time fields required for later joins. |
| Silver | `silver_weather_hourly` | One weather observation per hour | Stores validated weather data with a single hourly UTC timestamp. |
| Silver | `silver_air_quality_hourly` | One aggregated air-quality value per selected monitoring site and hour | Stores standardized air-quality data ready for time-based integration. |
| Silver | `silver_air_quality_hourly` | One aggregated air-quality value per selected monitoring site and hour | Stores standardized air-quality data ready for time-based integration. |
| Gold | `gold_enriched_taxi_trips` | One enriched taxi trip | Stores the final taxi trips enriched with pickup weather, air quality, and pickup/dropoff zone and borough information. |

* The Bronze Layer is the source of truth of our raw data. The silver layer is the source of truth for our clean/validated data. The Gold layer is for delta tables that are created for specific use-cases of the data (eg one user needs the taxi trips with some extra columns from the other dimension tables, while a different user wants the fact table completely normalized). Bronze layer is needed because if something changes in the transformation logic of our data, we just update the rule and run the scripts again against our bronze layer, producing a new silver layer (wouldn't be possible if we were using an ETL style approach). On the other hand, if something changes in how we want the final data, we can just create a new delta table in the gold layer (eg for a specific aggregation).

### Naming conventions
- Use `lowercase snake_case` for directories, tables, and columns in the cleaned and final layers but preserve original source column names in the raw layer.
- Name tables using `<layer>_<dataset>`
- Use meaningful column names (e.g. `PULocationID -> pickup_zone_id`)
- Follow these suffix rules:
    - `_id` for identifiers
    - `_datetime` for timestamps
    - `_date` for dates
- preserve original column names in `bronze` layer and apply standardization rules in the `silver` layer

### Partitioning strategy

According to Delta Lake's [best practices](https://docs.delta.io/best-practices/):
```
Amount of data in each partition: You can partition by a column if you expect data in that partition to be at least 1 GB.
```

- **Taxi trips (raw and cleaned):** are initially stored as unpartitioned Delta tables because the three source files total approximately 160MB. Monthly partitioning would produce small partitions, which is below the recommended partition-size guideline. An alternative layout partitioned by pickup year and month will be evaluated in Task 6.

- **Weather (raw and cleaned):** Stored without partitioning. The dataset contains hourly observations and is small enough that splitting it by year, month, or day would create unnecessarily small partitions.

- **Taxi-zone lookup (raw and cleaned):** Stored without partitioning. This small lookup table is used to map zone identifiers to zone and borough information. Partitioning would add overhead with little benefit. Also, since this is a lookup table, it is very unlikely to grow.

- **Raw air quality:** Initially stored without partitioning. Although the source CSV exceeds 2 GB, its CSV size alone does not justify partitioning: the stored Delta size and the amount of data per candidate partition must also be considered. Partitioning by observation year and month can be reconsidered if the dataset grows and queries frequently filter by time.

- **Cleaned hourly air quality:** Stored without partitioning. Selecting the required pollutant and monitoring sites, then aggregating readings by hour, is expected to produce a much smaller table. Its stored size will be checked after ingestion to confirm this choice.
    * Note: <b>I think we should still partition this (in the silver layer) and just mention that it isnt best practice</b>

- **Final enriched taxi trips:** Initially stored without partitioning, following the taxi-trips baseline. Because enrichment adds columns, its stored size will be checked separately before deciding whether monthly partitioning is justified.



## Discuss

### Which datasets should conceptually be treated as lookup tables?
- The taxi_zone_lookup dataset is a lookup table mapping zone identifiers to zone and borough information.
- <b>I dont know if the following can be considered 'lookup' tables, since they are just tables that are related to the fact table and have growing data of their own (while taxi_zone_lookup is just an index of some codes, not growing data):</b>Cleaned weather and hourly air quality serve as contextual lookup datasets for enriching trips by time and, where applicable, monitoring site. Their lookup keys must be unique to avoid duplicating taxi trips during joins.

### Which datasets should not be partitioned? Explain why.
- Taxi zones and weather: Small datasets where partitioning would add overhead with little benefit (also, we are not expecting them to grow out of proportion).
- Cleaned hourly air quality: Expected to be small after filtering and aggregation, so initially unpartitioned.
- Taxi trips, raw air quality, and enriched trips are initially unpartitioned. Reconsider after measuring Delta table sizes and query performance.

### Which datasets require different partitioning strategies?
The strategy should depend on each dataset’s size and usage. Taxi trips and enriched trips are candidates for partitioning by pickup year and month. Raw air quality could use observation year and month if time filtering justifies it. Small lookup and contextual tables can remain unpartitioned.

### Under what conditions does partitioning become harmful?
Partitioning can hurt performance when it creates many small files, produces uneven partitions, or uses columns that queries rarely filter on. Columns with many distinct values, such as exact timestamps or trip identifiers, can create excessive partitions. Delta Lake [suggests](https://docs.delta.io/best-practices/) approximately 1 GB per partition as a guideline.

### If the total data volume increased by 20×, what changes would you make to your storage design?
- Keep the existing raw, cleaned, and final layers.
- Use monthly partitioning for taxi trips, enriched trips, and raw air quality if monthly partitions become large enough and benchmarks show a benefit.
- Keep small tables unpartitioned and compact small files into larger ones.