# Data Operations

The operations in `operations.py` are reusable Spark transformations selected by each dataset configuration. They are applied in the order listed in the dataset's `operations` array.

## Pipeline Order

The ingestion job follows this sequence:

1. Read the source data into a Spark DataFrame.
2. Write the raw DataFrame to the bronze Delta layer.
3. Validate the source schema.
4. Run quality checks against the source columns.
5. Apply the configured operations in order.
6. Write the transformed DataFrame to the silver Delta layer.

Because quality checks run before operations, quality-rule column names must match the source DataFrame at validation time. For example, air-quality rules refer to `Sample Measurement` before `standardize_column_names` changes it to `sample_measurement`. Operations affect the silver output, not the original bronze input.

## Available Operations

### `rename_columns`

Renames columns using matching `source` and `target` lists.

```json
{
  "action": "rename_columns",
  "parameters": {
    "source": ["VendorID"],
    "target": ["vendor_id"]
  }
}
```

The source and target lists must have the same length. Each source column is renamed to the corresponding target column. This is useful for moving source-specific names into the project's standard naming convention.

### `standardize_column_names`

Standardizes every column name by replacing non-alphanumeric characters with underscores, removing leading and trailing underscores, and converting the result to lowercase.

For example:

```text
Sample Measurement -> sample_measurement
Date GMT           -> date_gmt
```

This is useful for sources whose headers contain spaces or other characters that are inconvenient in Spark expressions and downstream SQL.

### `drop_columns`

Removes the columns listed in `parameters.columns`.

```json
{
  "action": "drop_columns",
  "parameters": {
    "columns": ["unused_column"]
  }
}
```

This is used when a source column is entirely empty, irrelevant to the analysis, or would otherwise make later comparisons less useful.

### `convert_to_utc`

Converts timestamp columns from a configured source timezone to UTC.

- `columns`: timestamp columns to convert.
- `source_timezone`: the timezone in which the source timestamps are expressed.

The column names remain the same; only their timestamp values are converted. This allows datasets from different local timezones to be compared consistently.

### `create_hourly_datetime`

Builds a timestamp from separate year, month, day, and hour columns.

- `year_column`
- `month_column`
- `day_column`
- `hour_column`
- `target_column`

The generated timestamp uses minute and second values of zero. Weather uses this operation to create `observation_datetime` from its hourly time components.

### `create_timestamp_from_columns`

Combines a date column and a time column into one timestamp using the configured format.

- `date_column`: date text column.
- `time_column`: time text column.
- `target_column`: name of the generated timestamp column.
- `format`: Spark timestamp parsing format.

This is used for air-quality observations, where the date and time are supplied in separate GMT columns.

### `cast_columns`

Casts selected columns to the requested Spark data types.

```json
{
  "action": "cast_columns",
  "parameters": {
    "columns": {
      "latitude": "double",
      "date_gmt": "date"
    }
  }
}
```

This makes numeric, date, and timestamp fields usable for analysis instead of leaving them as source strings.

### `generate_primary_key`

Adds a generated identifier column.

- `column`: name of the new identifier.
- `strategy`: `incremental` or `uuid`.
- `start`: starting value for the incremental strategy.

The incremental strategy uses Spark's `monotonically_increasing_id()` plus the configured start value. The UUID strategy creates a UUID for each row. Generated identifiers are added after quality checks, so they are not used to hide duplicate source rows.

## Dataset Operations

### Taxi zones

Configured operation: `rename_columns`.

The lookup source columns are renamed as follows:

- `LocationID` -> `location_id`
- `Borough` -> `borough`
- `Zone` -> `zone`

`service_zone` is retained with its source name. The renamed fields give the silver lookup table consistent identifiers and make later joins to taxi trips easier to read.

### Taxi trips

Configured operations, in order:

1. `rename_columns`
   - `VendorID` -> `vendor_id`
   - `tpep_pickup_datetime` -> `pickup_datetime`
   - `tpep_dropoff_datetime` -> `dropoff_datetime`
   - `RatecodeID` -> `rate_code_id`
   - `PULocationID` -> `pickup_zone_id`
   - `DOLocationID` -> `dropoff_zone_id`
   - `Airport_fee` -> `airport_fee`
2. `generate_primary_key`
   - Adds an incremental `trip_id` starting at `1`.
3. `convert_to_utc`
   - Converts `pickup_datetime` and `dropoff_datetime` from `America/New_York` to UTC.

The renames create consistent silver-layer names, the generated ID gives each retained trip a stable pipeline identifier, and UTC conversion makes trip timestamps compatible with weather and air-quality observations.

### Weather

Configured operations, in order:

1. `drop_columns`
   - Removes `snwd`, `snwd_source`, `wpgt`, and `wpgt_source` (they are 100% null).
2. `create_hourly_datetime`
   - Creates `observation_datetime` from `year`, `month`, `day`, and `hour`.

The four removed columns are entirely null in the source data, so they cannot contribute useful information to the silver table or duplicate comparisons. The derived hourly timestamp provides a single time field for joins with trip and environmental data while preserving the original calendar fields.

### Air quality

Configured operations, in order:

1. `drop_columns`
   - Removes `Uncertainty` (100% null).
2. `standardize_column_names`
   - Converts headers such as `State Code`, `Date GMT`, and `Sample Measurement` to `state_code`, `date_gmt`, and `sample_measurement`.
3. `generate_primary_key`
   - Adds an incremental `observation_id` starting at `1`.
4. `create_timestamp_from_columns`
   - Combines `date_gmt` and `time_gmt` into `observation_datetime` using `yyyy-MM-dd HH:mm`.
5. `cast_columns`
   - Casts `latitude`, `longitude`, and `sample_measurement` to `double`.
   - Casts `mdl` to `double`.
   - Casts `date_local`, `date_gmt`, and `date_of_last_change` to `date`.

These operations turn the wide CSV source into a consistent analytical schema, create a timestamp for temporal joins, and convert important geographic, measurement, and date fields into usable types.
