# Quality Rules

The JSON files in this directory define the quality checks applied to each dataset. Quality checks run *before* the configured transformations, so their column names should match the original DataFrame schema (before transformation).

## Available Rules

### `validate_primary_key`

Checks whether the configured column or combination of columns can act as a primary key.

- `columns`: a list of columns that together identify a row.
- A row is invalid when any primary-key column is null.
- A primary-key combination is invalid when it appears more than once.
- This rule is useful when the dataset already provides a natural key.

### `validate_allowed_values`

Checks categorical or enumerated columns against an explicit list.

- `column`: the column to check.
- `allowed_values`: the complete list of accepted values.
- Non-null values outside the list are invalid.
- Null and NaN values are left to `validate_null_values` (this rule does not consider them).

Examples of allowed-value lists include:

- Vendor identifiers: `[1, 2, 6, 7]`
- Rate codes: `[1, 2, 3, 4, 5, 6, 99]`
- Store-and-forward flags: `["Y", "N"]`
- Payment types: `[0, 1, 2, 3, 4, 5, 6]`
- Months: `[1, ..., 12]`
- Hours: `[0, ..., 23]`
- Days: `[1, ..., 31]`

### `validate_non_negative_numbers`

Checks that a numeric column does not contain negative values.

- `column`: the numeric column to check.
- Values smaller than zero are invalid.
- Null and NaN values are not treated as negative; they should be handled separately by `validate_null_values` when required.

### `validate_null_values`

Checks for missing values in one column or, with `column: "*"`, across all columns.

- `column`: one column name or `"*"` for every column.
- `null_values`: optional additional values that represent missing data, such as `["N/A"]`.

### `validate_duplicates`

Checks whether rows are repeated for a selected set of columns.

- `columns`: a list of comparison columns or `"*"` for the complete row.
- Rows are duplicates when all selected values match another row.
- Repeated rows are ignored when any selected comparison column is null, because the row is incomplete for comparison.
- A generated identifier should not be included when the goal is to detect duplicate source records, since it makes every row unique (one of the reasons we need to the quality checks before generating primary keys).

## Failure Actions

Each rule can define an `on_failure` action:

- `warn`: print the quality error and keep processing. The data is not changed.
- `fail`: stop processing by terminating the quality check. The silver-layer write is not completed.
- `drop`: remove rows that violate the rule and continue with the remaining data.
- `fill`: replace invalid values in a column and continue. A literal `value` can be supplied, or a method can be selected:
  - `mean`: replace with the mean of the valid values.
  - `median`: replace with the median of the valid values.
  - `mode`: replace with the most frequent valid value.

The `fill` action is intended for column-level checks. It cannot be used for checks that do not identify one replacement column, such as a duplicate or primary-key failure.

### Current action policy

These actions are provisional choices for the current ingestion stage. We do not want missing values to stop ingestion yet, so null and NaN checks use `warn` and only report the problem. Invalid numeric ranges, disallowed categorical values, and duplicate rows are treated as unusable records and use `drop` when the dataset is cleaned. Primary-key checks are currently used mainly to assess whether the proposed key is reliable; their action should be chosen per dataset. Later analysis may change these decisions when the impact of the invalid records and the requirements of downstream analyses are better understood.


## Dataset-Specific Rules

### Taxi zones

The taxi-zone lookup is a reference table used later to enrich taxi trips with borough and zone information.

- `validate_primary_key` checks `LocationID`. The primary key has not been established as a guaranteed property of the incoming lookup data, so it is checked rather than assumed to be correct from the start.
- `validate_null_values` checks `Borough` and `Zone` because these are the two attributes needed for taxi-trip enrichment. The source represents missing values as `N/A`, so `null_values: ["N/A"]` is configured for both columns.
- `validate_duplicates` uses `*`, meaning the complete row is compared. This verifies that the lookup data does not contain repeated records.

For this reference dataset, missing borough or zone values are currently warned about rather than removed, because the issue needs to remain visible while the lookup is assessed. Duplicate lookup rows should be dropped when cleaning the data because they can make enrichment ambiguous (but for the first assignment we will only doing warn). The primary-key check assertion makes the build to fail if the primary keys were invalid, to act as a diagnostic tool because the uniqueness of `LocationID` should be verified rather than assumed.

### Taxi trips

Taxi trips are fact records. The pipeline generates `trip_id` itself, so the source does not provide a primary key that should be validated.

- `validate_duplicates` uses `*` to detect complete duplicate source rows.
- `validate_allowed_values` checks categorical columns:
  - `VendorID`: `[1, 2, 6, 7]`
  - `RatecodeID`: `[1, 2, 3, 4, 5, 6, 99]`
  - `store_and_fwd_flag`: `["Y", "N"]`
  - `payment_type`: `[0, 1, 2, 3, 4, 5, 6]`
- `validate_non_negative_numbers` checks the numeric measures and quantities:
  - `passenger_count`
  - `fare_amount`
  - `extra`
  - `mta_tax`
  - `tip_amount`
  - `tolls_amount`
  - `improvement_surcharge`
  - `total_amount`
  - `congestion_surcharge`
  - `Airport_fee`
  - `trip_distance`
- `validate_null_values` checks missing values where the configuration requires them. In particular, the rule can be applied to all columns when the dataset must not contain missing or NaN values anywhere.

For taxi trips, null values are warned about for now so potentially useful trip records are not discarded before their impact is understood. Disallowed categorical values, negative quantities or amounts, and complete duplicate rows should be dropped during cleaning because they violate the expected data rules or can bias trip-level analysis. These choices can be revisited after profiling how many trips each rule removes.

### Weather

Weather records are hourly observations. The combination of `year`, `month`, `day`, and `hour` identifies an observation.

- `validate_primary_key` checks the combination `[year, month, day, hour]`.
- `validate_duplicates` checks an explicit subset of the weather columns rather than using `*`. The subset includes the calendar fields and populated measurements and source fields, but excludes `snwd`, `snwd_source`, `wpgt`, and `wpgt_source` because all rows in those four columns are null. The duplicate rule only considers rows whose selected comparison columns are all non-null. Therefore, including these columns would cause otherwise matching rows to be marked as not duplicates because of the null-handling logic in the rule. This subset allows repeated observations to be detected using the fields that contain actual data.
- `validate_allowed_values` checks:
  - `month`: `1` through `12`
  - `day`: `1` through `31`
  - `hour`: `0` through `23`
- `validate_non_negative_numbers` checks `year` and the configured weather measurements:
  - `rhum`
  - `prcp`
  - `wdir`
  - `wspd`
  - `pres`
  - `cldc`

These checks catch impossible calendar values and negative measurements while preserving the hourly key used for the weather analysis. Take note that *temp* can take negative values so we don't place a quality check for it

Missing values, if checked later, should initially be warned about rather than dropped because they may affect only optional weather measurements. Invalid calendar ranges, negative measurements, and duplicate observations should be dropped because they cannot be trusted for hourly analysis. The primary-key result is also useful for deciding whether the proposed hourly key is reliable (we fail the build if validate_primary_key fails). These action choices may change after examining the amount of missing and invalid weather data.

### Air quality

Air-quality records contain measurements tied to geographic coordinates and observation metadata.

- `validate_duplicates` uses `*` to reject repeated complete records.
- `validate_null_values` checks `sample_measurement` because it is the observed air-quality value and is essential to the analysis.
- `validate_null_values` also checks `latitude` and `longitude` because geographic coordinates are required to interpret and analyze the measurements spatially.

These checks focus on the fields that are most important for using the air-quality data rather than rejecting every optional descriptive field.

Null or NaN values in the important measurement and coordinate columns are currently warned about rather than dropped, so their impact can be measured without losing observations prematurely. Complete duplicate records should be dropped because they would count the same observation more than once. Later analysis may show that some missing coordinate or measurement rows should instead be removed or handled with a more specific strategy.
