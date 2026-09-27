## Validation Framework

Each validator creates a temporary rejection-reason column when a rule is violated.
After all validation rules run, these temporary reasons are combined into the
`rejection_reasons` array.

- Records with no rejection reasons continue to the Silver layer.
- Records with one or more rejection reasons are excluded from Silver,
  Gold, and analytical data products.
- Rejected records are written to:

  `s3a://delta-data/metadata/rejected_records`

Each rejected-record entry contains the pipeline and execution IDs, stage, dataset, schema version, UTC rejection timestamp, rejection reasons, and the original source record as JSON.

### Supported validation rules

- `validate_primary_key`
- `validate_allowed_values`
- `validate_non_negative_numbers`
- `validate_null_values`
- `validate_duplicates`
- `validate_numeric_range`
- `validate_reference_exists`

`validate_null_values` uses an explicit `columns` list, so only genuinely required fields are rejected when missing.

```json
{
  "action": "validate_null_values",
  "parameters": {
    "columns": ["tpep_pickup_datetime", "PULocationID"]
  }
}
```

`validate_non_negative_numbers` accepts a `columns` list and adds a separate rejection reason for each negative value.

```json
{
  "action": "validate_non_negative_numbers",
  "parameters": {
    "columns": ["fare_amount", "total_amount"]
  }
}
```

`validate_numeric_range` validates inclusive numeric bounds.

```json
{
  "action": "validate_numeric_range",
  "parameters": {
    "column": "humidity",
    "minimum": 0,
    "maximum": 100
  }
}
```

`validate_reference_exists` checks a source key against a processed reference dataset.

```json
{
  "action": "validate_reference_exists",
  "parameters": {
    "column": "PULocationID",
    "reference_dataset": "taxi_zones",
    "reference_column": "location_id"
  }
}
```

Unsupported schema changes do not terminate the pipeline. They add schema errors to every record in the affected input batch, causing the batch to be stored in rejected_records instead of entering Silver.

Run `inspect_rejected_records.ipynb` to inspect individual rejected rows and summarize rejection reasons.

---



### Adding a validation rule

To add a standard row-level validation rule:

1. Add a validator function to `quality_validators.py`.
2. Give it the common signature
   `validator(df, parameters, validation_context)`.
3. Register it in `VALIDATOR_REGISTRY` in `quality.py`.
4. Add the rule to the appropriate dataset configuration.

The shared `rejection_reasons` mechanism automatically isolates and reports
records rejected by the new rule.