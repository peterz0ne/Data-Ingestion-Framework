import sys

def validate_schema(df, expected_schema):
    if expected_schema is not None:
        actual_schema = dict(df.dtypes)

        if actual_schema != expected_schema:
            print("Schema is invalid.")
            print(f"Expected: {expected_schema}")
            print(f"Actual:   {actual_schema}")
            sys.exit(1)

        print("Schema is valid.")
