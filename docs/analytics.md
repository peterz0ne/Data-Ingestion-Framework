# Analytical data products

Start the services and run baseline ingestion as described in [Getting started](../README.portfolio.md#getting-started).

The full pipeline automatically refreshes four analytical Delta tables:

- `monthly_zone_demand`
- `average_trip_distance_by_weather`
- `air_quality_vs_demand`
- `zone_weather_demand_variation`

After ingestion, refresh only these products with:

```sh
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/generate_data_products.py
```

Explore the [query notebooks](../notebooks/run_queries/) and [inspect the materialized products](../notebooks/monitoring/inspect_data_products.ipynb). For benchmarks, optimization experiments, and storage measurements, see the [notebook guide](../notebooks/README.md).
