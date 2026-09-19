# Task 4 analytical data products

Task 1 Queries 1–4 are materialized as Delta tables: `monthly_zone_demand`,
`average_trip_distance_by_weather`, `air_quality_vs_demand`, and
`zone_weather_demand_variation`. The full pipeline refreshes them automatically.

To refresh only the products, run:

```sh
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/jobs/generate_data_products.py
```
