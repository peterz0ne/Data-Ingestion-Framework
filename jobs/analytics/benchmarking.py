from collections.abc import Sequence
from math import isclose
from statistics import median
from time import perf_counter

from pyspark.sql import Row, SparkSession

from analytics.runner import execute_registered_query


def execute_and_collect(spark: SparkSession, query_name: str):
    return execute_registered_query(spark, query_name).collect()


def measure_registered_query(
    spark: SparkSession,
    query_name: str,
    repetitions: int = 3,
) -> dict:
    execute_and_collect(spark, query_name)

    samples = []
    result_rows = None
    for _ in range(repetitions):
        started_at = perf_counter()
        result_rows = execute_and_collect(spark, query_name)
        samples.append(perf_counter() - started_at)

    return {
        "rows": result_rows,
        "samples_seconds": samples,
        "median_seconds": median(samples),
    }


def _values_match(
    v1: object,
    v2: object,
    rel_tol: float = 1e-9,
    abs_tol: float = 1e-9,
) -> bool:
    if isinstance(v1, float) and isinstance(v2, float):
        return isclose(v1, v2, rel_tol=rel_tol, abs_tol=abs_tol)
    return v1 == v2


def _rows_match(
    r1: Row,
    r2: Row,
    rel_tol: float = 1e-9,
    abs_tol: float = 1e-9,
) -> bool:
    if len(r1) != len(r2):
        return False
    return all(_values_match(v1, v2, rel_tol, abs_tol) for v1, v2 in zip(r1, r2))


def rows_match(
    expected_rows: Sequence[Row],
    actual_rows: Sequence[Row],
    relative_tolerance: float = 1e-9,
    absolute_tolerance: float = 1e-9,
) -> bool:
    if len(expected_rows) != len(actual_rows):
        return False
    return all(
        _rows_match(r1, r2, rel_tol=relative_tolerance, abs_tol=absolute_tolerance)
        for r1, r2 in zip(expected_rows, actual_rows)
    )
