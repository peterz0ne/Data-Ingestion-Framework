# Quality package

Spark data-quality checks used by the Silver layer.

- `quality.py` — public entry point: `run_quality_checks`
- `quality_batching.py` — batches `warn` and `drop` row checks
- `quality_validators.py` — primary-key, duplicate, and legacy validators
- `quality_failure_handling.py` — `warn`, `drop`, `fail`, and `fill` behavior

The Silver job imports:

```python
from quality.quality import run_quality_checks