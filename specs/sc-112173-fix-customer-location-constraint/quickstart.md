# Quickstart: Remove Legacy Dataset Location Constraint

## Prerequisites

- Work on `sc-112173-fix-customer-location-constraint` in the Superset repository.
- Activate the project Python environment with development dependencies installed.
- Confirm the active Alembic head before creating revision `4f8c2d1a7b3e` at the exact path shown below.

## Implementation checkpoints

1. Add failing list/set/tuple discovery tests in `tests/unit_tests/utils/test_core.py`.
2. Normalize the shared helper input in `superset/utils/core.py`.
3. Add `tests/unit_tests/migrations/test_remove_customer_location_constraint.py` with real Alembic/SQLite upgrade and downgrade coverage, then confirm it fails because the corrective revision is absent.
4. Add `superset/migrations/versions/2026-07-13_12-00_4f8c2d1a7b3e_remove_customer_location_constraint.py` at the confirmed active head using the shared unique-constraint helpers.
5. Update the SC-112173 commentary in `tests/integration_tests/datasets/soft_delete_tests.py`.

## Focused verification

```bash
pytest tests/unit_tests/utils/test_core.py -k generic_find_uq_constraint
pytest tests/unit_tests/migrations/test_remove_customer_location_constraint.py
pre-commit run mypy --files superset/utils/core.py
```

The migration test must prove:

- Exact three-column discovery regardless of name, order, or collection representation.
- Preservation of unrelated constraints.
- Safe absent and repeated upgrade behavior.
- Catalog-distinct records can coexist after upgrade.
- Downgrade restores `_customer_location_uc` when data is compatible.
- Downgrade fails visibly when existing data conflicts with the restored constraint.

## Before pushing

Stage the intended changes, review the staged diff, and run the mandatory repository gate:

```bash
git add \
  AGENTS.md \
  specs/sc-112173-fix-customer-location-constraint/ \
  superset/utils/core.py \
  superset/migrations/versions/2026-07-13_12-00_4f8c2d1a7b3e_remove_customer_location_constraint.py \
  tests/unit_tests/utils/test_core.py \
  tests/unit_tests/migrations/test_remove_customer_location_constraint.py \
  tests/integration_tests/datasets/soft_delete_tests.py

pre-commit run --all-files
```

After pushing, require the `test-mysql`, `test-postgres`, and `test-sqlite` jobs in `.github/workflows/superset-python-integrationtest.yml` to pass before merge.
