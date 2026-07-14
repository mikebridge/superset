# Implementation Plan: Remove Legacy Dataset Location Constraint

**Branch**: `sc-112173-fix-customer-location-constraint` | **Date**: 2026-07-13 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `/specs/sc-112173-fix-customer-location-constraint/spec.md`

## Summary

Add an immutable follow-up Alembic revision that discovers and removes the obsolete unique constraint over `tables(database_id, schema, table_name)` by exact column set, while safely doing nothing when that constraint is absent. Harden shared unique-constraint discovery by accepting a finite collection of column names and normalizing it before comparison. Preserve reversible downgrade behavior, existing catalog-aware application safeguards, unrelated constraints, and cross-database DDL handling through the existing migration utilities.

## Technical Context

**Language/Version**: Python 3.10+
**Primary Dependencies**: Alembic 1.15.x, SQLAlchemy 1.4.x, Superset migration utilities
**Storage**: Superset metadata databases on PostgreSQL, MySQL, and SQLite
**Testing**: pytest, `MagicMock` inspector unit tests, and an in-memory SQLite Alembic `MigrationContext`/`Operations` test
**Target Platform**: Superset server metadata upgrade path
**Project Type**: Python backend in the Superset monorepo
**Performance Goals**: Perform one bounded unique-constraint metadata inspection and at most one schema alteration during upgrade; do not scan or rewrite application rows outside database-managed DDL
**Constraints**: Do not edit the recorded historical migration; match exact unordered column sets; preserve unrelated constraints; keep upgrade absent-safe and repeatable; propagate downgrade constraint violations; use shared batch helpers for database portability
**Scale/Scope**: One shared helper, one follow-up migration, two focused test locations, and one test-comment update; no model, API, UI, or user-facing documentation change

## Constitution Check

*GATE: Passed before Phase 0 research and re-checked after Phase 1 design.*

- **Type Safety — PASS**: Widen the helper input truthfully to `Collection[str]`; new and changed functions remain fully typed and MyPy-compatible.
- **Testing Strategy — PASS**: Fast helper unit tests cover normalization independently; a focused migration test is the smallest integration slice needed to exercise real Alembic DDL. No E2E coverage is added.
- **Modern Frontend Patterns — PASS / Not applicable**: No frontend files or dependencies change.
- **Security & Access Control — PASS / Not applicable**: The change repairs operator-controlled metadata schema state and does not alter principals, routes, RBAC, data access, or public identifiers.
- **Simplicity & YAGNI — PASS**: Reuse existing discovery and migration utilities; add no new abstraction, configuration, four-column constraint, or application behavior.
- **Apache License — PASS**: The new Python migration and test file will carry ASF headers.
- **Code and docstring rules — PASS**: New functions use typed signatures, PEP 257 docstrings where required, and timeless comments.
- **SQLAlchemy portability — PASS**: Constraint inspection and DDL remain in SQLAlchemy/Alembic constructs with the existing MySQL-aware batch helpers; no raw SQL is introduced.
- **Module import safety — PASS**: No model imports or foundational module dependency changes are planned.
- **Workflow — PASS**: Focused tests, MyPy/pre-commit hooks, and `pre-commit run --all-files` before push are part of verification.

## Project Structure

### Documentation (this feature)

```text
specs/sc-112173-fix-customer-location-constraint/
├── spec.md
├── plan.md
├── research.md
├── data-model.md
├── quickstart.md
└── checklists/
    └── requirements.md
```

No `contracts/` artifact is created because this change has no external API, CLI, event, or file-format interface.

### Source Code (repository root)

```text
superset/
├── migrations/
│   ├── migration_utils.py                         # reuse only
│   └── versions/
│       ├── 2024-07-19_16-11_df3d7e2eb9a4_remove__customer_location_uc.py  # immutable evidence
│       └── 2026-07-13_12-00_4f8c2d1a7b3e_remove_customer_location_constraint.py
└── utils/
    └── core.py                                    # normalize helper input

tests/
├── unit_tests/
│   ├── migrations/
│   │   └── test_remove_customer_location_constraint.py
│   └── utils/
│       └── test_core.py
└── integration_tests/
    └── datasets/
        └── soft_delete_tests.py                   # update historical commentary
```

**Structure Decision**: Keep behavior in the existing shared helper and migration layers. Place pure discovery regression cases beside sibling constraint-helper tests, and place real DDL coverage in the established isolated migration-test directory.

## Design

### Shared constraint discovery

- Change `generic_find_uq_constraint_name` to accept `Collection[str]` from `collections.abc`.
- Normalize once with `set(columns)` before iterating over reflected constraints.
- Continue exact set equality so order is irrelevant while subsets and supersets do not match.
- Retain the first matching reflected constraint name and `None` when no exact match exists.

### Corrective migration

- Generate a new revision from the implementation branch head; do not change `df3d7e2eb9a4` because deployed databases have already recorded it.
- In `upgrade()`, reflect `tables`, locate the exact three-column unique constraint using a set input, and call `drop_unique_constraint` only when a name is found.
- Reuse `migration_utils.drop_unique_constraint` so MySQL receives `type_="unique"` and SQLite uses Alembic batch table recreation.
- In `downgrade()`, call `create_unique_constraint` with `_customer_location_uc` and the three legacy columns.
- Do not catch database or DDL errors. Catalog-distinct data that conflicts with the restored legacy rule must cause downgrade to fail visibly.

### Regression coverage

- Parametrize helper tests for set, list, and tuple inputs; cover reordered matches, exact non-matches, subsets, supersets, and unrelated reflected constraints.
- Use a minimal real `tables` schema with named legacy and unrelated constraints in an in-memory SQLite engine.
- Verify upgrade removes only the exact three-column constraint regardless of its physical name/order, is safe when absent, and is repeatable.
- Verify catalog-distinct non-null records can coexist after upgrade.
- Verify downgrade restores `_customer_location_uc`; incompatible rows must make restoration fail rather than report success.
- Update the deliberate-absence comment in `soft_delete_tests.py` to reference SC-112173 and distinguish metadata-built four-column constraints from corrected migration-built schemas.

## Implementation Sequence

1. Add failing helper tests for sequence/set equivalence and exact-set matching.
2. Normalize `generic_find_uq_constraint_name` input and update its type annotation.
3. Add isolated migration regression tests covering present, absent, repeated, cross-catalog, unrelated-constraint, downgrade, and downgrade-conflict states; confirm they fail because the corrective revision is absent.
4. Create the follow-up revision at the active Alembic head and implement guarded upgrade plus reversible downgrade.
5. Update the soft-delete test commentary.
6. Run focused tests and type/lint hooks, then the mandatory all-files pre-commit gate before push and the MySQL/PostgreSQL/SQLite integration jobs before merge.

## Requirement Traceability

| Requirements | Design and verification coverage |
|---|---|
| FR-001–FR-006 | Exact-set reflected discovery, guarded shared-helper removal, preservation tests, and repeated/absent upgrade cases |
| FR-007–FR-008 | Catalog-distinct insertion after upgrade plus no changes to dataset command, DAO, model, or lifecycle code |
| FR-009 | Real Alembic migration regression matrix with legacy and unrelated constraints |
| FR-010 | Targeted update to the deliberate-absence commentary in `soft_delete_tests.py` |
| FR-011, FR-014 | `Collection[str]` normalization and parametrized set/list/tuple helper unit tests |
| FR-012–FR-013 | Shared-helper downgrade restoration plus success and conflicting-data failure tests |
| SC-001–SC-007 | Focused helper and migration tests, repository pre-commit gates, and the `test-mysql`, `test-postgres`, and `test-sqlite` jobs in `.github/workflows/superset-python-integrationtest.yml` |

## Post-Design Constitution Check

All pre-research gates remain passed. Research introduced no new dependency, public interface, raw SQL, model change, security boundary, or unjustified abstraction. The integration-style migration test is justified because mocked calls cannot verify SQLite batch DDL or reproduce the original schema no-op.
