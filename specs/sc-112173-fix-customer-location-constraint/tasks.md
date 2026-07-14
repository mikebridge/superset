---
description: "Dependency-ordered implementation tasks for SC-112173"
---

# Tasks: Remove Legacy Dataset Location Constraint

**Input**: Design documents from `/specs/sc-112173-fix-customer-location-constraint/`
**Prerequisites**: `plan.md`, `spec.md`, `research.md`, `data-model.md`, `quickstart.md`

**Tests**: Regression tests are required by FR-009 and FR-014. Write the focused tests before their corresponding implementation and observe the expected failure where noted.

**Organization**: Tasks are grouped by user story. Shared constraint discovery is foundational because every migration story depends on it.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel after its phase prerequisites because it uses different files and has no dependency on an incomplete task.
- **[Story]**: Maps the task to a user story in `spec.md`.
- Every task names the exact file or directory it changes or validates.

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Pin the migration identity and confirm the implementation starts from the active migration head.

- [x] T001 Confirm the active Alembic head using `superset/migrations/alembic.ini` and verify revision ID `4f8c2d1a7b3e` remains collision-free under `superset/migrations/versions/`; do not create `superset/migrations/versions/2026-07-13_12-00_4f8c2d1a7b3e_remove_customer_location_constraint.py` before T004

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Make shared unique-constraint discovery representation-independent before the corrective migration relies on it.

**⚠️ CRITICAL**: User story implementation starts only after T002–T003 are complete.

- [x] T002 [P] Add parametrized failing coverage for reordered matching and non-matching set, list, and tuple inputs to `generic_find_uq_constraint_name` in `tests/unit_tests/utils/test_core.py`
- [x] T003 Update `generic_find_uq_constraint_name` to accept `Collection[str]`, normalize once with `set(columns)`, and preserve exact-set/first-match semantics in `superset/utils/core.py`, then make the T002 tests pass

**Checkpoint**: Shared constraint discovery accepts finite column collections and rejects subset/superset mismatches.

---

## Phase 3: User Story 1 - Upgrade Existing Metadata Databases Safely (Priority: P1) 🎯 MVP

**Goal**: Operators can apply a guarded follow-up revision that removes only the obsolete three-column constraint and can reverse it when existing data is compatible.

**Independent Test**: Build a minimal `tables` schema with the legacy and unrelated constraints, run the revision through a real Alembic operations context, and verify exact removal, absent/repeated safety, unrelated-constraint preservation, and reversible downgrade behavior.

### Tests for User Story 1

> **NOTE: Write T004 first and confirm it fails because the corrective revision does not exist.**

- [x] T004 [US1] Create real Alembic/SQLite regression tests for renamed/reordered legacy constraint removal, unrelated constraint preservation, absent and repeated upgrades, successful downgrade restoration, and conflicting-data downgrade failure in `tests/unit_tests/migrations/test_remove_customer_location_constraint.py`

### Implementation for User Story 1

- [x] T005 [US1] Create `superset/migrations/versions/2026-07-13_12-00_4f8c2d1a7b3e_remove_customer_location_constraint.py` using the active head confirmed by T001, then implement guarded exact-column-set upgrade and `_customer_location_uc` downgrade with `generic_find_uq_constraint_name`, `drop_unique_constraint`, and `create_unique_constraint`
- [x] T006 [US1] Run the complete migration regression file and resolve only migration/test portability failures in `superset/migrations/versions/2026-07-13_12-00_4f8c2d1a7b3e_remove_customer_location_constraint.py` and `tests/unit_tests/migrations/test_remove_customer_location_constraint.py`

**Checkpoint**: User Story 1 is independently functional and testable; the historical `superset/migrations/versions/2024-07-19_16-11_df3d7e2eb9a4_remove__customer_location_uc.py` remains unchanged.

---

## Phase 4: User Story 2 - Use Catalog-Aware Dataset Identities (Priority: P2)

**Goal**: Prove the corrected schema accepts catalog-distinct dataset identities while preserving unrelated supported constraints and application safeguards.

**Independent Test**: After upgrade, insert two non-null records with equal database/schema/table values and different catalogs; both persist, while a duplicate that violates the preserved four-column constraint is still rejected.

### Tests for User Story 2

- [x] T007 [US2] Extend `tests/unit_tests/migrations/test_remove_customer_location_constraint.py` with catalog-distinct insertion success and preserved four-column constraint failure cases using non-null identity values

### Implementation and Integration for User Story 2

- [x] T008 [US2] Run `pytest tests/unit_tests/commands/dataset/restore_test.py tests/unit_tests/dao/dataset_test.py -k "logical_duplicate or catalog"` and fix only SC-112173 regressions in `superset/migrations/versions/2026-07-13_12-00_4f8c2d1a7b3e_remove_customer_location_constraint.py` or `tests/unit_tests/migrations/test_remove_customer_location_constraint.py`

**Checkpoint**: User Stories 1 and 2 pass independently, and no dataset command, DAO, or model behavior changes are required.

---

## Phase 5: User Story 3 - Preserve Actionable Regression Context (Priority: P3)

**Goal**: Maintainers can trace the schema divergence and its correction without reading stale commentary that describes the no-op as unresolved.

**Independent Test**: The deliberate-absence comment names SC-112173, explains the corrected migration-built schema, distinguishes `metadata.create_all` behavior, and retains links to the authoritative restore/DAO tests.

### Implementation for User Story 3

- [x] T009 [P] [US3] Rewrite the deliberate-absence migration commentary to reference SC-112173 and describe corrected migration-built versus metadata-built constraint states in `tests/integration_tests/datasets/soft_delete_tests.py`
- [x] T010 [US3] After T006 and T009, review `tests/integration_tests/datasets/soft_delete_tests.py` against the implemented migration for timeless wording, removal of unresolved no-op claims, and preservation of the existing restore/DAO test references

**Checkpoint**: All three user stories are independently verifiable and the regression context matches the corrected schema behavior.

---

## Phase 6: Polish & Cross-Cutting Verification

**Purpose**: Verify focused behavior, type safety, migration immutability, and repository-wide quality gates.

- [x] T011 [P] Run focused pytest verification for `tests/unit_tests/utils/test_core.py`, `tests/unit_tests/migrations/test_remove_customer_location_constraint.py`, `tests/unit_tests/commands/dataset/restore_test.py`, and `tests/unit_tests/dao/dataset_test.py`
- [x] T012 [P] Run the `mypy`, `ruff-format`, `ruff`, and `pylint` hooks from `.pre-commit-config.yaml` against `superset/utils/core.py`, `superset/migrations/versions/2026-07-13_12-00_4f8c2d1a7b3e_remove_customer_location_constraint.py`, `tests/unit_tests/utils/test_core.py`, `tests/unit_tests/migrations/test_remove_customer_location_constraint.py`, and `tests/integration_tests/datasets/soft_delete_tests.py`
- [x] T013 Review the final diff for SC-112173 scope and confirm `superset/migrations/versions/2024-07-19_16-11_df3d7e2eb9a4_remove__customer_location_uc.py` plus dataset command/DAO/model files remain unchanged
- [ ] T014 Stage only the intended SC-112173 source, test, and spec files listed in `specs/sc-112173-fix-customer-location-constraint/quickstart.md`, then run `pre-commit run --all-files` before pushing
- [ ] T015 After pushing, require the `test-mysql`, `test-postgres`, and `test-sqlite` jobs defined in `.github/workflows/superset-python-integrationtest.yml` to pass before merging the SC-112173 changes

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies; T001 establishes the exact revision identity and parent.
- **Foundational (Phase 2)**: Depends on T001 and blocks all user stories; T002 must precede T003 for red-green coverage.
- **User Story 1 (Phase 3)**: Depends on T003; T004 → T005 → T006 is sequential.
- **User Story 2 (Phase 4)**: Depends on T006 because it verifies behavior delivered by the corrective migration; T007 precedes T008.
- **User Story 3 (Phase 5)**: T009 depends on T003 and can run in parallel with User Story 1; final review T010 depends on both T006 and T009.
- **Polish (Phase 6)**: Depends on all selected user stories; T011 and T012 may run in parallel, followed by T013, T014, and the post-push database matrix gate T015.

### User Story Dependency Graph

```text
Setup T001
    └── Foundation T002 → T003
            ├── US1 T004 → T005 → T006 ──┬──> US2 T007 → T008
            │                             └──> US3 T010
            └── US3 T009 ────────────────────────┘
                 US2 T008 + US3 T010 ──> Polish T011/T012 → T013 → T014 → T015
```

### User Story Dependencies

- **User Story 1 (P1)**: Starts after Foundation and supplies the independently deployable migration MVP.
- **User Story 2 (P2)**: Depends on User Story 1's migration but adds an independently runnable catalog-aware acceptance slice.
- **User Story 3 (P3)**: T009 starts after Foundation, while final review T010 waits for the implemented migration at T006.

### Parallel Opportunities

- T002 can be prepared after T001 without touching migration or comment files.
- After T003, T004 (US1) and T009 (US3) can run in parallel in different files.
- While US1 is in progress, US3 can complete independently.
- After all stories, T011 and T012 can run in parallel before diff review, the all-files gate, and the post-push database matrix gate.

## Parallel Execution Examples

### User Story 1

There is no safe internal parallelism because the story uses a deliberate red-green sequence in two coupled files. After T003, run T004 while another worker runs US3 task T009.

```text
Worker A: T004 in tests/unit_tests/migrations/test_remove_customer_location_constraint.py
Worker B: T009 in tests/integration_tests/datasets/soft_delete_tests.py
```

### User Story 2

T007 and T008 are sequential because T008 validates the behavior captured by T007. T007 may run while another worker completes US3 review task T010.

```text
Worker A: T007 in tests/unit_tests/migrations/test_remove_customer_location_constraint.py
Worker B: T010 in tests/integration_tests/datasets/soft_delete_tests.py
```

### User Story 3

US3 drafting can proceed independently after Foundation while US1 implements the migration; final review T010 waits for T006.

```text
Worker A: T009 in tests/integration_tests/datasets/soft_delete_tests.py, then T010 after T006
Worker B: T004 → T006 in the migration and its unit test
```

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete T001–T003 to establish the revision identity and safe shared discovery.
2. Complete T004–T006 using red-green migration coverage.
3. Stop and validate User Story 1 independently before adding catalog acceptance or commentary work.

### Incremental Delivery

1. **Foundation**: T001–T003 establish exact, collection-independent constraint discovery.
2. **MVP / User Story 1**: T004–T006 repair upgraded schemas and reversible downgrade.
3. **User Story 2**: T007–T008 prove catalog-aware identity behavior and unchanged safeguards.
4. **User Story 3**: T009–T010 make regression context accurate and traceable.
5. **Quality gate**: T011–T015 verify, review, stage narrowly, run mandatory pre-commit, and require the supported metadata-database CI jobs before merge.

### Parallel Team Strategy

1. Complete Setup and Foundation sequentially.
2. Assign US1 migration work to one developer and US3 commentary to another.
3. Start US2 after US1 passes its independent migration test.
4. Run focused test and lint verification in parallel, perform the final diff/pre-commit gate, and require the MySQL/PostgreSQL/SQLite jobs before merge.

## Notes

- Tasks with `[P]` operate on different files or independent verification commands after their prerequisites.
- User-story labels provide traceability to the prioritized scenarios in `spec.md`.
- Do not modify the historical `df3d7e2eb9a4` revision or add the model's four-column constraint.
- Do not suppress or catch downgrade integrity/DDL errors.
- New Python files require ASF license headers and typed functions.
- Commit only after the required tests and pre-commit gates pass.
