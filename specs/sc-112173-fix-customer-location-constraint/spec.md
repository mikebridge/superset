# Feature Specification: Remove Legacy Dataset Location Constraint

**Feature Branch**: `sc-112173-fix-customer-location-constraint`
**Created**: 2026-07-13
**Status**: Draft
**Input**: Shortcut story [SC-112173](https://app.shortcut.com/preset/story/112173): "Fix no-op migration df3d7e2eb9a4: `_customer_location_uc` was never dropped (list == set bug)"

## Clarifications

### Session 2026-07-13

- Q: What must the corrective migration do on downgrade? → A: Recreate the legacy three-column constraint.
- Q: Should the feature harden shared unique-constraint discovery as well as add the follow-up migration? → A: Add the follow-up migration and harden the shared helper.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Upgrade Existing Metadata Databases Safely (Priority: P1)

As a Superset operator, I can upgrade a metadata database created by older releases and have the obsolete dataset-location uniqueness rule removed so that the upgraded schema enforces the intended dataset identity rules.

**Why this priority**: The earlier removal did not take effect on migrated databases, leaving production schemas with behavior that differs from newly created schemas.

**Independent Test**: Start with a metadata schema containing a unique constraint over `database_id`, `schema`, and `table_name`, perform the upgrade, and verify that no unique constraint remains over exactly those three columns.

**Acceptance Scenarios**:

1. **Given** an existing metadata database with the legacy three-column unique constraint, **When** the operator applies the upgrade, **Then** the legacy constraint is removed regardless of the physical name assigned to it.
2. **Given** an existing metadata database where the legacy constraint is already absent, **When** the operator applies the upgrade, **Then** the upgrade completes successfully without changing unrelated constraints.
3. **Given** the corrective upgrade has already completed, **When** its removal behavior is evaluated again, **Then** it completes without error and leaves the schema unchanged.

---

### User Story 2 - Use Catalog-Aware Dataset Identities (Priority: P2)

As a dataset administrator, I can register tables that have the same database, schema, and table name in different catalogs without an obsolete database rule rejecting the valid catalog-aware identities.

**Why this priority**: Catalog-aware application checks allow these distinct datasets, but migrated metadata databases can still reject them with an opaque uniqueness error.

**Independent Test**: After applying the corrective upgrade, create two active dataset records that differ only by catalog and verify that the legacy three-column uniqueness rule does not reject the second record.

**Acceptance Scenarios**:

1. **Given** an upgraded metadata database and two tables with equal database, schema, and table name but different catalogs, **When** both datasets are registered, **Then** registration is not blocked by the removed legacy constraint.
2. **Given** a dataset identity that violates another supported application or database rule, **When** it is registered, **Then** that rule continues to be enforced.

---

### User Story 3 - Preserve Actionable Regression Context (Priority: P3)

As a maintainer, I can understand why soft-delete tests do not reproduce the migrated-schema constraint failure and can trace the corrective work to SC-112173.

**Why this priority**: The existing test commentary records an intentional coverage gap and must not continue to describe the failed migration as unresolved after the correction exists.

**Independent Test**: Review the soft-delete regression commentary and verify that it links the former migrated-schema limitation to SC-112173 and accurately describes the post-fix behavior.

**Acceptance Scenarios**:

1. **Given** the corrective migration is present, **When** a maintainer reads the soft-delete test commentary, **Then** the commentary references SC-112173 and no longer presents the failed constraint removal as an unresolved condition.

### Edge Cases

- The legacy constraint is absent because the database was freshly created, manually repaired, or upgraded through another path.
- The legacy constraint covers the expected columns but has a database-generated or operator-modified name.
- Other unique constraints exist on the dataset table and must remain untouched.
- Constraint column order differs from the order used when the legacy rule was declared.
- The corrective upgrade is invoked more than once or after a partially completed deployment.
- Supported metadata database engines expose constraint metadata or perform constraint removal differently.
- A downgrade is attempted after catalog-distinct dataset records have been added; the downgrade MUST fail clearly if those records violate the restored legacy constraint rather than silently leaving an unexpected schema state.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The corrective upgrade MUST identify a unique constraint that covers exactly `database_id`, `schema`, and `table_name` on the dataset table without depending on its physical name or column order.
- **FR-002**: The corrective upgrade MUST remove the identified legacy three-column constraint.
- **FR-003**: The corrective upgrade MUST complete successfully when the legacy constraint is absent.
- **FR-004**: The corrective upgrade MUST leave all constraints with a different column set unchanged.
- **FR-005**: The corrective upgrade MUST behave consistently across every metadata database engine supported by Superset's shared migration conventions.
- **FR-006**: Re-evaluating the corrective removal after a successful upgrade MUST be a no-op rather than an error.
- **FR-007**: After the corrective upgrade, two active dataset records that differ by catalog MUST NOT be rejected by the obsolete three-column uniqueness rule.
- **FR-008**: Existing application-level duplicate prevention, soft-delete, restore, and catalog-normalization behavior MUST remain unchanged.
- **FR-009**: Automated regression coverage MUST demonstrate removal when the legacy constraint exists, safe behavior when it is absent, and preservation of unrelated constraints.
- **FR-010**: The soft-delete test commentary MUST reference SC-112173 and describe the corrected migrated-schema state accurately.
- **FR-011**: Shared unique-constraint discovery MUST normalize the requested column collection before comparison so equivalent sequence and set inputs produce the same result, while preserving behavior for existing callers.
- **FR-012**: Downgrading the corrective migration MUST recreate the legacy unique constraint over `database_id`, `schema`, and `table_name`.
- **FR-013**: If existing data conflicts with the legacy constraint during downgrade, the downgrade MUST surface the constraint restoration failure and MUST NOT report successful restoration.
- **FR-014**: Automated regression coverage MUST directly verify shared unique-constraint discovery with both sequence and set inputs, including matching and non-matching column collections.

### Key Entities

- **Dataset record**: A registered physical table identified by database, catalog, schema, and table name, with lifecycle state managed separately.
- **Legacy dataset-location constraint**: The obsolete uniqueness rule over database, schema, and table name that omits catalog and lifecycle state.
- **Corrective metadata upgrade**: The guarded, repeatable schema change that removes the legacy constraint from previously migrated databases.
- **Constraint column collection**: The caller-provided group of columns used to locate a database constraint; collection representation and order do not change its meaning.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: In all supported metadata database test environments, 100% of upgrades from a schema containing the legacy three-column constraint finish with that constraint absent.
- **SC-002**: In automated absent-constraint and repeated-evaluation scenarios, 100% of corrective upgrades complete without error or unrelated schema changes.
- **SC-003**: After the upgrade, registering two valid dataset identities that differ only by catalog succeeds without a uniqueness failure from the legacy three-column rule.
- **SC-004**: Regression coverage verifies both the affected migrated-schema path and preservation of unrelated constraints before the change is accepted.
- **SC-005**: Maintainers can trace the historical soft-delete test limitation to SC-112173 directly from the relevant test commentary.
- **SC-006**: Automated downgrade coverage verifies that the legacy three-column constraint is restored when existing data is compatible.
- **SC-007**: Shared constraint discovery returns identical results for equivalent sequence and set inputs in 100% of regression cases.

## Assumptions

- The intended dataset identity remains catalog-aware, while application-level guards continue to prevent invalid duplicates involving active or soft-deleted datasets.
- The correction is delivered as a new follow-up upgrade; the historical migration remains immutable for databases that have already recorded it as applied.
- Superset's existing shared migration conventions define the supported database-specific constraint removal behavior.
- Adding or changing a four-column database constraint is outside this story; the required outcome is removal of the obsolete three-column rule.
- Downgrade restoration may be rejected when post-upgrade data contains catalog-distinct identities that conflict under the legacy three-column rule; operators must resolve such data before retrying the downgrade.
