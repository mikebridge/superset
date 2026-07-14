# Research: Remove Legacy Dataset Location Constraint

## Decision 1: Add a follow-up migration

**Decision**: Create a new Alembic revision on the implementation branch head and leave `df3d7e2eb9a4` unchanged.

**Rationale**: Deployed databases have recorded the historical revision as applied even though its constraint lookup was a no-op. Editing that file would not repair those databases and would make migration history depend on when it was installed.

**Alternatives considered**:

- Edit `df3d7e2eb9a4`: rejected because recorded revisions are not replayed.
- Add a model-only change: rejected because metadata-created schemas already use model metadata; migrated schemas need explicit repair.

## Decision 2: Discover by exact unordered columns

**Decision**: Locate a unique constraint whose reflected columns equal `{database_id, schema, table_name}` and use its reflected physical name for removal.

**Rationale**: Exact set equality handles database/operator-specific naming and column order without touching the four-column model constraint or another unique rule.

**Alternatives considered**:

- Drop `_customer_location_uc` by literal name: rejected because names may be generated or altered.
- Match any constraint containing the three columns: rejected because that could remove a supported superset constraint.
- Drop every constraint on `tables`: rejected as destructive and outside scope.

## Decision 3: Harden the shared helper with `Collection[str]`

**Decision**: Type `generic_find_uq_constraint_name` with `Collection[str]`, normalize once with `set(columns)`, and compare exact sets.

**Rationale**: `Collection` truthfully supports existing sets and the affected list while requiring a finite, reusable input. One normalization eliminates representation and ordering differences.

**Alternatives considered**:

- `Iterable[str]`: rejected because it unnecessarily admits one-shot generators.
- `Sequence[str] | AbstractSet[str]`: rejected as verbose without improving this contract.
- `set[str] | list[str]`: rejected because it overfits current callers and excludes equivalent tuples.
- Fix only the new migration input: rejected because the clarified requirements include removing the shared foot-gun.

## Decision 4: Reuse shared batch DDL helpers

**Decision**: Use `migration_utils.drop_unique_constraint` for upgrade and `create_unique_constraint` for downgrade.

**Rationale**: These helpers own Alembic batch behavior, naming conventions, SQLite table recreation, MySQL's required unique-constraint type, and create-if-absent behavior.

**Alternatives considered**:

- Direct `op.drop_constraint`: rejected because it duplicates dialect handling and is unreliable for SQLite.
- Raw SQL per dialect: rejected because it is less portable and bypasses established project conventions.
- Add new shared migration helpers: rejected because existing helpers already cover the required operations.

## Decision 5: Propagate downgrade conflicts

**Decision**: Recreate `_customer_location_uc` on downgrade and allow database integrity/DDL errors to propagate when existing rows violate it.

**Rationale**: A visible failed downgrade is truthful and atomic; swallowing the error would repeat the original silent-divergence failure mode.

**Alternatives considered**:

- No-op downgrade: rejected by the clarified reversible-migration requirement.
- Delete or rewrite conflicting datasets automatically: rejected as destructive and beyond migration scope.
- Catch and log the error as success: rejected because the expected schema would not have been restored.

## Decision 6: Combine pure helper tests with real migration DDL tests

**Decision**: Add parametrized helper tests in `tests/unit_tests/utils/test_core.py` and a focused in-memory SQLite migration test in `tests/unit_tests/migrations/test_remove_customer_location_constraint.py`.

**Rationale**: Mocked inspector tests precisely pin list/set/tuple normalization. A real Alembic `MigrationContext` catches the original no-op and verifies batch constraint DDL, preservation of unrelated constraints, repeatability, cross-catalog rows, and downgrade behavior.

**Alternatives considered**:

- Mock migration operations only: rejected because it cannot prove reflected constraint discovery or SQLite table rebuild behavior.
- Full metadata-database upgrade tests only: rejected as slow and environmental for the core regression; normal PostgreSQL/MySQL migration CI remains the cross-backend gate.
- Test only the historical migration with a set input: rejected because it neither repairs deployed schemas nor directly covers the shared helper contract.

## Decision 7: Keep external behavior and schema additions out of scope

**Decision**: Do not add a four-column database constraint, change dataset commands/DAO behavior, or introduce an external contract.

**Rationale**: SC-112173 repairs the obsolete three-column rule. Catalog-aware duplicate and soft-delete guards already exist at the application layer, and the specification explicitly excludes a new four-column constraint.

**Alternatives considered**:

- Materialize the model's four-column constraint in migrated schemas: rejected as a separate schema-policy decision.
- Relax application-level soft-delete duplicate checks: rejected because those guards remain required.
