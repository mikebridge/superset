# Data Model: Remove Legacy Dataset Location Constraint

This feature changes metadata schema constraints only. It does not add or remove ORM entities, columns, API fields, or persisted domain records.

## Dataset Record

Relevant existing identity fields on the `tables` metadata table:

| Field | Role in identity | Change |
|---|---|---|
| `database_id` | Owning database connection | None |
| `catalog` | Catalog namespace | None |
| `schema` | Schema namespace | None |
| `table_name` | Physical table name | None |
| `deleted_at` | Dataset lifecycle state | None |

Application-level catalog normalization, duplicate prevention, soft deletion, and restoration remain unchanged.

## Constraint States

### Pre-upgrade: affected migrated schema

- A unique constraint covers exactly `database_id`, `schema`, and `table_name`.
- Its physical name may be `_customer_location_uc` or another reflected name.
- It incorrectly treats catalog-distinct datasets as the same database identity.

### Post-upgrade

- No unique constraint covers exactly the legacy three-column set.
- Constraints with any different column set remain unchanged.
- Catalog-distinct records are not rejected by the removed legacy rule.
- Application-level dataset identity and lifecycle validation still apply.

### Post-downgrade

- `_customer_location_uc` again covers `database_id`, `schema`, and `table_name`.
- If existing rows conflict under that rule, restoration fails visibly and the downgrade does not report success.

## State Transitions

```text
legacy constraint present ──upgrade──> legacy constraint absent
         │                                  │
         └──── absent-safe no-op             └──downgrade──> legacy constraint restored
                                                        └── conflict: downgrade fails
```

## Validation Rules

- Constraint identity is the exact unordered set of reflected column names.
- Subset and superset constraints are unrelated and must be preserved.
- List, tuple, and set representations of the same requested columns have equal discovery semantics.
- Repeating upgrade discovery after removal returns no match and performs no DDL.
- Downgrade does not delete, merge, or rewrite conflicting dataset rows.
