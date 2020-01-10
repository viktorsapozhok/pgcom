# ChangeLog

## [0.1.1] - 2019-01-10
### Added
- `Listener` class
- `fix_schema` decorator
- `select_one` method
- `where` argument to `resolve_foreign_conflicts` method
- `where` argument to `copy_from` method
- `_table_columns` method
- `_primary_key` method
- `_foreign_key` method

### Chaged
- Moved sql queries to `queries.py`

### Deprecated
- `f_key`, `filter_col` arguments of `resolve_foreign_conflicts` method
- `p_key`, `filter_col` argument of `resolve_primary_conflicts` method
- `return_scalar` argument of `select` method
- `get_columns` method

## [0.1.0] - 2019-01-02

- Birth