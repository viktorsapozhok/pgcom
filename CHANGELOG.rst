ChangeLog
=========

0.1.1 (2019-01-10)
------------------

* Added ``Listener`` class
* Added ``fix_schema`` decorator
* Added :func:`Commuter.select_one` method
* Added ``where`` argument to :func:`Commuter.resolve_foreign_conflicts` method
* Added ``where`` argument to :func:`Commuter.copy_from` method
* Added :func:`Commuter._table_columns` method
* Added :func:`Commuter._primary_key` method
* Added :func:`Commuter._foreign_key` method
* Moved sql queries to ``queries.py``
* Deprecated ``f_key``, ``filter_col`` arguments of :func:`Commuter.resolve_foreign_conflicts` method
* Deprecated ``p_key``, ``filter_col`` argument of :func:`Commuter.resolve_primary_conflicts` method
* Deprecated ``return_scalar`` argument of :func:`Commuter.select` method
* Deprecated :func:`Commuter.get_columns` method

0.1.0 (2019-01-02)
------------------

Pre-release