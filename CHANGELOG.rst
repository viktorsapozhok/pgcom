ChangeLog
=========

0.1.5 (2019-03-16)
------------------

* Fixed data formatting on integer columns with missed values (`#5 <https://github.com/viktorsapozhok/pgcom/issues/5>`_)

0.1.4 (2019-01-21)
------------------

* Changed ``where`` argument type in :func:`~pgcom.commuter.Commuter.resolve_primary_conflicts` from positional to optional
* Changed ``where`` argument type in :func:`~pgcom.commuter.Commuter.resolve_foreign_conflicts` from positional to optional
* Fixed bug in copying from DataFrame with incomplete set of columns (`#3 <https://github.com/viktorsapozhok/pgcom/issues/3>`_)
* Added new test

0.1.3 (2019-01-19)
------------------

* Added support for the missing SQLAlchemy dependency (`#1 <https://github.com/viktorsapozhok/pgcom/issues/1>`_)
* Added :func:`~pgcom.commuter.Commuter._execute` (`#2 <https://github.com/viktorsapozhok/pgcom/issues/2>`_)
* Added pending transaction handler to :func:`~pgcom.commuter.Commuter.copy_from`
* Raised :class:`~pgcom.commuter.exc.ExecutionError` when execute command fails
* Replaced :func:`pandas.to_sql` in :func:`~pgcom.commuter.Commuter.insert` by :func:`psycopg.execute_batch`
* Changed sqlalchemy engine url builder
* Added new tests

0.1.2 (2019-01-16)
------------------
* Changed :func:`~pgcom.commuter.Commuter.select` method
* Changed :func:`~pgcom.commuter.Commuter.insert` method
* Fixed exception in :func:`~pgcom.commuter.Commuter.copy_from`

0.1.1 (2019-01-10)
------------------

* Added :class:`~pgcom.listener.Listener` class
* Added ``fix_schema`` decorator
* Added :func:`~pgcom.commuter.Commuter.select_one` method
* Added ``where`` argument to :func:`~pgcom.commuter.Commuter.resolve_foreign_conflicts` method
* Added ``where`` argument to :func:`~pgcom.commuter.Commuter.copy_from` method
* Added :func:`~pgcom.commuter.Commuter._table_columns` method
* Added :func:`~pgcom.commuter.Commuter._primary_key` method
* Added :func:`~pgcom.commuter.Commuter._foreign_key` method
* Moved sql queries to queries.py
* Deprecated ``f_key``, ``filter_col`` arguments of :func:`~pgcom.commuter.Commuter.resolve_foreign_conflicts` method
* Deprecated ``p_key``, ``filter_col`` argument of :func:`~pgcom.commuter.Commuter.resolve_primary_conflicts` method
* Deprecated ``return_scalar`` argument of :func:`~pgcom.commuter.Commuter.select` method
* Deprecated :func:`~pgcom.commuter.Commuter.get_columns` method

0.1.0 (2019-01-02)
------------------

Pre-release