ChangeLog
=========

0.2.6 (2021-06-28)
------------------

* Fixed :method:`~pgcom.commuter.Commuter.copy_from` to make it compatible with `psycopg2 2.9` release.

0.2.5 (2021-03-11)
------------------

* Fixed `#17 <https://github.com/viktorsapozhok/pgcom/issues/17>`_
* Fixed `#18 <https://github.com/viktorsapozhok/pgcom/issues/18>`_
* Fixed `#19 <https://github.com/viktorsapozhok/pgcom/issues/19>`_

0.2.4 (2021-02-28)
------------------

* Fixed issue with custom placeholders in :func:`~pgcom.commuter.Commuter.insert` method

0.2.3 (2021-02-27)
------------------

* Deprecated ``schema`` parameter
* Added :method:`~pgcom.commuter.Commuter.is_entry_exist` method
* Added :method:`~pgcom.commuter.Commuter.delete_entry` method
* Added :method:`~pgcom.commuter.Commuter.encode_category` method
* Added :method:`~pgcom.commuter.Commuter.make_where` method
* Updated :method:`~pgcom.commuter.Commuter.insert` method to support customized placeholders

0.2.2 (2020-07-27)
------------------

* Added adaptation for numpy ``int64`` and ``float64``.

0.2.1 (2020-07-13)
------------------

* Added :class:`~pgcom.base.BaseConnector`
* Added :class:`~pgcom.base.BaseCommuter`

0.2.0 (2020-07-04)
------------------

* Added support of ``psycopg2.sql.Composed`` arguments to :class:`~pgcom.commuter.Commuter` methods
* Fixed query parameters issues in :class:`~pgcom.commuter.Commuter` methods (`#11 <https://github.com/viktorsapozhok/pgcom/issues/11>`__)
* Made connect args compatible with ``psycopg2.connect``
* Added ``pre_ping`` and ``max_connects`` options to :class:`~pgcom.commuter.Connector` (`#12 <https://github.com/viktorsapozhok/pgcom/issues/12>`__, `#14 <https://github.com/viktorsapozhok/pgcom/issues/14>`__)
* Added connection pooling to :class:`~pgcom.commuter.Connector`
* Deprecated SQLAlchemy dependencies

0.1.7 (2020-05-31)
------------------

* Fixed `#9 <https://github.com/viktorsapozhok/pgcom/issues/9>`_

0.1.6 (2020-05-28)
------------------

* Updated :method:`~pgcom.commuter.Commuter._format_data` to fix text fields with comma

0.1.5 (2020-03-16)
------------------

* Fixed data formatting on integer columns with missed values (`#5 <https://github.com/viktorsapozhok/pgcom/issues/5>`_)

0.1.4 (2020-01-21)
------------------

* Changed ``where`` argument type in :method:`~pgcom.commuter.Commuter.resolve_primary_conflicts` from positional to optional
* Changed ``where`` argument type in :method:`~pgcom.commuter.Commuter.resolve_foreign_conflicts` from positional to optional
* Fixed bug in copying from DataFrame with incomplete set of columns (`#3 <https://github.com/viktorsapozhok/pgcom/issues/3>`_)
* Added new test

0.1.3 (2020-01-19)
------------------

* Added support for the missing SQLAlchemy dependency (`#1 <https://github.com/viktorsapozhok/pgcom/issues/1>`_)
* Added :method:`~pgcom.commuter.Commuter._execute` (`#2 <https://github.com/viktorsapozhok/pgcom/issues/2>`_)
* Added pending transaction handler to :func:`~pgcom.commuter.Commuter.copy_from`
* Raised :class:`~pgcom.commuter.exc.ExecutionError` when execute command fails
* Replaced :func:`pandas.to_sql` in :method:`~pgcom.commuter.Commuter.insert` by :func:`psycopg.execute_batch`
* Changed sqlalchemy engine url builder
* Added new tests

0.1.2 (2020-01-16)
------------------
* Changed :method:`~pgcom.commuter.Commuter.select` method
* Changed :method:`~pgcom.commuter.Commuter.insert` method
* Fixed exception in :method:`~pgcom.commuter.Commuter.copy_from`

0.1.1 (2020-01-10)
------------------

* Added :class:`~pgcom.listener.Listener` class
* Added ``fix_schema`` decorator
* Added :method:`~pgcom.commuter.Commuter.select_one` method
* Added ``where`` argument to :method:`~pgcom.commuter.Commuter.resolve_foreign_conflicts` method
* Added ``where`` argument to :method:`~pgcom.commuter.Commuter.copy_from` method
* Added :method:`~pgcom.commuter.Commuter._table_columns` method
* Added :method:`~pgcom.commuter.Commuter._primary_key` method
* Added :method:`~pgcom.commuter.Commuter._foreign_key` method
* Moved sql queries to queries.py
* Deprecated ``f_key``, ``filter_col`` arguments of :method:`~pgcom.commuter.Commuter.resolve_foreign_conflicts` method
* Deprecated ``p_key``, ``filter_col`` argument of :method:`~pgcom.commuter.Commuter.resolve_primary_conflicts` method
* Deprecated ``return_scalar`` argument of :method:`~pgcom.commuter.Commuter.select` method
* Deprecated :method:`~pgcom.commuter.Commuter.get_columns` method

0.1.0 (2020-01-02)
------------------

Pre-release