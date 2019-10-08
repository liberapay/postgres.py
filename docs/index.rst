Postgres.py
===========

.. automodule:: postgres
    :members:
    :member-order: bysource


The Context Managers
--------------------

.. automodule:: postgres.context_managers
    :members:
    :member-order: bysource


.. _simple-cursors:

Simple Cursors
--------------

.. automodule:: postgres.cursors
    :members:
    :member-order: bysource


An Object-Relational Mapper (ORM)
---------------------------------

.. automodule:: postgres.orm
    :members:
    :member-order: bysource


Changelog
---------

**3.0.0 (Oct 19, 2019)**

- the :class:`ReadOnly` exception has been renamed to :class:`ReadOnlyAttribute`, and the :attr:`_read_only_attributes` attribute of the :class:`~postgres.orm.Model` class has been renamed to :attr:`attnames` (:pr:`91`)
- the ORM has been optimized and now supports `__slots__ <https://docs.python.org/3/reference/datamodel.html#slots>`_ (:pr:`88`)
- **BREAKING**: the :meth:`~postgres.Postgres.check_registration` method now always returns a list (:pr:`87`)
- PostgreSQL versions older than 9.2 are no longer supported (:pr:`83`)
- idle connections are now kept open for up to 10 minutes by default (:pr:`81`)
- the methods :meth:`~postgres.Postgres.run`, :meth:`~postgres.Postgres.one` and :meth:`~postgres.Postgres.all` now support receiving query paramaters as keyword arguments (:pr:`79`)
- **BREAKING**: the methods :meth:`~postgres.Postgres.run`, :meth:`~postgres.Postgres.one` and :meth:`~postgres.Postgres.all` no longer pass extra arguments to :meth:`~postgres.Postgres.get_cursor` (:pr:`79` and :pr:`67`)
- subtransactions are now supported (:pr:`78` and :pr:`90`)
- **BREAKING**: single-column rows aren't unpacked anymore when the `back_as` argument is provided (:pr:`77`)
- the cursor methods now also support the `back_as` argument (:pr:`77`)
- a new row type and cursor subclass are now available, see :class:`~postgres.cursors.SimpleRowCursor` for details (:pr:`75`)
- the ORM now supports non-default schemas (:pr:`74`)
- connections now also have a :meth:`get_cursor` method (:pr:`73` and :pr:`82`)
- the values accepted by the `back_as` argument can now be customized (:pr:`72`)
- the :meth:`~postgres.Postgres.one` and :meth:`~postgres.Postgres.all` no longer fail when a row is made up of a single column named :attr:`values` (:pr:`71`)
- any :exc:`~psycopg2.InterfaceError` exception raised during an automatic rollback is now suppressed (:pr:`70`)
- the :meth:`~postgres.Postgres.get_cursor` method has two new optional arguments: `autocommit` and `readonly` (:pr:`69`)
- :class:`~postgres.Postgres` objects now have a :attr:`readonly` attribute (:pr:`69`)
- the `url` argument is no longer required when creating a :class:`Postgres` object (:pr:`68`)

**2.2.2 (Sep 12, 2018)**

- the only dependency was changed from ``psycopg2 >= 2.5.0`` to ``psycopg2-binary >= 2.7.5`` (:pr:`64`)
- the license was changed from CC0 to MIT (:pr:`62`)

**2.2.1 (Oct 10, 2015)**

- a bug in the URL-to-DSN conversion function was fixed (:pr:`53`)

**2.2.0 (Sep 12, 2015)**

- the ORM was modified to detect some schema changes (:pr:`43`)
