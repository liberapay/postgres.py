"""This is a PostgreSQL client library for humans.


Installation
------------

:mod:`postgres` is available on `GitHub`_ and on `PyPI`_::

    $ pip install postgres

:mod:`postgres` requires `psycopg2`_ version 2.8 or higher.

We currently `test <https://travis-ci.org/liberapay/postgres.py>`_ against
Python 3.6, 3.7, 3.8 and 3.9. We don't have a testing matrix for different
versions of :mod:`psycopg2` or PostgreSQL.

:mod:`postgres` is released under the `MIT license`_.


See Also
--------

The `sql`_ library provides a run / one / all API for any DB API 2.0 driver.

The `Records`_ library provides a similar top-level API, and integration with
SQLAlchemy and Tablib.


Tutorial
--------

Instantiate a :class:`Postgres` object when your application starts:

    >>> from postgres import Postgres
    >>> db = Postgres()

Use :meth:`~postgres.Postgres.run` to run SQL statements:

    >>> db.run("CREATE TABLE foo (bar text, baz int)")
    >>> db.run("INSERT INTO foo VALUES ('buz', 42)")
    >>> db.run("INSERT INTO foo VALUES ('bit', 537)")

Use :meth:`~postgres.Postgres.one` to run SQL and fetch one result or
:class:`None`:

    >>> db.one("SELECT * FROM foo WHERE bar='buz'")
    Record(bar='buz', baz=42)
    >>> db.one("SELECT * FROM foo WHERE bar='blam'")

Use :meth:`~postgres.Postgres.all` to run SQL and fetch all results:

    >>> db.all("SELECT * FROM foo ORDER BY bar")
    [Record(bar='bit', baz=537), Record(bar='buz', baz=42)]

If your queries return one column then you get just the value or a list of
values instead of a record or list of records:

    >>> db.one("SELECT baz FROM foo WHERE bar='buz'")
    42
    >>> db.all("SELECT baz FROM foo ORDER BY bar")
    [537, 42]

Jump ahead for the :ref:`orm-tutorial`.


.. _bind-parameters:

Bind Parameters
+++++++++++++++

In case you're not familiar with bind parameters in `DB-API 2.0`_, the basic
idea is that you put ``%(foo)s`` in your SQL strings, and then pass in a second
argument, a :class:`dict`, containing parameters that :mod:`psycopg2` (as
an implementation of DB-API 2.0) will bind to the query in a way that is safe
against `SQL injection`_. (This is inspired by old-style Python string
formatting, but it is not the same.)

    >>> db.one("SELECT * FROM foo WHERE bar=%(bar)s", {"bar": "buz"})
    Record(bar='buz', baz=42)

As a convenience, passing parameters as keyword arguments is also supported:

    >>> db.one("SELECT * FROM foo WHERE bar=%(bar)s", bar="buz")
    Record(bar='buz', baz=42)

Never build SQL strings out of user input!

Always pass user input as bind parameters!


Context Managers
++++++++++++++++

Eighty percent of your database usage should be covered by the simple
:meth:`~postgres.Postgres.run`, :meth:`~postgres.Postgres.one`,
:meth:`~postgres.Postgres.all` API introduced above. For the other 20%,
:mod:`postgres` provides two context managers for working at increasingly
lower levels of abstraction. The lowest level of abstraction in
:mod:`postgres` is a :mod:`psycopg2` `connection pool
<http://initd.org/psycopg/docs/pool.html>`_ that we configure and manage for
you. Everything in :mod:`postgres`, both the simple API and the context
managers, uses this connection pool.

Use the :func:`~postgres.Postgres.get_cursor` context manager to work
directly with a `simple cursor`_, while still taking advantage of connection
pooling and automatic transaction management:

    >>> with db.get_cursor() as cursor:
    ...     cursor.run("INSERT INTO foo VALUES ('blam')")
    ...     cursor.all("SELECT * FROM foo ORDER BY bar")
    ...
    [Record(bar='bit', baz=537), Record(bar='blam', baz=None), Record(bar='buz', baz=42)]

Note that other calls won't see the changes on your transaction until the end
of your code block, when the context manager commits the transaction for you::

    >>> db.run("DELETE FROM foo WHERE bar='blam'")
    >>> with db.get_cursor() as cursor:
    ...     cursor.run("INSERT INTO foo VALUES ('blam')")
    ...     db.all("SELECT * FROM foo ORDER BY bar")
    ...
    [Record(bar='bit', baz=537), Record(bar='buz', baz=42)]
    >>> db.all("SELECT * FROM foo ORDER BY bar")
    [Record(bar='bit', baz=537), Record(bar='blam', baz=None), Record(bar='buz', baz=42)]

The :func:`~postgres.Postgres.get_cursor` method gives you a context manager
that wraps a `simple cursor`_. It has :attr:`autocommit` turned off on its
connection.  If the block under management raises an exception, the connection
is rolled back. Otherwise it's committed. Use this when you want a series of
statements to be part of one transaction, but you don't need fine-grained
control over the transaction. For fine-grained control, use
:func:`~postgres.Postgres.get_connection` to get a connection straight from
the connection pool:

    >>> db.run("DELETE FROM foo WHERE bar='blam'")
    >>> with db.get_connection() as connection:
    ...     cursor = connection.cursor()
    ...     cursor.all("SELECT * FROM foo ORDER BY bar")
    ...
    [Record(bar='bit', baz=537), Record(bar='buz', baz=42)]

A connection gotten in this way will have :attr:`autocommit` turned off, and
it'll never be implicitly committed otherwise. It'll actually be rolled back
when you're done with it, so it's up to you to explicitly commit as needed.
This is the lowest-level abstraction that :mod:`postgres` provides,
basically just a pre-configured connection pool from :mod:`psycopg2` that
uses `simple cursors`_.

.. _simple cursor: #simple-cursors


The Postgres Object
-------------------

.. _psycopg2: http://initd.org/psycopg/
.. _GitHub: https://github.com/liberapay/postgres.py
.. _PyPI: https://pypi.python.org/pypi/postgres
.. _this advice: http://initd.org/psycopg/docs/usage.html#unicode-handling
.. _MIT license: https://github.com/liberapay/postgres.py/blob/master/LICENSE
.. _sql: https://pypi.python.org/pypi/sql
.. _Records: https://github.com/kennethreitz/records
.. _DB-API 2.0: http://www.python.org/dev/peps/pep-0249/
.. _SQL injection: http://en.wikipedia.org/wiki/SQL_injection

"""

from collections import OrderedDict, namedtuple
from inspect import isclass

import psycopg2
from psycopg2 import DataError, InterfaceError, ProgrammingError
from psycopg2.extensions import register_type
from psycopg2.extras import CompositeCaster
from psycopg2_pool import ThreadSafeConnectionPool

from postgres.cache import Cache
from postgres.context_managers import (
    ConnectionContextManager, CursorContextManager, CursorSubcontextManager,
    ConnectionCursorContextManager,
)
from postgres.cursors import (
    make_dict, make_namedtuple, return_tuple_as_is,
    BadBackAs, Row, SimpleCursorBase, SimpleNamedTupleCursor,
)
from postgres.orm import Model


__version__ = '4.0'


# Exceptions
# ==========

class NotASimpleCursor(Exception):
    def __str__(self):
        return "We can only work with subclasses of SimpleCursorBase, " \
               "{} doesn't fit the bill." \
               .format(self.args[0].__name__)

class NotAModel(Exception):
    def __str__(self):
        return "Only subclasses of postgres.orm.Model can be registered as " \
               "orm models. {} doesn't fit the bill." \
               .format(self.args[0])

class NoTypeSpecified(Exception):
    def __str__(self):
        return "You tried to register {} as an orm model, but it has no "\
               "typname attribute.".format(self.args[0].__name__)

class NoSuchType(Exception):
    def __str__(self):
        return "You tried to register an orm model for typname {}, but no "\
               "such type exists in the pg_type table of your database." \
               .format(self.args[0])

class AlreadyRegistered(Exception):
    def __str__(self):
        return "The model {} is already registered for the typname {}." \
               .format(self.args[0].__name__, self.args[1])

class NotRegistered(Exception):
    def __str__(self):
        return "The model {} is not registered.".format(self.args[0].__name__)


# The Main Event
# ==============

default_back_as_registry = {
    tuple: return_tuple_as_is,
    'tuple': return_tuple_as_is,
    dict: make_dict,
    'dict': make_dict,
    namedtuple: make_namedtuple,
    'namedtuple': make_namedtuple,
    Row: Row,
    'Row': Row,
}


class Postgres:
    """Interact with a `PostgreSQL <http://www.postgresql.org/>`_ database.

    :param str url: A ``postgres://`` URL or a `PostgreSQL connection string
        <http://www.postgresql.org/docs/current/static/libpq-connect.html>`_
    :param int minconn: The minimum size of the connection pool
    :param int maxconn: The maximum size of the connection pool
    :param int idle_timeout: How many seconds to wait before closing an idle
        connection.
    :param bool readonly: Setting this to :obj:`True` makes all connections and
        cursors readonly by default.
    :param type cursor_factory: The type of cursor to use when none is specified.
        Defaults to :class:`~postgres.cursors.SimpleNamedTupleCursor`.
    :param dict back_as_registry: Defines the values that can be passed to
        various methods as a :obj:`back_as` argument.
    :param type pool_class: The type of pool to use. Defaults to
        :class:`~psycopg2_pool.ThreadSafeConnectionPool`.
    :param Cache cache: An instance of :class:`postgres.cache.Cache`.

    This is the main object that :mod:`postgres` provides, and you should
    have one instance per process for each PostgreSQL database your process
    wants to talk to using this library.

    >>> import postgres
    >>> db = postgres.Postgres()

    (Note that importing :mod:`postgres` under Python 2 will cause the
    registration of typecasters with :mod:`psycopg2` to ensure that you get
    unicode instead of bytestrings for text data, according to `this advice`_.)

    The `libpq environment variables
    <https://www.postgresql.org/docs/current/libpq-envars.html>`_ are used to
    determine the connection parameters which are not explicitly passed in the
    :attr:`url` argument.

    When instantiated, this object creates a connection pool by calling
    `pool_class` with the `minconn`, `maxconn` and `idle_timeout` arguments.
    Everything this object provides runs through this connection pool. See the
    documentation of the :class:`~psycopg2_pool.ConnectionPool` class for more
    information.

    :attr:`cursor_factory` sets the default cursor that connections managed
    by this :class:`~postgres.Postgres` instance will use. See the
    :ref:`simple-cursors` documentation below for additional options. Whatever
    default you set here, you can override that default on a per-call basis by
    passing :attr:`cursor_factory` to :meth:`~postgres.Postgres.get_cursor`.

    The names in our simple API, :meth:`~postgres.Postgres.run`,
    :meth:`~postgres.Postgres.one`, and :meth:`~postgres.Postgres.all`,
    were chosen to be short and memorable, and to not directly conflict with
    the DB-API 2.0 :meth:`execute`, :meth:`fetchone`, and
    :meth:`fetchall` methods, which have slightly different semantics (under
    DB-API 2.0 you call :meth:`execute` on a cursor and then call one of the
    :meth:`fetch*` methods on the same cursor to retrieve records; with our
    simple API there is no second :meth:`fetch` step, and we also provide
    automatic dereferencing). See issues `16`_ and `20`_ for more of the
    rationale behind these names. The context managers on this class are named
    starting with :meth:`get_` to set them apart from the simple-case API.

    .. _16: https://github.com/liberapay/postgres.py/issues/16
    .. _20: https://github.com/liberapay/postgres.py/issues/20

    """

    def __init__(self, url='', minconn=1, maxconn=10, idle_timeout=600,
                 readonly=False, cursor_factory=SimpleNamedTupleCursor,
                 back_as_registry=default_back_as_registry,
                 pool_class=ThreadSafeConnectionPool, cache=None):

        self.readonly = readonly
        self.cache = cache or Cache()

        # Set up connection pool.
        # =======================

        if not issubclass(cursor_factory, SimpleCursorBase):
            raise NotASimpleCursor(cursor_factory)
        self.back_as_registry = back_as_registry
        self.default_cursor_factory = cursor_factory
        Connection = make_Connection(self)
        self.pool = pool_class(
            minconn=minconn, maxconn=maxconn, idle_timeout=idle_timeout,
            dsn=url, connection_factory=Connection,
        )

        # Set up orm helpers.
        # ===================

        self.model_registry = {}


    def run(self, sql, parameters=None, **kw):
        """Execute a query and discard any results.

        :returns: :const:`None`

        This is a convenience method, it passes all its arguments to
        :meth:`.SimpleCursorBase.run` like this::

            with self.get_cursor() as cursor:
                cursor.run(sql, parameters, **kw)

        """
        with self.get_cursor() as cursor:
            cursor.run(sql, parameters, **kw)


    def one(self, sql, parameters=None, **kw):
        """Execute a query and return a single result or a default value.

        :returns: a single record or value, or :attr:`default` (if
            :attr:`default` is not an :class:`Exception`)
        :raises: :exc:`~postgres.TooFew` or :exc:`~postgres.TooMany`,
            or :attr:`default` (if :attr:`default` is an :class:`Exception`)

        This is a convenience method, it passes all its arguments to
        :meth:`.SimpleCursorBase.one` like this::

            with self.get_cursor() as cursor:
                return cursor.one(sql, parameters, **kw)

        """
        with self.get_cursor() as cursor:
            return cursor.one(sql, parameters, **kw)


    def all(self, sql, parameters=None, **kw):
        """Execute a query and return all results.

        :returns: :class:`list` of records or :class:`list` of single values

        This is a convenience method, it passes all its arguments to
        :meth:`.SimpleCursorBase.all` like this::

            with self.get_cursor() as cursor:
                return cursor.all(sql, parameters, **kw)

        """
        with self.get_cursor() as cursor:
            return cursor.all(sql, parameters, **kw)


    def get_cursor(self, cursor=None, **kw):
        """Return a :class:`.CursorContextManager` that uses our connection pool.

        :param cursor: use an existing cursor instead of creating a new one
            (see the explanations and caveats below)
        :param kw: passed through to :class:`.CursorContextManager` or
            :class:`.CursorSubcontextManager`

        >>> with db.get_cursor() as cursor:
        ...     cursor.all("SELECT * FROM foo")
        ...
        [Record(bar='buz', baz=42), Record(bar='bit', baz=537)]

        You can use our simple :meth:`~.SimpleCursorBase.run`,
        :meth:`~.SimpleCursorBase.one`, :meth:`~.SimpleCursorBase.all`
        API, and you can also use the traditional DB-API 2.0 methods:

        >>> with db.get_cursor() as cursor:
        ...     cursor.execute("SELECT * FROM foo")
        ...     cursor.fetchall()
        ...
        [Record(bar='buz', baz=42), Record(bar='bit', baz=537)]

        By default the cursor will have :attr:`autocommit` turned off on its
        connection. If your code block inside the :obj:`with` statement
        raises an exception, the transaction will be rolled back. Otherwise,
        it'll be committed. The context manager closes the cursor when the
        block ends and puts the connection back in the pool. The cursor is
        destroyed after use.

        Use this when you want a series of statements to be part of one
        transaction, but you don't need fine-grained control over the
        transaction.

        The `cursor` argument enables running queries in a subtransaction. The
        major difference between a transaction and a subtransaction is that the
        changes in the database are **not** committed (nor rolled back) at the
        end of a subtransaction.

        The `cursor` argument is typically used inside functions which have an
        optional `cursor` argument themselves, like this:

        >>> def do_something(cursor=None):
        ...     with db.get_cursor(cursor=cursor) as c:
        ...         foo = c.one("...")
        ...         # ... do more stuff
        ...     # Warning: At this point you cannot assume that the changes have
        ...     # been committed, so don't do anything that could be problematic
        ...     # or incoherent if the transaction ends up being rolled back.

        When the `cursor` argument isn't :obj:`None`, the `back_as` argument is
        still supported, but the other arguments (`autocommit`, `readonly`, and
        the arguments of the :meth:`psycopg2:connection.cursor` method) are
        **not** supported.

        """
        if cursor:
            return CursorSubcontextManager(cursor, **kw)
        kw.setdefault('readonly', self.readonly)
        return CursorContextManager(self.pool, **kw)


    def get_connection(self, **kw):
        """Return a :class:`~postgres.ConnectionContextManager` that uses
        our connection pool.

        :param kw: passed through to :class:`.ConnectionContextManager`

        >>> with db.get_connection() as connection:
        ...     cursor = connection.cursor()
        ...     cursor.all("SELECT * FROM foo")
        ...
        [Record(bar='buz', baz=42), Record(bar='bit', baz=537)]

        Use this when you want to take advantage of connection pooling and our
        simple :meth:`~postgres.Postgres.run`,
        :meth:`~postgres.Postgres.one`, :meth:`~postgres.Postgres.all`
        API, but otherwise need full control, for example, to do complex things
        with transactions.

        Cursors from connections gotten this way also support the traditional
        DB-API 2.0 methods:

        >>> with db.get_connection() as connection:
        ...     cursor = connection.cursor()
        ...     cursor.execute("SELECT * FROM foo")
        ...     cursor.fetchall()
        ...
        [Record(bar='buz', baz=42), Record(bar='bit', baz=537)]

        """
        kw.setdefault('readonly', self.readonly)
        return ConnectionContextManager(self.pool, **kw)


    def register_model(self, ModelSubclass, typname=None):
        """Register an ORM model.

        :param ModelSubclass: the :class:`~postgres.orm.Model` subclass to
            register with this :class:`~postgres.Postgres` instance

        :param typname: a string indicating the Postgres type to register this
            model for (``typname``, without an "e," is the name of the relevant
            column in the underlying ``pg_type`` table). If :class:`None`,
            we'll look for :attr:`ModelSubclass.typname`.

        :raises: :exc:`~postgres.NotAModel`,
            :exc:`~postgres.NoTypeSpecified`,
            :exc:`~postgres.NoSuchType`,
            :exc:`~postgres.AlreadyRegistered`

        .. note::

            See the :mod:`~postgres.orm` docs for instructions on
            subclassing :class:`~postgres.orm.Model`.

        """
        self._validate_model_subclass(ModelSubclass)

        if typname is None:
            typname = getattr(ModelSubclass, 'typname', None)
            if typname is None:
                raise NoTypeSpecified(ModelSubclass)

        if typname in self.model_registry:
            existing_model = self.model_registry[typname]
            raise AlreadyRegistered(existing_model, typname)

        # register a composite
        caster = ModelCaster._from_db(self, typname, ModelSubclass)
        register_type(caster.typecaster)
        if caster.array_typecaster is not None:
            register_type(caster.array_typecaster)

        self.model_registry[typname] = ModelSubclass


    def unregister_model(self, ModelSubclass):
        """Unregister an ORM model.

        :param ModelSubclass: the :class:`~postgres.orm.Model` subclass to
            unregister
        :raises: :exc:`~postgres.NotAModel`,
            :exc:`~postgres.NotRegistered`

        If :class:`ModelSubclass` is registered for multiple types, it is
        unregistered for all of them.

        """
        keys = self.check_registration(ModelSubclass)
        for key in keys:
            del self.model_registry[key]


    def check_registration(self, ModelSubclass, include_subsubclasses=False):
        """Check whether an ORM model is registered.

        :param ModelSubclass: the :class:`~postgres.orm.Model` subclass to
            check for
        :param bool include_subsubclasses: whether to also check for subclasses
            of :class:`ModelSubclass` or just :class:`ModelSubclass`
            itself

        :returns: the type names (:attr:`typname`) for which this model is registered
        :rtype: list

        :raises: :exc:`.NotAModel`, :exc:`.NotRegistered`

        """
        self._validate_model_subclass(ModelSubclass)

        if include_subsubclasses:
            filt = lambda v: v is ModelSubclass or issubclass(ModelSubclass, v)
        else:
            filt = lambda v: v is ModelSubclass
        keys = [k for k, v in self.model_registry.items() if filt(v)]
        if not keys:
            raise NotRegistered(ModelSubclass)
        return keys


    def _validate_model_subclass(self, ModelSubclass):
        if not isclass(ModelSubclass) or not issubclass(ModelSubclass, Model):
            raise NotAModel(ModelSubclass)


# Class Factories
# ===============

def make_Connection(postgres):
    """Define and return a subclass of :class:`psycopg2.extensions.connection`.

    :param postgres: the :class:`~postgres.Postgres` instance to bind to
    :returns: a :class:`Connection` class

    The class defined and returned here will be linked to the instance of
    :class:`~postgres.Postgres` that is passed in as :attr:`postgres`,
    which will use this class as the :attr:`connection_factory` for its
    connection pool.

    The :meth:`cursor` method of this class accepts a :attr:`back_as`
    keyword argument. By default the valid values for :attr:`back_as` are
    :class:`tuple`, :class:`namedtuple`, :class:`dict` and :class:`Row` (or the
    strings ``tuple``, ``namedtuple``, ``dict``, ``Row``), and :class:`None`.
    If :attr:`back_as` is not :class:`None`, then it modifies the default row
    type of the cursor.

    We also set client encoding to ``UTF-8``.

    """
    class Connection(psycopg2.extensions.connection):

        back_as_registry = postgres.back_as_registry

        def __init__(self, *a, **kw):
            psycopg2.extensions.connection.__init__(self, *a, **kw)
            self.set_client_encoding('UTF-8')
            self.postgres = postgres
            self.cursor_factory = self.postgres.default_cursor_factory

        def __exit__(self, exc_type, exc_val, exc_tb):
            """Commit the changes, or roll them back if there's an exception.

            This method doesn't close the connection.
            """
            if self.autocommit:
                pass
            elif exc_type is None and self.readonly is False:
                self.commit()
            else:
                try:
                    self.rollback()
                except InterfaceError:
                    pass

        def cursor(self, back_as=None, **kw):
            cursor = super(Connection, self).cursor(**kw)
            if back_as is not None:
                if back_as not in self.back_as_registry:
                    raise BadBackAs(back_as, self.back_as_registry)
                cursor.back_as = back_as
            return cursor

        def get_cursor(self, cursor=None, **kw):
            if cursor:
                if cursor.connection is not self:
                    raise ValueError(
                        "the provided cursor is from a different connection"
                    )
                return CursorSubcontextManager(cursor, **kw)
            return ConnectionCursorContextManager(self, **kw)

    return Connection


class ModelCaster(CompositeCaster):
    """A :class:`~psycopg2.extras.CompositeCaster` subclass for :class:`.Model`.
    """

    @classmethod
    def _from_db(cls, db, typname, ModelSubclass):
        # Override to set custom attributes.
        with db.get_cursor(autocommit=True, readonly=True) as cursor:
            try:
                caster = super(ModelCaster, cls)._from_db(typname, cursor)
            except ProgrammingError:
                raise NoSuchType(typname)
        caster.db = ModelSubclass.db = db
        caster.ModelSubclass = ModelSubclass
        ModelSubclass.attnames = OrderedDict.fromkeys(caster.attnames)
        return caster

    def parse(self, s, curs, retry=True):
        # Override to protect against some race conditions:
        #   https://github.com/liberapay/postgres.py/issues/26
        try:
            return super(ModelCaster, self).parse(s, curs)
        except (DataError, ValueError):
            if not retry:
                raise
            # Re-fetch the type info and retry once
            self._refetch_type_info(curs)
            return self.parse(s, curs, False)

    def make(self, values):
        """"""
        # Override to use ModelSubclass instead of a namedtuple class.
        return self.ModelSubclass(values)

    def _create_type(self, name, attnames):
        # Override to avoid the creation of an unwanted namedtuple class.
        pass

    def _refetch_type_info(self, curs):
        """Given a cursor, update the current object with a fresh type definition.
        """
        new_self = self._from_db(self.db, self.name, self.ModelSubclass)
        self.__dict__.update(new_self.__dict__)


if __name__ == '__main__':  # pragma: no cover
    db = Postgres()
    db.run("DROP SCHEMA IF EXISTS public CASCADE")
    db.run("CREATE SCHEMA public")
    import doctest
    doctest.testmod()
