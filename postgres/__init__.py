"""This is a PostgreSQL client library for humans.


Installation
------------

:py:mod:`postgres` is available on `GitHub`_ and on `PyPI`_::

    $ pip install postgres

:py:mod:`postgres` requires `psycopg2`_ version 2.5 or higher.

We `test <https://travis-ci.org/gratipay/postgres.py>`_ against Python 2.6, 2.7,
3.2, and 3.3. We don't yet have a testing matrix for different versions of
:py:mod:`psycopg2` or PostgreSQL.

:py:mod:`postgres` is in the `public domain`_.


See Also
--------

The `sql`_ library provides a run / one / all API for any DB API 2.0 driver.

The `Records`_ library provides a similar top-level API, and integration with
SQLAlchemy and Tablib.


Tutorial
--------

Instantiate a :py:class:`Postgres` object when your application starts:

    >>> from postgres import Postgres
    >>> db = Postgres("postgres://jrandom@localhost/test")

Use :py:meth:`~postgres.Postgres.run` to run SQL statements:

    >>> db.run("CREATE TABLE foo (bar text, baz int)")
    >>> db.run("INSERT INTO foo VALUES ('buz', 42)")
    >>> db.run("INSERT INTO foo VALUES ('bit', 537)")

Use :py:meth:`~postgres.Postgres.one` to run SQL and fetch one result or
:py:class:`None`:

    >>> db.one("SELECT * FROM foo WHERE bar='buz'")
    Record(bar='buz', baz=42)
    >>> db.one("SELECT * FROM foo WHERE bar='blam'")

Use :py:meth:`~postgres.Postgres.all` to run SQL and fetch all results:

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
argument, a :py:class:`dict`, containing parameters that :py:mod:`psycopg2` (as
an implementation of DB-API 2.0) will bind to the query in a way that is safe
against `SQL injection`_. (This is inspired by old-style Python string
formatting, but it is not the same.)

    >>> db.one("SELECT * FROM foo WHERE bar=%(bar)s", {"bar": "buz"})
    Record(bar='buz', baz=42)

Never build SQL strings out of user input!

Always pass user input as bind parameters!


Context Managers
++++++++++++++++

Eighty percent of your database usage should be covered by the simple
:py:meth:`~postgres.Postgres.run`, :py:meth:`~postgres.Postgres.one`,
:py:meth:`~postgres.Postgres.all` API introduced above. For the other 20%,
:py:mod:`postgres` provides two context managers for working at increasingly
lower levels of abstraction. The lowest level of abstraction in
:py:mod:`postgres` is a :py:mod:`psycopg2` `connection pool
<http://initd.org/psycopg/docs/pool.html>`_ that we configure and manage for
you. Everything in :py:mod:`postgres`, both the simple API and the context
managers, uses this connection pool.

Use the :py:func:`~postgres.Postgres.get_cursor` context manager to work
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

The :py:func:`~postgres.Postgres.get_cursor` method gives you a context manager
that wraps a `simple cursor`_. It has :py:attr:`autocommit` turned off on its
connection.  If the block under management raises an exception, the connection
is rolled back. Otherwise it's committed. Use this when you want a series of
statements to be part of one transaction, but you don't need fine-grained
control over the transaction. For fine-grained control, use
:py:func:`~postgres.Postgres.get_connection` to get a connection straight from
the connection pool:

    >>> db.run("DELETE FROM foo WHERE bar='blam'")
    >>> with db.get_connection() as connection:
    ...     cursor = connection.cursor()
    ...     cursor.all("SELECT * FROM foo ORDER BY bar")
    ...
    [Record(bar='bit', baz=537), Record(bar='buz', baz=42)]

A connection gotten in this way will have :py:attr:`autocommit` turned off, and
it'll never be implicitly committed otherwise. It'll actually be rolled back
when you're done with it, so it's up to you to explicitly commit as needed.
This is the lowest-level abstraction that :py:mod:`postgres` provides,
basically just a pre-configured connection pool from :py:mod:`psycopg2` that
uses `simple cursors`_.

.. _simple cursor: #simple-cursors


The Postgres Object
-------------------

.. _psycopg2: http://initd.org/psycopg/
.. _GitHub: https://github.com/gratipay/postgres.py
.. _PyPI: https://pypi.python.org/pypi/postgres
.. _this advice: http://initd.org/psycopg/docs/usage.html#unicode-handling
.. _public domain: http://creativecommons.org/publicdomain/zero/1.0/
.. _sql: https://pypi.python.org/pypi/sql
.. _Records: https://github.com/kennethreitz/records
.. _DB-API 2.0: http://www.python.org/dev/peps/pep-0249/
.. _SQL injection: http://en.wikipedia.org/wiki/SQL_injection

"""
from __future__ import absolute_import, division, print_function, unicode_literals

import sys
if sys.version_info[0] == 2:    # Python 2
    import urlparse

    # "Note: In Python 2, if you want to uniformly receive all your database
    # input in Unicode, you can register the related typecasters globally as
    # soon as Psycopg is imported."
    #   -- http://initd.org/psycopg/docs/usage.html#unicode-handling

    import psycopg2.extensions
    psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
    psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)

else:                           # Python 3
    import urllib.parse as urlparse
from collections import namedtuple

import psycopg2
from inspect import isclass
from postgres.context_managers import ConnectionContextManager
from postgres.context_managers import CursorContextManager
from postgres.cursors import SimpleTupleCursor, SimpleNamedTupleCursor
from postgres.cursors import SimpleDictCursor, SimpleCursorBase
from postgres.orm import Model
from psycopg2 import DataError
from psycopg2.extras import register_composite, CompositeCaster
from psycopg2.pool import ThreadedConnectionPool as ConnectionPool


__version__ = '2.2.2'


# A Helper
# ========
# Heroku gives us an URL, psycopg2 wants a DSN. Convert!

if 'postgres' not in urlparse.uses_netloc:
    # Teach urlparse about postgres:// URLs.
    urlparse.uses_netloc.append('postgres')

def url_to_dsn(url):
    parsed = urlparse.urlparse(url)
    dbname = parsed.path[1:] # /foobar
    user = parsed.username
    password = parsed.password
    host = parsed.hostname
    port = parsed.port
    if port is None:
        port = '5432' # postgres default port

    dsn = "dbname=" + dbname
    if user is not None:
        dsn += " user=" + user
    if password is not None:
        dsn += " password=" + password
    if host is not None:
        dsn += " host=" + host
    if port is not None:
        dsn += " port=" + str(port)

    return dsn


# Exceptions
# ==========

class NotASimpleCursor(Exception):
    def __str__(self):
        return "We can only work with subclasses of postgres.cursors." \
               "SimpleCursorBase. {} doesn't fit the bill." \
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

class BadBackAs(Exception):
    def __str__(self):
        return "Bad back_as: {}. Available back_as values are: tuple, " \
               "namedtuple, dict, or None (to use the default)." \
               .format(self.args[0])


# The Main Event
# ==============

class Postgres(object):
    """Interact with a `PostgreSQL <http://www.postgresql.org/>`_ database.

    :param unicode url: A ``postgres://`` URL or a `PostgreSQL connection
        string
        <http://www.postgresql.org/docs/current/static/libpq-connect.html>`_
    :param int minconn: The minimum size of the connection pool
    :param int maxconn: The maximum size of the connection pool
    :param cursor_factory: Defaults to
        :py:class:`~postgres.cursors.SimpleNamedTupleCursor`

    This is the main object that :py:mod:`postgres` provides, and you should
    have one instance per process for each PostgreSQL database your process
    wants to talk to using this library.

    >>> import postgres
    >>> db = postgres.Postgres("postgres://jrandom@localhost/test")

    (Note that importing :py:mod:`postgres` under Python 2 will cause the
    registration of typecasters with :py:mod:`psycopg2` to ensure that you get
    unicode instead of bytestrings for text data, according to `this advice`_.)

    When instantiated, this object creates a `thread-safe connection pool
    <http://initd.org/psycopg/docs/pool.html#psycopg2.pool.ThreadedConnectionPool>`_,
    which opens :py:attr:`minconn` connections immediately, and up to
    :py:attr:`maxconn` according to demand. Everything this object provides
    runs through this connection pool.

    :py:attr:`cursor_factory` sets the default cursor that connections managed
    by this :py:class:`~postgres.Postgres` instance will use. See the
    :ref:`simple-cursors` documentation below for additional options. Whatever
    default you set here, you can override that default on a per-call basis by
    passing :py:attr:`back_as` or :py:attr:`cursor_factory` to
    :py:meth:`~postgres.Postgres.one`, :py:meth:`~postgres.Postgres.all`, and
    :py:meth:`~postgres.Postgres.get_cursor`.

    The names in our simple API, :py:meth:`~postgres.Postgres.run`,
    :py:meth:`~postgres.Postgres.one`, and :py:meth:`~postgres.Postgres.all`,
    were chosen to be short and memorable, and to not directly conflict with
    the DB-API 2.0 :py:meth:`execute`, :py:meth:`fetchone`, and
    :py:meth:`fetchall` methods, which have slightly different semantics (under
    DB-API 2.0 you call :py:meth:`execute` on a cursor and then call one of the
    :py:meth:`fetch*` methods on the same cursor to retrieve records; with our
    simple API there is no second :py:meth:`fetch` step, and we also provide
    automatic dereferencing). See issues `16`_ and `20`_ for more of the
    rationale behind these names. The context managers on this class are named
    starting with :py:meth:`get_` to set them apart from the simple-case API.

    .. _16: https://github.com/gratipay/postgres.py/issues/16
    .. _20: https://github.com/gratipay/postgres.py/issues/20

    """

    def __init__(self, url, minconn=1, maxconn=10, \
                                        cursor_factory=SimpleNamedTupleCursor):
        if url.startswith("postgres://"):
            dsn = url_to_dsn(url)
        else:
            dsn = url


        # Set up connection pool.
        # =======================

        if not issubclass(cursor_factory, SimpleCursorBase):
            raise NotASimpleCursor(cursor_factory)
        self.default_cursor_factory = cursor_factory
        Connection = make_Connection(self)
        self.pool = ConnectionPool( minconn=minconn
                                  , maxconn=maxconn
                                  , dsn=dsn
                                  , connection_factory=Connection
                                   )

        # Set up orm helpers.
        # ===================

        self.model_registry = {}
        self.DelegatingCaster = make_DelegatingCaster(self)


    def run(self, sql, parameters=None, *a, **kw):
        """Execute a query and discard any results.

        :param string sql: the SQL statement to execute
        :param parameters: the `bind parameters`_ for the SQL statement
        :type parameters: dict or tuple
        :param a: passed through to :py:meth:`~postgres.Postgres.get_cursor`
        :param kw: passed through to :py:meth:`~postgres.Postgres.get_cursor`
        :returns: :py:const:`None`

        .. _bind parameters: #bind-parameters

        This is a convenience method. Here is what it does::

            with self.get_cursor(*a, **kw) as cursor:
                cursor.run(sql, parameters)

        Use it like this:

        >>> db.run("DROP TABLE IF EXISTS foo CASCADE")
        >>> db.run("CREATE TABLE foo (bar text, baz int)")
        >>> db.run("INSERT INTO foo VALUES ('buz', 42)")
        >>> db.run("INSERT INTO foo VALUES ('bit', 537)")

        """
        with self.get_cursor(*a, **kw) as cursor:
            cursor.run(sql, parameters)


    def one(self, sql, parameters=None, default=None, back_as=None, *a, **kw):
        """Execute a query and return a single result or a default value.

        :param string sql: the SQL statement to execute
        :param parameters: the `bind parameters`_ for the SQL statement
        :type parameters: dict or tuple
        :param default: the value to return or raise if no results are found
        :param back_as: the type of record to return
        :type back_as: type or string
        :param a: passed through to :py:meth:`~postgres.Postgres.get_cursor`
        :param kw: passed through to :py:meth:`~postgres.Postgres.get_cursor`
        :returns: a single record or value, or :py:attr:`default` (if
            :py:attr:`default` is not an :py:class:`Exception`)
        :raises: :py:exc:`~postgres.TooFew` or :py:exc:`~postgres.TooMany`,
            or :py:attr:`default` (if :py:attr:`default` is an
            :py:class:`Exception`)

        .. _bind parameters: #bind-parameters

        This is a convenience method. Here is what it does::

            with self.get_cursor(back_as=back_as, *a, **kw) as cursor:
                return cursor.one(sql, parameters, default)

        Use this for the common case where there should only be one record, but
        it may not exist yet.

        >>> db.one("SELECT * FROM foo WHERE bar='buz'")
        Record(bar='buz', baz=42)

        If the record doesn't exist, we return :py:class:`None`:

        >>> record = db.one("SELECT * FROM foo WHERE bar='blam'")
        >>> if record is None:
        ...     print("No blam yet.")
        ...
        No blam yet.

        If you pass :py:attr:`default` we'll return that instead of
        :py:class:`None`:

        >>> db.one("SELECT * FROM foo WHERE bar='blam'", default=False)
        False

        If you pass an :py:class:`Exception` instance or subclass for
        :py:attr:`default`, we will raise that for you:

        >>> db.one("SELECT * FROM foo WHERE bar='blam'", default=Exception)
        Traceback (most recent call last):
            ...
        Exception

        We specifically stop short of supporting lambdas or other callables for
        the :py:attr:`default` parameter. That gets complicated quickly, and
        it's easy to just check the return value in the caller and do your
        extra logic there.

        You can use :py:attr:`back_as` to override the type associated with the
        default :py:attr:`cursor_factory` for your
        :py:class:`~postgres.Postgres` instance:

        >>> db.default_cursor_factory
        <class 'postgres.cursors.SimpleNamedTupleCursor'>
        >>> db.one( "SELECT * FROM foo WHERE bar='buz'"
        ...       , back_as=dict
        ...        )
        {'bar': 'buz', 'baz': 42}

        That's a convenience so you don't have to go to the trouble of
        remembering where :py:class:`~postgres.cursors.SimpleDictCursor` lives
        and importing it in order to get dictionaries back. If you do need more
        control (maybe you have a custom cursor class), you can pass
        :py:attr:`cursor_factory` explicitly, and that will override any
        :py:attr:`back_as`:

        >>> from postgres.cursors import SimpleTupleCursor
        >>> db.one( "SELECT * FROM foo WHERE bar='buz'"
        ...       , back_as=dict
        ...       , cursor_factory=SimpleTupleCursor
        ...        )
        ('buz', 42)

        If the query result has only one column, then we dereference that for
        you.

        >>> db.one("SELECT baz FROM foo WHERE bar='buz'")
        42

        And if the dereferenced value is :py:class:`None`, we return the value
        of :py:attr:`default`:

        >>> db.one("SELECT sum(baz) FROM foo WHERE bar='nope'", default=0)
        0

        Dereferencing will use :py:meth:`.values` if it exists on the record,
        so it should work for both mappings and sequences.

        >>> db.one( "SELECT sum(baz) FROM foo WHERE bar='nope'"
        ...       , back_as=dict
        ...       , default=0
        ...        )
        0

        """
        with self.get_cursor(back_as=back_as, *a, **kw) as cursor:
            return cursor.one(sql, parameters, default)


    def all(self, sql, parameters=None, back_as=None, *a, **kw):
        """Execute a query and return all results.

        :param string sql: the SQL statement to execute
        :param parameters: the `bind parameters`_ for the SQL statement
        :type parameters: dict or tuple
        :param back_as: the type of record to return
        :type back_as: type or string
        :param a: passed through to :py:meth:`~postgres.Postgres.get_cursor`
        :param kw: passed through to :py:meth:`~postgres.Postgres.get_cursor`
        :returns: :py:class:`list` of records or :py:class:`list` of single
            values

        .. _bind parameters: #bind-parameters

        This is a convenience method. Here is what it does::

            with self.get_cursor(back_as=back_as, *a, **kw) as cursor:
                return cursor.all(sql, parameters)

        Use it like this:

        >>> db.all("SELECT * FROM foo ORDER BY bar")
        [Record(bar='bit', baz=537), Record(bar='buz', baz=42)]

        You can use :py:attr:`back_as` to override the type associated with the
        default :py:attr:`cursor_factory` for your
        :py:class:`~postgres.Postgres` instance:

        >>> db.default_cursor_factory
        <class 'postgres.cursors.SimpleNamedTupleCursor'>
        >>> db.all("SELECT * FROM foo ORDER BY bar", back_as=dict)
        [{'bar': 'bit', 'baz': 537}, {'bar': 'buz', 'baz': 42}]

        That's a convenience so you don't have to go to the trouble of
        remembering where :py:class:`~postgres.cursors.SimpleDictCursor` lives
        and importing it in order to get dictionaries back. If you do need more
        control (maybe you have a custom cursor class), you can pass
        :py:attr:`cursor_factory` explicitly, and that will override any
        :py:attr:`back_as`:

        >>> from postgres.cursors import SimpleTupleCursor
        >>> db.all( "SELECT * FROM foo ORDER BY bar"
        ...       , back_as=dict
        ...       , cursor_factory=SimpleTupleCursor
        ...        )
        [('bit', 537), ('buz', 42)]

        If the query results in records with a single column, we return a list
        of the values in that column rather than a list of records of values.

        >>> db.all("SELECT baz FROM foo ORDER BY bar")
        [537, 42]

        This works for record types that are mappings (anything with a
        :py:meth:`__len__` and a :py:meth:`values` method) as well those that
        are sequences:

        >>> db.all("SELECT baz FROM foo ORDER BY bar", back_as=dict)
        [537, 42]

        """
        with self.get_cursor(back_as=back_as, *a, **kw) as cursor:
            return cursor.all(sql, parameters)


    def get_cursor(self, *a, **kw):
        """Return a :py:class:`~postgres.CursorContextManager` that uses
        our connection pool.

        :param a: passed through to the :py:meth:`cursor` method of instances
            of the class returned by :py:func:`~postgres.make_Connection`
        :param kw: passed through to the :py:meth:`cursor` method of instances
            of the class returned by :py:func:`~postgres.make_Connection`

        >>> with db.get_cursor() as cursor:
        ...     cursor.all("SELECT * FROM foo")
        ...
        [Record(bar='buz', baz=42), Record(bar='bit', baz=537)]

        You can use our simple :py:meth:`~postgres.Postgres.run`,
        :py:meth:`~postgres.Postgres.one`, :py:meth:`~postgres.Postgres.all`
        API, and you can also use the traditional DB-API 2.0 methods:

        >>> with db.get_cursor() as cursor:
        ...     cursor.execute("SELECT * FROM foo")
        ...     cursor.fetchall()
        ...
        [Record(bar='buz', baz=42), Record(bar='bit', baz=537)]

        The cursor will have :py:attr:`autocommit` turned off on its
        connection. If your code block inside the :py:obj:`with` statement
        raises an exception, the transaction will be rolled back. Otherwise,
        it'll be committed. The context manager closes the cursor when the
        block ends, resets :py:attr:`autocommit` to off on the connection, and
        puts the connection back in the pool. The cursor is destroyed after
        use.

        Use this when you want a series of statements to be part of one
        transaction, but you don't need fine-grained control over the
        transaction.

        """
        return CursorContextManager(self.pool, *a, **kw)


    def get_connection(self):
        """Return a :py:class:`~postgres.ConnectionContextManager` that uses
        our connection pool.

        >>> with db.get_connection() as connection:
        ...     cursor = connection.cursor()
        ...     cursor.all("SELECT * FROM foo")
        ...
        [Record(bar='buz', baz=42), Record(bar='bit', baz=537)]

        Use this when you want to take advantage of connection pooling and our
        simple :py:meth:`~postgres.Postgres.run`,
        :py:meth:`~postgres.Postgres.one`, :py:meth:`~postgres.Postgres.all`
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
        return ConnectionContextManager(self.pool)


    def register_model(self, ModelSubclass, typname=None):
        """Register an ORM model.

        :param ModelSubclass: the :py:class:`~postgres.orm.Model` subclass to
            register with this :py:class:`~postgres.Postgres` instance

        :param typname: a string indicating the Postgres type to register this
            model for (``typname``, without an "e," is the name of the relevant
            column in the underlying ``pg_type`` table). If :py:class:`None`,
            we'll look for :py:attr:`ModelSubclass.typname`.

        :raises: :py:exc:`~postgres.NotAModel`,
            :py:exc:`~postgres.NoTypeSpecified`,
            :py:exc:`~postgres.NoSuchType`,
            :py:exc:`~postgres.AlreadyRegistered`

        .. note::

            See the :py:mod:`~postgres.orm` docs for instructions on
            subclassing :py:class:`~postgres.orm.Model`.

        """
        self._validate_model_subclass(ModelSubclass)

        if typname is None:
            typname = getattr(ModelSubclass, 'typname', None)
            if typname is None:
                raise NoTypeSpecified(ModelSubclass)

        n = self.one( "SELECT count(*) FROM pg_type WHERE typname=%s"
                    , (typname,)
                     )
        if n < 1:
            # Could be more than one since we don't constrain by typnamespace.
            # XXX What happens then?
            raise NoSuchType(typname)

        if typname in self.model_registry:
            existing_model = self.model_registry[typname]
            raise AlreadyRegistered(existing_model, typname)

        self.model_registry[typname] = ModelSubclass
        ModelSubclass.db = self

        # register a composite
        with self.get_connection() as conn:
            cursor = conn.cursor()
            name = typname
            if sys.version_info[0] < 3:
                name = name.encode('UTF-8')
            register_composite( name
                              , cursor
                              , globally=True
                              , factory=self.DelegatingCaster
                               )


    def unregister_model(self, ModelSubclass):
        """Unregister an ORM model.

        :param ModelSubclass: the :py:class:`~postgres.orm.Model` subclass to
            unregister
        :raises: :py:exc:`~postgres.NotAModel`,
            :py:exc:`~postgres.NotRegistered`

        If :py:class:`ModelSubclass` is registered for multiple types, it is
        unregistered for all of them.

        """
        keys = self.check_registration(ModelSubclass)
        if not isinstance(keys, list):
            # Wrap single string in a list. Flip-side of XXX just below.
            keys = [keys]
        for key in keys:
            del self.model_registry[key]


    def check_registration(self, ModelSubclass, include_subsubclasses=False):
        """Check whether an ORM model is registered.

        :param ModelSubclass: the :py:class:`~postgres.orm.Model` subclass to
            check for
        :param bool include_subsubclasses: whether to also check for subclasses
            of :py:class:`ModelSubclass` or just :py:class:`ModelSubclass`
            itself

        :returns: the :py:attr:`typname` (a string) for which this model is
            registered, or a list of strings if it's registered for multiple
            types

        :rettype: string
        :raises: :py:exc:`~postgres.NotAModel`,
            :py:exc:`~postgres.NotRegistered`

        """
        self._validate_model_subclass(ModelSubclass)

        if include_subsubclasses:
            filt = lambda v: v is ModelSubclass or issubclass(ModelSubclass, v)
        else:
            filt = lambda v: v is ModelSubclass
        keys = [k for k,v in self.model_registry.items() if filt(v)]
        if not keys:
            raise NotRegistered(ModelSubclass)
        if len(keys) == 1:
            # Dereference a single-item list, for backwards-compatibility.
            # XXX If/when we go to 3.0, lose this cruft (always return list).
            keys = keys[0]
        return keys


    def _validate_model_subclass(self, ModelSubclass, ):
        if not isclass(ModelSubclass) or not issubclass(ModelSubclass, Model):
            raise NotAModel(ModelSubclass)


# Class Factories
# ===============

def make_Connection(postgres):
    """Define and return a subclass of
    :py:class:`psycopg2.extensions.connection`.

    :param postgres: the :py:class:`~postgres.Postgres` instance to bind to
    :returns: a :py:class:`Connection` class

    The class defined and returned here will be linked to the instance of
    :py:class:`~postgres.Postgres` that is passed in as :py:attr:`postgres`,
    which will use this class as the :py:attr:`connection_factory` for its
    connection pool.

    The :py:meth:`cursor` method of this class accepts a :py:attr:`back_as`
    keyword argument. If a :py:attr:`cursor_factory` keyword argument is also
    given, then any :py:attr:`back_as` is ignored and discarded.  Valid values
    for :py:attr:`back_as` are :py:class:`tuple`, :py:class:`namedtuple`,
    :py:class:`dict` (or the strings ``tuple``, ``namedtuple``, and ``dict``),
    and :py:class:`None`. If the value of :py:attr:`back_as` is
    :py:class:`None`, then we'll use the default :py:attr:`cursor_factory` with
    which our parent :py:class:`~postgres.Postgres` instance was instantiated.
    If :py:attr:`back_as` is not :py:class:`None`, then we'll specify a
    :py:attr:`cursor_factory` that will result in records of the designated
    type: :py:class:`postgres.cursor.SimpleTupleCursor` for :py:class:`tuple`,
    :py:class:`postgres.cursor.SimpleNamedTupleCursor` for
    :py:class:`namedtuple`, and :py:class:`postgres.cursor.SimpleDictCursor`
    for :py:class:`dict`.

    We also set client encoding to ``UTF-8``.

    """
    class Connection(psycopg2.extensions.connection):

        def __init__(self, *a, **kw):
            psycopg2.extensions.connection.__init__(self, *a, **kw)
            self.set_client_encoding('UTF-8')
            self.postgres = postgres

        def cursor(self, *a, **kw):
            if 'back_as' in kw:
                back_as = kw.pop('back_as')
                kw = self.handle_back_as(back_as, **kw)
            if 'cursor_factory' not in kw:
                kw['cursor_factory'] = self.postgres.default_cursor_factory
            return psycopg2.extensions.connection.cursor(self, *a, **kw)

        def handle_back_as(self, back_as, **kw):

            if 'cursor_factory' not in kw:

                # Compute cursor_factory from back_as.
                # ====================================

                registry = { tuple: SimpleTupleCursor
                           , 'tuple': SimpleTupleCursor
                           , namedtuple: SimpleNamedTupleCursor
                           , 'namedtuple': SimpleNamedTupleCursor
                           , dict: SimpleDictCursor
                           , 'dict': SimpleDictCursor
                           , None: None
                             }

                if back_as not in registry:
                    raise BadBackAs(back_as)

                cursor_factory = registry[back_as]
                if cursor_factory is not None:
                    kw['cursor_factory'] = cursor_factory

            return kw

    return Connection


def make_DelegatingCaster(postgres):
    """Define a :py:class:`~psycopg2.extras.CompositeCaster` subclass that
        delegates to :py:attr:`~postgres.Postgres.model_registry`.

    :param postgres: the :py:class:`~postgres.Postgres` instance to bind to
    :returns: a :py:class:`DelegatingCaster` class

    The class we return will use the :py:attr:`model_registry` of the given
    :py:class:`~postgres.Postgres` instance to look up a
    :py:class:`~postgres.orm.Model` subclass to use in mapping
    :py:mod:`psycopg2` return values to higher-order Python objects. Yeah, it's
    a little squirrelly. :-/

    """
    class DelegatingCaster(CompositeCaster):

        def parse(self, s, curs, retry=True):
            # Override to protect against race conditions:
            #   https://github.com/gratipay/postgres.py/issues/26

            try:
                return super(DelegatingCaster, self).parse(s, curs)
            except (DataError, ValueError):
                if not retry:
                    raise
                # Re-fetch the type info and retry once
                self._refetch_type_info(curs)
                return self.parse(s, curs, False)

        def make(self, values):
            # Override to delegate to the model registry.
            if self.name not in postgres.model_registry:

                # This is probably a bug, not a normal user error. It means
                # we've called register_composite for this typname without also
                # registering with model_registry.

                raise NotImplementedError

            ModelSubclass = postgres.model_registry[self.name]
            record = dict(zip(self.attnames, values))
            instance = ModelSubclass(record)
            return instance

        def _refetch_type_info(self, curs):
            """Given a cursor, update the current object with a fresh type definition.
            """
            new_self = self._from_db(self.name, curs)
            self.__dict__.update(new_self.__dict__)

    return DelegatingCaster


if __name__ == '__main__':
    db = Postgres("postgres://jrandom@localhost/test")
    db.run("DROP SCHEMA IF EXISTS public CASCADE")
    db.run("CREATE SCHEMA public")
    import doctest
    doctest.testmod()
