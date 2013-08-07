""":py:mod:`postgres` is a high-value abstraction over `psycopg2`_.


Installation
------------

:py:mod:`postgres` is available on `GitHub`_ and `PyPI`_::

    $ pip install postgres


Tutorial
--------

Instantiate a :py:class:`Postgres` object when your application starts:

    >>> from postgres import Postgres
    >>> db = Postgres("postgres://jdoe@localhost/testdb")

Use it to run SQL statements:

    >>> db.execute("CREATE TABLE foo (bar text)")
    >>> db.execute("INSERT INTO foo VALUES ('baz')")
    >>> db.execute("INSERT INTO foo VALUES ('buz')")

Use it to fetch all results:

    >>> db.fetchall("SELECT * FROM foo ORDER BY bar")
    [{"bar": "baz"}, {"bar": "buz"}]

Use it to fetch one result:

    >>> db.fetchone("SELECT * FROM foo ORDER BY bar")
    {"bar": "baz"}
    >>> db.fetchone("SELECT * FROM foo WHERE bar='blam'")
    None


Context Managers
++++++++++++++++

Eighty percent of your database usage should be covered by the simple API
above. For the other 20%, :py:mod:`postgres` provides context managers for
working at increasingly lower levels of abstraction. The lowest level of
abstraction in :py:mod:`postgres` is a :py:mod:`psycopg2` connection pool that
we configure and manage for you. Everything in :py:mod:`postgres`, both the
simple API and the context managers, uses this connection pool.

Here's how to work directly with a `psycogpg2 cursor
<http://initd.org/psycopg/docs/cursor.html>`_ while still taking advantage of
connection pooling:

    >>> with db.get_cursor() as cursor:
    ...     cursor.execute("SELECT * FROM foo ORDER BY bar")
    ...     cursor.fetchall()
    [{"bar": "baz"}, {"bar": "buz"}]

A cursor you get from :py:func:`~postgres.Postgres.get_cursor` has
:py:attr:`autocommit` turned on for its connection, so every call you make
using such a cursor will be isolated in a separate transaction. Need to include
multiple calls in a single transaction? Use the
:py:func:`~postgres.Postgres.get_transaction` context manager:

    >>> with db.get_transaction() as txn:
    ...     txn.execute("INSERT INTO foo VALUES ('blam')")
    ...     txn.execute("SELECT * FROM foo ORDER BY bar")
    ...     txn.fetchall()
    [{"bar": "baz"}, {"bar": "blam"}, {"bar": "buz"}]
    ...
    ...     db.fetchall("SELECT * FROM foo ORDER BY bar")
    [{"bar": "baz"}, {"bar": "buz"}]
    ...
    ... db.fetchall("SELECT * FROM foo ORDER BY bar")
    [{"bar": "baz"}, {"bar": "blam"}, {"bar": "buz"}]

The :py:func:`~postgres.Postgres.get_transaction` manager gives you a cursor
with :py:attr:`autocommit` turned off on its connection. If the block under
management raises, the connection is rolled back. Otherwise it's committed.
Use this when you want a series of statements to be part of one transaction,
but you don't need fine-grained control over the transaction. For fine-grained
control, use :py:func:`~postgres.Postgres.get_connection` to get a connection
straight from the connection pool:

    >>> with db.get_connection() as connection:
    ...     cursor = connection.cursor()
    ...     cursor.execute("SELECT * FROM foo ORDER BY bar")
    ...     cursor.fetchall()
    [{"bar": "baz"}, {"bar": "buz"}]

A connection gotten in this way will have :py:attr:`autocommit` turned off, and
it'll never be implicitly committed otherwise. It'll actually be rolled back
when you're done with it, so it's up to you to explicitly commit as needed.


API
---

.. _psycopg2: http://initd.org/psycopg/
.. _GitHub: https://github.com/gittip/postgres
.. _PyPI: https://pypi.python.org/pypi/postgres

"""
from __future__ import unicode_literals

try:                    # Python 2
    import urlparse
except ImportError:     # Python 3
    import urllib.parse as urlparse

import psycopg2

# "Note: In Python 2, if you want to uniformly receive all your database input
#  in Unicode, you can register the related typecasters globally as soon as
#  Psycopg is imported."
#   -- http://initd.org/psycopg/docs/usage.html#unicode-handling

import psycopg2.extensions
psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)

from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool as ConnectionPool


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
    dsn = "dbname=%s user=%s password=%s host=%s port=%s"
    dsn %= (dbname, user, password, host, port)
    return dsn


# The Main Event
# ==============

class Postgres(object):
    """Interact with a `PostgreSQL <http://www.postgresql.org/>`_ database.

    :param unicode url: A ``postgres://`` URL or a `PostgreSQL connection string <http://www.postgresql.org/docs/current/static/libpq-connect.html>`_
    :param int minconn: The minimum size of the connection pool
    :param int maxconn: The minimum size of the connection pool

    This is the main object that :py:mod:`postgres` provides, and you should
    have one instance per process for each database your process needs to talk
    to. When instantiated, this object creates a `thread-safe connection pool
    <http://initd.org/psycopg/docs/pool.html#psycopg2.pool.ThreadedConnectionPool>`_,
    which opens :py:attr:`minconn` connections immediately, and up to
    :py:attr:`maxconn` according to demand. The fundamental value of a
    :py:class:`~postgres.Postgres` instance is that it runs everything through
    its connection pool.

    Features:

      - Get back unicode instead of bytestrings.


    """

    def __init__(self, url, minconn=1, maxconn=10):
        if url.startswith("postgres://"):
            dsn = url_to_dsn(url)
        self.pool = ConnectionPool( minconn=minconn
                                  , maxconn=maxconn
                                  , dsn=dsn
                                  , connection_factory=Connection
                                   )

    def execute(self, sql, parameters=None):
        """Execute the query and discard any results.

        :param unicode sql: the SQL statement to execute
        :param parameters: the bind parameters for the SQL statement
        :type parameters: tuple or dict
        :returns: :py:const:`None`

        """
        with self.get_cursor() as cursor:
            cursor.execute(sql, parameters)

    def fetchone(self, sql, parameters=None):
        """Execute the query and return a single result.

        :param unicode sql: the SQL statement to execute
        :param parameters: the bind parameters for the SQL statement
        :type parameters: tuple or dict
        :returns: :py:class:`dict` or :py:const:`None`

        """
        with self.get_cursor() as cursor:
            cursor.execute(sql, parameters)
            return cursor.fetchone()

    def fetchall(self, sql, parameters=None):
        """Execute the query and return all results.

        :param unicode sql: the SQL statement to execute
        :param parameters: the bind parameters for the SQL statement
        :type parameters: tuple or dict
        :returns: :py:class:`list` of :py:class:`dict`

        """
        with self.get_cursor() as cursor:
            cursor.execute(sql, parameters)
            return list(cursor)

    def get_cursor(self, *a, **kw):
        """Return a :py:class:`~postgres.CursorContextManager` that uses our
        connection pool.

        This is what :py:meth:`~postgres.Postgres.execute`,
        :py:meth:`~postgres.Postgres.fetchone`, and
        :py:meth:`~postgres.Postgres.fetchall` use under the hood. It's
        probably less directly useful than
        :py:meth:`~postgres.Postgres.get_transaction` and
        :py:meth:`~postgres.Postgres.get_connection`.

        """
        return CursorContextManager(self.pool, *a, **kw)

    def get_transaction(self, *a, **kw):
        """Return a :py:class:`~postgres.TransactionContextManager` that uses
        our connection pool.

        Use this when you want a series of statements to be part of one
        transaction, but you don't need fine-grained control over the
        transaction.

        """
        return TransactionContextManager(self.pool, *a, **kw)

    def get_connection(self):
        """Return a :py:class:`~postgres.ConnectionContextManager` that uses
        our connection pool.

        Use this when you want to take advantage of connection pooling, but
        otherwise need full control, for example, to do complex things with
        transactions.

        """
        return ConnectionContextManager(self.pool)


class Connection(psycopg2.extensions.connection):
    """This is a subclass of :py:class:`psycopg2.extensions.connection`.

    This class is used as the :py:attr:`connection_factory` for the connection
    pool in :py:class:`Postgres`. Here are the differences from the base class:

        - We set :py:attr:`autocommit` to :py:const:`True`.
        - We set the client encoding to ``UTF-8``.
        - We use :py:class:`psycopg2.extras.RealDictCursor`.

    """

    def __init__(self, *a, **kw):
        psycopg2.extensions.connection.__init__(self, *a, **kw)
        self.set_client_encoding('UTF-8')
        self.autocommit = True

    def cursor(self, *a, **kw):
        if 'cursor_factory' not in kw:
            kw['cursor_factory'] = RealDictCursor
        return psycopg2.extensions.connection.cursor(self, *a, **kw)


# Context Managers
# ================

class CursorContextManager(object):
    """Instantiated once per :py:func:`~postgres.Postgres.get_cursor` call.

    The return value of :py:func:`CursorContextManager.__enter__` is a
    :py:class:`psycopg2.extras.RealDictCursor`. Any positional and keyword
    arguments to our constructor are passed through to the cursor constructor.
    The :py:class:`~postgres.Connection` underlying the cursor is checked
    out of the connection pool when the block starts, and checked back in when
    the block ends.

    """

    def __init__(self, pool, *a, **kw):
        self.pool = pool
        self.a = a
        self.kw = kw
        self.conn = None

    def __enter__(self):
        """Get a connection from the pool.
        """
        self.conn = self.pool.getconn()
        return self.conn.cursor(*self.a, **self.kw)

    def __exit__(self, *exc_info):
        """Put our connection back in the pool.
        """
        self.pool.putconn(self.conn)


class TransactionContextManager(object):
    """Instantiated once per :py:func:`~postgres.Postgres.get_transaction`
    call.

    The return value of :py:func:`TransactionContextManager.__enter__` is a
    :py:class:`psycopg2.extras.RealDictCursor`. Any positional and keyword
    arguments to our constructor are passed through to the cursor constructor.
    When the block starts, the :py:class:`~postgres.Connection` underlying the
    cursor is checked out of the connection pool and :py:attr:`autocommit` is
    set to :py:const:`False`. If the block raises an exception, the
    :py:class:`~postgres.Connection` is rolled back. Otherwise it's committed.
    In either case, :py:attr:`autocommit` is restored to :py:const:`True` and
    the :py:class:`~postgres.Connection` is put back in the pool.

    """

    def __init__(self, pool, *a, **kw):
        self.pool = pool
        self.a = a
        self.kw = kw
        self.conn = None

    def __enter__(self):
        """Get a connection from the pool.
        """
        self.conn = self.pool.getconn()
        self.conn.autocommit = False
        return self.conn.cursor(*self.a, **self.kw)

    def __exit__(self, *exc_info):
        """Put our connection back in the pool.
        """
        if exc_info == (None, None, None):
            self.conn.commit()
        else:
            self.conn.rollback()
        self.conn.autocommit = True
        self.pool.putconn(self.conn)


class ConnectionContextManager(object):
    """Instantiated once per :py:func:`~postgres.Postgres.get_connection` call.

    The return value of :py:func:`ConnectionContextManager.__enter__` is a
    :py:class:`postgres.Connection`. When the block starts, a
    :py:class:`~postgres.Connection` is checked out of the connection pool and
    :py:attr:`autocommit` is set to :py:const:`False`. When the block ends,
    :py:attr:`autocommit` is restored to :py:const:`True` and the
    :py:class:`~postgres.Connection` is rolled back before being put back in
    the pool.

    """

    def __init__(self, pool):
        self.pool = pool
        self.conn = None

    def __enter__(self):
        """Get a connection from the pool.
        """
        self.conn = self.pool.getconn()
        self.conn.autocommit = False
        return self.conn

    def __exit__(self, *exc_info):
        """Put our connection back in the pool.
        """
        self.conn.rollback()
        self.conn.autocommit = True
        self.pool.putconn(self.conn)
