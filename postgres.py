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

    >>> with db.get_cursor('SELECT * FROM foo ORDER BY bar') as cursor:
    ...     results = cursor.fetchall()

A cursor you get from :py:func:`~postgres.Postgres.get_cursor` has
``autocommit`` turned on for its connection, so every call you make using such
a cursor will be isolated in a separate transaction. Need to include multiple
calls in a single transaction? Use the
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
with ``autocommit`` turned off on its connection. If the block under management
raises, the connection is rolled back. Otherwise it's committed.  Use this when
you want a series of statements to be part of one transaction, but you don't
need fine-grained control over the transaction. For fine-grained control, use
:py:func:`~postgres.Postgres.get_connection` to get a connection straight from
the connection pool:

    >>> with db.get_connection() as connection:
    ...     cursor = connection.cursor()
    ...     cursor.execute('SELECT * FROM foo ORDER BY bar')
    ...     cursor.fetchall()
    [{"bar": "baz"}, {"bar": "buz"}]

A connection gotten in this way will have ``autocommit`` turned off, and it'll
never be implicitly committed otherwise. It'll actually be rolled back when
you're done with it, so it's up to you to explicitly commit as needed.


API
---

.. _psycopg2: http://initd.org/psycopg/
.. _GitHub: https://github.com/gittip/postgres
.. _PyPI: https://pypi.python.org/pypi/postgres

"""
from __future__ import unicode_literals
import urlparse

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


# Teach urlparse about postgres:// URLs.
if 'postgres' not in urlparse.uses_netloc:
    urlparse.uses_netloc.append('postgres')


def url_to_dsn(url):
    """Heroku gives us an URL, psycopg2 wants a DSN. Convert!
    """
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


class Postgres(object):
    """Interact with a `PostgreSQL <http://www.postgresql.org/>`_ datastore.

    :param unicode url: A ``postgres://`` URL or a `PostgreSQL connection string <http://www.postgresql.org/docs/current/static/libpq-connect.html>`_
    :param int minconn: The minimum size of the connection pool
    :param int maxconn: The minimum size of the connection pool

    This is the main object that :py:mod:`postgres` provides, and you should
    have one instance per process. When instantiated, this object creates a
    `thread-safe connection pool
    <http://initd.org/psycopg/docs/pool.html#psycopg2.pool.ThreadedConnectionPool>`_,
    which opens ``minconn`` connections immediately and up to ``maxconn``
    according to demand.

    Features:

      - Calls are isolated in transactions.
      - Get back unicode instead of bytestrings.


    """

    def __init__(self, url, minconn=1, maxconn=10):
        if url.startswith("postgres://"):
            dsn = url_to_dsn(url)
        self.pool = ConnectionPool( minconn=minconn
                                  , maxconn=maxconn
                                  , dsn=dsn
                                  , connection_factory=PostgresConnection
                                   )

    def execute(self, *a, **kw):
        """Execute the query and discard any results.
        """
        with self.get_cursor(*a, **kw):
            pass

    def fetchone(self, *a, **kw):
        """Execute the query and return a single result (``dict`` or ``None``).
        """
        with self.get_cursor(*a, **kw) as cursor:
            return cursor.fetchone()

    def fetchall(self, *a, **kw):
        """Execute the query and yield the results (``dict``).
        """
        with self.get_cursor(*a, **kw) as cursor:
            for row in cursor:
                yield row

    def get_cursor(self, *a, **kw):
        """Execute the query and return a context manager wrapping the cursor.

        The cursor is a psycopg2 RealDictCursor. The connection underlying the
        cursor will be checked out of the connection pool and checked back in
        upon both successful and exceptional executions against the cursor.

        """
        return PostgresCursorContextManager(self.pool, *a, **kw)

    def get_transaction(self, *a, **kw):
        """Return a context manager wrapping a transactional cursor.

        This manager returns a cursor with autocommit turned off on its
        connection. If the block under management raises then the connection is
        rolled back. Otherwise it's committed. Use this when you want a series
        of statements to be part of one transaction, but you don't need
        fine-grained  control over the transaction.

        """
        return PostgresTransactionContextManager(self.pool, *a, **kw)

    def get_connection(self):
        """Return a context manager wrapping a PostgresConnection.

        This manager turns autocommit off, and back on when you're done with
        the connection. The connection is rolled back on exit, so be sure to
        call commit as needed. The idea is that you'd use this when you want
        full fine-grained transaction control.

        """
        return PostgresConnectionContextManager(self.pool)


class PostgresConnection(psycopg2.extensions.connection):
    """This is a subclass of psycopg2.extensions.connection.

    Changes:

         - The DB-API 2.0 spec calls for transactions to be left open by
           default. I don't think we want this. We set autocommit to
           :py:class:True.

        - We enforce UTF-8.

        - We use RealDictCursor.

    """

    def __init__(self, *a, **kw):
        psycopg2.extensions.connection.__init__(self, *a, **kw)
        self.set_client_encoding('UTF-8')
        self.autocommit = True

    def cursor(self, *a, **kw):
        if 'cursor_factory' not in kw:
            kw['cursor_factory'] = RealDictCursor
        return psycopg2.extensions.connection.cursor(self, *a, **kw)


class PostgresTransactionContextManager(object):
    """Instantiated once per db.get_transaction call.
    """

    def __init__(self, pool, *a, **kw):
        self.pool = pool
        self.conn = None

    def __enter__(self, *a, **kw):
        """Get a connection from the pool.
        """
        self.conn = self.pool.getconn()
        self.conn.autocommit = False
        return self.conn.cursor(*a, **kw)

    def __exit__(self, *exc_info):
        """Put our connection back in the pool.
        """
        if exc_info == (None, None, None):
            self.conn.commit()
        else:
            self.conn.rollback()
        self.conn.autocommit = True
        self.pool.putconn(self.conn)


class PostgresConnectionContextManager(object):
    """Instantiated once per db.get_connection call.
    """

    def __init__(self, pool, *a, **kw):
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


class PostgresCursorContextManager(object):
    """Instantiated once per cursor-level db access.
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
        cursor = self.conn.cursor()
        try:
            cursor.execute(*self.a, **self.kw)
        except:
            # If we get an exception from execute (like, the query fails:
            # pretty common), then the __exit__ clause is not triggered. We
            # trigger it ourselves to avoid draining the pool.
            self.__exit__()
            raise
        return cursor

    def __exit__(self, *exc_info):
        """Put our connection back in the pool.
        """
        self.pool.putconn(self.conn)
