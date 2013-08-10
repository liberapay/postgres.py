""":py:mod:`postgres` is a high-value abstraction over `psycopg2`_.


Installation
------------

:py:mod:`postgres` is available on `GitHub`_ and on `PyPI`_::

    $ pip install postgres

We `test <https://travis-ci.org/gittip/postgres.py>`_ against Python 2.6, 2.7,
3.2, and 3.3. We don't yet have a testing matrix for different versions of
:py:mod:`psycopg2` or PostgreSQL.

:py:mod:`postgres` is in the `public domain`_.


Tutorial
--------

Instantiate a :py:class:`Postgres` object when your application starts:

    >>> from postgres import Postgres
    >>> db = Postgres("postgres://jrandom@localhost/testdb")

Use :py:meth:`~postgres.Postgres.run` to run SQL statements:

    >>> db.run("CREATE TABLE foo (bar text)")
    >>> db.run("INSERT INTO foo VALUES ('baz')")
    >>> db.run("INSERT INTO foo VALUES ('buz')")

Use :py:meth:`~postgres.Postgres.all` to fetch all results:

    >>> db.all("SELECT * FROM foo ORDER BY bar")
    [{'bar': 'baz'}, {'bar': 'buz'}]

Use :py:meth:`~postgres.Postgres.one_or_zero` to fetch one result or
:py:class:`None`:

    >>> db.one_or_zero("SELECT * FROM foo WHERE bar='baz'")
    {'bar': 'baz'}
    >>> db.one_or_zero("SELECT * FROM foo WHERE bar='blam'")


Bind Parameters
+++++++++++++++

In case you're not familiar with bind parameters in `DB-API 2.0`_, the basic
idea is that you put ``%(foo)s`` in your SQL strings, and then pass in a second
argument, a :py:class:`dict`, containing parameters that :py:mod:`psycopg2` (as
an implementation of DB-API 2.0) will bind to the query in a way that is safe
against `SQL injection`_. (This is inspired by old-style Python string
formatting, but it is not the same.)

    >>> db.one("SELECT * FROM foo WHERE bar=%(bar)s", {"bar": "baz"})
    {'bar': 'baz'}

Never build SQL strings out of user input!

Always pass user input as bind parameters!


Context Managers
++++++++++++++++

Eighty percent of your database usage should be covered by the simple
:py:meth:`~postgres.Postgres.run`, :py:meth:`~postgres.Postgres.all`,
:py:meth:`~postgres.Postgres.one_or_zero` API introduced above. For the other
20%, :py:mod:`postgres` provides context managers for working at increasingly
lower levels of abstraction. The lowest level of abstraction in
:py:mod:`postgres` is a :py:mod:`psycopg2` `connection pool
<http://initd.org/psycopg/docs/pool.html>`_ that we configure and manage for
you. Everything in :py:mod:`postgres`, both the simple API and the context
managers, uses this connection pool.

Here's how to work directly with a :py:mod:`psycogpg2` `cursor
<http://initd.org/psycopg/docs/cursor.html>`_ while still taking advantage of
connection pooling:

    >>> with db.get_cursor() as cursor:
    ...     cursor.execute("SELECT * FROM foo ORDER BY bar")
    ...     cursor.fetchall()
    ...
    [{'bar': 'baz'}, {'bar': 'buz'}]

A cursor you get from :py:func:`~postgres.Postgres.get_cursor` has
:py:attr:`autocommit` turned on for its connection, so every call you make
using such a cursor will be isolated in a separate transaction. Need to include
multiple calls in a single transaction? Use the
:py:func:`~postgres.Postgres.get_transaction` context manager:

    >>> with db.get_transaction() as txn:
    ...     txn.execute("INSERT INTO foo VALUES ('blam')")
    ...     txn.execute("SELECT * FROM foo ORDER BY bar")
    ...     txn.fetchall()
    ...
    [{'bar': 'baz'}, {'bar': 'blam'}, {'bar': 'buz'}]

Note that other calls won't see the changes on your transaction until the end
of your code block, when the context manager commits the transaction for you::

    >>> with db.get_transaction() as txn:
    ...     txn.execute("INSERT INTO foo VALUES ('blam')")
    ...     db.all("SELECT * FROM foo ORDER BY bar")
    ...
    [{'bar': 'baz'}, {'bar': 'buz'}]
    >>> db.all("SELECT * FROM foo ORDER BY bar")
    [{'bar': 'baz'}, {'bar': 'blam'}, {'bar': 'buz'}]

The :py:func:`~postgres.Postgres.get_transaction` manager gives you a cursor
with :py:attr:`autocommit` turned off on its connection. If the block under
management raises an exception, the connection is rolled back. Otherwise it's
committed. Use this when you want a series of statements to be part of one
transaction, but you don't need fine-grained control over the transaction. For
fine-grained control, use :py:func:`~postgres.Postgres.get_connection` to get a
connection straight from the connection pool:

    >>> with db.get_connection() as connection:
    ...     cursor = connection.cursor()
    ...     cursor.execute("SELECT * FROM foo ORDER BY bar")
    ...     cursor.fetchall()
    ...
    [{'bar': 'baz'}, {'bar': 'buz'}]

A connection gotten in this way will have :py:attr:`autocommit` turned off, and
it'll never be implicitly committed otherwise. It'll actually be rolled back
when you're done with it, so it's up to you to explicitly commit as needed.
This is the lowest-level abstraction that :py:mod:`postgres` provides,
basically just a pre-configured connection pool from :py:mod:`psycopg2`.


API
---

.. _psycopg2: http://initd.org/psycopg/
.. _GitHub: https://github.com/gittip/postgres
.. _PyPI: https://pypi.python.org/pypi/postgres
.. _this advice: http://initd.org/psycopg/docs/usage.html#unicode-handling
.. _public domain: http://creativecommons.org/publicdomain/zero/1.0/
.. _DB-API 2.0: http://www.python.org/dev/peps/pep-0249/
.. _SQL injection: http://en.wikipedia.org/wiki/SQL_injection

"""
from __future__ import unicode_literals

try:                    # Python 2
    import urlparse

    # "Note: In Python 2, if you want to uniformly receive all your database
    # input in Unicode, you can register the related typecasters globally as
    # soon as Psycopg is imported."
    #   -- http://initd.org/psycopg/docs/usage.html#unicode-handling

    import psycopg2.extensions
    psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
    psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)

except ImportError:     # Python 3
    import urllib.parse as urlparse

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool as ConnectionPool


__version__ = '1.1.0'


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


# Exceptions
# ==========

class OutOfBounds(Exception):

    def __init__(self, n, lo, hi):
        self.n = n
        self.lo = lo
        self.hi = hi

    def __str__(self):
        msg = "Got {n} rows; expecting "
        if self.lo == self.hi:
            msg += "exactly {lo}."
        elif self.hi - self.lo == 1:
            msg += "{lo} or {hi}."
        else:
            msg += "between {lo} and {hi} (inclusive)."
        return msg.format(**self.__dict__)

class TooFew(OutOfBounds): pass
class TooMany(OutOfBounds): pass


# The Main Event
# ==============

class Postgres(object):
    """Interact with a `PostgreSQL <http://www.postgresql.org/>`_ database.

    :param unicode url: A ``postgres://`` URL or a `PostgreSQL connection string <http://www.postgresql.org/docs/current/static/libpq-connect.html>`_
    :param int minconn: The minimum size of the connection pool
    :param int maxconn: The maximum size of the connection pool
    :param cursor_factory: Defaults to :py:class:`~psycopg2.extras.RealDictCursor`
    :param strict_one: The default :py:attr:`strict` parameter for :py:meth:`~postgres.Postgres.one`
    :type strict_one: :py:class:`bool`

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
    :py:attr:`maxconn` according to demand. The fundamental value of a
    :py:class:`~postgres.Postgres` instance is that it runs everything through
    its connection pool.

    Check the :py:mod:`psycopg2` `docs
    <http://initd.org/psycopg/docs/extras.html#connection-and-cursor-subclasses>`_
    for additional :py:attr:`cursor_factories`, such as
    :py:class:`NamedTupleCursor`.

    The names in our simple API, :py:meth:`~postgres.Postgres.run`,
    :py:meth:`~postgres.Postgres.all`, and
    :py:meth:`~postgres.Postgres.one_or_zero`, were chosen to be short and
    memorable, and to not conflict with the DB-API 2.0 :py:meth:`execute`,
    :py:meth:`fetchone`, and :py:meth:`fetchall` methods, which have slightly
    different semantics (under DB-API 2.0 you call :py:meth:`execute` on a
    cursor and then call one of the :py:meth:`fetch*` methods on the same
    cursor to retrieve rows; with our simple API there is no second
    :py:meth:`fetch` step). See `this ticket`_ for more of the rationale behind
    these names. The context managers on this class are named starting with
    :py:meth:`get_` to set them apart from the simple-case API.  Note that when
    working inside a block under one of the context managers, you're using
    DB-API 2.0 (:py:meth:`execute` + :py:meth:`fetch*`), not our simple API
    (:py:meth:`~postgres.Postgres.run` / :py:meth:`~postgres.Postgres.one` /
    :py:meth:`~postgres.Postgres.all`).

    .. _this ticket: https://github.com/gittip/postgres.py/issues/16

    """

    def __init__(self, url, minconn=1, maxconn=10, \
                               cursor_factory=RealDictCursor, strict_one=None):
        if url.startswith("postgres://"):
            dsn = url_to_dsn(url)

        Connection.cursor_factory = cursor_factory

        self.pool = ConnectionPool( minconn=minconn
                                  , maxconn=maxconn
                                  , dsn=dsn
                                  , connection_factory=Connection
                                   )

        if strict_one not in (True, False, None):
            raise ValueError("strict_one must be True, False, or None.")
        self.strict_one = strict_one


    def run(self, sql, parameters=None):
        """Execute a query and discard any results.

        :param unicode sql: the SQL statement to execute
        :param parameters: the bind parameters for the SQL statement
        :type parameters: dict or tuple
        :returns: :py:const:`None`

        >>> db.run("CREATE TABLE foo (bar text)")
        >>> db.run("INSERT INTO foo VALUES ('baz')")
        >>> db.run("INSERT INTO foo VALUES ('buz')")

        """
        with self.get_cursor() as cursor:
            cursor.execute(sql, parameters)


    def all(self, sql, parameters=None):
        """Execute a query and return all results.

        :param unicode sql: the SQL statement to execute
        :param parameters: the bind parameters for the SQL statement
        :type parameters: dict or tuple
        :returns: :py:class:`list` of rows

        >>> for row in db.all("SELECT bar FROM foo"):
        ...     print(row["bar"])
        ...
        baz
        buz

        """
        with self.get_cursor() as cursor:
            cursor.execute(sql, parameters)
            return cursor.fetchall()

    def rows(self, *a, **kw):

        # This is for backwards compatibility, see #16. It is stubbed instead
        # of aliased to avoid showing up in our docs via sphinx autodoc.

        return self.all(*a, **kw)


    def one_or_zero(self, sql, parameters=None):
        """Execute a query and return a single result or :py:class:`None`.

        :param unicode sql: the SQL statement to execute
        :param parameters: the bind parameters for the SQL statement
        :type parameters: dict or tuple
        :returns: a single row or :py:const:`None`
        :raises: :py:exc:`~postgres.TooFew` or :py:exc:`~postgres.TooMany`

        Use this for the common case where there should only be one record, but
        it may not exist yet.

        >>> row = db.one_or_zero("SELECT * FROM foo WHERE bar='blam'")
        >>> if row is None:
        ...     print("No blam yet.")
        ...
        No blam yet.

        """
        return self._some(sql, parameters, 0, 1)


    def one(self, sql, parameters=None, strict=None):

        # I'm half-considering dropping this. Now that one_or_zero exists, this
        # is really only useful for what should really be called db.first, and
        # in that case why aren't you using a LIMIT 1?

        """Execute a query and return a single result.

        :param unicode sql: the SQL statement to execute
        :param parameters: the bind parameters for the SQL statement
        :type parameters: dict or tuple
        :param strict: whether to raise when there isn't exactly one result
        :type strict: :py:class:`bool`
        :returns: a single row or :py:const:`None`
        :raises: :py:exc:`~postgres.TooFew` or :py:exc:`~postgres.TooMany`

        By default, :py:attr:`strict` ends up evaluating to :py:class:`True`,
        in which case we raise :py:exc:`postgres.TooFew` or
        :py:exc:`postgres.TooMany` if the number of rows returned isn't exactly
        one (both are subclasses of :py:exc:`postgres.OutOfBounds`). You can
        override this behavior per-call with the :py:attr:`strict` argument
        here, or globally by passing :py:attr:`strict_one` to the
        :py:class:`~postgres.Postgres` constructor. If you use both, the
        :py:attr:`strict` argument here wins. If you pass :py:class:`False`
        for :py:attr:`strict`, then we return :py:class:`None` if there are no
        results, and the first if there is more than one.

        >>> row = db.one("SELECT * FROM foo WHERE bar='baz'")
        >>> print(row["bar"])
        baz

        """
        if strict not in (True, False, None):
            raise ValueError("strict must be True, False, or None.")

        if strict is None:
            if self.strict_one is None:
                strict = True               # library default
            else:
                strict = self.strict_one    # user default

        if strict:
            out = self._some(sql, parameters, 1, 1)
        else:
            with self.get_cursor() as cursor:
                cursor.execute(sql, parameters)
                out = cursor.fetchone()
        return out


    def _some(self, sql, parameters=None, lo=0, hi=1):

        # This is undocumented (and largely untested) because I think it's a
        # rare case where this is wanted directly. It's here to make one and
        # one_or_zero DRY. Help yourself to it now that you've found it. :^)

        with self.get_transaction() as txn:
            txn.execute(sql, parameters)

            if txn.rowcount < lo:
                raise TooFew(txn.rowcount, lo, hi)
            elif txn.rowcount > hi:
                raise TooMany(txn.rowcount, lo, hi)

            return txn.fetchone()


    def get_cursor(self, *a, **kw):
        """Return a :py:class:`~postgres.CursorContextManager` that uses our
        connection pool.

        This gets you a cursor with :py:attr:`autocommit` turned on on its
        connection. The context manager closes the cursor when the block ends.

        Use this when you want a simple cursor.

        >>> with db.get_cursor() as cursor:
        ...     cursor.execute("SELECT * FROM foo")
        ...     cursor.rowcount
        ...
        2

        """
        return CursorContextManager(self.pool, *a, **kw)

    def get_transaction(self, *a, **kw):
        """Return a :py:class:`~postgres.TransactionContextManager` that uses
        our connection pool.

        This gets you a cursor with :py:attr:`autocommit` turned off on its
        connection. If your code block inside the :py:obj:`with` statement
        raises an exception, the transaction will be rolled back. Otherwise,
        it'll be committed. The context manager closes the cursor when the
        block ends.

        Use this when you want a series of statements to be part of one
        transaction, but you don't need fine-grained control over the
        transaction.

        >>> with db.get_transaction() as txn:
        ...     txn.execute("SELECT * FROM foo")
        ...     txn.fetchall()
        ...
        [{'bar': 'baz'}, {'bar': 'buz'}]

        """
        return TransactionContextManager(self.pool, *a, **kw)

    def get_connection(self):
        """Return a :py:class:`~postgres.ConnectionContextManager` that uses
        our connection pool.

        Use this when you want to take advantage of connection pooling, but
        otherwise need full control, for example, to do complex things with
        transactions.

        >>> with db.get_connection() as connection:
        ...     cursor = connection.cursor()
        ...     cursor.execute("SELECT * FROM foo")
        ...     cursor.fetchall()
        ...
        [{'bar': 'baz'}, {'bar': 'buz'}]

        """
        return ConnectionContextManager(self.pool)


class Connection(psycopg2.extensions.connection):
    """This is a subclass of :py:class:`psycopg2.extensions.connection`.

    :py:class:`Postgres` uses this class as the :py:attr:`connection_factory`
    for its connection pool. Here are the differences from the base class:

        - We set :py:attr:`autocommit` to :py:const:`True`.
        - We set the client encoding to ``UTF-8``.
        - We use :py:attr:`self.cursor_factory`.

    """

    cursor_factory = None   # set this before using this object

    def __init__(self, *a, **kw):
        psycopg2.extensions.connection.__init__(self, *a, **kw)
        self.set_client_encoding('UTF-8')
        self.autocommit = True

    def cursor(self, *a, **kw):
        if 'cursor_factory' not in kw:
            kw['cursor_factory'] = self.cursor_factory
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
    the block ends. Also when the block ends, the cursor is closed.

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
        self.cursor = self.conn.cursor(*self.a, **self.kw)
        return self.cursor

    def __exit__(self, *exc_info):
        """Put our connection back in the pool.
        """
        self.cursor.close()
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
    In either case, the cursor is closed, :py:attr:`autocommit` is restored to
    :py:const:`True`, and the :py:class:`~postgres.Connection` is put back in
    the pool.

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
        self.cursor = self.conn.cursor(*self.a, **self.kw)
        return self.cursor

    def __exit__(self, *exc_info):
        """Put our connection back in the pool.
        """
        if exc_info == (None, None, None):
            self.conn.commit()
        else:
            self.conn.rollback()
        self.cursor.close()
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
