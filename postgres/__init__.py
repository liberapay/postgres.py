"""The :py:mod:`postgres` Python library is a high-value abstraction over the
`psycopg2`_ database driver.


Installation
------------

:py:mod:`postgres` is available on `GitHub`_ and on `PyPI`_::

    $ pip install postgres

:py:mod:`postgres` requires :py:mod:`psycopg2` version 2.5 or higher.

We `test <https://travis-ci.org/gittip/postgres.py>`_ against Python 2.6, 2.7,
3.2, and 3.3. We don't yet have a testing matrix for different versions of
:py:mod:`psycopg2` or PostgreSQL.

:py:mod:`postgres` is in the `public domain`_.


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
directly with a :py:mod:`psycogpg2` `cursor
<http://initd.org/psycopg/docs/cursor.html>`_ while still taking advantage of
connection pooling and automatic transaction management:

    >>> with db.get_cursor() as cursor:
    ...     cursor.execute("INSERT INTO foo VALUES ('blam')")
    ...     cursor.execute("SELECT * FROM foo ORDER BY bar")
    ...     cursor.fetchall()
    ...
    [Record(bar='bit', baz=537), Record(bar='blam', baz=None), Record(bar='buz', baz=42)]

Note that other calls won't see the changes on your transaction until the end
of your code block, when the context manager commits the transaction for you::

    >>> db.run("DELETE FROM foo WHERE bar='blam'")
    >>> with db.get_cursor() as cursor:
    ...     cursor.execute("INSERT INTO foo VALUES ('blam')")
    ...     db.all("SELECT * FROM foo ORDER BY bar")
    ...
    [Record(bar='bit', baz=537), Record(bar='buz', baz=42)]
    >>> db.all("SELECT * FROM foo ORDER BY bar")
    [Record(bar='bit', baz=537), Record(bar='blam', baz=None), Record(bar='buz', baz=42)]

The :py:func:`~postgres.Postgres.get_cursor` method gives you a context manager
that wraps a cursor. It has :py:attr:`autocommit` turned off on its connection.
If the block under management raises an exception, the connection is rolled
back. Otherwise it's committed. Use this when you want a series of statements
to be part of one transaction, but you don't need fine-grained control over the
transaction. For fine-grained control, use
:py:func:`~postgres.Postgres.get_connection` to get a connection straight from
the connection pool:

    >>> db.run("DELETE FROM foo WHERE bar='blam'")
    >>> with db.get_connection() as connection:
    ...     cursor = connection.cursor()
    ...     cursor.execute("SELECT * FROM foo ORDER BY bar")
    ...     cursor.fetchall()
    ...
    [Record(bar='bit', baz=537), Record(bar='buz', baz=42)]

A connection gotten in this way will have :py:attr:`autocommit` turned off, and
it'll never be implicitly committed otherwise. It'll actually be rolled back
when you're done with it, so it's up to you to explicitly commit as needed.
This is the lowest-level abstraction that :py:mod:`postgres` provides,
basically just a pre-configured connection pool from :py:mod:`psycopg2`.


The Postgres Object
-------------------

.. _psycopg2: http://initd.org/psycopg/
.. _GitHub: https://github.com/gittip/postgres
.. _PyPI: https://pypi.python.org/pypi/postgres
.. _this advice: http://initd.org/psycopg/docs/usage.html#unicode-handling
.. _public domain: http://creativecommons.org/publicdomain/zero/1.0/
.. _DB-API 2.0: http://www.python.org/dev/peps/pep-0249/
.. _SQL injection: http://en.wikipedia.org/wiki/SQL_injection

"""
from __future__ import print_function, unicode_literals

import sys
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
from collections import namedtuple

import psycopg2
from postgres.orm import Model
from psycopg2.extensions import cursor as RegularCursor
from psycopg2.extras import register_composite, CompositeCaster
from psycopg2.extras import NamedTupleCursor, RealDictCursor
from psycopg2.pool import ThreadedConnectionPool as ConnectionPool


__version__ = '2.0.0-dev'


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

class NotAModel(Exception):
    def __str__(self):
        return "Only subclasses of postgres.orm.Model can be registered as " \
               "orm models. {} (registered for {}) doesn't fit the bill." \
               .format(self.args[0].__name__, self.args[1])

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

class BadRecordType(Exception):
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
        :py:class:`~psycopg2.extras.NamedTupleCursor`

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

    :py:attr:`cursor_factory` sets the default cursor that connections managed
    by this :py:class:`~postgres.Postgres` instance will use. Check the
    :py:mod:`psycopg2` `docs
    <http://initd.org/psycopg/docs/extras.html#connection-and-cursor-subclasses>`_
    for additional options, such as :py:class:`psycopg2.extras.RealDictCursor`.
    Use :py:class:`psycopg2.extensions.cursor` for the default
    :py:mod:`psycopg2` behavior, which is to return tuples. Whatever default
    you set here, you can override that default on a per-call basis by passing
    :py:attr:`back_as` or :py:attr:`cursor_factory` to
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
    Note that when working inside a block under one of the context managers,
    you're using DB-API 2.0 (:py:meth:`execute` + :py:meth:`fetch*`, with no
    automatic dereferencing), not our simple API
    (:py:meth:`~postgres.Postgres.run`, :py:meth:`~postgres.Postgres.one`,
    :py:meth:`~postgres.Postgres.all`).

    .. _16: https://github.com/gittip/postgres.py/issues/16
    .. _20: https://github.com/gittip/postgres.py/issues/20

    """

    def __init__(self, url, minconn=1, maxconn=10, \
                                              cursor_factory=NamedTupleCursor):
        if url.startswith("postgres://"):
            dsn = url_to_dsn(url)
        else:
            dsn = url


        # Set up connection pool.
        # =======================

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
        :param parameters: the bind parameters for the SQL statement
        :type parameters: dict or tuple
        :param a: passed through to :py:meth:`~postgres.Postgres.get_cursor`
        :param kw: passed through to :py:meth:`~postgres.Postgres.get_cursor`
        :returns: :py:const:`None`

        >>> db.run("DROP TABLE IF EXISTS foo CASCADE")
        >>> db.run("CREATE TABLE foo (bar text, baz int)")
        >>> db.run("INSERT INTO foo VALUES ('buz', 42)")
        >>> db.run("INSERT INTO foo VALUES ('bit', 537)")

        """
        with self.get_cursor(*a, **kw) as cursor:
            cursor.execute(sql, parameters)


    def one(self, sql, parameters=None, back_as=None, default=None, *a, **kw):
        """Execute a query and return a single result or a default value.

        :param string sql: the SQL statement to execute
        :param parameters: the bind parameters for the SQL statement
        :type parameters: dict or tuple
        :param back_as: the type of record to return
        :type back_as: type or string
        :param default: the value to return if no results are found
        :param a: passed through to
            :py:meth:`~postgres.Postgres.get_cursor`
        :param kw: passed through to
            :py:meth:`~postgres.Postgres.get_cursor`
        :returns: a single record or value or the value of the
            :py:attr:`default` argument
        :raises: :py:exc:`~postgres.TooFew` or :py:exc:`~postgres.TooMany`

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

        We specifically don't support passing lambdas or other callables for
        the :py:attr:`default` parameter. That gets complicated quickly, and
        it's easy to just check the return value in the caller and do your
        extra logic there.

        You can use :py:attr:`back_as` to override the type associated with the
        default :py:attr:`cursor_factory` for your
        :py:class:`~postgres.Postgres` instance:

        >>> db.default_cursor_factory
        <class 'psycopg2.extras.NamedTupleCursor'>
        >>> db.one( "SELECT * FROM foo WHERE bar='buz'"
        ...       , back_as=dict
        ...        )
        {'bar': 'buz', 'baz': 42}

        That's a convenience so you don't have to go to the trouble of
        remembering where :py:class:`~psycopg2.extras.RealDictCursor` lives and
        importing it in order to get dictionaries back. If you do need more
        control (maybe you have a custom cursor class), you can pass
        :py:attr:`cursor_factory` explicitly, and that will override any
        :py:attr:`back_as`:

        >>> from psycopg2.extensions import cursor
        >>> db.one( "SELECT * FROM foo WHERE bar='buz'"
        ...       , back_as=dict
        ...       , cursor_factory=cursor
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

        out = self._some(sql, parameters, 0, 1, back_as, *a, **kw)

        # dereference
        if out is not None and len(out) == 1:
            seq = list(out.values()) if hasattr(out, 'values') else out
            out = seq[0]

        # default
        if out is None:
            out = default

        return out


    def all(self, sql, parameters=None, back_as=None, *a, **kw):
        """Execute a query and return all results.

        :param string sql: the SQL statement to execute
        :param parameters: the bind parameters for the SQL statement
        :type parameters: dict or tuple
        :param back_as: the type of record to return
        :type back_as: type or string
        :param a: passed through to
            :py:meth:`~postgres.Postgres.get_cursor`
        :param kw: passed through to
            :py:meth:`~postgres.Postgres.get_cursor`
        :returns: :py:class:`list` of records or :py:class:`list` of single
            values

        >>> db.all("SELECT * FROM foo ORDER BY bar")
        [Record(bar='bit', baz=537), Record(bar='buz', baz=42)]

        You can use :py:attr:`back_as` to override the type associated with the
        default :py:attr:`cursor_factory` for your
        :py:class:`~postgres.Postgres` instance:

        >>> db.default_cursor_factory
        <class 'psycopg2.extras.NamedTupleCursor'>
        >>> db.all("SELECT * FROM foo ORDER BY bar", back_as=dict)
        [{'bar': 'bit', 'baz': 537}, {'bar': 'buz', 'baz': 42}]

        That's a convenience so you don't have to go to the trouble of
        remembering where :py:class:`~psycopg2.extras.RealDictCursor` lives and
        importing it in order to get dictionaries back. If you do need more
        control (maybe you have a custom cursor class), you can pass
        :py:attr:`cursor_factory` explicitly, and that will override any
        :py:attr:`back_as`:

        >>> from psycopg2.extensions import cursor
        >>> db.all( "SELECT * FROM foo ORDER BY bar"
        ...       , back_as=dict
        ...       , cursor_factory=cursor
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
            cursor.execute(sql, parameters)
            recs = cursor.fetchall()
            if recs and len(recs[0]) == 1:          # dereference
                if hasattr(recs[0], 'values'):      # mapping
                    recs = [list(rec.values())[0] for rec in recs]
                else:                               # sequence
                    recs = [rec[0] for rec in recs]
            return recs


    def _some(self, sql, parameters, lo, hi, back_as, *a, **kw):

        # This is undocumented because I think it's a rare case where this is
        # wanted directly. It was added to make one and one_or_zero DRY when we
        # had those two methods. Help yourself to _some now that you've found
        # it. :^)

        with self.get_cursor(back_as=back_as, *a, **kw) as cursor:
            cursor.execute(sql, parameters)

            if cursor.rowcount < lo:
                raise TooFew(cursor.rowcount, lo, hi)
            elif cursor.rowcount > hi:
                raise TooMany(cursor.rowcount, lo, hi)

            return cursor.fetchone()


    def get_cursor(self, *a, **kw):
        """Return a :py:class:`~postgres.CursorContextManager` that uses
        our connection pool.

        >>> with db.get_cursor() as cursor:
        ...     cursor.execute("SELECT * FROM foo")
        ...     cursor.fetchall()
        ...
        [Record(bar='buz', baz=42), Record(bar='bit', baz=537)]

        This gets you a cursor with :py:attr:`autocommit` turned off on its
        connection. If your code block inside the :py:obj:`with` statement
        raises an exception, the transaction will be rolled back. Otherwise,
        it'll be committed. The context manager closes the cursor when the
        block ends, resets :py:attr:`autocommit` to off on the connection, and
        puts the connection back in the pool.

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
        ...     cursor.execute("SELECT * FROM foo")
        ...     cursor.fetchall()
        ...
        [Record(bar='buz', baz=42), Record(bar='bit', baz=537)]

        Use this when you want to take advantage of connection pooling, but
        otherwise need full control, for example, to do complex things with
        transactions.

        """
        return ConnectionContextManager(self.pool)


    def register_model(self, ModelSubclass):
        """Register an ORM model.

        :param ModelSubclass: the :py:class:`~postgres.orm.Model` subclass to
            register with this :py:class:`~postgres.Postgres` instance
        :raises: :py:exc:`~postgres.NotAModel`,
            :py:exc:`~postgres.NoTypeSpecified`,
            :py:exc:`~postgres.NoSuchType`,
            :py:exc:`~postgres.AlreadyRegistered`

        .. note::

            See the :py:mod:`~postgres.orm` docs for instructions on
            subclassing :py:class:`~postgres.orm.Model`.

        """
        if not issubclass(ModelSubclass, Model):
            raise NotAModel(ModelSubclass)

        if getattr(ModelSubclass, 'typname', None) is None:
            raise NoTypeSpecified(ModelSubclass)

        n = self.one( "SELECT count(*) FROM pg_type WHERE typname=%s"
                    , (ModelSubclass.typname,)
                     )
        if n < 1:
            # Could be more than one since we don't constrain by typnamespace.
            # XXX What happens then?
            raise NoSuchType(ModelSubclass.typname)

        if ModelSubclass.typname in self.model_registry:
            existing_model = self.model_registry[ModelSubclass.typname]
            raise AlreadyRegistered(existing_model, ModelSubclass.typname)

        self.model_registry[ModelSubclass.typname] = ModelSubclass
        ModelSubclass.db = self

        # register a composite (but don't use RealDictCursor, not sure why)
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=RegularCursor)
            name = ModelSubclass.typname
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
        :raises: :py:exc:`~postgres.NotRegistered`

        """
        key = None
        for key, v in self.model_registry.items():
            if v is ModelSubclass:
                break
        if key is None:
            raise NotRegistered(ModelSubclass)
        del self.model_registry[key]


# Context Managers
# ================

class CursorContextManager(object):
    """Instantiated once per :py:func:`~postgres.Postgres.get_cursor`
    call.

    :param pool: a :py:class:`psycopg2.pool.*ConnectionPool`

    The return value of :py:func:`CursorContextManager.__enter__` is a
    :py:mod:`psycopg2` cursor. Any positional and keyword arguments to our
    constructor are passed through to the cursor constructor. If you pass
    :py:attr:`back_as` as a keyword argument then we'll infer a
    :py:attr:`cursor_factory` from that, though any explicit
    :py:attr:`cursor_factory` keyword argument will take precedence.

    When the block starts, a connection is checked out of the connection pool
    and :py:attr:`autocommit` is set to :py:const:`False`. Then a cursor is
    constructed and that is returned to the :py:attr:`with` statement. If the
    block raises an exception, the connection is rolled back. Otherwise, it's
    committed. In either case, the cursor is closed, :py:attr:`autocommit` is
    reset to :py:class:`False` (just in case) and the connection is put back in
    the pool.

    """

    def __init__(self, pool, *a, **kw):
        self.pool = pool
        self.a = a
        self.kw = self.compute_cursor_factory(**kw)
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
        self.conn.autocommit = False
        self.pool.putconn(self.conn)


    def compute_cursor_factory(self, **kw):
        """Pull :py:attr:`back_as` out of :py:attr:`kw` and maybe add
        :py:attr:`cursor_factory`.

         Valid values for :py:attr:`back_as` are :py:class:`tuple`,
         :py:class:`namedtuple`, :py:class:`dict` (or the strings ``tuple``,
         ``namedtuple``, and ``dict``), and :py:class:`None`. If the value of
         :py:attr:`back_as` is :py:class:`None`, then we won't insert any
         :py:attr:`cursor_factory` keyword argument. Otherwise we'll specify a
         :py:attr:`cursor_factory` that will result in records of the specific
         type: :py:class:`psycopg2.extensions.cursor` for :py:class:`tuple`,
         :py:class:`psycopg2.extras.NamedTupleCursor` for
         :py:class:`namedtuple`, and
         :py:class:`psycopg2.extensions.RealDictCursor` for :py:class:`dict`.

        """

        # Pull back_as out of kw.
        # ===========================
        # If we leave it in psycopg2 will complain. Our internal calls to
        # get_cursor always have it but external use might not.

        back_as = kw.pop('back_as', None)


        if 'cursor_factory' not in kw:

            # Compute cursor_factory from back_as.
            # ====================================

            cursor_factory_registry = { tuple: RegularCursor
                                      , 'tuple': RegularCursor
                                      , namedtuple: NamedTupleCursor
                                      , 'namedtuple': NamedTupleCursor
                                      , dict: RealDictCursor
                                      , 'dict': RealDictCursor
                                      , None: None
                                        }

            if back_as not in cursor_factory_registry:
                raise BadRecordType(back_as)

            cursor_factory = cursor_factory_registry[back_as]
            if cursor_factory is not None:
                kw['cursor_factory'] = cursor_factory

        return kw


class ConnectionContextManager(object):
    """Instantiated once per :py:func:`~postgres.Postgres.get_connection` call.

    :param pool: a :py:class:`psycopg2.pool.*ConnectionPool`

    The return value of :py:func:`ConnectionContextManager.__enter__` is a
    :py:class:`postgres.Connection`. When the block starts, a
    :py:class:`~postgres.Connection` is checked out of the connection pool and
    :py:attr:`autocommit` is set to :py:const:`False`. When the block ends, the
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
        self.conn.autocommit = False
        self.pool.putconn(self.conn)


# Class Factories
# ===============

def make_Connection(postgres):
    """Define and return a subclass of
    :py:class:`psycopg2.extensions.connection`.

    :param postgres: the :py:class:`~postgres.Postgres` instance to bind to
    :returns: a :py:class:`Connection` class

    The class defined and returned here will be linked to the instance of
    :py:class:`~postgres.Postgres` that is passed in as :py:attr:`postgres` and
    will use the :py:attr:`default_cursor_factory` attribute of that object.
    The :py:class:`~postgres.Postgres` instance will use this class as the
    :py:attr:`connection_factory` for its connection pool.

    We also set client encoding to ``UTF-8``.

    """
    class Connection(psycopg2.extensions.connection):

        def __init__(self, *a, **kw):
            psycopg2.extensions.connection.__init__(self, *a, **kw)
            self.set_client_encoding('UTF-8')
            self.postgres = postgres

        def cursor(self, *a, **kw):
            """Extend the :py:meth:`psycopg2.extensions.connection.cursor`
            method to take the default cursor factory from
            :py:class:`~postgres.Postgres`. You can override the default by
            passing the :py:attr:`cursor_factory` keyword argument.

            """
            if 'cursor_factory' not in kw:
                kw['cursor_factory'] = self.postgres.default_cursor_factory
            return psycopg2.extensions.connection.cursor(self, *a, **kw)

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
        def make(self, values):
            if self.name not in postgres.model_registry:

                # This is probably a bug, not a normal user error. It means
                # we've called register_composite for this typname without also
                # registering with model_registry.

                raise NotImplementedError

            ModelSubclass = postgres.model_registry[self.name]
            return ModelSubclass(**dict(zip(self.attnames, values)))

    return DelegatingCaster


if __name__ == '__main__':
    db = Postgres("postgres://jrandom@localhost/test")
    db.run("DROP TABLE IF EXISTS foo CASCADE")
    import doctest
    doctest.testmod()
