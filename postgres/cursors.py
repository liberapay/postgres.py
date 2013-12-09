"""

The :py:mod:`postgres` library extends the cursors provided by
:py:mod:`psycopg2` to add simpler API methods: :py:meth:`run`, :py:meth:`one`,
and :py:meth:`all`.

"""
from __future__ import absolute_import, division, print_function, unicode_literals

from inspect import isclass

from psycopg2.extensions import cursor as TupleCursor
from psycopg2.extras import NamedTupleCursor, RealDictCursor


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


# Cursors
# =======

class SimpleCursorBase(object):
    """

    This is a mixin to provide a simpler API atop the usual DB-API 2.0 API
    provided by :py:mod:`psycopg2`. Any custom cursor class you would like to
    use as the :py:attr:`cursor_factory` argument to
    :py:class:`~postgres.Postgres` must subclass this base.

    >>> from psycopg2.extras import LoggingCursor
    >>> from postgres.cursors import SimpleCursorBase
    >>> class SimpleLoggingCursor(LoggingCursor, SimpleCursorBase):
    ...     pass
    ...
    >>> from postgres import Postgres
    >>> db = Postgres( "postgres://jrandom@localhost/test"
    ...              , cursor_factory=SimpleLoggingCursor
    ...               )

    If you try to use a cursor that doesn't subclass
    :py:class:`~postgres.cursors.SimpleCursorBase` as the default
    :py:attr:`cursor_factory` for a :py:class:`~postgres.Postgres` instance, we
    won't let you:

    >>> db = Postgres( "postgres://jrandom@localhost/test"
    ...              , cursor_factory=LoggingCursor
    ...               )
    ...
    Traceback (most recent call last):
        ...
    postgres.NotASimpleCursor: We can only work with subclasses of postgres.cursors.SimpleCursorBase. LoggingCursor doesn't fit the bill.

    However, we do allow you to use whatever you want as the
    :py:attr:`cursor_factory` argument for individual calls:

    >>> db.all("SELECT * FROM foo", cursor_factory=LoggingCursor)
    Traceback (most recent call last):
        ...
    AttributeError: 'LoggingCursor' object has no attribute 'all'

    """

    def run(self, sql, parameters=None):
        """Execute a query and discard any results.

        .. note::

            See the documentation at :py:meth:`postgres.Postgres.run`.

        """
        self.execute(sql, parameters)


    def one(self, sql, parameters=None, default=None):
        """Execute a query and return a single result or a default value.

        .. note::

            See the documentation at :py:meth:`postgres.Postgres.one`.

        """

        # fetch
        out = self._some(sql, parameters, lo=0, hi=1)
        if out:
            assert len(out) == 1
            out = out[0]
        else:
            out = None

        # dereference
        if out is not None and len(out) == 1:
            seq = list(out.values()) if hasattr(out, 'values') else out
            out = seq[0]

        # default
        if out is None:
            if isexception(default):
                raise default
            out = default

        return out


    def _some(self, sql, parameters, lo, hi):
        self.execute(sql, parameters)

        if self.rowcount < lo:
            raise TooFew(self.rowcount, lo, hi)
        elif self.rowcount > hi:
            raise TooMany(self.rowcount, lo, hi)

        return self.fetchall()


    def all(self, sql, parameters=None):
        """Execute a query and return all results.

        .. note::

            See the documentation at :py:meth:`postgres.Postgres.all`.

        """
        self.execute(sql, parameters)
        recs = self.fetchall()
        if recs and len(recs[0]) == 1:          # dereference
            if hasattr(recs[0], 'values'):      # mapping
                recs = [list(rec.values())[0] for rec in recs]
            else:                               # sequence
                recs = [rec[0] for rec in recs]
        return recs


class SimpleTupleCursor(SimpleCursorBase, TupleCursor):
    """A `simple cursor`_ that returns tuples.
    """

class SimpleNamedTupleCursor(SimpleCursorBase, NamedTupleCursor):
    """A `simple cursor`_ that returns namedtuples.
    """

class SimpleDictCursor(SimpleCursorBase, RealDictCursor):
    """A `simple cursor`_ that returns dicts.
    """


def isexception(obj):
    """Given an object, return a boolean indicating whether it is an instance
    or subclass of :py:class:`Exception`.
    """
    if isinstance(obj, Exception):
        return True
    if isclass(obj) and issubclass(obj, Exception):
        return True
    return False


if __name__ == '__main__':
    from postgres import Postgres
    db = Postgres("postgres://jrandom@localhost/test")
    db.run("DROP SCHEMA IF EXISTS public CASCADE")
    db.run("CREATE SCHEMA public")
    import doctest
    doctest.testmod()
