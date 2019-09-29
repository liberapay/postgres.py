"""

The :mod:`postgres` library extends the cursors provided by
:mod:`psycopg2` to add simpler API methods: :meth:`run`, :meth:`one`,
and :meth:`all`.

"""
from __future__ import absolute_import, division, print_function, unicode_literals

from inspect import isclass
from operator import itemgetter

from psycopg2.extensions import cursor as TupleCursor
from psycopg2.extras import NamedTupleCursor


itemgetter0 = itemgetter(0)


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
    provided by :mod:`psycopg2`. Any custom cursor class you would like to
    use as the :attr:`cursor_factory` argument to
    :class:`~postgres.Postgres` must subclass this base.

    >>> from psycopg2.extras import LoggingCursor
    >>> from postgres.cursors import SimpleCursorBase
    >>> class SimpleLoggingCursor(LoggingCursor, SimpleCursorBase):
    ...     pass
    ...
    >>> from postgres import Postgres
    >>> db = Postgres(cursor_factory=SimpleLoggingCursor)

    If you try to use a cursor that doesn't subclass
    :class:`~postgres.cursors.SimpleCursorBase` as the default
    :attr:`cursor_factory` for a :class:`~postgres.Postgres` instance, we
    won't let you:

    >>> db = Postgres(cursor_factory=LoggingCursor)
    ...
    Traceback (most recent call last):
        ...
    postgres.NotASimpleCursor: We can only work with subclasses of postgres.cursors.SimpleCursorBase. LoggingCursor doesn't fit the bill.

    However, we do allow you to use whatever you want as the
    :attr:`cursor_factory` argument for individual calls:

    >>> db.all("SELECT * FROM foo", cursor_factory=LoggingCursor)
    Traceback (most recent call last):
        ...
    AttributeError: 'LoggingCursor' object has no attribute 'all'

    """

    back_as = None

    def __iter__(self):
        it = TupleCursor.__iter__(self)
        back_as = self.back_as
        if back_as:
            try:
                back_as = self.connection.back_as_registry[back_as]
            except KeyError:
                raise BadBackAs(back_as)
        while True:
            try:
                t = next(it)
            except StopIteration:
                return
            yield (back_as(self.description, t) if back_as else t)

    def fetchone(self, back_as=None):
        t = TupleCursor.fetchone(self)
        if t is not None:
            back_as = back_as or self.back_as
            if back_as:
                try:
                    back_as = self.connection.back_as_registry[back_as]
                except KeyError:
                    raise BadBackAs(back_as)
                return back_as(self.description, out)
            else:
                return t

    def fetchmany(self, size=None, back_as=None):
        ts = TupleCursor.fetchmany(self, size)
        cols = self.description
        back_as = back_as or self.back_as
        if back_as:
            try:
                back_as = self.connection.back_as_registry[back_as]
            except KeyError:
                raise BadBackAs(back_as)
            return [back_as(cols, t) for t in ts]
        else:
            return t

    def fetchall(self, back_as=None):
        ts = TupleCursor.fetchall(self)
        cols = self.description
        back_as = back_as or self.back_as
        if back_as:
            try:
                back_as = self.connection.back_as_registry[back_as]
            except KeyError:
                raise BadBackAs(back_as)
            return [back_as(cols, t) for t in ts]
        else:
            return t

    def run(self, sql, parameters=None):
        """Execute a query and discard any results.

        .. note::

            See the documentation at :meth:`postgres.Postgres.run`.

        """
        self.execute(sql, parameters)

    def one(self, sql, parameters=None, default=None, back_as=None):
        """Execute a query and return a single result or a default value.

        .. note::

            See the documentation at :meth:`postgres.Postgres.one`.

        """

        # fetch
        self.execute(sql, parameters)
        if self.rowcount == 1:
            out = TupleCursor.fetchone(self)
        elif self.rowcount == 0:
            if isexception(default):
                raise default
            return default
        elif self.rowcount < 0:
            raise TooFew(self.rowcount, 0, 1)
        else:
            raise TooMany(self.rowcount, 0, 1)

        if len(out) == 1:
            # dereference
            out = out[0]
            if out is None:
                if isexception(default):
                    raise default
                return default
        else:
            # transform
            back_as = back_as or self.back_as
            if back_as:
                try:
                    back_as = self.connection.back_as_registry[back_as]
                except KeyError:
                    raise BadBackAs(back_as)
                out = back_as(self.description, out)

        return out

    def all(self, sql, parameters=None, back_as=None):
        """Execute a query and return all results.

        .. note::

            See the documentation at :meth:`postgres.Postgres.all`.

        """
        self.execute(sql, parameters)
        recs = TupleCursor.fetchall(self)
        if recs:
            if len(recs[0]) == 1:
                # dereference
                recs = list(map(itemgetter0, recs))
            else:
                # transform
                back_as = back_as or self.back_as
                if back_as:
                    try:
                        back_as = self.connection.back_as_registry[back_as]
                    except KeyError:
                        raise BadBackAs(back_as)
                    recs = [back_as(self.description, r) for r in recs]
        return recs


def make_dict(cols, vals):
    return dict(zip(map(itemgetter0, cols), vals))


def make_namedtuple(cols, vals):
    # We use the NamedTupleCursor cache introduced in psycopg2 2.8.
    # See https://github.com/psycopg/psycopg2/issues/838 for details.
    key = tuple(map(itemgetter0, cols))
    cls = NamedTupleCursor._cached_make_nt(key)
    return cls(*vals)


def return_tuple_as_is(cols, vals):
    return vals


class Row(object):
    """A versatile row type.
    """

    __slots__ = ('_cols', '__dict__')

    def __init__(self, cols, values):
        self._cols = cols
        self.__dict__.update(zip(map(itemgetter0, cols), values))

    def __getitem__(self, key):
        if isinstance(key, int):
            return self.__dict__[self._cols[key][0]]
        elif isinstance(key, slice):
            return [self.__dict__[col[0]] for col in self._cols[key]]
        else:
            return self.__dict__[key]

    def __setitem__(self, key, value):
        if isinstance(key, (int, slice)):
            raise TypeError('index-based assignments are not allowed')
        self.__dict__[key] = value

    def __contains__(self, key):
        return key in self.__dict__

    def __eq__(self, other):
        if isinstance(other, Row):
            return other.__dict__ == self.__dict__
        elif isinstance(other, dict):
            return other == self.__dict__
        elif isinstance(other, tuple):
            return len(self.__dict__) == len(self._cols) and other == tuple(self)
        return False

    def __len__(self):
        return len(self.__dict__)

    def __repr__(self):
        col_indexes = {col[0]: i for i, col in enumerate(self._cols)}
        after = len(self._cols)
        key = lambda t: (col_indexes.get(t[0], after), t[0])
        return 'Row(%s)' % (
            ', '.join(map('%s=%r'.__mod__, sorted(self.__dict__.items(), key=key)))
        )

    def __getstate__(self):
        # We only save the column names, not the other column attributes.
        return tuple(map(itemgetter0, self._cols)), self.__dict__.copy()

    def __setstate__(self, data):
        self._cols = tuple((col_name,) for col_name in data[0])
        self.__dict__.update(data[1])

    def _asdict(self):
        """For compatibility with namedtuple classes."""
        return self.__dict__.copy()

    @property
    def _fields(self):
        """For compatibility with namedtuple classes."""
        return tuple(map(itemgetter0, self._cols))


class SimpleTupleCursor(SimpleCursorBase, TupleCursor):
    """A `simple cursor`_ that returns tuples.

    This type of cursor is especially well suited if you need to fetch and process
    a large number of rows at once, because tuples occupy less memory than dicts.
    """


class SimpleNamedTupleCursor(SimpleCursorBase, TupleCursor):
    """A `simple cursor`_ that returns namedtuples.

    This type of cursor is especially well suited if you need to fetch and process
    a large number of similarly-structured rows at once, and you also need the row
    objects to be more evolved than simple tuples.
    """

    back_as = 'namedtuple'


class SimpleDictCursor(SimpleCursorBase, TupleCursor):
    """A `simple cursor`_ that returns dicts.

    This type of cursor is especially well suited if you don't care about the
    order of the columns and don't need to access them as attributes.
    """

    back_as = 'dict'


class SimpleRowCursor(SimpleCursorBase, TupleCursor):
    """A `simple cursor`_ that returns :class:`Row` objects.

    This type of cursor is especially well suited if you want rows to be mutable.

    The Row class implements both dict-style and attribute-style lookups and
    assignments, in addition to index-based lookups. However, index-based
    assigments aren't allowed.

        >>> from postgres import Postgres
        >>> from postgres.cursors import SimpleRowCursor
        >>> db = Postgres(cursor_factory=SimpleRowCursor)
        >>> row = db.one("SELECT 1 as key, 'foo' as value")
        >>> row[0] == row['key'] == row.key == 1
        True
        >>> key, value = row
        >>> (key, value)
        (1, 'foo')
        >>> row.value = 'bar'
        >>> row.timestamp = '2019-09-20 13:15:22.060537+00'
        >>> row
        Row(key=1, value='bar', timestamp='2019-09-20 13:15:22.060537+00')

    Although Row objects support item lookups and assigments, they are not
    instances of the :class:`dict` class and they don't have its methods
    (:meth:`~dict.get`, :meth:`~dict.items`, etc.).
    """

    back_as = 'Row'


def isexception(obj):
    """Given an object, return a boolean indicating whether it is an instance
    or subclass of :class:`Exception`.
    """
    if isinstance(obj, Exception):
        return True
    if isclass(obj) and issubclass(obj, Exception):
        return True
    return False


if __name__ == '__main__':
    from postgres import Postgres
    db = Postgres()
    db.run("DROP SCHEMA IF EXISTS public CASCADE")
    db.run("CREATE SCHEMA public")
    import doctest
    doctest.testmod()
