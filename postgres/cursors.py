"""

The :mod:`postgres` library extends the cursors provided by
:mod:`psycopg2` to add simpler API methods: :meth:`run`, :meth:`one`,
and :meth:`all`.

"""

from inspect import isclass
from operator import itemgetter

from psycopg2.extensions import cursor as TupleCursor
from psycopg2.extras import NamedTupleCursor

from postgres.cache import CacheEntry


itemgetter0 = itemgetter(0)


# Exceptions
# ==========

class BadBackAs(Exception):

    def __init__(self, bad_value, back_as_registry):
        self.bad_value = bad_value
        self.back_as_registry = back_as_registry

    def __str__(self):
        available_values = ', '.join(sorted([
            k for k in self.back_as_registry.keys() if isinstance(k, type(''))
        ]))
        return "{!r} is not a valid value for the back_as argument.\n" \
               "The available values are: {}." \
               .format(self.bad_value, available_values)


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

class TooFew(OutOfBounds):
    pass

class TooMany(OutOfBounds):
    pass


# Cursors
# =======

class SimpleCursorBase:
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

    >>> db = Postgres(cursor_factory=LoggingCursor)  # doctest: +NORMALIZE_WHITESPACE
    ...
    Traceback (most recent call last):
        ...
    postgres.NotASimpleCursor: We can only work with subclasses of SimpleCursorBase,
    LoggingCursor doesn't fit the bill.

    However, we do allow you to use whatever you want as the
    :attr:`cursor_factory` argument for individual calls:

    >>> with db.get_cursor(cursor_factory=LoggingCursor) as cursor:
    ...     cursor.all("SELECT * FROM foo")
    Traceback (most recent call last):
        ...
    AttributeError: 'LoggingCursor' object has no attribute 'all'

    .. attribute:: back_as

        Determines which type of row is returned by the various methods. The valid
        values are the keys of the :attr:`~postgres.Postgres.back_as_registry`.

    """

    back_as = None

    def __iter__(self):
        it = TupleCursor.__iter__(self)
        back_as = self.back_as
        if back_as:
            try:
                back_as = self.connection.back_as_registry[back_as]
            except KeyError:
                raise BadBackAs(back_as, self.connection.back_as_registry)
        while True:
            try:
                t = next(it)
            except StopIteration:
                return
            yield (back_as(self.description, t) if back_as else t)

    def execute(self, sql, **kw):
        """This method is an alias of :meth:`run`.
        """
        self.run(sql, **kw)

    def fetchone(self, back_as=None):
        t = TupleCursor.fetchone(self)
        if t is not None:
            back_as = back_as or self.back_as
            if back_as:
                try:
                    back_as = self.connection.back_as_registry[back_as]
                except KeyError:
                    raise BadBackAs(back_as, self.connection.back_as_registry)
                return back_as(self.description, t)
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
                raise BadBackAs(back_as, self.connection.back_as_registry)
            return [back_as(cols, t) for t in ts]
        else:
            return ts

    def fetchall(self, back_as=None):
        ts = TupleCursor.fetchall(self)
        cols = self.description
        back_as = back_as or self.back_as
        if back_as:
            try:
                back_as = self.connection.back_as_registry[back_as]
            except KeyError:
                raise BadBackAs(back_as, self.connection.back_as_registry)
            return [back_as(cols, t) for t in ts]
        else:
            return ts

    def mogrify(self, sql, parameters, **kw):
        if kw:
            if parameters:
                parameters.update(kw)
            else:
                parameters = kw
        return TupleCursor.mogrify(self, sql, parameters)

    def run(self, sql, parameters=None, **kw):
        """Execute a query, without returning any results.

        :param str sql: the SQL statement to execute
        :param parameters: the `bind parameters`_ for the SQL statement
        :type parameters: dict or tuple
        :param kw: alternative to passing a :class:`dict` as `parameters`

        .. _bind parameters: #bind-parameters

        Example usage:

        >>> db.run("DROP TABLE IF EXISTS foo CASCADE")
        >>> db.run("CREATE TABLE foo (bar text, baz int)")
        >>> bar, baz = 'buz', 42
        >>> db.run("INSERT INTO foo VALUES (%s, %s)", (bar, baz))
        >>> db.run("INSERT INTO foo VALUES (%(bar)s, %(baz)s)", dict(bar=bar, baz=baz))
        >>> db.run("INSERT INTO foo VALUES (%(bar)s, %(baz)s)", bar=bar, baz=baz)

        """
        if kw:
            if parameters:
                parameters.update(kw)
            else:
                parameters = kw
        TupleCursor.execute(self, sql, parameters)

    def one(self, sql, parameters=None, default=None, back_as=None, max_age=None, **kw):
        """Execute a query and return a single result or a default value.

        :param str sql: the SQL statement to execute
        :param parameters: the `bind parameters`_ for the SQL statement
        :type parameters: dict or tuple
        :param default: the value to return or raise if no results are found
        :param back_as: the type of record to return
        :type back_as: type or string
        :param float max_age: how long to keep the result in the cache (in seconds)
        :param kw: alternative to passing a :class:`dict` as `parameters`

        :returns: a single record or value, or :attr:`default` (if
            :attr:`default` is not an :class:`Exception`)

        :raises: :exc:`~postgres.TooFew` or :exc:`~postgres.TooMany`,
            or :attr:`default` (if :attr:`default` is an
            :class:`Exception`)

        .. _bind parameters: #bind-parameters

        Use this for the common case where there should only be one record, but
        it may not exist yet.

        >>> db.one("SELECT * FROM foo WHERE bar='buz'")
        Record(bar='buz', baz=42)

        If the record doesn't exist, we return :class:`None`:

        >>> record = db.one("SELECT * FROM foo WHERE bar='blam'")
        >>> if record is None:
        ...     print("No blam yet.")
        ...
        No blam yet.

        If you pass :attr:`default` we'll return that instead of :class:`None`:

        >>> db.one("SELECT * FROM foo WHERE bar='blam'", default=False)
        False

        If you pass an :class:`Exception` instance or subclass for
        :attr:`default`, we will raise that for you:

        >>> db.one("SELECT * FROM foo WHERE bar='blam'", default=Exception)
        Traceback (most recent call last):
            ...
        Exception

        We specifically stop short of supporting lambdas or other callables for
        the :attr:`default` parameter. That gets complicated quickly, and
        it's easy to just check the return value in the caller and do your
        extra logic there.

        You can use :attr:`back_as` to override the type associated with the
        default :attr:`cursor_factory` for your
        :class:`~postgres.Postgres` instance:

        >>> db.default_cursor_factory
        <class 'postgres.cursors.SimpleNamedTupleCursor'>
        >>> db.one( "SELECT * FROM foo WHERE bar='buz'"
        ...       , back_as=dict
        ...        )
        {'bar': 'buz', 'baz': 42}

        That's a convenience so you don't have to go to the trouble of
        remembering where :class:`~postgres.cursors.SimpleDictCursor` lives
        and importing it in order to get dictionaries back.

        If the query result has only one column, then we dereference that for
        you.

        >>> db.one("SELECT baz FROM foo WHERE bar='buz'")
        42

        And if the dereferenced value is :class:`None`, we return the value
        of :attr:`default`:

        >>> db.one("SELECT sum(baz) FROM foo WHERE bar='nope'", default=0)
        0

        Dereferencing isn't performed if a :attr:`back_as` argument is provided:

        >>> db.one("SELECT null as foo", back_as=dict)
        {'foo': None}

        """
        query = self.mogrify(sql, parameters, **kw)
        if max_age:
            entry = self._cached_fetchall(query, max_age)
            columns = entry.columns
            rowcount = len(entry.rows)
            if rowcount == 1:
                row_tuple = entry.rows[0]
        else:
            self.run(query)
            columns = self.description
            rowcount = self.rowcount
            if rowcount == 1:
                row_tuple = TupleCursor.fetchone(self)

        if rowcount == 1:
            pass
        elif rowcount == 0:
            if isexception(default):
                raise default
            return default
        elif rowcount < 0:
            raise TooFew(rowcount, 0, 1)
        else:
            raise TooMany(rowcount, 0, 1)

        if len(row_tuple) == 1 and back_as is None:
            # dereference
            out = row_tuple[0]
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
                    raise BadBackAs(back_as, self.connection.back_as_registry)
                out = back_as(columns, row_tuple)
            else:
                out = row_tuple

        return out

    def all(self, sql, parameters=None, back_as=None, max_age=None, **kw):
        """Execute a query and return all results.

        :param str sql: the SQL statement to execute
        :param parameters: the `bind parameters`_ for the SQL statement
        :type parameters: dict or tuple
        :param back_as: the type of record to return
        :type back_as: type or string
        :param float max_age: how long to keep the results in the cache (in seconds)
        :param kw: alternative to passing a :class:`dict` as `parameters`

        :returns: :class:`list` of records or :class:`list` of single values

        .. _bind parameters: #bind-parameters

        Use it like this:

        >>> db.all("SELECT * FROM foo ORDER BY bar")
        [Record(bar='bit', baz=537), Record(bar='buz', baz=42)]

        You can use :attr:`back_as` to override the type associated with the
        default :attr:`cursor_factory` for your
        :class:`~postgres.Postgres` instance:

        >>> db.default_cursor_factory
        <class 'postgres.cursors.SimpleNamedTupleCursor'>
        >>> db.all("SELECT * FROM foo ORDER BY bar", back_as=dict)
        [{'bar': 'bit', 'baz': 537}, {'bar': 'buz', 'baz': 42}]

        That's a convenience so you don't have to go to the trouble of
        remembering where :class:`~postgres.cursors.SimpleDictCursor` lives
        and importing it in order to get dictionaries back.

        If the query results in records with a single column, we return a list
        of the values in that column rather than a list of records of values.

        >>> db.all("SELECT baz FROM foo ORDER BY bar")
        [537, 42]

        Unless a :attr:`back_as` argument is provided:

        >>> db.all("SELECT baz FROM foo ORDER BY bar", back_as=dict)
        [{'baz': 537}, {'baz': 42}]

        """
        query = self.mogrify(sql, parameters, **kw)
        if max_age:
            entry = self._cached_fetchall(query, max_age)
            columns, recs = entry.columns, entry.rows
        else:
            self.run(query)
            recs = TupleCursor.fetchall(self)
            columns = self.description
        if recs:
            if len(recs[0]) == 1 and back_as is None:
                # dereference
                recs = list(map(itemgetter0, recs))
            else:
                # transform
                back_as = back_as or self.back_as
                if back_as:
                    try:
                        back_as = self.connection.back_as_registry[back_as]
                    except KeyError:
                        raise BadBackAs(back_as, self.connection.back_as_registry)
                    recs = [back_as(columns, r) for r in recs]
                elif max_age:
                    recs = recs.copy()
        return recs

    def _cached_fetchall(self, query, max_age):
        cache = self.connection.postgres.cache
        entry = cache.lookup(query, max_age)
        if entry:
            return entry
        with cache.get_lock(query):
            # Check that an entry hasn't been inserted after our first lookup
            # but before we obtained the lock.
            entry = cache.lookup(query, max_age)
            if entry:
                return entry
            # Okay, send the query to the database and cache the result.
            self.run(query)
            rows = TupleCursor.fetchall(self)
            entry = CacheEntry(query, max_age, self.description, rows)
            cache[query] = entry
            return entry


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


class Row:
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


if __name__ == '__main__':  # pragma: no cover
    from postgres import Postgres
    db = Postgres()
    db.run("DROP SCHEMA IF EXISTS public CASCADE")
    db.run("CREATE SCHEMA public")
    db.run("CREATE TABLE foo (bar text, baz int)")
    db.run("INSERT INTO foo VALUES ('buz', 42)")
    db.run("INSERT INTO foo VALUES ('bit', 537)")
    import doctest
    doctest.testmod()
