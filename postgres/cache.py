"""
A query's results can be stored in the :attr:`cache` attribute of the
:class:`~postgres.Postgres` object to avoid burdening the database with
redundant requests. The caching is enabled by the `max_age` argument of the
`one` and `all` methods. For example, this call fetches a row from the `foo`
table and caches it for 5 seconds:

    >>> db.one("SELECT * FROM foo WHERE bar = 'bit'", max_age=5)
    Record(bar='bit', baz=537)

Any other thread trying to send the same query while it's still being processed
waits for the results to be received instead of sending a duplicate query.

The cache key is the query as it's sent to the server, so any difference in the
parameter values, casing or even whitespace, will result in a cache miss. You
might need to refactor your code if it sends queries that are similar but not
exactly identical.

The `max_age` argument doesn't interfere with the `back_as` argument. Moreover,
the `one` and `all` methods can each use the results cached by the other.
Consequently, the following call hits the cache if it's executed within 5
seconds of the previous call above:

    >>> db.all("SELECT * FROM foo WHERE bar = 'bit'", back_as=dict, max_age=5)
    [{'bar': 'bit', 'baz': 537}]

It's also possible to use different `max_age` values for the same query. If a
specified `max_age` is greater than the previous one, then the lifetime of the
cache entry is extended accordingly.

The Cache class
---------------

"""

from collections import OrderedDict
from threading import RLock
from time import perf_counter as time


class CacheEntry:

    __slots__ = ('query', 'columns', 'rows', 'max_age', 'lock', 'time')

    def __init__(self, query, max_age, columns, rows):
        self.query = query
        self.max_age = max_age
        self.columns = columns
        self.rows = rows
        self.lock = RLock()
        self.time = time()


class Cache:
    """A cache for rows fetched from a database.

    :arg int max_size: The maximum number of entries allowed in the cache.

    .. warning::
        This cache is only designed to be thread-safe in CPython >= 3.6 and similar
        Python implementations in which the :class:`~collections.OrderedDict`
        class is thread-safe.

    A separate lock is used for each entry so that unrelated queries don't block
    each other.

    After inserting a new entry, the oldest one is removed if the cache now has
    more than `max_size` entries.
    """

    __slots__ = ('entries', 'max_size')

    def __init__(self, max_size=128):
        self.entries = OrderedDict()
        self.max_size = max_size

    def __setitem__(self, key, entry):
        """Insert an entry into the cache.
        """
        self.entries[key] = entry
        try:
            self.entries.move_to_end(key)
        except KeyError:
            return
        while len(self.entries) > self.max_size:
            self.entries.popitem(last=False)

    def clear(self):
        """Empty the cache. This method isn't used internally.
        """
        self.entries.clear()

    def get_lock(self, key):
        """Get the lock object for the specified key.
        """
        temporary_entry = CacheEntry(key, 60, None, None)
        return self.entries.setdefault(key, temporary_entry).lock

    def lookup(self, key, max_age):
        """Look up a cache entry and check its age.

        This function returns :obj:`None` if there isn't an entry in the cache
        for the specified key or if the entry is older than `max_age`.
        """
        entry = self.entries.get(key)
        if entry is None or entry.rows is None:
            return
        now = time()
        if entry.time < (now - entry.max_age):
            if entry and entry.time >= (now - max_age):
                # Extend the lifetime of the entry.
                entry.max_age = max_age
            else:
                # Don't attempt to drop the entry, just return.
                return
        return entry

    def pop_entry(self, entry, blocking=True):
        """Remove the specified entry from the cache.

        If `blocking` is `False`, then the entry will only be removed if its
        lock isn't currently held by another thread.
        """
        if entry.lock.acquire(blocking=blocking):
            try:
                popped_entry = self.entries.pop(entry.query, None)
                if popped_entry is not None and popped_entry is not entry:
                    # We popped a different entry inserted by another thread,
                    # put it back in.
                    self.entries[popped_entry.query] = popped_entry
            finally:
                entry.lock.release()

    def prune(self):
        """Drop stale entries from the cache. This method isn't used internally.
        """
        for key, entry in list(self.entries.items()):
            if entry.time >= (time() - entry.max_age):
                continue
            self.pop_entry(entry, blocking=False)
