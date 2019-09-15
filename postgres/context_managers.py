from __future__ import absolute_import, division, print_function, unicode_literals


class CursorContextManager(object):
    """Instantiated once per :func:`~postgres.Postgres.get_cursor`
    call.

    :param pool: a :class:`psycopg2.pool.*ConnectionPool`

    The return value of :func:`CursorContextManager.__enter__` is a
    :mod:`psycopg2` cursor. Any positional and keyword arguments to our
    constructor are passed through to the cursor constructor.

    When the block starts, a connection is checked out of the connection pool
    and :attr:`autocommit` is set to :const:`False`. Then a cursor is
    constructed, and the :meth:`one` and :meth:`all` methods are scabbed
    on (this allows us to provide our simple API no matter the
    :attr:`cursor_factory`). The cursor is returned to the :attr:`with`
    statement. If the block raises an exception, the connection is rolled back.
    Otherwise, it's committed. In either case, the cursor is closed,
    :attr:`autocommit` is reset to :class:`False` (just in case) and the
    connection is put back in the pool.

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
        self.conn.autocommit = False
        self.pool.putconn(self.conn)


class ConnectionContextManager(object):
    """Instantiated once per :func:`~postgres.Postgres.get_connection` call.

    :param pool: a :class:`psycopg2.pool.*ConnectionPool`

    The return value of :func:`ConnectionContextManager.__enter__` is a
    :class:`postgres.Connection`. When the block starts, a
    :class:`~postgres.Connection` is checked out of the connection pool and
    :attr:`autocommit` is set to :const:`False`. When the block ends, the
    :class:`~postgres.Connection` is rolled back before being put back in
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
