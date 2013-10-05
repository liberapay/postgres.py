from __future__ import absolute_import, division, print_function, unicode_literals


class CursorContextManager(object):
    """Instantiated once per :py:func:`~postgres.Postgres.get_cursor`
    call.

    :param pool: a :py:class:`psycopg2.pool.*ConnectionPool`

    The return value of :py:func:`CursorContextManager.__enter__` is a
    :py:mod:`psycopg2` cursor. Any positional and keyword arguments to our
    constructor are passed through to the cursor constructor.

    When the block starts, a connection is checked out of the connection pool
    and :py:attr:`autocommit` is set to :py:const:`False`. Then a cursor is
    constructed, and the :py:meth:`one` and :py:meth:`all` methods are scabbed
    on (this allows us to provide our simple API no matter the
    :py:attr:`cursor_factory`). The cursor is returned to the :py:attr:`with`
    statement. If the block raises an exception, the connection is rolled back.
    Otherwise, it's committed. In either case, the cursor is closed,
    :py:attr:`autocommit` is reset to :py:class:`False` (just in case) and the
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
