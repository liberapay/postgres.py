from __future__ import absolute_import, division, print_function, unicode_literals

from psycopg2 import InterfaceError


class CursorContextManager(object):
    """Instantiated once per :func:`~postgres.Postgres.get_cursor` call.

    :param pool: see :mod:`psycopg2_pool`
    :param bool autocommit: see :attr:`psycopg2:connection.autocommit`
    :param bool readonly: see :attr:`psycopg2:connection.readonly`
    :param cursor_kwargs: passed to :meth:`psycopg2:connection.cursor`

    During construction, a connection is checked out of the connection pool
    and its :attr:`autocommit` and :attr:`readonly` attributes are set, then a
    :class:`psycopg2:cursor` is created from that connection.

    Upon exit of the ``with`` block, the connection is rolled back if an
    exception was raised, or committed otherwise. There are two exceptions to
    this:

    1. if :attr:`autocommit` is :obj:`True`, then the connection is neither
       rolled back nor committed;
    2. if :attr:`readonly` is :obj:`True`, then the connection is always rolled
       back, never committed.

    In all cases the cursor is closed and the connection is put back in the pool.

    """

    __slots__ = ('pool', 'conn', 'cursor')

    def __init__(self, pool, autocommit=False, readonly=False, **cursor_kwargs):
        self.pool = pool
        conn = self.pool.getconn()
        conn.autocommit = autocommit
        conn.readonly = readonly
        self.cursor = conn.cursor(**cursor_kwargs)
        self.conn = conn

    def __enter__(self):
        return self.cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Put our connection back in the pool.
        """
        self.cursor.close()
        self.conn.__exit__(exc_type, exc_val, exc_tb)
        self.pool.putconn(self.conn)


class ConnectionCursorContextManager(object):
    """Creates a cursor from the given connection, then wraps it in a context
    manager that automatically commits or rolls back the changes on exit.

    :param conn: a :class:`psycopg2:connection`
    :param bool autocommit: see :attr:`psycopg2:connection.autocommit`
    :param bool readonly: see :attr:`psycopg2:connection.readonly`
    :param cursor_kwargs: passed to :meth:`psycopg2:connection.cursor`

    During construction, the connection's :attr:`autocommit` and :attr:`readonly`
    attributes are set, then :meth:`psycopg2:connection.cursor` is called with
    `cursor_kwargs`.

    Upon exit of the ``with`` block, the connection is rolled back if an
    exception was raised, or committed otherwise. There are two exceptions to
    this:

    1. if :attr:`autocommit` is :obj:`True`, then the connection is neither
       rolled back nor committed;
    2. if :attr:`readonly` is :obj:`True`, then the connection is always rolled
       back, never committed.

    In all cases the cursor is closed.

    """

    __slots__ = ('conn', 'cursor')

    def __init__(self, conn, autocommit=False, readonly=False, **cursor_kwargs):
        conn.autocommit = autocommit
        conn.readonly = readonly
        self.conn = conn
        self.cursor = conn.cursor(**cursor_kwargs)

    def __enter__(self):
        return self.cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()
        self.conn.__exit__(exc_type, exc_val, exc_tb)


class CursorSubcontextManager(object):
    """Wraps a cursor so that it can be used for a subtransaction.

    See :meth:`~postgres.Postgres.get_cursor` for an explanation of subtransactions.

    :param cursor: the :class:`psycopg2:cursor` to wrap
    :param back_as: temporarily overwrites the cursor's
        :attr:`~postgres.cursors.SimpleCursorBase.back_as` attribute

    """

    __slots__ = ('cursor', 'back_as', 'outer_back_as')

    PRESERVE = object()

    def __init__(self, cursor, back_as=PRESERVE):
        self.cursor = cursor
        self.back_as = back_as

    def __enter__(self):
        if self.back_as is not self.PRESERVE:
            self.outer_back_as = self.cursor.back_as
            self.cursor.back_as = self.back_as
        return self.cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.back_as is not self.PRESERVE:
            self.cursor.back_as = self.outer_back_as


class ConnectionContextManager(object):
    """Instantiated once per :func:`~postgres.Postgres.get_connection` call.

    :param pool: see :mod:`psycopg2_pool`
    :param bool autocommit: see :attr:`psycopg2:connection.autocommit`
    :param bool readonly: see :attr:`psycopg2:connection.readonly`

    This context manager checks out a connection out of the specified pool, sets
    its :attr:`autocommit` and :attr:`readonly` attributes.

    The :meth:`__enter__` method returns the :class:`~postgres.Connection`.

    The :meth:`__exit__` method rolls back the connection and puts it back in
    the pool.

    """

    __slots__ = ('pool', 'conn')

    def __init__(self, pool, autocommit=False, readonly=False):
        self.pool = pool
        conn = self.pool.getconn()
        conn.autocommit = autocommit
        conn.readonly = readonly
        self.conn = conn

    def __enter__(self):
        return self.conn

    def __exit__(self, *exc_info):
        """Put our connection back in the pool.
        """
        try:
            self.conn.rollback()
        except InterfaceError:
            pass
        self.pool.putconn(self.conn)
