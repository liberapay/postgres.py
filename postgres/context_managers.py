from __future__ import print_function, unicode_literals

from collections import namedtuple

from postgres.cursors import SimpleTupleCursor, SimpleNamedTupleCursor
from postgres.cursors import SimpleDictCursor


class BadRecordType(Exception):
    def __str__(self):
        return "Bad back_as: {}. Available back_as values are: tuple, " \
               "namedtuple, dict, or None (to use the default)." \
               .format(self.args[0])


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
    constructed, and the :py:meth:`one` and :py:meth:`all` methods are scabbed
    on (this allows us to provide our simple API no matter the
    :py:attr:`cursor_factory`). The cursor is returned to the :py:attr:`with`
    statement. If the block raises an exception, the connection is rolled back.
    Otherwise, it's committed. In either case, the cursor is closed,
    :py:attr:`autocommit` is reset to :py:class:`False` (just in case) and the
    connection is put back in the pool.

    """

    def __init__(self, pool, back_as=None, *a, **kw):
        self.pool = pool
        self.a = a
        self.kw = handle_back_as(back_as, **kw)
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


def handle_back_as(back_as, **kw):
    """Add :py:attr:`cursor_factory` to :py:attr:`kw`, maybe.

    Valid values for :py:attr:`back_as` are :py:class:`tuple`,
    :py:class:`namedtuple`, :py:class:`dict` (or the strings ``tuple``,
    ``namedtuple``, and ``dict``), and :py:class:`None`. If the value of
    :py:attr:`back_as` is :py:class:`None`, then we won't insert any
    :py:attr:`cursor_factory` keyword argument. Otherwise we'll specify a
    :py:attr:`cursor_factory` that will result in records of the designated
    type: :py:class:`postgres.cursor.SimpleTupleCursor` for :py:class:`tuple`,
    :py:class:`postgres.cursor.SimpleNamedTupleCursor` for
    :py:class:`namedtuple`, and :py:class:`postgres.cursor.SimpleDictCursor`
    for :py:class:`dict`.

    """

    if 'cursor_factory' not in kw:

        # Compute cursor_factory from back_as.
        # ====================================

        cursor_factory_registry = { tuple: SimpleTupleCursor
                                  , 'tuple': SimpleTupleCursor
                                  , namedtuple: SimpleNamedTupleCursor
                                  , 'namedtuple': SimpleNamedTupleCursor
                                  , dict: SimpleDictCursor
                                  , 'dict': SimpleDictCursor
                                  , None: None
                                    }

        if back_as not in cursor_factory_registry:
            raise BadRecordType(back_as)

        cursor_factory = cursor_factory_registry[back_as]
        if cursor_factory is not None:
            kw['cursor_factory'] = cursor_factory

    return kw
