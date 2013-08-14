""":py:mod:`postgres` implements an object-relational mapper based on
:py:mod:`psycopg2` composite casters. The fundamental technique, introduced by
`Michael Robbelard at PyOhio 2013`_, is to write SQL queries that typecast
results to table types, and then use a :py:mod:`psycopg2`
:py:class:`~psycopg2.extra.CompositeCaster` to map these to Python objects.
This means we get to define our schema in SQL, and we get to write our queries
in SQL, and we get to explicitly indicate in our SQL queries how Python should
map the results to objects, and then we can write Python objects that contain
only business logic and not schema definitions.

.. _Michael Robbelard at PyOhio 2013: https://www.youtube.com/watch?v=Wz1_GYc4GmU#t=25m06s

Every table in PostgreSQL has a type associated with it, which is the column
definition for that table. These are composite types just like any other
composite type in PostgreSQL, meaning we can use them to cast query results.
When we do, we get a single field that contains our query result, nested one
level::

    test=# CREATE TABLE foo (bar text, baz int);
    CREATE TABLE
    test=# INSERT INTO foo VALUES ('blam', 42);
    INSERT 0 1
    test=# INSERT INTO foo VALUES ('whit', 537);
    INSERT 0 1
    test=# SELECT * FROM foo;
    +------+-----+
    | bar  | baz |
    +------+-----+
    | blam |  42 |
    | whit | 537 |
    +------+-----+
    (2 rows)

    test=# SELECT foo.*::foo FROM foo;
    +------------+
    |    foo     |
    +------------+
    | (blam,42)  |
    | (whit,537) |
    +------------+
    (2 rows)

    test=#

The same thing works for views::

    test=# CREATE VIEW bar AS SELECT bar FROM foo;
    CREATE VIEW
    test=# SELECT * FROM bar;
    +------+
    | bar  |
    +------+
    | blam |
    | whit |
    +------+
    (2 rows)

    test=# SELECT bar.*::bar FROM bar;
    +--------+
    |  bar   |
    +--------+
    | (blam) |
    | (whit) |
    +--------+
    (2 rows)

    test=#

:py:mod:`psycopg2` provides a :py:func:`~psycopg2.extras.register_composite`
function that lets us map PostgreSQL composite types to Python objects. This
includes table and view types, and that is the basis for
:py:mod:`postgres.orm`. We map based on types, not tables.


ORM Tutorial
------------

First, write a Python class that subclasses :py:class:`~postgres.orm.Model`::

    >>> from postgres.orm import Model
    >>> class Foo(Model):
    ...     typname = "foo"
    ...

Your model must have a :py:attr:`typname` attribute, which is the name of the
PostgreSQL type for which this class is an object mapping. (``typname``,
spelled without an "e," is the name of the relevant column in the ``pg_type``
table in your database.)

Second, register your model with your :py:class:`~postgres.Postgres` instance:

    >>> db.register_model(Foo)

That will plug your model into the :py:mod:`psycopg2` composite casting
machinery, and you'll now get instances of your model back from
:py:meth:`~postgres.Postgres.all` and :py:meth:`~postgres.Postgres.one_or_zero`
when you cast to the relevant type in your query. If your query returns more
than one column, you'll need to dereference the column containing the model
just as with any other query:

    >>> rec = db.one_or_zero("SELECT foo.*::foo, bar.* "
    ...                      "FROM foo JOIN bar ON foo.bar = bar.bar "
    ...                      "ORDER BY foo.bar LIMIT 1")
    >>> rec.foo.bar
    'blam'
    >>> rec.bar
    'blam'

As usual, if your query only returns one column, then
:py:meth:`~postgres.Postgres.all` and :py:meth:`~postgres.Postgres.one_or_zero`
will do the dereferencing for you:

    >>> foo = db.one_or_zero("SELECT foo.*::foo FROM foo WHERE bar='blam'")
    >>> foo.bar
    'blam'
    >>> [foo.bar for foo in db.all("SELECT foo.*::foo FROM foo")]
    ['blam', 'whit']


The Model Base Class
--------------------

"""
from __future__ import print_function, unicode_literals


# Exceptions
# ==========

class ReadOnly(Exception):
    def __str__(self):
        return "{} is a read-only attribute. Your Model should implement " \
               "methods to change data; use update_local from your methods " \
               "to sync local state.".format(self.args[0])

class UnknownAttributes(Exception):
    def __str__(self):
        return "The following attribute(s) are unknown to us: {}." \
               .format(", ".join(self.args[0]))

class NotBound(Exception):
    def __str__(self):
        return "You have to set {}.typname to the name of a type in your " \
               "database.".format(self.args[0].__name__)

class NotRegistered(Exception):
    def __str__(self):
        return "You have to register {} with a Postgres instance." \
               .format(self.args[0].__name__)



# Stuff
# =====

class Model(object):
    """

    This is the base class for models in :py:mod:`postgres.orm`. Instances of
    subclasses of :py:class:`~postgres.orm.Model` will have an attribute for
    each field in the composite type for which the subclass is registered (for
    table and view types, these will be the columns of the table or view).
    These attributes are read-only. We don't update your database. You are
    expected to do that yourself in methods on your subclass. To keep instance
    attributes in sync after a database update, use the
    :py:meth:`~postgres.orm.Model.update_attributes` helper.

    """

    typname = None                          # an entry in pg_type
    db = None                               # will be set to a Postgres object
    __read_only_attributes = []             # bootstrap

    def __init__(self, **kw):
        if self.db is None:
            raise NotBound(self)
        if self.db is None:
            raise NotRegistered(self)
        self.__read_only_attributes = []    # overwrite class-level one
        for k,v in kw.items():
            self.__read_only_attributes.append(k)
            self.__dict__[k] = v

    def __setattr__(self, name, value):
        if name in self.__read_only_attributes:
            raise ReadOnly(name)
        return super(Model, self).__setattr__(name, value)

    def update_attributes(self, **kw):
        """Update instance attributes, according to :py:attr:`kw`.

        :raises: :py:exc:`~postgres.orm.UnknownAttributes`

        Call this when you update state in the database and you want to keep
        instance attributes in sync. Note that the only attributes we can
        update here are the ones that were given to us by the
        :py:mod:`psycopg2` composite caster machinery when we were first
        instantiated. These will be the fields of the composite type for which
        we were registered, which will be column names for table and view
        types.

        """
        unknown = []
        for name in kw:
            if name not in self.__read_only_attributes:
                unknown.append(name)
        if unknown:
            raise UnknownAttributes(unknown)
        self.__dict__.update(**kw)


if __name__ == '__main__':
    from postgres import Postgres
    db = Postgres("postgres://jrandom@localhost/test")
    db.run("DROP TABLE IF EXISTS foo CASCADE")
    db.run("CREATE TABLE foo (bar text, baz int)")
    db.run("INSERT INTO foo VALUES ('blam', 42)")
    db.run("INSERT INTO foo VALUES ('whit', 537)")
    db.run("CREATE VIEW bar AS SELECT bar FROM foo")
    import doctest
    doctest.testmod()
