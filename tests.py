from __future__ import absolute_import, division, print_function, unicode_literals

from collections import namedtuple
import sys
from unittest import TestCase

from postgres import (
    AlreadyRegistered, NotAModel, NotRegistered, NoSuchType, NoTypeSpecified,
    Postgres,
)
from postgres.cursors import (
    BadBackAs, TooFew, TooMany,
    Row, SimpleDictCursor, SimpleNamedTupleCursor, SimpleRowCursor, SimpleTupleCursor,
)
from postgres.orm import Model, ReadOnlyAttribute, UnknownAttributes
from psycopg2.errors import InterfaceError, ProgrammingError, ReadOnlySqlTransaction
from pytest import mark, raises


class Heck(Exception):
    pass


# harnesses
# =========

class WithSchema(TestCase):

    def setUp(self):
        self.db = Postgres()
        self.db.run("DROP SCHEMA IF EXISTS public CASCADE")
        self.db.run("CREATE SCHEMA public")

    def tearDown(self):
        self.db.run("DROP SCHEMA IF EXISTS public CASCADE")
        del self.db


class WithData(WithSchema):

    def setUp(self):
        WithSchema.setUp(self)
        self.db.run("CREATE TABLE foo (bar text)")
        self.db.run("INSERT INTO foo VALUES ('baz')")
        self.db.run("INSERT INTO foo VALUES ('buz')")


# db.run
# ======

class TestRun(WithSchema):

    def test_run_runs(self):
        self.db.run("CREATE TABLE foo (bar text)")
        actual = self.db.all("SELECT tablename FROM pg_tables "
                             "WHERE schemaname='public'")
        assert actual == ["foo"]

    def test_run_inserts(self):
        self.db.run("CREATE TABLE foo (bar text)")
        self.db.run("INSERT INTO foo VALUES ('baz')")
        actual = self.db.one("SELECT * FROM foo ORDER BY bar")
        assert actual == "baz"

    def test_run_accepts_bind_parameters_as_keyword_arguments(self):
        self.db.run("CREATE TABLE foo (bar text)")
        self.db.run("INSERT INTO foo VALUES (%(bar)s)", bar='baz')
        actual = self.db.one("SELECT * FROM foo ORDER BY bar")
        assert actual == "baz"


# db.all
# ======

class TestRows(WithData):

    def test_all_fetches_all_rows(self):
        actual = self.db.all("SELECT * FROM foo ORDER BY bar")
        assert actual == ["baz", "buz"]

    def test_all_fetches_one_row(self):
        actual = self.db.all("SELECT * FROM foo WHERE bar='baz'")
        assert actual == ["baz"]

    def test_all_fetches_no_rows(self):
        actual = self.db.all("SELECT * FROM foo WHERE bar='blam'")
        assert actual == []

    def test_all_doesnt_choke_on_values_column(self):
        actual = self.db.all("SELECT bar AS values FROM foo")
        assert actual == ["baz", "buz"]

    def test_bind_parameters_as_dict_work(self):
        params = {"bar": "baz"}
        actual = self.db.all("SELECT * FROM foo WHERE bar=%(bar)s", params)
        assert actual == ["baz"]

    def test_bind_parameters_as_tuple_work(self):
        actual = self.db.all("SELECT * FROM foo WHERE bar=%s", ("baz",))
        assert actual == ["baz"]

    def test_bind_parameters_as_kwargs_work(self):
        actual = self.db.all("SELECT * FROM foo WHERE bar=%(bar)s", bar='baz')
        assert actual == ["baz"]

    def test_all_raises_BadBackAs(self):
        with self.assertRaises(BadBackAs) as context:
            self.db.all("SELECT * FROM foo", back_as='foo')
        assert str(context.exception) == (
            "%r is not a valid value for the back_as argument.\n"
            "The available values are: Row, dict, namedtuple, tuple."
        ) % 'foo'


# db.one
# ======

class TestWrongNumberException(WithData):

    def test_TooFew_message_is_helpful(self):
        try:
            actual = self.db.one("CREATE TABLE foux (baar text)")
        except TooFew as exc:
            actual = str(exc)
        assert actual == "Got -1 rows; expecting 0 or 1."

    def test_TooMany_message_is_helpful_for_two_options(self):
        actual = str(TooMany(2, 1, 1))
        assert actual == "Got 2 rows; expecting exactly 1."

    def test_TooMany_message_is_helpful_for_a_range(self):
        actual = str(TooMany(4, 1, 3))
        assert actual == "Got 4 rows; expecting between 1 and 3 (inclusive)."


class TestOne(WithData):

    def test_one_raises_TooFew(self):
        with self.assertRaises(TooFew):
            self.db.one("CREATE TABLE foux (baar text)")

    def test_one_rollsback_on_error(self):
        try:
            self.db.one("CREATE TABLE foux (baar text)")
        except TooFew:
            pass
        with self.assertRaises(ProgrammingError):
            self.db.all("SELECT * FROM foux")

    def test_one_returns_None(self):
        actual = self.db.one("SELECT * FROM foo WHERE bar='blam'")
        assert actual is None

    def test_one_returns_default(self):
        class WHEEEE: pass  # noqa: E701
        actual = self.db.one("SELECT * FROM foo WHERE bar='blam'", default=WHEEEE)
        assert actual is WHEEEE

    def test_one_raises_default(self):
        exception = RuntimeError('oops')
        try:
            self.db.one("SELECT * FROM foo WHERE bar='blam'", default=exception)
        except Exception as e:
            if e is not exception:
                raise
        else:
            raise AssertionError('exception not raised')

    def test_one_returns_default_after_derefencing(self):
        default = 0
        actual = self.db.one("SELECT NULL AS foo", default=default)
        assert actual is default

    def test_one_raises_default_after_derefencing(self):
        exception = RuntimeError('oops')
        try:
            self.db.one("SELECT NULL AS foo", default=exception)
        except Exception as e:
            if e is not exception:
                raise
        else:
            raise AssertionError('exception not raised')

    def test_one_returns_one(self):
        actual = self.db.one("SELECT * FROM foo WHERE bar='baz'")
        assert actual == "baz"

    def test_one_accepts_a_dict_for_bind_parameters(self):
        actual = self.db.one("SELECT %(bar)s as bar", {"bar": "baz"})
        assert actual == "baz"

    def test_one_accepts_a_tuple_for_bind_parameters(self):
        actual = self.db.one("SELECT %s as bar", ("baz",))
        assert actual == "baz"

    def test_one_accepts_bind_parameters_as_keyword_arguments(self):
        actual = self.db.one("SELECT %(bar)s as bar", bar='baz')
        assert actual == "baz"

    def test_one_doesnt_choke_on_values_column(self):
        actual = self.db.one("SELECT 1 AS values")
        assert actual == 1

    def test_one_raises_TooMany(self):
        self.assertRaises(TooMany, self.db.one, "SELECT * FROM foo")

    def test_one_raises_BadBackAs(self):
        with self.assertRaises(BadBackAs) as context:
            self.db.one("SELECT * FROM foo LIMIT 1", back_as='foo')
        assert str(context.exception) == (
            "%r is not a valid value for the back_as argument.\n"
            "The available values are: Row, dict, namedtuple, tuple."
        ) % 'foo'


# db.get_cursor
# =============

class TestCursor(WithData):

    def test_get_cursor_gets_a_cursor(self):
        with self.db.get_cursor(cursor_factory=SimpleDictCursor) as cursor:
            cursor.execute("INSERT INTO foo VALUES ('blam')")
            cursor.execute("SELECT * FROM foo ORDER BY bar")
            actual = cursor.fetchall()
        assert actual == [{"bar": "baz"}, {"bar": "blam"}, {"bar": "buz"}]

    def test_transaction_is_isolated(self):
        with self.db.get_cursor() as cursor:
            cursor.execute("INSERT INTO foo VALUES ('blam')")
            cursor.execute("SELECT * FROM foo ORDER BY bar")
            actual = self.db.all("SELECT * FROM foo ORDER BY bar")
        assert actual == ["baz", "buz"]

    def test_transaction_commits_on_success(self):
        with self.db.get_cursor() as cursor:
            cursor.execute("INSERT INTO foo VALUES ('blam')")
            cursor.execute("SELECT * FROM foo ORDER BY bar")
        actual = self.db.all("SELECT * FROM foo ORDER BY bar")
        assert actual == ["baz", "blam", "buz"]

    def test_transaction_rolls_back_on_failure(self):
        try:
            with self.db.get_cursor() as cursor:
                cursor.execute("INSERT INTO foo VALUES ('blam')")
                cursor.execute("SELECT * FROM foo ORDER BY bar")
                raise Heck
        except Heck:
            pass
        actual = self.db.all("SELECT * FROM foo ORDER BY bar")
        assert actual == ["baz", "buz"]

    def test_cursor_rollback_exception_is_ignored(self):
        try:
            with self.db.get_cursor() as cursor:
                cursor.connection.close()
                raise Heck
        except Heck:
            pass

    def test_we_close_the_cursor(self):
        with self.db.get_cursor() as cursor:
            cursor.execute("SELECT * FROM foo ORDER BY bar")
        with self.assertRaises(InterfaceError):
            cursor.fetchall()

    def test_monkey_patch_execute(self):
        expected = "SELECT 1"
        def execute(this, sql, params=[]):
            return sql
        from postgres.cursors import SimpleCursorBase
        SimpleCursorBase.execute = execute
        with self.db.get_cursor() as cursor:
            actual = cursor.execute(expected)
        del SimpleCursorBase.execute
        assert actual == expected

    def test_autocommit_cursor(self):
        try:
            with self.db.get_cursor(autocommit=True) as cursor:
                try:
                    cursor.execute("INVALID QUERY")
                except ProgrammingError:
                    pass
                cursor.execute("INSERT INTO foo VALUES ('blam')")
                with self.db.get_cursor() as cursor:
                    n = cursor.one("SELECT count(*) FROM foo")
                    assert n == 3
                raise KeyboardInterrupt()
        except KeyboardInterrupt:
            pass
        with self.db.get_cursor() as cursor:
            n = cursor.one("SELECT count(*) FROM foo")
            assert n == 3

    def test_readonly_cursor(self):
        try:
            with self.db.get_cursor(readonly=True) as cursor:
                cursor.execute("INSERT INTO foo VALUES ('blam')")
        except ReadOnlySqlTransaction:
            pass

    def test_get_cursor_supports_subtransactions(self):
        before_count = self.db.one("SELECT count(*) FROM foo")
        with self.db.get_cursor(back_as='dict') as outer_cursor:
            outer_cursor.execute("INSERT INTO foo VALUES ('lorem')")
            with self.db.get_cursor(cursor=outer_cursor) as inner_cursor:
                assert inner_cursor is outer_cursor
                assert inner_cursor.back_as == 'dict'
                inner_cursor.execute("INSERT INTO foo VALUES ('ipsum')")
        after_count = self.db.one("SELECT count(*) FROM foo")
        assert after_count == (before_count + 2)

    def test_subtransactions_do_not_swallow_exceptions(self):
        before_count = self.db.one("SELECT count(*) FROM foo")
        try:
            with self.db.get_cursor() as cursor:
                cursor.execute("INSERT INTO foo VALUES ('lorem')")
                with self.db.get_cursor(cursor=cursor) as c:
                    c.execute("INSERT INTO foo VALUES ('ipsum')")
                    raise Heck
        except Heck:
            pass
        after_count = self.db.one("SELECT count(*) FROM foo")
        assert after_count == before_count


# db.get_connection
# =================

class TestConnection(WithData):

    def test_get_connection_gets_a_connection(self):
        with self.db.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=SimpleDictCursor)
            cursor.execute("SELECT * FROM foo ORDER BY bar")
            actual = cursor.fetchall()
        assert actual == [{"bar": "baz"}, {"bar": "buz"}]

    def test_connection_rollback_exception_is_ignored(self):
        try:
            with self.db.get_connection() as conn:
                conn.close()
                raise Heck
        except Heck:
            pass

    def test_connection_has_get_cursor_method(self):
        with self.db.get_connection() as conn:
            with conn.get_cursor() as cursor:
                cursor.execute("DELETE FROM foo WHERE bar = 'baz'")
        with self.db.get_cursor(cursor_factory=SimpleDictCursor) as cursor:
            cursor.execute("SELECT * FROM foo ORDER BY bar")
            actual = cursor.fetchall()
        assert actual == [{"bar": "buz"}]

    def test_get_cursor_method_checks_cursor_argument(self):
        with self.db.get_connection() as conn, self.db.get_cursor() as cursor:
            with self.assertRaises(ValueError):
                conn.get_cursor(cursor=cursor)


# orm
# ===

class TestORM(WithData):

    class MyModel(Model):

        __slots__ = ('bar', '__dict__')

        typname = "foo"

        def __init__(self, values):
            Model.__init__(self, values)
            self.bar_from_init = self.bar

        def update_bar(self, bar):
            self.db.run("UPDATE foo SET bar=%s WHERE bar=%s", (bar, self.bar))
            self.set_attributes(bar=bar)

    def setUp(self):
        WithData.setUp(self)
        self.db.register_model(self.MyModel)

    def tearDown(self):
        self.db.model_registry = {}

    def installFlah(self):
        self.db.run("CREATE TABLE flah (bar text)")
        self.db.register_model(self.MyModel, 'flah')

    def test_register_model_handles_schema(self):
        self.db.run("DROP SCHEMA IF EXISTS foo CASCADE")
        self.db.run("CREATE SCHEMA foo")
        self.db.run("CREATE TABLE foo.flah (bar text)")
        self.db.register_model(self.MyModel, 'foo.flah')

    def test_register_model_raises_AlreadyRegistered(self):
        with self.assertRaises(AlreadyRegistered) as context:
            self.db.register_model(self.MyModel)
        assert context.exception.args == (self.MyModel, self.MyModel.typname)
        assert str(context.exception) == (
            "The model MyModel is already registered for the typname foo."
        )

    def test_register_model_raises_NoSuchType(self):
        with self.assertRaises(NoSuchType):
            self.db.register_model(self.MyModel, 'nonexistent')

    def test_register_model_raises_NoTypeSpecified(self):
        with self.assertRaises(NoTypeSpecified):
            self.db.register_model(Model)

    def test_orm_basically_works(self):
        one = self.db.one("SELECT foo FROM foo WHERE bar='baz'")
        assert one.__class__ == self.MyModel

    def test_orm_models_get_kwargs_to_init(self):
        one = self.db.one("SELECT foo FROM foo WHERE bar='baz'")
        assert one.bar_from_init == 'baz'

    def test_updating_attributes_works(self):
        one = self.db.one("SELECT foo FROM foo WHERE bar='baz'")
        one.update_bar("blah")
        bar = self.db.one("SELECT bar FROM foo WHERE bar='blah'")
        assert bar == one.bar

    def test_setting_unknown_attributes(self):
        one = self.db.one("SELECT foo FROM foo WHERE bar='baz'")
        with self.assertRaises(UnknownAttributes) as context:
            one.set_attributes(bar='blah', x=0, y=1)
        assert sorted(context.exception.args[0]) == ['x', 'y']
        assert str(context.exception) == (
            "The following attribute(s) are unknown to us: %s."
        ) % ', '.join(context.exception.args[0])

    def test_attributes_are_read_only(self):
        one = self.db.one("SELECT foo FROM foo WHERE bar='baz'")
        with self.assertRaises(ReadOnlyAttribute) as context:
            one.bar = "blah"
        assert context.exception.args == ("bar",)
        assert str(context.exception).startswith("bar is a read-only attribute.")

    def test_check_register_raises_if_passed_a_model_instance(self):
        obj = self.MyModel(['baz'])
        raises(NotAModel, self.db.check_registration, obj)

    def test_check_register_doesnt_include_subsubclasses(self):
        class Other(self.MyModel): pass  # noqa: E701
        raises(NotRegistered, self.db.check_registration, Other)

    def test_dot_dot_dot_unless_you_ask_it_to(self):
        class Other(self.MyModel): pass  # noqa: E701
        assert self.db.check_registration(Other, True) == ['foo']

    def test_check_register_handles_complex_cases(self):
        self.installFlah()

        class Second(Model): pass  # noqa: E701
        self.db.run("CREATE TABLE blum (bar text)")
        self.db.register_model(Second, 'blum')
        assert self.db.check_registration(Second) == ['blum']

        class Third(self.MyModel, Second): pass  # noqa: E701
        actual = list(sorted(self.db.check_registration(Third, True)))
        assert actual == ['blum', 'flah', 'foo']

    def test_a_model_can_be_used_for_a_second_type(self):
        self.installFlah()
        self.db.run("INSERT INTO flah VALUES ('double')")
        self.db.run("INSERT INTO flah VALUES ('trouble')")
        flah = self.db.one("SELECT flah FROM flah WHERE bar='double'")
        assert flah.bar == "double"

    def test_check_register_returns_string_for_single(self):
        assert self.db.check_registration(self.MyModel) == ['foo']

    def test_check_register_returns_list_for_multiple(self):
        self.installFlah()
        actual = list(sorted(self.db.check_registration(self.MyModel)))
        assert actual == ['flah', 'foo']

    def test_unregister_unregisters_one(self):
        self.db.unregister_model(self.MyModel)
        assert self.db.model_registry == {}

    def test_unregister_leaves_other(self):
        self.db.run("CREATE TABLE flum (bar text)")
        class OtherModel(Model): pass  # noqa: E701
        self.db.register_model(OtherModel, 'flum')
        self.db.unregister_model(self.MyModel)
        assert self.db.model_registry == {'flum': OtherModel}

    def test_unregister_unregisters_multiple(self):
        self.installFlah()
        self.db.unregister_model(self.MyModel)
        assert self.db.model_registry == {}

    def test_add_column_doesnt_break_anything(self):
        self.db.run("ALTER TABLE foo ADD COLUMN boo text")
        one = self.db.one("SELECT foo FROM foo WHERE bar='baz'")
        assert one.boo is None

    def test_replace_column_different_type(self):
        self.db.run("CREATE TABLE grok (bar int)")
        self.db.run("INSERT INTO grok VALUES (0)")
        class EmptyModel(Model): pass  # noqa: E701
        self.db.register_model(EmptyModel, 'grok')
        # Add a new column then drop the original one
        self.db.run("ALTER TABLE grok ADD COLUMN biz text NOT NULL DEFAULT 'x'")
        self.db.run("ALTER TABLE grok DROP COLUMN bar")
        # The number of columns hasn't changed but the names and types have
        one = self.db.one("SELECT grok FROM grok LIMIT 1")
        assert one.biz == 'x'
        assert not hasattr(one, 'bar')

    @mark.xfail(raises=AttributeError)
    def test_replace_column_same_type_different_name(self):
        self.db.run("ALTER TABLE foo ADD COLUMN biz text NOT NULL DEFAULT 0")
        self.db.run("ALTER TABLE foo DROP COLUMN bar")
        one = self.db.one("SELECT foo FROM foo LIMIT 1")
        assert one.biz == 0
        assert not hasattr(one, 'bar')


# SimpleCursorBase
# ================

class TestSimpleCursorBase(WithData):

    def test_fetchone(self):
        with self.db.get_cursor(cursor_factory=SimpleTupleCursor) as cursor:
            cursor.execute("SELECT 1 as foo")
            r = cursor.fetchone()
            assert r == (1,)

    def test_fetchone_supports_back_as(self):
        with self.db.get_cursor() as cursor:
            cursor.execute("SELECT 1 as foo")
            r = cursor.fetchone(back_as=dict)
            assert r == {'foo': 1}
            cursor.execute("SELECT 2 as foo")
            r = cursor.fetchone(back_as=tuple)
            assert r == (2,)

    def test_fetchone_raises_BadBackAs(self):
        with self.db.get_cursor() as cursor:
            cursor.execute("SELECT 1 as foo")
            with self.assertRaises(BadBackAs) as context:
                cursor.fetchone(back_as='bar')
            assert str(context.exception) == (
                "%r is not a valid value for the back_as argument.\n"
                "The available values are: Row, dict, namedtuple, tuple."
            ) % 'bar'

    def test_fetchmany(self):
        with self.db.get_cursor(cursor_factory=SimpleTupleCursor) as cursor:
            cursor.execute("SELECT 1 as foo")
            r = cursor.fetchmany()
            assert r == [(1,)]

    def test_fetchmany_supports_back_as(self):
        with self.db.get_cursor() as cursor:
            cursor.execute("SELECT 1 as foo")
            r = cursor.fetchmany(back_as=dict)
            assert r == [{'foo': 1}]
            cursor.execute("SELECT 2 as foo")
            r = cursor.fetchmany(back_as=tuple)
            assert r == [(2,)]

    def test_fetchmany_raises_BadBackAs(self):
        with self.db.get_cursor() as cursor:
            cursor.execute("SELECT 1 as foo")
            with self.assertRaises(BadBackAs) as context:
                cursor.fetchmany(back_as='bar')
            assert str(context.exception) == (
                "%r is not a valid value for the back_as argument.\n"
                "The available values are: Row, dict, namedtuple, tuple."
            ) % 'bar'

    def test_fetchall(self):
        with self.db.get_cursor(cursor_factory=SimpleTupleCursor) as cursor:
            cursor.execute("SELECT 1 as foo")
            r = cursor.fetchall()
            assert r == [(1,)]

    def test_fetchall_supports_back_as(self):
        with self.db.get_cursor() as cursor:
            cursor.execute("SELECT 1 as foo")
            r = cursor.fetchall(back_as=dict)
            assert r == [{'foo': 1}]
            cursor.execute("SELECT 2 as foo")
            r = cursor.fetchall(back_as=tuple)
            assert r == [(2,)]

    def test_fetchall_raises_BadBackAs(self):
        with self.db.get_cursor() as cursor:
            cursor.execute("SELECT 1 as foo")
            with self.assertRaises(BadBackAs) as context:
                cursor.fetchall(back_as='bar')
            assert str(context.exception) == (
                "%r is not a valid value for the back_as argument.\n"
                "The available values are: Row, dict, namedtuple, tuple."
            ) % 'bar'


# cursor_factory
# ==============

class WithCursorFactory(WithSchema):

    def setUp(self):                    # override
        self.db = Postgres(cursor_factory=self.cursor_factory)
        self.db.run("DROP SCHEMA IF EXISTS public CASCADE")
        self.db.run("CREATE SCHEMA public")
        self.db.run("CREATE TABLE foo (key text, value int)")
        self.db.run("INSERT INTO foo VALUES ('buz', 42)")
        self.db.run("INSERT INTO foo VALUES ('biz', 43)")


class TestNamedTupleCursorFactory(WithCursorFactory):

    cursor_factory = SimpleNamedTupleCursor

    def test_NamedDictCursor_results_in_namedtuples(self):
        Record = namedtuple("Record", ["key", "value"])
        expected = [Record(key="biz", value=43), Record(key="buz", value=42)]
        actual = self.db.all("SELECT * FROM foo ORDER BY key")
        assert actual == expected
        assert actual[0].__class__.__name__ == 'Record'

    def test_namedtuples_can_be_unrolled(self):
        actual = self.db.all("SELECT value FROM foo ORDER BY key")
        assert actual == [43, 42]


class TestRowCursorFactory(WithCursorFactory):

    cursor_factory = SimpleRowCursor

    def test_RowCursor_returns_Row_objects(self):
        row = self.db.one("SELECT * FROM foo ORDER BY key LIMIT 1")
        assert isinstance(row, Row)
        rows = self.db.all("SELECT * FROM foo ORDER BY key")
        assert all(isinstance(r, Row) for r in rows)

    def test_Row_objects_can_be_unrolled(self):
        actual = self.db.all("SELECT value FROM foo ORDER BY key")
        assert actual == [43, 42]

    def test_one(self):
        r = self.db.one("SELECT * FROM foo ORDER BY key LIMIT 1")
        assert isinstance(r, Row)
        assert r[0] == 'biz'
        assert r.key == 'biz'
        assert r['key'] == 'biz'
        assert r[1] == 43
        assert r.value == 43
        assert r['value'] == 43
        if sys.version_info >= (3, 0):
            assert repr(r) == "Row(key='biz', value=43)"

    def test_all(self):
        rows = self.db.all("SELECT * FROM foo ORDER BY key")
        assert isinstance(rows[0], Row)
        assert rows[0].key == 'biz'
        assert rows[0].value == 43
        assert rows[1].key == 'buz'
        assert rows[1].value == 42

    def test_iter(self):
        with self.db.get_cursor() as cursor:
            cursor.execute("SELECT * FROM foo ORDER BY key")
            i = iter(cursor)
            assert cursor.rownumber == 0

            t = next(i)
            assert isinstance(t, Row)
            assert t.key == 'biz'
            assert t.value == 43
            assert cursor.rownumber == 1
            assert cursor.rowcount == 2

            t = next(i)
            assert isinstance(t, Row)
            assert t.key == 'buz'
            assert t.value == 42
            assert cursor.rownumber == 2
            assert cursor.rowcount == 2

            with self.assertRaises(StopIteration):
                next(i)
            assert cursor.rownumber == 2
            assert cursor.rowcount == 2

    def test_row_unpack(self):
        foo, bar = self.db.one("SELECT 1 as foo, 2 as bar")
        assert foo == 1
        assert bar == 2

    def test_row_comparison(self):
        r = self.db.one("SELECT 1 as foo, 2 as bar")
        assert r == r
        assert r == (1, 2)
        assert r == {'foo': 1, 'bar': 2}
        assert r != None  # noqa: E711

    def test_special_col_names(self):
        r = self.db.one('SELECT 1 as "foo.bar_baz", 2 as "?column?", 3 as "3"')
        assert r['foo.bar_baz'] == 1
        assert r['?column?'] == 2
        assert r['3'] == 3

    @mark.xfail(sys.version_info < (3, 0),
                reason="Unicode attribute names require Python >= 3.0")
    def test_nonascii_names(self):
        r = self.db.one('SELECT 1 as \xe5h\xe9, 2 as \u2323')
        assert getattr(r, '\xe5h\xe9') == 1
        assert getattr(r, '\u2323') == 2
