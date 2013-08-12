from __future__ import unicode_literals

import os
from collections import namedtuple
from unittest import TestCase

from postgres import Postgres, TooFew, TooMany
from psycopg2.extras import NamedTupleCursor
from psycopg2 import InterfaceError, ProgrammingError


DATABASE_URL = os.environ['DATABASE_URL']


# harnesses
# =========

class WithSchema(TestCase):

    def setUp(self):
        self.db = Postgres(DATABASE_URL)
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
        assert actual == [{"tablename": "foo"}]

    def test_run_inserts(self):
        self.db.run("CREATE TABLE foo (bar text)")
        self.db.run("INSERT INTO foo VALUES ('baz')")
        actual = len(self.db.one_or_zero("SELECT * FROM foo ORDER BY bar"))
        assert actual == 1


# db.all
# ======

class TestRows(WithData):

    def test_rows_fetches_all_rows(self):
        actual = self.db.all("SELECT * FROM foo ORDER BY bar")
        assert actual == [{"bar": "baz"}, {"bar": "buz"}]

    def test_rows_fetches_one_row(self):
        actual = self.db.all("SELECT * FROM foo WHERE bar='baz'")
        assert actual == [{"bar": "baz"}]

    def test_rows_fetches_no_rows(self):
        actual = self.db.all("SELECT * FROM foo WHERE bar='blam'")
        assert actual == []

    def test_bind_parameters_as_dict_work(self):
        params = {"bar": "baz"}
        actual = self.db.all("SELECT * FROM foo WHERE bar=%(bar)s", params)
        assert actual == [{"bar": "baz"}]

    def test_bind_parameters_as_tuple_work(self):
        actual = self.db.all("SELECT * FROM foo WHERE bar=%s", ("baz",))
        assert actual == [{"bar": "baz"}]


# db.one_or_zero
# ==============

class TestWrongNumberException(WithData):

    def test_TooFew_message_is_helpful(self):
        try:
            exc = self.db.one_or_zero("CREATE TABLE foux (baar text)")
        except TooFew as exc:
            pass
        actual = str(exc)
        assert actual == "Got -1 rows; expecting 0 or 1."

    def test_TooMany_message_is_helpful_for_two_options(self):
        try:
            exc = self.db._some("SELECT * FROM foo", lo=1, hi=1)
        except TooMany as exc:
            pass
        actual = str(exc)
        assert actual == "Got 2 rows; expecting exactly 1."

    def test_TooMany_message_is_helpful_for_a_range(self):
        self.db.run("INSERT INTO foo VALUES ('blam')")
        self.db.run("INSERT INTO foo VALUES ('blim')")
        try:
            exc = self.db._some("SELECT * FROM foo", lo=1, hi=3)
        except TooMany as exc:
            pass
        actual = str(exc)
        assert actual == "Got 4 rows; expecting between 1 and 3 (inclusive)."


class TestOneOrZero(WithData):

    def test_one_or_zero_raises_TooFew(self):
        self.assertRaises( TooFew
                         , self.db.one_or_zero
                         , "CREATE TABLE foux (baar text)"
                          )

    def test_one_or_zero_rollsback_on_error(self):
        try:
            self.db.one_or_zero("CREATE TABLE foux (baar text)")
        except TooFew:
            pass
        self.assertRaises( ProgrammingError
                         , self.db.all
                         , "SELECT * FROM foux"
                          )

    def test_one_or_zero_returns_None(self):
        actual = self.db.one_or_zero("SELECT * FROM foo WHERE bar='blam'")
        assert actual is None

    def test_one_or_zero_returns_whatever(self):
        class WHEEEE: pass
        actual = self.db.one_or_zero( "SELECT * FROM foo WHERE bar='blam'"
                                    , zero=WHEEEE
                                     )
        assert actual is WHEEEE

    def test_one_or_zero_returns_one(self):
        actual = self.db.one_or_zero("SELECT * FROM foo WHERE bar='baz'")
        assert actual == {"bar": "baz"}

    def test_with_strict_True_one_raises_TooMany(self):
        self.assertRaises(TooMany, self.db.one_or_zero, "SELECT * FROM foo")


# db.get_transaction
# ==================

class TestTransaction(WithData):

    def test_get_transaction_gets_a_transaction(self):
        with self.db.get_transaction() as txn:
            txn.execute("INSERT INTO foo VALUES ('blam')")
            txn.execute("SELECT * FROM foo ORDER BY bar")
            actual = txn.fetchall()
        assert actual == [{"bar": "baz"}, {"bar": "blam"}, {"bar": "buz"}]

    def test_transaction_is_isolated(self):
        with self.db.get_transaction() as txn:
            txn.execute("INSERT INTO foo VALUES ('blam')")
            txn.execute("SELECT * FROM foo ORDER BY bar")
            actual = self.db.all("SELECT * FROM foo ORDER BY bar")
        assert actual == [{"bar": "baz"}, {"bar": "buz"}]

    def test_transaction_commits_on_success(self):
        with self.db.get_transaction() as txn:
            txn.execute("INSERT INTO foo VALUES ('blam')")
            txn.execute("SELECT * FROM foo ORDER BY bar")
        actual = self.db.all("SELECT * FROM foo ORDER BY bar")
        assert actual == [{"bar": "baz"}, {"bar": "blam"}, {"bar": "buz"}]

    def test_transaction_rolls_back_on_failure(self):
        class Heck(Exception): pass
        try:
            with self.db.get_transaction() as txn:
                txn.execute("INSERT INTO foo VALUES ('blam')")
                txn.execute("SELECT * FROM foo ORDER BY bar")
                raise Heck
        except Heck:
            pass
        actual = self.db.all("SELECT * FROM foo ORDER BY bar")
        assert actual == [{"bar": "baz"}, {"bar": "buz"}]

    def test_we_close_the_cursor(self):
        with self.db.get_transaction() as txn:
            txn.execute("SELECT * FROM foo ORDER BY bar")
        self.assertRaises( InterfaceError
                         , txn.fetchall
                          )


# db.get_connection
# =================

class TestConnection(WithData):

    def test_get_connection_gets_a_connection(self):
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM foo ORDER BY bar")
            actual = cursor.fetchall()
        assert actual == [{"bar": "baz"}, {"bar": "buz"}]


# cursor_factory
# ==============

class TestCursorFactory(WithData):

    def setUp(self):                    # override
        self.db = Postgres(DATABASE_URL, cursor_factory=NamedTupleCursor)
        self.db.run("DROP SCHEMA IF EXISTS public CASCADE")
        self.db.run("CREATE SCHEMA public")
        self.db.run("CREATE TABLE foo (bar text)")
        self.db.run("INSERT INTO foo VALUES ('baz')")
        self.db.run("INSERT INTO foo VALUES ('buz')")

    def test_NamedDictCursor_results_in_namedtuples(self):
        Record = namedtuple("Record", ["bar"])
        expected = [Record(bar="baz"), Record(bar="buz")]
        actual = self.db.all("SELECT * FROM foo ORDER BY bar")
        assert actual == expected
