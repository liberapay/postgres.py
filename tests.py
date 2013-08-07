from __future__ import unicode_literals

import os
from unittest import TestCase

from postgres import Postgres


DATABASE_URL = os.environ['DATABASE_URL']


class WithSchema(TestCase):

    def setUp(self):
        self.db = Postgres(DATABASE_URL)
        self.db.run("DROP SCHEMA IF EXISTS public CASCADE")
        self.db.run("CREATE SCHEMA public")

    def tearDown(self):
        self.db.run("DROP SCHEMA IF EXISTS public CASCADE")


class WithData(WithSchema):

    def setUp(self):
        WithSchema.setUp(self)
        self.db.run("CREATE TABLE foo (bar text)")
        self.db.run("INSERT INTO foo VALUES ('baz')")
        self.db.run("INSERT INTO foo VALUES ('buz')")


class TestRun(WithSchema):

    def test_run_runs(self):
        self.db.run("CREATE TABLE foo (bar text)")
        actual = self.db.rows("SELECT tablename FROM pg_tables "
                              "WHERE schemaname='public'")
        assert actual == [{"tablename": "foo"}]

    def test_run_inserts(self):
        self.db.run("CREATE TABLE foo (bar text)")
        self.db.run("INSERT INTO foo VALUES ('baz')")
        actual = len(self.db.one("SELECT * FROM foo ORDER BY bar"))
        assert actual == 1


class TestOneAndRows(WithData):

    def test_one_fetches_the_first_one(self):
        actual = self.db.one("SELECT * FROM foo ORDER BY bar")
        assert actual == {"bar": "baz"}

    def test_one_returns_None_if_theres_none(self):
        actual = self.db.one("SELECT * FROM foo WHERE bar='blam'")
        assert actual is None

    def test_rows_fetches_all_rows(self):
        actual = self.db.rows("SELECT * FROM foo ORDER BY bar")
        assert actual == [{"bar": "baz"}, {"bar": "buz"}]

    def test_bind_parameters_as_dict_work(self):
        params = {"bar": "baz"}
        actual = self.db.rows("SELECT * FROM foo WHERE bar=%(bar)s", params)
        assert actual == [{"bar": "baz"}]

    def test_bind_parameters_as_tuple_work(self):
        actual = self.db.rows("SELECT * FROM foo WHERE bar=%s", ("baz",))
        assert actual == [{"bar": "baz"}]


class TestCursor(WithData):

    def test_get_cursor_gets_a_cursor(self):
        with self.db.get_cursor() as cursor:
            cursor.execute("SELECT * FROM foo ORDER BY bar")
            actual = cursor.fetchall()
        assert actual == [{"bar": "baz"}, {"bar": "buz"}]

    def test_we_can_use_cursor_rowcount(self):
        with self.db.get_cursor() as cursor:
            cursor.execute("SELECT * FROM foo ORDER BY bar")
            actual = cursor.rowcount
        assert actual == 2

    def test_we_can_use_cursor_closed(self):
        with self.db.get_cursor() as cursor:
            cursor.execute("SELECT * FROM foo ORDER BY bar")
            actual = cursor.closed
        assert not actual


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
            actual = self.db.rows("SELECT * FROM foo ORDER BY bar")
        assert actual == [{"bar": "baz"}, {"bar": "buz"}]

    def test_transaction_commits_on_success(self):
        with self.db.get_transaction() as txn:
            txn.execute("INSERT INTO foo VALUES ('blam')")
            txn.execute("SELECT * FROM foo ORDER BY bar")
        actual = self.db.rows("SELECT * FROM foo ORDER BY bar")
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
        actual = self.db.rows("SELECT * FROM foo ORDER BY bar")
        assert actual == [{"bar": "baz"}, {"bar": "buz"}]


class TestConnection(WithData):

    def test_get_connection_gets_a_connection(self):
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM foo ORDER BY bar")
            actual = cursor.fetchall()
        assert actual == [{"bar": "baz"}, {"bar": "buz"}]
