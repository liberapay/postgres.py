from __future__ import unicode_literals

import os
from unittest import TestCase

from postgres import Postgres


DATABASE_URL = os.environ['DATABASE_URL']


class WithSchema(TestCase):

    def setUp(self):
        self.db = Postgres(DATABASE_URL)
        self.db.execute("DROP SCHEMA IF EXISTS public CASCADE")
        self.db.execute("CREATE SCHEMA public")

    def tearDown(self):
        self.db.execute("DROP SCHEMA IF EXISTS public CASCADE")


class WithData(WithSchema):

    def setUp(self):
        WithSchema.setUp(self)
        self.db.execute("CREATE TABLE foo (bar text)")
        self.db.execute("INSERT INTO foo VALUES ('baz')")
        self.db.execute("INSERT INTO foo VALUES ('buz')")


class TestExecute(WithSchema):

    def test_execute_executes(self):
        self.db.execute("CREATE TABLE foo (bar text)")
        actual = list(self.db.fetchall("SELECT tablename FROM pg_tables "
                                       "WHERE schemaname='public'"))
        assert actual == [{"tablename": "foo"}]

    def test_execute_inserts(self):
        self.db.execute("CREATE TABLE foo (bar text)")
        self.db.execute("INSERT INTO foo VALUES ('baz')")
        actual = len(list(self.db.fetchone("SELECT * FROM foo ORDER BY bar")))
        assert actual == 1


class TestFetch(WithData):

    def test_fetchone_fetches_the_first_one(self):
        actual = self.db.fetchone("SELECT * FROM foo ORDER BY bar")
        assert actual == {"bar": "baz"}

    def test_fetchone_returns_None_if_theres_none(self):
        actual = self.db.fetchone("SELECT * FROM foo WHERE bar='blam'")
        assert actual is None

    def test_fetchall_fetches_all(self):
        actual = list(self.db.fetchall("SELECT * FROM foo ORDER BY bar"))
        assert actual == [{"bar": "baz"}, {"bar": "buz"}]


class TestCursor(WithData):

    def test_get_cursor_gets_a_cursor(self):
        with self.db.get_cursor("SELECT * FROM foo ORDER BY bar") as cursor:
            actual = cursor.fetchall()
        assert actual == [{"bar": "baz"}, {"bar": "buz"}]
