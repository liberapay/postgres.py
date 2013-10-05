#!/bin/sh
DATABASE_URL=postgres://jrandom@localhost/test py.test tests.py -v
python postgres/__init__.py -v
python postgres/cursors.py -v
python postgres/orm.py -v
