#!/bin/sh
DATABASE_URL=postgres://jrandom@localhost/test py.test tests.py -v
echo "Starting doctests."
python postgres/__init__.py
python postgres/cursors.py
python postgres/orm.py
echo "Done with doctests."
