#!/bin/sh
#DATABASE_URL=postgres://jrandom@localhost/test py.test tests.py -v
echo "Starting doctests."
python postgres/__init__.py
python postgres/cursors.py
python postgres/orm.py
echo ""
python --version 2>&1 | grep 'Python 3' > /dev/null && \
    echo "\x1b[31mYou may see errors\x1b[0m due to dict ordering not being stable."
python --version 2>&1 | grep 'Python 2' > /dev/null && \
    echo "\x1b[31mYou're using Python 2.\x1b[0m Expect errors comparing '' and u''." && \
    echo "You may also see errors due to dict ordering not being stable."
echo "We don't fix these because that would make docs uglier."
echo ""
echo "Done with doctests."
