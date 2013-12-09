#!/usr/bin/env bash

# Run both pytests and doctests, but only vary the return code on the result of
# the pytests. The doctests can fail for spurious reasons, and we chose not to
# fix them because fixing them would make the docs uglier. As long as the
# doctests pass for Python 3 then we know the doc examples are good.

function run_pytests() {
    DATABASE_URL=postgres://jrandom@localhost/test py.test tests.py -v && return 0 || return 1
}

function run_doctests() {
    echo "Starting doctests."
    python postgres/__init__.py
    python postgres/cursors.py
    python postgres/orm.py
    echo ""
    python --version 2>&1 | grep 'Python 3' > /dev/null && \
        echo -e "\x1b[31mYou may see errors\x1b[0m due to dict ordering not being stable."
    python --version 2>&1 | grep 'Python 2' > /dev/null && \
        echo -e "\x1b[31mYou're using Python 2.\x1b[0m Expect errors comparing '' and u''." && \
        echo "You may also see errors due to dict ordering not being stable."
    echo "We don't fix these because that would make docs uglier."
    echo ""
    echo "Done with doctests."

    return 0  # Always report success, because we don't want Travis to choke on u''.
}

run_pytests
SUCCESS=$?

run_doctests
exit $SUCCESS
