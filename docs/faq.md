# Datafeeds FAQ

## Troubleshooting mypy errors

If a change adds a third-party library, it may cause a mypy error like this if the library does not include type annotations:

    datafeeds/db.py:5: error: No library stub file for module 'sqlalchemy'

The fix is to add a section to mypy.ini to ignore that library for type checking:

    [mypy-sqlalchemy.*]
    ignore_missing_imports = True

If mypy produces an error like this for a Gridium module:

    datafeeds/urjanet/tests/test_urjanet_pymysql_adapter.py:5:
        error: Cannot find implementation or library stub for module named 'datafeeds.urjanet.datasource.pymysql_adapter'

The module (`datafeeds/urjanet/datasource`) needs an (empty) `__init__.py` file:

    touch datafeeds/urjanet/datasource/__init__.py
