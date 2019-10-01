from functools import wraps
import logging

import pymysql
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.pool import NullPool

from datafeeds import config


log = logging.getLogger(__name__)

engine = session = session_factory = None


def init(connstr=None,
         application_name="datafeeds",
         statement_timeout=60000):
    """Initialize the ORM for this process.

    use_null_pool : If set to true, create and destroy database
    connections as they"re needed, rather than sharing them in a
    pool.
    """
    global engine, session, session_factory
    if not connstr:
        connstr = config.POSTGRES_URL

    kwargs = {
        "echo": config.POSTGRES_ECHO,
        "connect_args": {
            "options": "-c statement_timeout={}".format(statement_timeout),
            "application_name": application_name
        },
        "poolclass": NullPool
    }

    if engine is None or session is None or session_factory is None:
        engine = create_engine(connstr, **kwargs)
        session_factory = sessionmaker(bind=engine)
        session = scoped_session(session_factory)


def dbtask(fn=None, commit_on_fail=False, **conn_options):
    """Initialize and tear down a database transaction around a single function call.

    Example:
    @dbtask
    def fn1(*args, **kwargs):
        ...

    @dbtask(application_name="tasks.greenbutton")
    def fn2(*args, **kwargs):
        ...

    Optionally allows for committing transaction on exception, rather than
    rolling back, via `commit_on_fail=True`
    """
    # When decorator has keyword args, it does *not* receive the function
    # so need to return another decorator that *does* and closes over options
    # See: http://typeandflow.blogspot.com/2011/06/python-decorator-with-optional-keyword.html)
    if not fn:
        def _partial(_fn):
            return dbtask(_fn, commit_on_fail=commit_on_fail, **conn_options)

        return _partial

    # When decorator used without `(..)` it receives the function directly
    @wraps(fn)
    def _decorated(*args, **kwargs):
        opts = dict()
        opts.update(**conn_options)
        init(**opts)

        try:
            rval = fn(*args, **kwargs)

            if not config.UNDER_TEST:
                log.debug("Committing transaction.")
                session.commit()

            return rval

        except Exception:
            if commit_on_fail:
                if not config.UNDER_TEST:
                    log.debug("Committing transaction with an exception.")
                    session.commit()
            else:
                log.debug("Aborting and rolling back DB transaction.")
                session.rollback()
            raise

        finally:
            if not config.UNDER_TEST:
                log.debug("Closing DB session.")
                session.close()

    return _decorated


def urjanet_connection():
    return pymysql.connect(
        host=config.URJANET_MYSQL_HOST,
        user=config.URJANET_MYSQL_USER,
        passwd=config.URJANET_MYSQL_PASSWORD,
        db=config.URJANET_MYSQL_DB)
