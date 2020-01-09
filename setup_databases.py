#
# Intialize mongo and postgres for development.
#
# This only needs to be run once, when setting up the
# environment.
#
# Username and password settings used here are specified in the
# dev docker-compose file.
#

from pymongo import MongoClient
import os

from sqlalchemy_utils.functions import database_exists, create_database

from datafeeds import db
from datafeeds.config import DATAFEEDS_ROOT as ROOT


CONNSTR = "postgresql+psycopg2://postgres@pg/gridium"


def init_mongo():
    client = MongoClient(host="localhost", port=27017)
    # db = client['admin']
    db = client["webapps"]
    db.command("createUser", "gridium", pwd="mongo_pwd", roles=["readWrite"])


def init_test_db():
    exists = database_exists(CONNSTR)
    if not exists:
        print("Creating PG database...")
        create_database(CONNSTR)

    db.init(CONNSTR, statement_timeout=10000)

    def source_sql_file(pathname):
        with open(os.path.join(ROOT, pathname)) as f:
            sql = f.read()
        with db.engine.begin() as connection:
            connection.execute(sql)

    if not exists:
        print("Loading PG fixtures...")
        # turn off echo for now
        old_echo_value = db.engine.echo
        db.engine.echo = False
        source_sql_file("fixtures/init.sql")
        source_sql_file("fixtures/schema.sql")
        db.engine.echo = old_echo_value


if __name__ == "__main__":
    init_test_db()
    init_mongo()
