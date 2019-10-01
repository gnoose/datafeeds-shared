# Database Setup

This document explains how to set up databases in a test environment and load them with fixtures.

# Steps

1. Run `docker-compose up -d`.
2. Using `docker-compose logs -f pg`, confirm that Postgres has fully loaded 
   (it takes the longest, due to GIS integrations).
3. With your development virtual environment enabled, run `python setup_databases.py`. You should see:
    ```
    Creating PG database...
    Loading PG fixtures...
    ```
4. Finally, use `testdata` to load an account into your local `pg` and `mongo` databases.

# Logins

- Postgres: Username `gridium`, no password.
- Mongo: Username `gridium`, password `mongo_pwd`.
- MySql: Username `gridium`, password `gridium`.
