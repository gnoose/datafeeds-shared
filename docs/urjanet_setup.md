# Setting up for Urjanet Scraper Development

This document explains how to obtain a local urjanet MySQL instance for development.

## Steps

0. Update your `/etc/hosts` to include the line `127.0.0.1 urjanet`.

1. [Optional] To create a snapshot of the productin urjanet database. Log in to ops and run:
    ```
    mysqldump -h urjanet-datastore-v2.cu2ndqyknjku.us-west-1.rds.amazonaws.com \
       -u gridium -p --databases urjanet \
       --compress --quick --set-gtid-purged=OFF > /builds/urjanet_dumps/urjanet.sql
    ```
2. Or download a copy (warning: 1.1GB unzipped): `scp ops:/builds/urjanet_dumps/urjanet.sql.gz .`
3. Start your local mysql instance via `docker-compose up -d`.
4. Now load the snapshot into your local database via:
    ```
    gzcat urjanet.sql.gz | mysql -h urjanet -u gridium -p
    ```
    The username/password for the local DB instance are `gridium/gridium`.

## Other Notes
- You may not have the mysql client installed on your development machine. On Mac, you can get this tool with homebrew
 via `brew install mysql-client`. (I had to follow the post install instructions that `brew` prints in order to add
  `mysql` to my `PATH`.)
- Sometimes you need to get back to a clean state. Running the `./teardown_databases.sh` 
deletes all of the local files associated with postgres/mysql for the project.
- [SequelPro](https://www.sequelpro.com/) can be helpful for browsing your local database, if you prefer a GUI. 
This can be especially helpful if you're not familiar MySQL-specific management commands (connect to database, list
 tables, etc.). and just want to write SQL queries.

## restore on dev

```
zcat /tmp/urjanet.sql.gz | mysql -h dev-urjanet-serverless.cluster-cz1oabary77u.us-east-1.rds.amazonaws.com -u gridium -p urjanet
```

The -f will ignore the `Access denied` errors from running as a non-root user.
