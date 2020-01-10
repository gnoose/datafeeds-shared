# Database Setup

This document explains how to set up databases under docker and load them with fixtures.

In production, datafeeds receives jobs from `webapps`, which is responsible for managing scheduling
and scraper configuration. Most developers utilize the `docker-compose` file in `webapps` to start/manage
their local Postgres instance, and then allow their datafeeds instance to connect to those containers. See
the webapps documentation for more details. In particular, we assume that you will have the following
standard aliases in your `/etc/hosts` file:

```
127.0.0.1       pg
127.0.0.1       urjanet
```

One exception to this rule is the Urjanet MySQL database, which webapps developers typically do not need.
The docker compose file in this repo will allow you to launch this database container, the username/password
for this DB is `gridium`/`gridium`.
