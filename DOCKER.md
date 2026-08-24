# Running the suite in Docker

The repository ships a compose file that starts two PostgreSQL 17 servers and two PostgreSQL 18 servers
(one pair per tox environment, so mirrored and multi-node cases have a second node) plus a `test`
container that has Python 3.14 and tox and bind-mounts the project at `/app`.

Run everything the way CI would:

```bash
docker compose run --rm test tox
```

Run one environment, or hand extra arguments through tox to `manage.py test`:

```bash
docker compose run --rm test tox -e py314-dj60-pg17
docker compose run --rm test tox -e py314-dj60-pg17 -- djanquiltdb_tests.router
```

The `ruff`, `docs` and `coverage` environments need no database; add `--no-deps` to skip starting the
PostgreSQL containers for those:

```bash
docker compose run --no-deps --rm test tox -e docs
```

The Postgres containers keep their data in named volumes; `docker compose down -v` resets them. The tox
environments live in a container-internal volume (`/app/.tox`), so they never pollute the host checkout.
