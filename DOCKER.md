# Running the suites in Docker

The repository ships a compose file that starts two PostgreSQL 17 servers and two PostgreSQL 18 servers
(one pair per tox environment, so mirrored and multi-node cases have a second node) plus a `test`
container that has Python 3.14 and tox and bind-mounts the repository at `/app`.

Every Django minor the packages claim gets a tox environment of its own against each PostgreSQL, so
`tox` runs each suite once per combination rather than resolving to whichever Django is newest.

Run everything the way CI would:

```bash
docker compose run --rm test tox
```

Run one package's environments by label, one environment, or hand extra arguments through tox to
`manage.py test`:

```bash
docker compose run --rm test tox -m core
docker compose run --rm test tox -m postgres-objects
docker compose run --rm test tox -e core-py314-dj60-pg17
docker compose run --rm test tox -e core-py314-dj60-pg17 -- djanquiltdb_tests.router
docker compose run --rm test tox -e postgres-objects-py314-dj60-pg17 -- -k ViewPlacementTestCase
```

The plugin environment names its suite before the posargs, so a second dotted label cannot follow; narrow that
one with `-k` instead, as above.

The `ruff`, `docs` and the two coverage environments need no database; add `--no-deps` to skip starting the
PostgreSQL containers for those:

```bash
docker compose run --no-deps --rm test tox -e docs
```

The Postgres containers keep their data in named volumes; `docker compose down -v` resets them. The tox
environments live in a container-internal volume (`/app/.tox`), so they never pollute the host checkout.

CI runs these same tox environments, one per job, against a pair of PostgreSQL service containers rather
than this compose file; `.github/workflows/ci.yml` is the whole of the difference.
