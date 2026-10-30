.. The logo needs an absolute URL: this README is the PyPI long description, where a relative path renders broken.

.. image:: https://raw.githubusercontent.com/DjanQuiltDB/djanquiltdb/master/assets/icon-128.png
    :alt: DjanQuiltDB
    :width: 128

================================
djanquiltdb-plugin-oauth-toolkit
================================

.. image:: https://img.shields.io/pypi/v/djanquiltdb-plugin-oauth-toolkit.svg
    :target: https://pypi.org/project/djanquiltdb-plugin-oauth-toolkit/
    :alt: PyPI

.. image:: https://img.shields.io/pypi/pyversions/djanquiltdb-plugin-oauth-toolkit.svg
    :target: https://pypi.org/project/djanquiltdb-plugin-oauth-toolkit/
    :alt: Supported Python versions

.. image:: https://img.shields.io/pypi/frameworkversions/django/djanquiltdb-plugin-oauth-toolkit.svg
    :target: https://pypi.org/project/djanquiltdb-plugin-oauth-toolkit/
    :alt: Supported Django versions

.. image:: https://img.shields.io/badge/postgres-17%20%7C%2018-4169e1?logo=postgresql&logoColor=white
    :target: https://www.postgresql.org/
    :alt: Supported PostgreSQL versions

.. image:: https://github.com/DjanQuiltDB/djanquiltdb/actions/workflows/ci.yml/badge.svg
    :target: https://github.com/DjanQuiltDB/djanquiltdb/actions/workflows/ci.yml
    :alt: CI

.. image:: https://readthedocs.org/projects/djanquiltdb/badge/?version=latest
    :target: https://djanquiltdb.readthedocs.io/en/latest/plugins/oauth-toolkit/index.html
    :alt: Documentation

.. image:: https://img.shields.io/pypi/l/djanquiltdb-plugin-oauth-toolkit.svg
    :target: https://github.com/DjanQuiltDB/djanquiltdb/blob/master/LICENSE
    :alt: BSD-3-Clause licence

`DjanQuiltDB <https://github.com/djanquiltdb/djanquiltdb>`_ shards a database across schemas and nodes. This library
generates client ids and tokens for `django-oauth-toolkit <https://django-oauth-toolkit.readthedocs.io/>`_ that have
the mapping value of their tenant in them, which is the value that the mapping table of the project maps onto the
shard of the tenant. For the tenant whose mapping value is ``acme``, they look like ``OacmeI...`` and ``OacmeT...``::

    OAUTH2_PROVIDER = {
        'CLIENT_ID_GENERATOR_CLASS': 'djanquiltdb.base.oauth_toolkit.ShardSelectorClientIdGenerator',
        'ACCESS_TOKEN_GENERATOR': 'djanquiltdb.utils.oauth_toolkit.generate_access_token',
        'REFRESH_TOKEN_GENERATOR': 'djanquiltdb.utils.oauth_toolkit.generate_refresh_token',
    }

Its middleware reads the shard selector back from the client id or the token of each request, and enters that shard
before django-oauth-toolkit looks up the client id or the token. Put it in place of ``UseShardForMiddleware``, after
``SessionMiddleware`` and before the toolkit's ``OAuth2TokenMiddleware``::

    MIDDLEWARE = [
        'django.contrib.sessions.middleware.SessionMiddleware',
        'djanquiltdb.middleware.oauth_toolkit.OAuthToolkitUseShardForMiddleware',
        'django.contrib.auth.middleware.AuthenticationMiddleware',
        'oauth2_provider.middleware.OAuth2TokenMiddleware',
    ]

Install it through DjanQuiltDB's extra::

    pip install djanquiltdb[oauth-toolkit]

The `installation guide <https://djanquiltdb.readthedocs.io/en/latest/plugins/oauth-toolkit/installation.html>`_
describes each step in full. The toolkit's models are sharded, so that each tenant has its own applications, grants and
tokens. The project needs a ``MAPPING_MODEL``, because the shard selector is the mapping value of a tenant. This example
maps tenants onto shards by slug, so ``SHARD_SELECTOR_REGEX`` matches slugs::

    QUILT_DB = {
        ...
        'MAPPING_MODEL': 'myapp.models.TenantShard',
        'SHARD_SELECTOR_REGEX': '[-a-z0-9]+',
        'OVERRIDE_SHARDING_MODE': {
            ...
            ('oauth2_provider',): ShardingMode.SHARDED,
        },
    }

List the plugin in ``INSTALLED_APPS`` before ``oauth2_provider``, so that its ``cleartokens`` command, which clears the
expired tokens of every active shard, replaces the toolkit's::

    INSTALLED_APPS = [
        ...
        'djanquiltdb_plugin_oauth_toolkit',
        'oauth2_provider',
        ...
    ]

Full documentation is at https://djanquiltdb.readthedocs.io/en/latest/plugins/oauth-toolkit/index.html.

Development
===========

This package is developed in the `DjanQuiltDB repository <https://github.com/DjanQuiltDB/djanquiltdb>`_ beside
``djanquiltdb`` itself. Tests run through tox against the Postgres containers in the repository's
``docker-compose.yml``; see ``DOCKER.md`` at the repository root::

    docker compose run --rm test tox

Requirements
------------

* Python 3.14
* Django 6.0 or 6.1
* PostgreSQL 17 or 18
* djanquiltdb 4.6.0 or newer 4.x
* django-oauth-toolkit 3.4 or newer 3.x
