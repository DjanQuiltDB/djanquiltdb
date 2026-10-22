.. The logo needs an absolute URL: this README is the PyPI long description, where an image with a relative path does
   not render.

.. image:: https://raw.githubusercontent.com/DjanQuiltDB/djanquiltdb/master/assets/icon-128.png
    :alt: DjanQuiltDB
    :width: 128

=========================
djanquiltdb-plugin-celery
=========================

.. image:: https://img.shields.io/pypi/v/djanquiltdb-plugin-celery.svg
    :target: https://pypi.org/project/djanquiltdb-plugin-celery/
    :alt: PyPI

.. image:: https://img.shields.io/pypi/pyversions/djanquiltdb-plugin-celery.svg
    :target: https://pypi.org/project/djanquiltdb-plugin-celery/
    :alt: Supported Python versions

.. image:: https://img.shields.io/pypi/frameworkversions/django/djanquiltdb-plugin-celery.svg
    :target: https://pypi.org/project/djanquiltdb-plugin-celery/
    :alt: Supported Django versions

.. image:: https://img.shields.io/badge/postgres-17%20%7C%2018-4169e1?logo=postgresql&logoColor=white
    :target: https://www.postgresql.org/
    :alt: Supported PostgreSQL versions

.. image:: https://github.com/DjanQuiltDB/djanquiltdb/actions/workflows/ci.yml/badge.svg
    :target: https://github.com/DjanQuiltDB/djanquiltdb/actions/workflows/ci.yml
    :alt: CI

.. image:: https://readthedocs.org/projects/djanquiltdb/badge/?version=latest
    :target: https://djanquiltdb.readthedocs.io/en/latest/plugins/celery/index.html
    :alt: Documentation

.. image:: https://img.shields.io/pypi/l/djanquiltdb-plugin-celery.svg
    :target: https://github.com/DjanQuiltDB/djanquiltdb/blob/master/LICENSE
    :alt: BSD-3-Clause licence

`DjanQuiltDB <https://github.com/djanquiltdb/djanquiltdb>`_ shards a database across schemas and nodes. This library
provides `Celery <https://docs.celeryq.dev/>`_ tasks that run in the shard that they are sent to, and that are retried
while that shard is in maintenance::

    from djanquiltdb.decorators.celery import shared_task_for_shard


    @shared_task_for_shard
    def send_invoice(invoice_id):
        Invoice.objects.get(pk=invoice_id).send()


    send_invoice.for_shard(shard).delay(invoice_id=12)
    send_invoice.delay(invoice_id=12, tenant_id=3)

A task that is decorated with ``shared_task_for_node`` runs in the public schema of the node that its ``node_name`` identifies.
A task that is decorated with ``shared_task_for_active_shard`` runs in the shard that is active where the task is sent. A task
that is decorated with ``shared_task_for_every_shard`` runs once in every active shard when it is sent without a shard::

    from djanquiltdb.decorators.celery import shared_task_for_every_shard


    @shared_task_for_every_shard(queue='fanout', spread=(0, 300))
    def clean_up_drafts():
        Draft.objects.filter(updated_at__lt=a_month_ago()).delete()


    clean_up_drafts.delay()

Install the plugin through the ``celery`` extra of DjanQuiltDB::

    pip install djanquiltdb[celery]

Full documentation is at https://djanquiltdb.readthedocs.io/en/latest/plugins/celery/index.html.

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
* djanquiltdb 4.4.0 or newer 4.x
* Celery 5.6 or newer 5.x
