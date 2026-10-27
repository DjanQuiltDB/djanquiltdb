.. _async_code:

==========
Async code
==========

DjanQuiltDB is not async-safe. Use it from sync code only, and from async code only through ``sync_to_async`` or
``database_sync_to_async``, as this guide describes.


Why
---

DjanQuiltDB keeps the active shard per thread: ``use_shard`` and ``use_shard_for`` set it for the thread that enters
them, and the router reads it for every query of that thread. Async code and its queries run on different threads.
Django's async ORM runs each query in a worker thread (``await Model.objects.aget(...)`` is
``sync_to_async(Model.objects.get)``), and an ASGI server gives each request a worker thread of its own. A shard that
async code enters on the thread of the event loop is not the active shard of the thread that runs the query.

Async code goes wrong in these places:

* A ``with use_shard(...)`` block in an async view or consumer. Its async ORM queries run outside the shard.
* ``@use_shard(...)`` or ``@use_shard_for(...)`` on an ``async def``. The decorator leaves the shard as soon as it has
  created the coroutine, before the body of the function runs.
* The async methods of the session backend (``aget()``, ``aset()``, ``aflush()`` and the others). Django's database
  backend reads and writes the session table in them without the sync methods that DjanQuiltDB wraps in the shard of
  the session.
* An async ORM call that enters no shard. It runs in whichever shard the worker thread happens to have entered, if any.
* The consumers of an ASGI application, such as those of Channels, which run on the event loop.


What is safe
------------

Sync views and sync middleware are safe, also under ASGI: Django runs them, and the hooks of a middleware that is sync
capable, in the worker thread of the request, so the shard that ``UseShardForMiddleware`` enters is the active shard of
every query they make.

Async code is safe when it enters the shard in sync code, and makes its queries there::

    from channels.db import database_sync_to_async
    from channels.generic.websocket import AsyncJsonWebsocketConsumer

    from djanquiltdb.utils import use_shard_for
    from myapp.models import Report


    def count_reports(tenant_slug):
        with use_shard_for(tenant_slug):
            return Report.objects.count()


    class ReportConsumer(AsyncJsonWebsocketConsumer):
        async def connect(self):
            count = await database_sync_to_async(count_reports)(self.scope['shard_selector'])
            ...

The whole of ``count_reports`` runs in one worker thread: the shard it enters is the active shard of its query. Do not
enter the shard in the consumer and query in the function, or the other way round.


What DjanQuiltDB refuses
------------------------

DjanQuiltDB fails loudly where it can tell that async code reaches it:

* The system checks ``djanquiltdb.E007``, ``djanquiltdb.E008`` and ``djanquiltdb.E009`` report an async view, an
  async-only middleware and an ``ASGI_APPLICATION`` setting (see :doc:`/reference/checks`). Each is an error, so it
  stops ``runserver``, ``migrate`` and the test runner. Once your async code enters shards only as above, acknowledge
  the error through ``SILENCED_SYSTEM_CHECKS``.
* Entering ``use_shard`` or ``use_shard_for`` on the thread of the event loop raises Django's
  ``SynchronousOnlyOperation``. Like Django's own check of the database layer, it is skipped while the environment
  variable ``DJANGO_ALLOW_ASYNC_UNSAFE`` is set, which some test setups set for Playwright.
* ``use_shard`` and ``use_shard_for`` raise ``NotImplementedError`` when they decorate an ``async def``, and
  ``for_each_shard()`` and ``for_each_node()`` when they are given one.
* The async methods of the session backend raise ``NotImplementedError``.

These do not catch everything. An async ORM call that enters no shard, and async code that is neither a view, nor a
middleware, nor served through ``ASGI_APPLICATION``, are not refused: such code has to follow the rule of the previous
section without help.
