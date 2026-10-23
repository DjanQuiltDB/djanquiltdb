============
Installation
============

Install through DjanQuiltDB's extra::

    pip install djanquiltdb[celery]

The plugin itself needs no setup: DjanQuiltDB finds the plugin through its entry point once the plugin is installed. The
project sets up Celery as `Celery's documentation for Django
<https://docs.celeryq.dev/en/stable/django/first-steps-with-django.html>`_ describes. The Celery app reads the
``CELERY_`` settings and discovers the ``tasks`` modules of the installed apps:

.. code-block:: python

    # myproject/celery.py
    from celery import Celery

    app = Celery('myproject')
    app.config_from_object('django.conf:settings', namespace='CELERY')
    app.autodiscover_tasks()

The plugin's decorators create shared tasks, so these tasks belong to that app, like any task that ``shared_task``
creates.

A task of the plugin also inherits from the task class of the app, which Celery's ``task_cls`` sets, so that a task
class of the project, for example one that records a context around each run, applies to the tasks of the plugin as
well:

.. code-block:: python

    # myproject/celery.py
    app = Celery('myproject', task_cls='myproject.tasks:ContextTask')

The decorators combine :class:`~djanquiltdb.base.celery.QuiltTask` with that class, with the methods of ``QuiltTask``
first, because they send and enter the target of the task. An app without a ``task_cls`` of its own has Celery's
``DjangoTask``, which ``QuiltTask`` already inherits from. Each app combines ``QuiltTask`` with its own task class when
it registers the task, as ``shared_task`` gives each app a copy of its tasks, so a ``tasks`` module that is imported
before the Celery app of the project is created gets the task class of that app as well.
``QUILT_CELERY['TASK_BASE']``, below, names a base class for every task of the plugin instead, and the ``base`` of a
decorator takes precedence over both.

The plugin requires Celery 5.6 or a newer 5.x release. Its tests run a worker with the solo pool in a thread of the test
process. The active shard is local to the thread, and each task enters its shard itself, so a worker with the prefork or
the threads pool runs each task in its own shard as well.


Settings
--------

The optional ``QUILT_CELERY`` setting is a dict whose keys override these defaults:

.. code-block:: python

    QUILT_CELERY = {
        # The keyword argument that identifies a shard by its mapping value, other than shard_id and node_name. None
        # means the mapping field of the MAPPING_MODEL, such as tenant_slug.
        'MAPPING_KWARG': None,
        # Celery's max_retries, retry_backoff, retry_backoff_max and retry_jitter options for a task whose shard is in
        # maintenance. These defaults retry after 5, 10, 20, 40 and 60 seconds.
        'MAX_RETRIES': 5,
        'RETRY_BACKOFF': 5,
        'RETRY_BACKOFF_MAX': 60,
        'RETRY_JITTER': False,
        # The queue of each task that a fan-out sends. None means the routing of the Celery app.
        'FANOUT_QUEUE': None,
        # The countdown of each task that a fan-out sends: a random number of seconds from 0 to an integer, or between
        # a pair of integers, which are not negative and in ascending order. None means no countdown.
        'FANOUT_SPREAD': None,
        # The dotted path of the base class of a task whose decorator is not given a base, which inherits from
        # QuiltTask. None means QuiltTask combined with the task class of the Celery app.
        'TASK_BASE': None,
    }

The arguments of each decorator take precedence over these settings. The retry settings and ``TASK_BASE`` are read when
a task is decorated, and the other settings when a task is sent. The task class of the Celery app is read when the app
registers the task.

The plugin's system checks report a ``QUILT_CELERY`` key that the plugin does not read
(``djanquiltdb_plugin_celery.E001``). They also report a value that a key does not accept, a ``MAPPING_KWARG`` that is
``shard_id`` or ``node_name``, a ``MAPPING_KWARG`` in a project without a ``MAPPING_MODEL``, a ``TASK_BASE`` that does
not import or does not inherit from ``QuiltTask``, and a ``QUILT_CELERY`` that is not a dict
(``djanquiltdb_plugin_celery.E002``). While ``QUILT_CELERY`` is not a dict, the plugin uses the defaults,
so that the ``tasks`` modules can be imported and the check can report the setting.
