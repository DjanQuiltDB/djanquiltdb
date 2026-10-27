============
Installation
============

Install through DjanQuiltDB's extra::

    pip install djanquiltdb[celery]

The plugin itself needs no setup: DjanQuiltDB finds the plugin through its entry point once the plugin is installed. The
project sets up Celery as `Celery's documentation for Django
<https://docs.celeryq.dev/en/stable/django/first-steps-with-django.html>`_ describes. The Celery app reads the
``CELERY_`` settings and discovers the ``tasks`` modules of the installed apps. The app is a
:class:`~djanquiltdb.base.celery.QuiltCelery`, a subclass of ``Celery``, so that its ``task_for_*`` methods can create
a task of that app:

.. code-block:: python

    # myproject/celery.py
    from djanquiltdb.base.celery import QuiltCelery

    app = QuiltCelery('myproject')
    app.config_from_object('django.conf:settings', namespace='CELERY')
    app.autodiscover_tasks()

The plugin creates a task in the two ways that Celery does. The ``shared_task_for_*`` decorators create a shared task,
as ``celery.shared_task()`` does, and work with any Celery app, also a plain ``Celery`` app. The ``task_for_*`` methods
of a ``QuiltCelery`` app create a task of that app, as ``app.task()`` does. :doc:`tasks` describes how the two differ.

A ``QuiltCelery`` app has Celery's ``DjangoTask`` as its task class, as a plain ``Celery`` app in a Django project has,
unless the arguments of the app or a class of the app set a task class of their own. Celery's Django fixup gives
``DjangoTask`` only to a plain ``Celery`` app, and ``QuiltCelery`` gives it to its own apps.

A task of the plugin also inherits from the task class of the app, which Celery's ``task_cls`` sets, so that a task
class of the project, for example one that records a context around each run, applies to the tasks of the plugin as
well:

.. code-block:: python

    # myproject/celery.py
    app = QuiltCelery('myproject', task_cls='myproject.tasks:ContextTask')

The plugin combines :class:`~djanquiltdb.base.celery.QuiltTask` with that class, with the methods of ``QuiltTask``
first, because they send and enter the target of the task. ``QuiltTask`` inherits from ``DjangoTask``, so a task of the
plugin is a ``DjangoTask`` also when the task class of the app is not. Each app combines ``QuiltTask`` with its own task
class when it registers the task, for a shared task and for a task of one app alike, so a ``tasks`` module that is
imported before the Celery app of the project is created gets the task class of that app as well.
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

The arguments of each decorator take precedence over these settings. The retry settings, ``TASK_BASE`` and the task
class of the Celery app are read when the app registers the task, as Celery reads the task class, and the other
settings when a task is sent. So a tasks module can be imported whatever the settings are, and a ``TASK_BASE`` that does
not import fails when the app registers the task.

The plugin's system checks report a ``QUILT_CELERY`` key that the plugin does not read
(``djanquiltdb_plugin_celery.E001``). They also report a value that a key does not accept, a ``MAPPING_KWARG`` that is
``shard_id`` or ``node_name``, a ``MAPPING_KWARG`` in a project without a ``MAPPING_MODEL``, a ``TASK_BASE`` that does
not import or does not inherit from ``QuiltTask``, and a ``QUILT_CELERY`` that is not a dict
(``djanquiltdb_plugin_celery.E002``). While ``QUILT_CELERY`` is not a dict, every read of a setting raises
``ImproperlyConfigured``.
