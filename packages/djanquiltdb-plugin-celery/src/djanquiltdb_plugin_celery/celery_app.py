"""
The Celery app class of the plugin. DjanQuiltDB serves it from ``djanquiltdb.base.celery``.
"""

from celery import Celery
from celery.fixups.django import DjangoFixup

from djanquiltdb_plugin_celery.decorators import (
    attributes_of_every_node_task,
    attributes_of_every_shard_task,
    attributes_of_shard_task,
    task_decorator_or_task,
)

#: The task class that Celery's Django fixup gives an app without a task class of its own.
DJANGO_TASK_CLASS = 'celery.contrib.django.task:DjangoTask'


class QuiltCelery(Celery):
    """
    A Celery app whose ``task_for_*`` methods create a task of this app, as ``app.task()`` does, which runs in a shard
    or on a node as the ``shared_task_for_*`` decorator of the same name describes. The task belongs to this app
    whichever app is current, and is published through this app. Each method takes the options of ``app.task()``:
    ``shared``, which is True by default, also registers the task in each app that is finalized later, and ``lazy`` and
    ``filter`` work as they do for ``app.task()``.

    Celery's Django fixup makes ``DjangoTask`` the task class of a plain ``Celery`` app without a task class of its own,
    but treats every subclass of ``Celery`` as an app with a task class of its own. So ``QuiltCelery`` gives
    ``DjangoTask`` to its own apps, when the Django fixup is installed and neither the arguments nor a class of the app
    set a task class.

    :Example:
        .. code-block:: python

            # myproject/celery.py
            from djanquiltdb.base.celery import QuiltCelery

            app = QuiltCelery('myproject')

            # myapp/tasks.py
            from myproject.celery import app


            @app.task_for_shard
            def send_invoice(invoice_id):
                Invoice.objects.get(pk=invoice_id).send()
    """

    def __init__(self, *args, task_cls=None, **kwargs):
        super().__init__(*args, task_cls=task_cls, **kwargs)

        task_class_is_set = task_cls is not None or any(
            'task_cls' in vars(app_class) for app_class in type(self).__mro__ if app_class is not Celery
        )
        django_fixup_is_installed = any(isinstance(fixup, DjangoFixup) for fixup in self._fixups)

        if django_fixup_is_installed and not task_class_is_set:
            self.task_cls = DJANGO_TASK_CLASS

    def task_for_shard(self, func=None, *, inherit_active_shard=False, lock=None, **options):
        """
        :func:`~djanquiltdb.decorators.celery.shared_task_for_shard` for a task of this app.
        """
        return task_decorator_or_task(attributes_of_shard_task(inherit_active_shard, lock), func, options, app=self)

    def task_for_node(self, func=None, **options):
        """
        :func:`~djanquiltdb.decorators.celery.shared_task_for_node` for a task of this app.
        """
        return task_decorator_or_task({'runs_in': 'node'}, func, options, app=self)

    def task_for_active_shard(self, func=None, **options):
        """
        :func:`~djanquiltdb.decorators.celery.shared_task_for_active_shard` for a task of this app.
        """
        return self.task_for_shard(func, inherit_active_shard=True, **options)

    def task_for_every_shard(
        self, func=None, *, queue=None, spread=None, include_maintenance=False, lock=None, **options
    ):
        """
        :func:`~djanquiltdb.decorators.celery.shared_task_for_every_shard` for a task of this app.
        """
        task_attributes = attributes_of_every_shard_task(queue, spread, include_maintenance, lock)

        return task_decorator_or_task(task_attributes, func, options, app=self)

    def task_for_every_node(self, func=None, *, queue=None, spread=None, **options):
        """
        :func:`~djanquiltdb.decorators.celery.shared_task_for_every_node` for a task of this app.
        """
        return task_decorator_or_task(attributes_of_every_node_task(queue, spread), func, options, app=self)
