"""
The classes of the plugin that a project subclasses or refers to. DjanQuiltDB serves this module as
``djanquiltdb.base.celery``. The classes are defined in :mod:`djanquiltdb_plugin_celery.tasks`.
"""

from djanquiltdb_plugin_celery.tasks import QuiltTask, TaskSenderForTarget

__all__ = ['QuiltTask', 'TaskSenderForTarget']
