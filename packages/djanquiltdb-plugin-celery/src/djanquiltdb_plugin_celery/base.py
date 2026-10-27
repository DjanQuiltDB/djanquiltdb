"""
The classes of the plugin that a project subclasses or refers to. DjanQuiltDB serves this module as
``djanquiltdb.base.celery``. The task classes are defined in :mod:`djanquiltdb_plugin_celery.tasks`, and the Celery app
class in :mod:`djanquiltdb_plugin_celery.celery_app`.
"""

from djanquiltdb_plugin_celery.celery_app import QuiltCelery
from djanquiltdb_plugin_celery.tasks import QuiltTask, TaskSenderForTarget

__all__ = ['QuiltCelery', 'QuiltTask', 'TaskSenderForTarget']
