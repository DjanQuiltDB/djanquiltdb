"""
Task classes that the tests set as the task class of the Celery app or as QUILT_CELERY['TASK_BASE'].
"""

from celery import Task
from celery.contrib.django.task import DjangoTask

from djanquiltdb_plugin_celery.tasks import QuiltTask

#: The name of each task that a task of RecordingCallTask or PlainRecordingCallTask ran, in the order of the runs.
RECORDED_CALLS = []


class RecordingCallTask(DjangoTask):
    """
    A task class of a project, which records the name of each task that runs, around the run, as a project records a
    context for each task, such as the pghistory context.
    """

    def __call__(self, *args, **kwargs):
        RECORDED_CALLS.append(self.name)
        return super().__call__(*args, **kwargs)


class PlainRecordingCallTask(Task):
    """
    RecordingCallTask for a project whose task class inherits from Celery's Task, and not from DjangoTask.
    """

    def __call__(self, *args, **kwargs):
        RECORDED_CALLS.append(self.name)
        return super().__call__(*args, **kwargs)


class ProjectQuiltTask(QuiltTask):
    """
    A task class of a project that already inherits from QuiltTask.
    """
