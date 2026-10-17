import pgtrigger
from djanquiltdb.testing import ShardingTransactionTestCase
from djanquiltdb.utils import use_shard

from example.models import Comment, Organization, Project, Task


def create_stale_rows(shard):
    with use_shard(shard):
        project = Project.objects.create(organization=Organization.objects.create(name='Acme'), name='Apollo')
        Comment.objects.create(task=Task.objects.create(project=project, title='Launch'))

        # The set triggers are ignored, so the copies end up stale, as after a write that the triggers do not see.
        with pgtrigger.ignore('example.Task:denormalize_set', 'example.Comment:denormalize_set'):
            Task.objects.update(project_name='stale')
            Comment.objects.update(project_name='stale')


class PluginTestCase(ShardingTransactionTestCase):
    # available_apps restricts the migration graph, so it lists every app of the project.
    available_apps = [
        'django.contrib.auth',
        'django.contrib.contenttypes',
        'djanquiltdb',
        'pgtrigger',
        'postgres_denormalize',
        'example',
        'antipattern',
    ]
