from django.db import models
from djanquiltdb.decorators import mirrored_model, sharded_model
from djanquiltdb.models import BaseShard
from postgres_denormalize.fields import DenormalizedField


@mirrored_model()
class Shard(BaseShard):
    class Meta:
        app_label = 'example'


# The root object of the data in a shard, which move_data_to_shard moves with everything below it.
@sharded_model()
class Organization(models.Model):
    name = models.CharField('name', max_length=100)

    class Meta:
        app_label = 'example'


@sharded_model()
class Project(models.Model):
    organization = models.ForeignKey('example.Organization', on_delete=models.CASCADE)
    name = models.CharField('name', max_length=100)

    class Meta:
        app_label = 'example'


# A copy whose lookup path stays inside the shard, as the plugin documentation recommends.
@sharded_model()
class Task(models.Model):
    project = models.ForeignKey('example.Project', on_delete=models.CASCADE)
    title = models.CharField('title', max_length=100)
    project_name = DenormalizedField('project__name')

    class Meta:
        app_label = 'example'


# A second leaf, whose lookup path is two hops long.
@sharded_model()
class Comment(models.Model):
    task = models.ForeignKey('example.Task', on_delete=models.CASCADE)
    project_name = DenormalizedField('task__project__name')

    class Meta:
        app_label = 'example'
