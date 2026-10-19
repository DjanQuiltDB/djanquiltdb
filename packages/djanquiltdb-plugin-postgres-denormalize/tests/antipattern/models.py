"""
A copy of a column of a mirrored model on a sharded model. The cascade and fill triggers that keep the copy up to date
are on the mirrored table in the public schema, and write the sharded table, so DjanQuiltDB's system check reports each
of them as djanquiltdb.E001. The test settings silence that error, so that the suite can show it.
"""

from django.db import models
from djanquiltdb.decorators import mirrored_model, sharded_model
from postgres_denormalize.fields import DenormalizedField


@mirrored_model()
class TaskType(models.Model):
    name = models.CharField('name', max_length=100)

    class Meta:
        app_label = 'antipattern'


@sharded_model()
class TypedTask(models.Model):
    type = models.ForeignKey('antipattern.TaskType', on_delete=models.PROTECT)
    type_name = DenormalizedField('type__name')

    class Meta:
        app_label = 'antipattern'
