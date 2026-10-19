from django.db import models
from djanquiltdb import STATES, State
from djanquiltdb.decorators import mirrored_model, shard_mapping_model, sharded_model
from djanquiltdb.models import BaseShard, MappingQuerySet


@mirrored_model()
class Shard(BaseShard):
    class Meta:
        app_label = 'example'


# Maps each tenant onto the shard that has its data, so that a task can identify its shard by tenant_id.
@mirrored_model()
@shard_mapping_model(mapping_field='tenant_id')
class TenantShard(models.Model):
    shard = models.ForeignKey('example.Shard', on_delete=models.CASCADE)
    tenant_id = models.PositiveIntegerField(unique=True)
    state = models.CharField(choices=STATES, max_length=1, default=State.ACTIVE)

    objects = MappingQuerySet.as_manager()

    class Meta:
        app_label = 'example'


@sharded_model()
class Widget(models.Model):
    name = models.CharField('name', max_length=100)

    class Meta:
        app_label = 'example'
