from django.db import models
from djanquiltdb.decorators import mirrored_model, sharded_model

__all__ = [
    'SuperMirroredModel',
    'MirroredModel',
    'SuperShardedModel',
    'ShardedModel',
]


@mirrored_model()
class SuperMirroredModel(models.Model):
    name = models.CharField('name', max_length=100)

    class Meta:
        app_label = 'migration_tests'


@mirrored_model()
class MirroredModel(models.Model):
    name = models.CharField('name', max_length=100)
    super = models.ForeignKey('SuperMirroredModel', on_delete=models.DO_NOTHING, verbose_name='super', null=True)

    class Meta:
        app_label = 'migration_tests'


@sharded_model()
class SuperShardedModel(models.Model):
    name = models.CharField('name', max_length=100)
    to_mirrored = models.ForeignKey('MirroredModel', on_delete=models.DO_NOTHING, verbose_name='to_mirrored', null=True)

    class Meta:
        app_label = 'migration_tests'


@sharded_model()
class ShardedModel(models.Model):
    name = models.CharField('name', max_length=100)
    super = models.ForeignKey('SuperShardedModel', on_delete=models.DO_NOTHING, verbose_name='super', null=True)

    class Meta:
        app_label = 'migration_tests'
