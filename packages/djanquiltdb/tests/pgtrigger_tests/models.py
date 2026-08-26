import pgtrigger
from django.db import models

from djanquiltdb.decorators import public_model, sharded_model

__all__ = [
    'ShardedProtected',
    'PublicProtected',
]


@sharded_model()
class ShardedProtected(models.Model):
    name = models.CharField('name', max_length=100)

    class Meta:
        app_label = 'pgtrigger_tests'
        triggers = [pgtrigger.Protect(name='protect_deletes', operation=pgtrigger.Delete)]


class PublicProtectedManager(models.Manager):
    def get_by_natural_key(self, name):
        return self.get(name=name)


@public_model(allow_copy=False)
class PublicProtected(models.Model):
    name = models.CharField('name', max_length=100)

    objects = PublicProtectedManager()

    class Meta:
        app_label = 'pgtrigger_tests'
        unique_together = [['name']]
        triggers = [pgtrigger.Protect(name='protect_deletes', operation=pgtrigger.Delete)]

    def natural_key(self):
        return (self.name,)
