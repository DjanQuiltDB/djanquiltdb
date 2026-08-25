from django.db import models

from djanquiltdb.decorators import mirrored_model, public_model, sharded_model
from djanquiltdb.models import BaseShard


@mirrored_model()
class Shard(BaseShard):
    class Meta:
        app_label = 'example'


@sharded_model()
class Cake(models.Model):
    name = models.CharField('name', max_length=128)

    class Meta:
        app_label = 'example'


class CakeTypeManager(models.Manager):
    def get_by_natural_key(self, name):
        return self.get(name=name)


@public_model(allow_copy=False)
class CakeType(models.Model):
    name = models.CharField('name', max_length=100)

    objects = CakeTypeManager()

    class Meta:
        app_label = 'example'
        unique_together = [['name']]

    def natural_key(self):
        return (self.name,)
