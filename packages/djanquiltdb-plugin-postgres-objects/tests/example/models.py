from django.db import models

from djanquiltdb.decorators import mirrored_model, sharded_model
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
