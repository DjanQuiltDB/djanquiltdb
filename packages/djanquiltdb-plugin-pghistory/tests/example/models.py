import pghistory
from django.contrib.auth.models import Group
from django.db import models
from djanquiltdb.decorators import mirrored_model, public_model, sharded_model
from djanquiltdb.decorators.pghistory import track, track_mirrored, track_public, track_sharded
from djanquiltdb.models import BaseShard
from djanquiltdb.models.pghistory import ShardedEvent

import example.tracking


@mirrored_model()
class Shard(BaseShard):
    class Meta:
        app_label = 'example'


@track()
@sharded_model()
class Cake(models.Model):
    name = models.CharField('name', max_length=128)
    # CakeToppings below tracks the auto-created through model.
    toppings = models.ManyToManyField('example.CakeType')

    class Meta:
        app_label = 'example'


# track_mirrored() does not read the mode from the model, so it works with the mode decorator above it. The proxy field
# reads a key from the denormalized context.
@mirrored_model()
@track_mirrored(attrs={'baked_by': pghistory.ProxyField('pgh_context__baked_by', models.TextField(null=True))})
class Bakery(models.Model):
    name = models.CharField('name', max_length=128)

    class Meta:
        app_label = 'example'


class CakeTypeManager(models.Manager):
    def get_by_natural_key(self, name):
        return self.get(name=name)


@track_public()
@public_model(allow_copy=False)
class CakeType(models.Model):
    name = models.CharField('name', max_length=100)

    objects = CakeTypeManager()

    class Meta:
        app_label = 'example'
        unique_together = [['name']]

    def natural_key(self):
        return (self.name,)


class CustomCakeEvent(ShardedEvent):
    """
    The base_model of Pie, whose events are therefore named PieCustomcakeevent. The trigger that writes the events only
    sets the snapshot and pgh_* columns, so the extra field has a db_default.
    """

    baked_with_love = models.BooleanField(db_default=True)

    class Meta:
        abstract = True


@track_sharded(base_model=CustomCakeEvent)
@sharded_model()
class Pie(models.Model):
    name = models.CharField('name', max_length=128)

    class Meta:
        app_label = 'example'


@example.tracking.track()
@sharded_model()
class Muffin(models.Model):
    name = models.CharField('name', max_length=128)

    class Meta:
        app_label = 'example'


class GroupProxy(Group):
    """
    A proxy of a third-party model, as a project tracking one would declare it. It carries no mode of its own: the mode
    resolves through the concrete model's app, auth, which OVERRIDE_SHARDING_MODE marks as mirrored.
    """

    class Meta:
        app_label = 'example'
        proxy = True
