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


# Named trackers: a label for each operation, a condition on the update tracker, a delete tracker and a manual event.
@track(
    pghistory.InsertEvent('cookie.baked'),
    pghistory.UpdateEvent('cookie.reglazed', condition=pghistory.AnyChange('glaze')),
    pghistory.DeleteEvent('cookie.eaten'),
    pghistory.ManualEvent('cookie.inspected'),
)
@sharded_model()
class Cookie(models.Model):
    name = models.CharField('name', max_length=128)
    glaze = models.CharField('glaze', max_length=128)

    class Meta:
        app_label = 'example'


# Two track() decorators, each of which records a different part of the row, shaped by fields, exclude, model_name and
# obj_field. Neither event model records every field, so neither can revert.
@track(
    pghistory.InsertEvent('scone.iced_insert'),
    pghistory.UpdateEvent('scone.iced_update'),
    fields=['icing'],
    model_name='SconeIcingEvent',
    obj_field=pghistory.ObjForeignKey(related_name='icing_events'),
)
@track(
    pghistory.InsertEvent('scone.plain_insert'),
    exclude=['icing'],
    obj_field=None,
    model_name='SconePlainEvent',
)
@sharded_model()
class Scone(models.Model):
    name = models.CharField('name', max_length=128)
    icing = models.CharField('icing', max_length=128)

    class Meta:
        app_label = 'example'


# The call site asks for a normalized context. Depending on the run of the suite, pghistory's Context table is sharded
# or public. The default trackers are named next to a manual one, which pghistory.create_event() writes through its own
# SQL compiler, because the event model has a foreign key to the Context table.
@track(
    pghistory.InsertEvent(),
    pghistory.UpdateEvent(),
    pghistory.ManualEvent('waffle.inspected'),
    context_field=pghistory.ContextForeignKey(),
    # A proxy field on a sharded event model, which reads the metadata through the context foreign key.
    attrs={'baked_by': pghistory.ProxyField('pgh_context__metadata__baked_by', models.TextField(null=True))},
)
@sharded_model()
class Waffle(models.Model):
    name = models.CharField('name', max_length=128)

    class Meta:
        app_label = 'example'


# Danish and Strudel cover the known gaps in EventQueryCompiler._get_cte: a db_column that is different from the
# attname, and a proxy field that reads a key inside a key of the context metadata.
@track(
    context_field=pghistory.ContextForeignKey(),
    attrs={'baked_by': pghistory.ProxyField('pgh_context__metadata__baked_by', models.TextField(null=True))},
)
@sharded_model()
class Danish(models.Model):
    name = models.CharField('name', max_length=128, db_column='pastry_name')

    class Meta:
        app_label = 'example'


@track(
    context_field=pghistory.ContextForeignKey(),
    attrs={'baker_name': pghistory.ProxyField('pgh_context__metadata__baker__name', models.TextField(null=True))},
)
@sharded_model()
class Strudel(models.Model):
    name = models.CharField('name', max_length=128)

    class Meta:
        app_label = 'example'


# Tracks the through model of Cake.toppings through a proxy, as pghistory documents, because the auto-created through
# model cannot be decorated. It gets the mode of Cake.
@track(
    pghistory.InsertEvent('topping.added'),
    pghistory.DeleteEvent('topping.removed'),
    obj_field=None,
)
class CakeToppings(Cake.toppings.through):
    class Meta:
        app_label = 'example'
        proxy = True


# Statement-level triggers call _pgh_attach_context() even with a denormalized context, so the function has to be on
# the search path of the schema that they run in.
@track(level=pghistory.Statement)
@sharded_model()
class Croissant(models.Model):
    name = models.CharField('name', max_length=128)

    class Meta:
        app_label = 'example'


class GroupProxy(Group):
    """
    A proxy of a third-party model, without a mode of its own. It gets the mode of auth, which OVERRIDE_SHARDING_MODE
    marks as mirrored.
    """

    class Meta:
        app_label = 'example'
        proxy = True
