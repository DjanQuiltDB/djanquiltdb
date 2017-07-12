from django.core.exceptions import ImproperlyConfigured
from django.conf import settings
from django.db import models

from sharding.utils import ShardingMode

shard_mapping_models = False


def _reset_shard_mapping_models():
    # for internal testing use only.
    global shard_mapping_models
    shard_mapping_models = False


def mirrored_model():
    """
    A decorator for marking a model for being mirror across the various nodes.
    """
    def configure(cls):
        cls.sharding_mode = ShardingMode.MIRRORED
        return cls

    return configure


def shard_mapping_model():
    """
    A decorator for marking a model that maps shards and their content.
    This model will hold the foreignkey to the appropriate Shard model.

    :note: This model will NOT be sharded. It would defeat its purpose if it did.

    :Example:
        .. code-block:: python

            from django.db import models
            from sharding.decorators import shard_mapping_model, sharded_model
            from sharding.models import BaseShard

            class Shard(BaseShard):
                class Meta:
                    app_label = 'example'

            @shard_mapping_model()
            class ShardMapping(models.Model):
                # List every organization in this model, so you can easily request in which shard they live.
                shard = models.ForeignKey('Shard', verbose_name='shard')
                organization_name = models.CharField('organization name', max_length=100)
                organization_id = models.PositiveIntegerField(_('organization id'))

                class Meta:
                    app_label = 'example'

            @sharded_model()
            class Organization(models.Model):
                name = models.CharField('name', max_length=100)
                created_at = models.DateTimeField(_('created at'), null=True, blank=True, default=timezone.now)

                class Meta:
                    app_label = 'example'

    """
    def configure(cls):
        shard_field = None
        for field in cls._meta.fields:
            if field.name == 'shard':
                shard_field = field
                break
        if not shard_field:
            raise ImproperlyConfigured(
                '{} model is missing a foreignkey field named "shard". '
                'The @shard_mapping_model decorator requires this.'
                .format(cls.__name__))
        elif not isinstance(shard_field, models.ForeignKey):
            raise ImproperlyConfigured(
                'The shard field of model {} is not a Foreignkey to the shard model. '
                'The @shard_mapping_model decorator requires this.'
                .format(cls.__name__))
        else:
            related_to = shard_field.rel.to if type(shard_field.rel.to) is str else \
                shard_field.rel.to.__module__.replace('.models', '') + '.' + shard_field.rel.to.__name__
            if related_to != settings.SHARDING['SHARD_CLASS'].replace('.models', ''):
                raise ImproperlyConfigured(
                    'The shard field of model {} is points to \'{}\' instead of \'{}\'. '
                    'The @shard_mapping_model decorator requires this.'
                    .format(cls.__name__, related_to,
                            settings.SHARDING['SHARD_CLASS'].replace('.models', '')))

        # set global counter to detect multiple usages of this decorator, which is not allowed.
        global shard_mapping_models
        if shard_mapping_models:
            raise ImproperlyConfigured(
                'More than one model uses the @shard_mapping_model decorator. This is not allowed.')
        else:
            shard_mapping_models = True

        cls.sharding_mode = ShardingMode.DEFINING
        return cls

    return configure


def sharded_model():
    """
    A decorator for marking a model as being sharded.
    This means this model will be migrated to new shards, and not appear in the public schema.

    :Example:
        .. code-block:: python

            from django.db import models
            from sharding.decorators import sharded_model
            from sharding.models import BaseShard

            class Shard(BaseShard):
                class Meta:
                    app_label = 'example'

            @sharded_model()
            class Organization(models.Model):
                name = models.CharField('name', max_length=100)
                created_at = models.DateTimeField(_('created at'), null=True, blank=True, default=timezone.now)

                class Meta:
                    app_label = 'example'

    """
    def configure(cls):
        cls.sharding_mode = ShardingMode.SHARDED
        return cls

    return configure
