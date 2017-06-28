from django.core.exceptions import ImproperlyConfigured
from django.conf import settings
from django.db import models

from sharding.utils import ShardingMode

defining_shard_models = False


def _reset_defining_shard_models():
    # for internal testing use only.
    global defining_shard_models
    defining_shard_models = False


def mirrored_model():
    """
    A decorator for marking a model for being mirror across the various nodes.
    """
    def configure(cls):
        cls.sharding_mode = ShardingMode.MIRRORED
        return cls

    return configure


def defining_shard_model():
    """
    A decorator for marking a model as the defining model for sharding.
    This model will hold the foreignkey to the appropriate Shard model.
    This model will NOT be sharded. It would defeat its purpose if it did.
    """
    def configure(cls):
        shard_field = None
        for field in cls._meta.fields:
            if field.name == 'shard':
                shard_field = field
                break
        if not shard_field:
            raise ImproperlyConfigured(
                '{} model is missing a foreignkey field named "shard". The defining_sharded_model requires this.'
                .format(cls.__name__))
        elif not isinstance(shard_field, models.ForeignKey):
            raise ImproperlyConfigured(
                'The shard field of model {} is not a Foreignkey to the shard model. '
                'The defining_sharded_model requires this.'
                .format(cls.__name__))
        else:
            related_to = shard_field.rel.to if type(shard_field.rel.to) is str else \
                shard_field.rel.to.__module__.replace('.models', '') + '.' + shard_field.rel.to.__name__
            if related_to != settings.SHARDING['SHARD_CLASS'].replace('.models', ''):
                raise ImproperlyConfigured(
                    'The shard field of model {} is points to \'{}\' instead of \'{}\'. '
                    'The defining_sharded_model requires this.'
                    .format(cls.__name__, related_to,
                            settings.SHARDING['SHARD_CLASS'].replace('.models', '')))

        # set global counter to detect multiple usages of this decorator, which is not allowed.
        global defining_shard_models
        if defining_shard_models:
            raise ImproperlyConfigured(
                'More than one model uses the @defining_shard_model decorator. This is not allowed.')
        else:
            defining_shard_models = True

        cls.sharding_mode = ShardingMode.DEFINING

        return cls

    return configure


def sharded_model():
    """
    A decorator for marking a model as being sharded.
    This means this model will be migrated to new shards
    """
    def configure(cls):
        cls.sharding_mode = ShardingMode.SHARDED
        return cls

    return configure
