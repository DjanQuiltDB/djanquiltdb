from django.conf import settings
from django.db import models
from django.utils.module_loading import import_string

from sharding.utils import use_shard, create_schema


def get_shard_class():
    """ Helper function to get implemented Shard class """
    return import_string(settings.SHARDING['SHARD_CLASS'])


def get_node_class():
    """ Helper function to get implemented Node class """
    return import_string(settings.SHARDING['NODE_CLASS'])


class BaseShard(models.Model):
    """ Base class for Shard models """
    alias = models.CharField(max_length=128, db_index=True)
    schema_name = models.CharField(max_length=64)  # PostgreSQL default max limit = 63 chars

    class Meta:
        app_label = 'sharding'
        abstract = True

    def save(self, **kwargs):
        create_schema(self.schema_name)
        with use_shard(self.schema_name):
            super().save(**kwargs)


class BaseNode(models.Model):
    """ Base class for Node models """
    uri = models.CharField(max_length=128)

    class Meta:
        app_label = 'sharding'
        abstract = True
