from django.conf import settings
from django.db import models
from django.utils.module_loading import import_string

from sharding.utils import create_schema_on_node


def get_shard_class():
    """ Helper function to get implemented Shard class """
    return import_string(settings.SHARDING['SHARD_CLASS'])


class BaseShard(models.Model):
    """ Base class for Shard models """
    alias = models.CharField(max_length=128, db_index=True)
    schema_name = models.CharField(max_length=64)  # PostgreSQL default max limit = 63 chars
    node_name = models.CharField(max_length=64)

    class Meta:
        app_label = 'sharding'
        abstract = True

    def save(self, **kwargs):
        create_schema_on_node(self.schema_name, self.node_name)
        super().save(**kwargs)  # save to default database
