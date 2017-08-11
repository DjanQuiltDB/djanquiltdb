from django.conf import settings
from django.db import models, connections
from django.utils.module_loading import import_string

from sharding.utils import State, STATES


def get_shard_class():
    """ Helper function to get implemented Shard class """
    return import_string(settings.SHARDING['SHARD_CLASS'])


class MappingQuerySet(models.QuerySet):
    def active(self):
        return self.filter(shard__state=State.ACTIVE)

    def in_maintenance(self):
        return self.filter(shard__state=State.MAINTENANCE)

    def for_target(self, target_value):
        return self.get(**{self.model.mapping_field: target_value})


class BaseShard(models.Model):
    """ Base class for Shard models """
    alias = models.CharField(max_length=128, db_index=True, unique=True)
    schema_name = models.CharField(max_length=64)  # PostgreSQL default max limit = 63 chars
    node_name = models.CharField(max_length=64)
    state = models.CharField(choices=STATES, max_length=1, default=State.MAINTENANCE)

    class Meta:
        app_label = 'sharding'
        abstract = True
        unique_together = ('schema_name', 'node_name')

    def save(self, **kwargs):
        if not self.pk:  # only create a new shard if the shard is newly created.
            self.node_name = self.node_name or settings.SHARDING.get('NEW_SHARD_NODE', None)
            if not self.node_name:
                raise ValueError("No node_name given, or no NEW_SHARD_NODE set in the SHARING settings.")

            from sharding.utils import create_schema_on_node  # import it here, to prevent circle dependencies
            create_schema_on_node(schema_name=self.schema_name, node_name=self.node_name, migrate=True)

        super().save(**kwargs)  # save to default database

    def clean(self):
        if self.node_name not in connections:
            raise ValueError("Connection '{}' does not exist. Is it listed in settings.DATABASES?"
                             .format(self.node_name))
