from django.conf import settings
from django.db import models, connections, transaction
from django.db.models import Q

from sharding import State, STATES, ShardingMode
from sharding.options import ShardOptions
from sharding.utils import get_shard_class, use_shard, delete_schema


class MappingQuerySet(models.QuerySet):
    def active(self):
        return self.filter(state=State.ACTIVE, shard__state=State.ACTIVE)

    def in_maintenance(self):
        return self.filter(Q(state=State.MAINTENANCE) | Q(shard__state=State.MAINTENANCE))

    def for_target(self, target_value, field=None):
        if not field:
            field = self.model.mapping_field

        return self.get(**{field: target_value})

    def for_shard(self, shard):
        return self.filter(shard_id=shard.id)


class BaseShard(models.Model):
    """
    Base class for Shard models.

    You will need to extend this model to have it live in your own application.
    You often don't need additional fields, so it could just be::

        class Shard(BaseShard):

            class Meta:
                app_label = 'example'

    Mirroring

    You can, if you wish, apply the ``@mirrored_model`` decorator to this model as well.
    Like all mirrored models, you will have to keep them in sync yourself.
    Though this library does provide helper functions to accomplish that.
    Since this model will create a schema when saved, it has logic to only do so on the node is targets.

    """
    alias = models.CharField(max_length=128, db_index=True, unique=True)
    schema_name = models.CharField(max_length=64)  # PostgreSQL default max limit = 63 chars
    node_name = models.CharField(max_length=64)
    state = models.CharField(choices=STATES, max_length=1, default=State.MAINTENANCE)

    class Meta:
        app_label = 'sharding'
        abstract = True
        unique_together = ('schema_name', 'node_name')

    def save(self, using=None, **kwargs):
        self.node_name = self.node_name or settings.SHARDING.get('NEW_SHARD_NODE', None)
        if not self.node_name:
            raise ValueError("No node_name given, or no NEW_SHARD_NODE set in the SHARDING settings.")

        # If this is an update, no need to create a schema
        if self.pk and get_shard_class().objects.filter(pk=self.pk).exists():
            return super().save(using=using, **kwargs)

        from sharding.utils import schema_exists, create_schema_on_node  # Prevent cyclic imports

        # If we have a mirrored sharding mode, we need to work on our target node to create a schema.
        # If this object is made within a 'use_shard' manager we will have 'using' set.
        # Else we need to get our db alias form the manager
        if (not hasattr(self, 'sharding_mode')
            or (getattr(self, 'sharding_mode') == ShardingMode.MIRRORED
                and ShardOptions.from_alias(using or self._base_manager.db).node_name) == self.node_name) \
                and not schema_exists(node_name=self.node_name, schema_name=self.schema_name):
            create_schema_on_node(schema_name=self.schema_name, node_name=self.node_name, migrate=True)

        super().save(using=using, **kwargs)

    @transaction.atomic()
    def delete(self, *args, delete_from_db=False, **kwargs):
        if delete_from_db:
            delete_schema(schema_name=self.schema_name, node_name=self.node_name)

        super().delete(*args, **kwargs)

    def clean(self):
        if self.node_name not in connections:
            raise ValueError("Connection '{}' does not exist. Is it listed in settings.DATABASES?"
                             .format(self.node_name))

    def __str__(self):
        return "{}({}|{})".format(self.alias, self.node_name, self.schema_name)

    def use(self, *args, **kwargs):
        return use_shard(self, *args, **kwargs)
