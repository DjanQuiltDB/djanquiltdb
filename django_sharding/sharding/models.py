from django.apps import apps
from django.conf import settings
from django.db import models, connections
from django.db.models import Q
from django.db.models.signals import post_init
from django.dispatch import receiver

from sharding import State, STATES, ShardingMode
from sharding.exceptions import ShardingError
from sharding.utils import get_shard_class, use_shard_for, use_shard, get_model_sharding_mode


class MappingQuerySet(models.QuerySet):
    def active(self):
        return self.filter(state=State.ACTIVE, shard__state=State.ACTIVE)

    def in_maintenance(self):
        return self.filter(Q(state=State.MAINTENANCE) | Q(shard__state=State.MAINTENANCE))

    def for_target(self, target_value):
        return self.get(**{self.model.mapping_field: target_value})

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
            raise ValueError("No node_name given, or no NEW_SHARD_NODE set in the SHARING settings.")

        # If this is an update, no need to create a schema
        if self.pk and get_shard_class().objects.filter(pk=self.pk).exists():
            return super().save(**kwargs)

        # If we have a Mirrored sharding mode, we need to work on our target node to create a schema.
        # If this object is made within a 'use_shard' manager we will have 'using' set.
        # Else we need to get our db alias form the manager
        if not hasattr(self, 'sharding_mode') \
                or (getattr(self, 'sharding_mode') == ShardingMode.MIRRORED
                    and (using or self._base_manager.db) == self.node_name):
            from sharding.utils import create_schema_on_node  # import it here, to prevent circle dependencies
            create_schema_on_node(schema_name=self.schema_name, node_name=self.node_name, migrate=True)

        super().save(**kwargs)

    def clean(self):
        if self.node_name not in connections:
            raise ValueError("Connection '{}' does not exist. Is it listed in settings.DATABASES?"
                             .format(self.node_name))

    def __str__(self):
        return "Shard {}({}|{})".format(self.alias, self.node_name, self.schema_name)


class InstanceShardOptions:
    def __init__(self, schema_name, node_name, id, mapping_value, active_only_schemas):
        self.schema_name = schema_name
        self.node_name = node_name
        self.id = id
        self.mapping_value = mapping_value
        self.active_only_schemas = active_only_schemas

    def get(self):
        """
        Lazy method that returns the shard the instance was retrieved from
        """
        if not self.id:
            raise ShardingError('Shard ID is not known for this instance')

        return get_shard_class().objects.get(id=self.id)

    def use(self):
        """
        Returns the context manager to enter a shard with the same parameters the instance was fetched with
        """
        if self.mapping_value:
            # It turns out we fetched the model instance with use_shard_for, so we know the mapping value.
            # So let's use that value to target the shard in the model method. use_shard_for will do a DB query,
            # which allows us to see if the mapping object still has state active.
            use_shard_func = use_shard_for
            use_shard_kwargs = {'target_value': self.mapping_value}
        else:
            # If we didn't use the mapping value to fetch the model instance, then we did it with use_shard.
            # We could've done it with selecting the shard or by providing the node name and schema name.
            # If we did it with providing the shard, then use that now to target the shard in the model methods.
            # We do a new DB request to the shard to see if it still has state active.
            use_shard_func = use_shard
            use_shard_kwargs = {'shard': self.get()} if self.id else {'node_name': self.node_name,
                                                                      'schema_name': self.schema_name}

            # If the model instance has been fetched with active_only_schemas set to True, then we pass that to
            # the model methods as well. This is needed for commands like move_data_to_shard, that do model
            # methods like delete() when the shard is in maintenance.
            use_shard_kwargs['active_only_schemas'] = self.active_only_schemas

        return use_shard_func(**use_shard_kwargs)


@receiver(post_init)
def store_initial_shard(sender, instance, **kwargs):
    """
    Stores information about the shard we retrieved the model instance with
    """

    # Since we are going to check whether the sender is a sharded model, we need to take the base model when having a
    # deferred model.
    model = sender.__base__ if sender._deferred else sender

    # Check if the sender is a sharded model
    if model in apps.get_models() and get_model_sharding_mode(model) == ShardingMode.SHARDED:
        from django.db import connection

        instance._shard = InstanceShardOptions(
            schema_name=connection.get_schema(),
            node_name=connection.alias,
            id=connection._shard_id,
            mapping_value=connection._mapping_value,
            active_only_schemas=connection._active_only_schemas
        )
