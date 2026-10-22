from django.conf import settings
from django.contrib.sessions.base_session import AbstractBaseSession
from django.db import connections, models, router, transaction
from django.db.models import Q

from djanquiltdb import STATES, State
from djanquiltdb.plugins import install_plugin_importer, served_plugin_module
from djanquiltdb.utils import delete_schema, get_shard_class, use_shard

# A plugin's models module is served as the submodule djanquiltdb.models.<plugin>, and the import system only asks
# PluginImporter for a submodule when the parent module has a __path__. The list is empty, because no submodule is found
# by searching it. Setting __path__ does not change how Django imports this module as the models module of the app.
__path__ = []

install_plugin_importer()


class MappingQuerySet(models.QuerySet):
    """
    Manager for a model decorated with ``@shard_mapping_model``, which maps a value of your own onto a shard.

    Assigning it as the model's manager is what lets :func:`djanquiltdb.utils.use_shard_for` look a shard up from
    that value.
    """

    def active(self):
        """Only the mappings whose own state and whose shard's state are both ``ACTIVE``."""
        return self.filter(state=State.ACTIVE, shard__state=State.ACTIVE)

    def in_maintenance(self):
        """Only the mappings that are in maintenance themselves, or whose shard is."""
        return self.filter(Q(state=State.MAINTENANCE) | Q(shard__state=State.MAINTENANCE))

    def for_target(self, target_value, field=None):
        """
        The single mapping for ``target_value``, looked up on the model's ``mapping_field`` unless ``field`` names
        another. Raises the model's ``DoesNotExist`` when there is none.
        """
        if not field:
            field = self.model.mapping_field

        return self.get(**{field: target_value})

    def for_shard(self, shard):
        """Every mapping pointing at ``shard``."""
        return self.filter(shard_id=shard.id)


class BaseShard(models.Model):
    """
    Base class for Shard models.

    You will need to extend this model to have it live in your own application.
    You often don't need additional fields, so it could just be::

        @mirrored_model()
        class Shard(BaseShard):
            class Meta:
                app_label = 'example'

    .. rubric:: Mirroring

    Decorate this model with ``@mirrored_model()``, so that every node has the shard registry, which DjanQuiltDB reads
    on the node that is active. As for every mirrored model, keep the copies the same on every node.
    Saving a shard creates its schema on the node of the shard only.
    """

    alias = models.CharField(max_length=128, db_index=True, unique=True)
    schema_name = models.CharField(max_length=64)  # PostgreSQL default max limit = 63 chars
    node_name = models.CharField(max_length=64)
    state = models.CharField(choices=STATES, max_length=1, default=State.MAINTENANCE)

    class Meta:
        app_label = 'djanquiltdb'
        abstract = True
        unique_together = ('schema_name', 'node_name')

    def save(self, using=None, **kwargs):
        """
        Save the shard, creating and migrating its schema first when that schema does not exist yet.

        The node defaults to the ``NEW_SHARD_NODE`` setting, and saving the same shard again on another node (the
        save-on-every-node style of replication) does not re-create a schema that is already there.
        """
        self.node_name = self.node_name or settings.QUILT_DB.get('NEW_SHARD_NODE', None)
        if not self.node_name:
            raise ValueError('No node_name given, or no NEW_SHARD_NODE set in the QUILT_DB settings.')

        # If this is an update, no need to create a schema. The shard is looked up in the registry that the save writes
        # to.
        if self.pk:
            shard_class = get_shard_class()
            registry = using or router.db_for_write(shard_class, instance=self)
            if shard_class.objects.using(registry).filter(pk=self.pk).exists():
                return super().save(using=using, **kwargs)

        from djanquiltdb.utils import create_schema_on_node, schema_exists  # Prevent cyclic imports

        # Only create the schema is if does not exist yet. This prevents re-creation if the shard object is saved
        # multiple times for different nodes. (The save-on-all-nodes style of data replication across nodes)
        if not schema_exists(node_name=self.node_name, schema_name=self.schema_name):
            create_schema_on_node(schema_name=self.schema_name, node_name=self.node_name, migrate=True)

        super().save(using=using, **kwargs)

    @transaction.atomic()
    def delete(self, *args, delete_from_db=False, **kwargs):
        """
        Delete the shard record. Its schema, and so the data in it, survives unless ``delete_from_db`` says otherwise.
        """
        if delete_from_db:
            delete_schema(schema_name=self.schema_name, node_name=self.node_name)

        super().delete(*args, **kwargs)

    def clean(self):
        """Reject a ``node_name`` that is not one of the connections in ``settings.DATABASES``."""
        if self.node_name not in connections:
            raise ValueError(
                "Connection '{}' does not exist. Is it listed in settings.DATABASES?".format(self.node_name)
            )

    def __str__(self):
        return '{}({}|{})'.format(self.alias, self.node_name, self.schema_name)

    def use(self, *args, **kwargs):
        """Shorthand for :func:`djanquiltdb.utils.use_shard` on this shard."""
        return use_shard(self, *args, **kwargs)


class BaseQuiltSession(AbstractBaseSession):
    """
    Base class for QuiltSession models.

    You will need to extend this model to have it live in your own application.
    You often don't need additional fields, so it could just be::

        @sharded_model()
        class QuiltSession(BaseQuiltSession):
            class Meta:
                app_label = 'example'
    """

    session_key = models.CharField(max_length=255, primary_key=True)

    @classmethod
    def get_session_store_class(cls):
        from djanquiltdb.sessions import SessionStore

        return SessionStore

    class Meta:
        abstract = True


def __getattr__(name):
    """
    Serves the models module of a plugin under the plugin's registered name, for plain attribute access.
    :class:`~djanquiltdb.plugins.PluginImporter` handles import statements.
    """
    module = served_plugin_module(__name__, name)

    if module is None:
        raise AttributeError(f'module {__name__!r} has no attribute {name!r}')

    return module
