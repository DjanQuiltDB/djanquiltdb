from djanquiltdb import State
from djanquiltdb.testing import ShardingTransactionTestCase
from djanquiltdb.utils import create_template_schema

from example.models import Shard, TenantShard


class PluginTestCase(ShardingTransactionTestCase):
    """
    Base class for the tests that create shards, and the schemas of those shards, in the database. ``available_apps``
    restricts the migration graph, so it lists every app of the project.

    setUp creates the shard first on the primary node, the shard remote on the other node, and the shard maintained on
    the primary node, in maintenance. Tenant 1 is in first and tenant 2 is in remote. The Shard table is mirrored, so
    setUp also saves each shard on the other node, as a project does to keep a mirrored table the same on every node.
    """

    available_apps = [
        'django.contrib.auth',
        'django.contrib.contenttypes',
        'djanquiltdb',
        'example',
    ]

    def setUp(self):
        super().setUp()
        create_template_schema('default')
        create_template_schema('other')
        self.first = Shard.objects.create(
            alias='first', node_name='default', schema_name='first_schema', state=State.ACTIVE
        )
        self.remote = Shard.objects.create(
            alias='remote', node_name='other', schema_name='remote_schema', state=State.ACTIVE
        )
        self.maintained = Shard.objects.create(
            alias='maintained', node_name='default', schema_name='maintained_schema', state=State.MAINTENANCE
        )
        # The flush after the test enters every shard, so maintained has to be active by then.
        self.addCleanup(self.make_shard_active, self.maintained)

        for shard in (self.first, self.remote, self.maintained):
            shard.save(using='other')

        TenantShard.objects.create(shard=self.first, tenant_id=1)
        TenantShard.objects.create(shard=self.remote, tenant_id=2)

    @staticmethod
    def make_shard_active(shard):
        shard.state = State.ACTIVE
        shard.save(update_fields=['state'])
