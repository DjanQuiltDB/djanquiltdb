from django.core.management import call_command
from django.db import connections

from example.models import Shard, Type, Organization
from sharding import State
from sharding.postgresql_backend.base import PUBLIC_SCHEMA_NAME
from sharding.tests import ShardingTransactionTestCase
from sharding.utils import create_template_schema, use_shard


class FlushTestCase(ShardingTransactionTestCase):
    def setUp(self):
        create_template_schema()
        create_template_schema('other')

        self.shard1 = Shard.objects.create(alias='sinaloa', schema_name='el_chapo', node_name='default',
                                           state=State.ACTIVE)

        self.shard2 = Shard.objects.create(alias='medellin', schema_name='pablo_escobar', node_name='other',
                                           state=State.ACTIVE)

        with use_shard(node_name='default', schema_name=PUBLIC_SCHEMA_NAME):
            Type.objects.create(name='narco')

        with use_shard(node_name='other', schema_name=PUBLIC_SCHEMA_NAME):
            Type.objects.create(name='narco')

        with use_shard(self.shard1):
            Organization.objects.create(name='Federation')

        with use_shard(self.shard2):
            Organization.objects.create(name='Isodor')

    def test(self):
        """
        Case: Call the flush command without extra options
        Expected: All schemas to be flushed
        """
        call_command('flush', verbosity=0, interactive=False)

        with use_shard(node_name='default'):
            self.assertFalse(Type.objects.exists())

        with use_shard(node_name='other'):
            self.assertFalse(Type.objects.exists())

        with use_shard(self.shard1):
            self.assertFalse(Organization.objects.exists())

        with use_shard(self.shard2):
            self.assertFalse(Organization.objects.exists())

    def test_single_database(self):
        """
        Case: Call the flush command with a single database specified
        Expected: All schemas on that database flushed, other schemas not touched
        """
        call_command('flush', verbosity=0, interactive=False, database='default')

        with use_shard(node_name='default', schema_name=PUBLIC_SCHEMA_NAME):
            self.assertFalse(Type.objects.exists())

        with use_shard(node_name='other', schema_name=PUBLIC_SCHEMA_NAME):
            self.assertTrue(Type.objects.exists())

        with use_shard(self.shard1):
            self.assertFalse(Organization.objects.exists())

        with use_shard(self.shard2):
            self.assertTrue(Organization.objects.exists())

    def test_single_schema(self):
        """
        Case: Call the flush command with a single database and single schema specified
        Expected: Only that schema to be flushed, other schemas not touched
        """
        call_command('flush', verbosity=0, interactive=False, database='default', schema_name='el_chapo')

        with use_shard(node_name='default', schema_name=PUBLIC_SCHEMA_NAME):
            self.assertTrue(Type.objects.exists())

        with use_shard(node_name='other', schema_name=PUBLIC_SCHEMA_NAME):
            self.assertTrue(Type.objects.exists())

        with use_shard(self.shard1):
            self.assertFalse(Organization.objects.exists())

        with use_shard(self.shard2):
            self.assertTrue(Organization.objects.exists())
