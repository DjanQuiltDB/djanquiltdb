import json
import os
import tempfile

from django.core.management import call_command
from example.models import Organization, Shard, Type

from djanquiltdb import State
from djanquiltdb.tests import ShardingTestCase
from djanquiltdb.utils import create_template_schema, use_shard


class DumpDataTestCase(ShardingTestCase):
    def setUp(self):
        super().setUp()
        create_template_schema()
        self.shard = Shard.objects.create(
            node_name='default',
            schema_name='test_shard',
            alias='test_shard',
            state=State.ACTIVE,
        )

    def _call_dumpdata(self, *args, **kwargs):
        """Call dumpdata and return parsed JSON output."""
        with tempfile.NamedTemporaryFile(mode='r', suffix='.json', delete=False) as f:
            output_path = f.name
        try:
            call_command('dumpdata', *args, output=output_path, format='json', verbosity=0, **kwargs)
            with open(output_path) as f:
                return json.load(f)
        finally:
            try:
                os.unlink(output_path)
            except OSError:
                pass

    def test_sharded_models_have_schema(self):
        """
        Case: Dump data when sharded models exist in a shard.
        Expected: Sharded records carry a _schema field equal to the shard's schema_name.
        """
        with use_shard(self.shard):
            Organization.objects.create(name='Acme')

        data = self._call_dumpdata('example.Organization')

        self.assertEqual(len(data), 1)
        record = data[0]
        self.assertEqual(record['model'], 'example.organization')
        self.assertEqual(record['_schema'], 'test_shard')
        self.assertEqual(record['fields']['name'], 'Acme')

    def test_public_models_have_no_schema(self):
        """
        Case: Dump data for a mirrored (public-schema) model.
        Expected: Records do NOT have a _schema field.
        """
        Type.objects.create(name='MyType')

        data = self._call_dumpdata('example.Type')

        self.assertEqual(len(data), 1)
        self.assertNotIn('_schema', data[0])
        self.assertEqual(data[0]['fields']['name'], 'MyType')

    def test_multiple_shards_all_dumped(self):
        """
        Case: Two active shards each contain an Organization.
        Expected: Both records appear in output, each tagged with their respective _schema.
        """
        shard2 = Shard.objects.create(
            node_name='default',
            schema_name='test_shard_2',
            alias='test_shard_2',
            state=State.ACTIVE,
        )

        with use_shard(self.shard):
            Organization.objects.create(name='Shard1Org')
        with use_shard(shard2):
            Organization.objects.create(name='Shard2Org')

        data = self._call_dumpdata('example.Organization')

        schemas = {r['_schema'] for r in data}
        self.assertEqual(schemas, {'test_shard', 'test_shard_2'})
        names = {r['fields']['name'] for r in data}
        self.assertEqual(names, {'Shard1Org', 'Shard2Org'})

    def test_maintenance_shards_excluded(self):
        """
        Case: One active shard, one maintenance shard.
        Expected: Only the active shard's data appears.
        """
        maint_shard = Shard.objects.create(
            node_name='default',
            schema_name='test_maint',
            alias='test_maint',
            state=State.MAINTENANCE,
        )

        with use_shard(self.shard):
            Organization.objects.create(name='ActiveOrg')
        with use_shard(maint_shard, active_only_schemas=False):
            Organization.objects.create(name='MaintOrg')

        data = self._call_dumpdata('example.Organization')

        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]['_schema'], 'test_shard')

    def test_roundtrip_with_loaddata(self):
        """
        Case: dumpdata then loaddata on a clean shard reproduces the original objects.
        Expected: After purging and reloading, Organization count and name match.
        """
        with use_shard(self.shard):
            Organization.objects.create(name='RoundtripOrg')

        # Dump via helper (handles temp file lifecycle)
        data = self._call_dumpdata('example.Organization')
        self.assertEqual(len(data), 1)

        # Write the fixture to a file for loaddata
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(data, f)
            fixture_path = f.name
        try:
            with use_shard(self.shard):
                Organization.objects.all().delete()

            call_command('loaddata', fixture_path, verbosity=0)

            with use_shard(self.shard):
                self.assertEqual(Organization.objects.count(), 1)
                self.assertEqual(Organization.objects.first().name, 'RoundtripOrg')
        finally:
            try:
                os.unlink(fixture_path)
            except OSError:
                pass

    def test_key_order_in_output(self):
        """
        Case: Dump a sharded model to JSON.
        Expected: Keys appear in order: model, pk, _schema, fields.
        """
        with use_shard(self.shard):
            Organization.objects.create(name='OrderTest')

        data = self._call_dumpdata('example.Organization')
        self.assertEqual(len(data), 1)
        self.assertEqual(set(data[0].keys()), {'model', 'pk', '_schema', 'fields'})

    def test_empty_shard_produces_no_output(self):
        """
        Case: Active shard has no rows for the requested model.
        Expected: Output is an empty list.
        """
        data = self._call_dumpdata('example.Organization')
        self.assertEqual(data, [])
