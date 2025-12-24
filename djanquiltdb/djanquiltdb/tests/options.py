from unittest import mock

from example.models import Organization, OrganizationShards, Shard

from djanquiltdb import State
from djanquiltdb.db import connection
from djanquiltdb.decorators import override_sharding_setting
from djanquiltdb.options import ShardOptions
from djanquiltdb.postgresql_backend.base import PUBLIC_SCHEMA_NAME
from djanquiltdb.tests import ShardingTestCase
from djanquiltdb.utils import StateException, create_template_schema


class ShardOptionsTestCase(ShardingTestCase):
    def setUp(self):
        super().setUp()

        create_template_schema()
        self.shard = Shard.objects.create(
            alias='test', schema_name='test_schema', node_name='default', state=State.ACTIVE
        )

    def test(self):
        """
        Case: Initialize the shard options with only a node name and a schema name
        Expected: Default setting should apply
        """
        shard_options = ShardOptions(node_name='default', schema_name='test_schema')

        self.assertEqual(
            shard_options.options,
            frozenset(
                {
                    ('node_name', 'default'),
                    ('schema_name', 'test_schema'),
                }
            ),
        )
        self.assertEqual(shard_options.node_name, 'default')
        self.assertEqual(shard_options.schema_name, 'test_schema')
        self.assertIsNone(shard_options.shard_id)
        self.assertIsNone(shard_options.mapping_value)
        self.assertFalse(shard_options.use_shard)
        self.assertEqual(shard_options.kwargs, {})
        self.assertTrue(shard_options.lock)

    def test_public_schema(self):
        """
        Case: Initialize the shard options with only a node name and a schema name being the public schema
        Expected: Lock should default to false
        """
        shard_options = ShardOptions(node_name='default', schema_name=PUBLIC_SCHEMA_NAME)
        self.assertFalse(shard_options.lock)

    def test_lock_false(self):
        """
        Case: Initialize the shard options while providing lock=False
        Expected: The shard options should have lock=False
        """
        shard_options = ShardOptions(node_name='default', schema_name='test_schema', lock=False)
        self.assertFalse(shard_options.lock)

    def test_lock_public_schema(self):
        """
        Case: Intialize the shard options with schema name being the public schema and lock=True
        Expected: ValueError raised
        """
        with self.assertRaisesMessage(ValueError, 'You cannot lock the public schema'):
            ShardOptions(node_name='default', schema_name=PUBLIC_SCHEMA_NAME, lock=True)

    def test_hash(self):
        """
        Case: Hash the shard options
        Expected: The hash should be based on the options we provide to the ShardOptions instance
        """
        options = {'node_name': 'default', 'schema_name': 'test_schema'}
        shard_options = ShardOptions(**options)
        self.assertEqual(hash(shard_options), hash(frozenset(options.items())))

    def test_equal(self):
        """
        Case: Initialize the ShardOptions twice with the same options
        Expected: They should be marked as being the same
        """
        self.assertEqual(
            ShardOptions(node_name='default', schema_name='test_schema'),
            ShardOptions(node_name='default', schema_name='test_schema'),
        )

    def test_not_equal(self):
        """
        Case: Initialize the ShardOptions with different options
        Expected: They should be marked as being different
        """
        shard_options = ShardOptions(node_name=self.shard.node_name, schema_name=self.shard.schema_name)

        self.assertNotEqual(
            shard_options,
            ShardOptions(node_name=self.shard.node_name, schema_name=self.shard.schema_name, shard_id=self.shard.id),
        )

        self.assertNotEqual(
            shard_options, ShardOptions(node_name=self.shard.node_name, schema_name=self.shard.schema_name, lock=True)
        )

        self.assertNotEqual(
            shard_options,
            ShardOptions(node_name=self.shard.node_name, schema_name=self.shard.schema_name, use_shard=True),
        )

        self.assertNotEqual(shard_options, self.shard)

    def test_str(self):
        """
        Case: Call the string method of a ShardOptions instance
        Expected: Returns the node name and the schema name separated by a pipe
        """
        options = {'node_name': 'default', 'schema_name': 'test_schema'}
        self.assertEqual(str(ShardOptions(**options)), 'ShardOptions for {node_name}|{schema_name}'.format(**options))

    def test_from_shard(self):
        """
        Case: Get ShardOptions from a shard instance
        Expected: ShardOptions initialized with node_name, schema_name and shard_id set
        """
        shard_options = ShardOptions.from_shard(self.shard)
        self.assertEqual(shard_options.node_name, self.shard.node_name)
        self.assertEqual(shard_options.schema_name, self.shard.schema_name)
        self.assertEqual(shard_options.shard_id, self.shard.id)

    def test_from_shard_in_maintenance(self):
        """
        Case: Get ShardOptions for a shard that is in maintenance
        Expected: StateException raised
        """
        self.shard.state = State.MAINTENANCE
        self.shard.save(update_fields=['state'])

        with self.assertRaisesMessage(StateException, 'Shard {} state is {}'.format(self.shard, self.shard.state)):
            ShardOptions.from_shard(self.shard)

    def test_from_shard_in_maintenance_active_only_schemas_false(self):
        """
        Case: Get ShardOptions for a shard that is in maintenance while providing active_only_schemas=False
        Expected: State ignored, and ShardOptions returned with node_name schema_name and shard_id set. The
                  active_only_schemas=False is saved in kwargs.
        """
        self.shard.state = State.MAINTENANCE
        self.shard.save(update_fields=['state'])

        shard_options = ShardOptions.from_shard(self.shard, active_only_schemas=False)
        self.assertEqual(shard_options.node_name, self.shard.node_name)
        self.assertEqual(shard_options.schema_name, self.shard.schema_name)
        self.assertEqual(shard_options.shard_id, self.shard.id)

        self.assertEqual(shard_options.kwargs, {'active_only_schemas': False})

    @override_sharding_setting('MAPPING_MODEL')
    def test_from_shard_check_active_mapping_values_no_mapping_model(self):
        """
        Case: Get ShardOptions for a shard with check_active_mapping_values=True while not having a mapping model set
        Expected: ValueError raised
        """
        with self.assertRaisesMessage(
            ValueError, "You set 'check_active_mapping_values' to True while you didn't define the mapping model."
        ):
            ShardOptions.from_shard(self.shard, check_active_mapping_values=True)

    def test_from_shard_check_active_mapping_values(self):
        """
        Case: Get ShardOptions for a shard with check_active_mapping_value=True while having a mapping model which state
              is in maintenance.
        Expected: StateException raised
        """
        OrganizationShards.objects.create(shard=self.shard, state=State.MAINTENANCE, organization_id=1)
        with self.assertRaisesMessage(
            StateException, 'Shard {} contains mapping objects that are in maintenance'.format(self.shard)
        ):
            ShardOptions.from_shard(self.shard, check_active_mapping_values=True)

    def test_from_shard_check_active_mapping_values_no_maintenance(self):
        """
        Case: Get ShardOptions for a shard with check_active_mapping_value=True while having a mapping model which state
              is active and having no mapping models which state is inactive for the current shard.
        Expected: No StateException raised
        """
        OrganizationShards.objects.create(shard=self.shard, state=State.ACTIVE, organization_id=1)

        # Inactive mapping model for a different shard
        shard = Shard.objects.create(alias='test2', schema_name='test_schema2', node_name='default', state=State.ACTIVE)
        OrganizationShards.objects.create(shard=shard, state=State.MAINTENANCE, organization_id=2)

        # No StateException raised
        ShardOptions.from_shard(self.shard, check_active_mapping_values=True)

    def test_from_alias_shard_options(self):
        """
        Case: Get the ShardOptions from an alias being a ShardOptions instance
        Expected: Same ShardOptions instance returned
        """
        shard_options = ShardOptions(node_name='default', schema_name='test_schema')
        self.assertIs(ShardOptions.from_alias(shard_options), shard_options)

    @mock.patch.object(ShardOptions, 'from_shard')
    def test_from_alias_shard_instance(self, mock_from_shard):
        """
        Case: Get the ShardOptions from an alias being a Shard model instance
        Expected: ShardOptions.from_shard() called with that shard
        """
        shard_options = mock.MagicMock(autospec=ShardOptions)
        mock_from_shard.return_value = shard_options

        self.assertIs(ShardOptions.from_alias(self.shard), shard_options)

        mock_from_shard.assert_called_once_with(self.shard)

    def test_from_alias_pipe_node_name_schema_name(self):
        """
        Case: Get the ShardOptions from an alias being the node name and schema name separated by a pipe
        Expected: ShardOptions returned with node name and schema name being the ones provided
        """
        shard_options = ShardOptions.from_alias('default|test_schema')
        self.assertEqual(
            shard_options.options,
            frozenset(
                {
                    ('node_name', 'default'),
                    ('schema_name', 'test_schema'),
                }
            ),
        )
        self.assertEqual(shard_options.node_name, 'default')
        self.assertEqual(shard_options.schema_name, 'test_schema')

    def test_from_alias_node_name_only(self):
        """
        Case: Get the ShardOptions from an alias being the node name only
        Expected: ShardOptions returned with node name being the one provided and the schema name being the public
                  schema
        """
        shard_options = ShardOptions.from_alias('default')
        self.assertEqual(
            shard_options.options,
            frozenset(
                {
                    ('node_name', 'default'),
                    ('schema_name', PUBLIC_SCHEMA_NAME),
                }
            ),
        )
        self.assertEqual(shard_options.node_name, 'default')
        self.assertEqual(shard_options.schema_name, PUBLIC_SCHEMA_NAME)

    def test_from_alias_tuple(self):
        """
        Case: Get the ShardOptions from an alias being a tuple of node name and schema name
        Expected: ShardOptions returned with node name and schema name being the ones provided
        """
        shard_options = ShardOptions.from_alias(('default', 'test_schema'))
        self.assertEqual(
            shard_options.options,
            frozenset(
                {
                    ('node_name', 'default'),
                    ('schema_name', 'test_schema'),
                }
            ),
        )
        self.assertEqual(shard_options.node_name, 'default')
        self.assertEqual(shard_options.schema_name, 'test_schema')

    def test_from_alias_invalid(self):
        """
        Case: Get the ShardOptions from an alias that is invalid
        Expected: ValueError raised
        """
        alias = ('default',)
        with self.assertRaisesMessage(ValueError, '{} is an invalid connection alias.'.format(alias)):
            ShardOptions.from_alias(alias)

    def test_lock_keys_none(self):
        """
        Case: Get the lock keys from a ShardOptions instance that has no shard id and no mapping value
        Expected: Empty list returned
        """
        shard_options = ShardOptions(node_name='default', schema_name='test_schema')
        self.assertEqual(shard_options.lock_keys, [])

    def test_lock_keys_shard(self):
        """
        Case: Get the lock keys from a ShardOptions instance that has a shard id
        Expected: List with `shard_<SHARD_ID>` returned
        """
        shard_options = ShardOptions(
            node_name=self.shard.node_name, schema_name=self.shard.schema_name, shard_id=self.shard.id
        )
        self.assertEqual(shard_options.lock_keys, ['shard_{}'.format(self.shard.id)])

    def test_lock_keys_mapping_value(self):
        """
        Case: Get the lock keys from a ShardOptions instance that has a shard id and a mapping value
        Expected: List with `shard_<SHARD_ID>` and `mapping_<MAPPING_VALUE>` returned
        """
        shard_options = ShardOptions(
            node_name=self.shard.node_name, schema_name=self.shard.schema_name, shard_id=self.shard.id, mapping_value=42
        )
        self.assertEqual(shard_options.lock_keys, ['shard_{}'.format(self.shard.id), 'mapping_42'])

    def test_is_public_schema(self):
        """
        Case: Check ShardOptions.is_public_schema() for a public schema and for a non-public schema
        Expected: Returns True when the schema is the public schema and returns False otherwise
        """
        self.assertTrue(ShardOptions(node_name='default', schema_name=PUBLIC_SCHEMA_NAME).is_public_schema())
        self.assertFalse(ShardOptions(node_name='default', schema_name='test_schema').is_public_schema())

    @mock.patch('djanquiltdb.options.use_shard_for')
    def test_use_mapping_value(self, mock_use_shard_for):
        """
        Case: Call ShardOptions's use() method while having a mapping value set
        Expected: Return a use_shard_for instance with the mapping value provided and the kwargs passed
        """
        _use_shard_for = mock.MagicMock()
        mock_use_shard_for.return_value = _use_shard_for
        kwargs = {'lock': False}

        shard_options = ShardOptions(
            node_name=self.shard.node_name,
            schema_name=self.shard.schema_name,
            shard_id=self.shard.id,
            mapping_value=42,
            **kwargs,
        )

        self.assertEqual(shard_options.use(), _use_shard_for)

        mock_use_shard_for.assert_called_once_with(42, **kwargs)

    @mock.patch('djanquiltdb.options.use_shard')
    def test_use_shard_id(self, mock_use_shard):
        """
        Case: Call ShardOptions's use() method while having a shard id set
        Expected: Return a use_shard instance with the shard provided and the kwargs passed
        """
        _use_shard = mock.MagicMock()
        mock_use_shard.return_value = _use_shard
        kwargs = {'lock': False}

        shard_options = ShardOptions(
            node_name=self.shard.node_name, schema_name=self.shard.schema_name, shard_id=self.shard.id, **kwargs
        )

        self.assertEqual(shard_options.use(), _use_shard)

        mock_use_shard.assert_called_once_with(self.shard, **kwargs)

    @mock.patch('djanquiltdb.options.use_shard')
    def test_use(self, mock_use_shard):
        """
        Case: Call ShardOptions's use() method while having a no mapping value and no shard id set
        Expected: Return a use_shard instance with the node name and schema name provided and the kwargs passed
        """
        _use_shard = mock.MagicMock()
        mock_use_shard.return_value = _use_shard
        kwargs = {'lock': False}

        shard_options = ShardOptions(node_name=self.shard.node_name, schema_name=self.shard.schema_name, **kwargs)

        self.assertEqual(shard_options.use(), _use_shard)

        mock_use_shard.assert_called_once_with(
            node_name=self.shard.node_name, schema_name=self.shard.schema_name, **kwargs
        )

    def test_use_mapping_value_integration(self):
        """
        Case: Call ShardOptions's use() method while having a mapping value set
        Expected: Correctly switches the connection to the correct shard
        """
        with self.shard.use():
            organization = Organization.objects.create(name='Foo')
            OrganizationShards.objects.create(shard=self.shard, organization_id=organization.id, slug='foo')

        shard_options = ShardOptions(
            node_name=self.shard.node_name,
            schema_name=self.shard.schema_name,
            shard_id=self.shard.id,
            mapping_value=organization.id,
        )

        with shard_options.use():
            self.assertTrue(Organization.objects.filter(name='Foo').exists())
            self.assertEqual(connection.alias, '{}|{}'.format(self.shard.node_name, self.shard.schema_name))
            self.assertEqual(connection.shard_options.mapping_value, organization.id)

    def test_use_shard_id_integration(self):
        """
        Case: Call ShardOptions's use() method while having a shard id value set
        Expected: Correctly switches the connection to the correct shard
        """
        with self.shard.use():
            Organization.objects.create(name='Foo')

        shard_options = ShardOptions(
            node_name=self.shard.node_name, schema_name=self.shard.schema_name, shard_id=self.shard.id
        )

        with shard_options.use():
            self.assertTrue(Organization.objects.filter(name='Foo').exists())
            self.assertEqual(connection.alias, '{}|{}'.format(self.shard.node_name, self.shard.schema_name))
            self.assertEqual(connection.shard_options.shard_id, self.shard.id)

    def test_use_integration(self):
        """
        Case: Call ShardOptions's use() method while having a no mapping value and no shard id set
        Expected: Correctly switches the connection to the correct shard
        """
        with self.shard.use():
            Organization.objects.create(name='Foo')

        shard_options = ShardOptions(node_name=self.shard.node_name, schema_name=self.shard.schema_name)

        with shard_options.use():
            self.assertEqual(connection.alias, '{}|{}'.format(self.shard.node_name, self.shard.schema_name))
            self.assertTrue(Organization.objects.filter(name='Foo').exists())
