from unittest import mock

from django.db import connections, models
from django.test.utils import CaptureQueriesContext, isolate_apps

from djanquiltdb import STATES, State
from djanquiltdb.db import connection
from djanquiltdb.decorators import override_sharding_setting
from djanquiltdb.options import ShardOptions
from djanquiltdb.postgresql_backend.base import PUBLIC_SCHEMA_NAME
from djanquiltdb.router import get_active_connection
from djanquiltdb.utils import StateException, create_template_schema, use_shard, use_shard_for
from djanquiltdb_tests import ShardingTestCase, ShardingTransactionTestCase
from example.models import MirroredUser, Organization, OrganizationShard, Shard


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
        OrganizationShard.objects.create(shard=self.shard, state=State.MAINTENANCE, organization_id=1)
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
        OrganizationShard.objects.create(shard=self.shard, state=State.ACTIVE, organization_id=1)

        # Inactive mapping model for a different shard
        shard = Shard.objects.create(alias='test2', schema_name='test_schema2', node_name='default', state=State.ACTIVE)
        OrganizationShard.objects.create(shard=shard, state=State.MAINTENANCE, organization_id=2)

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

    def test_from_alias_tells_whether_it_names_only_the_node(self):
        """
        Case: Get the ShardOptions from a bare node name, from a node name and schema name separated by a pipe, from a
              tuple of a node name and the public schema, and from an existing ShardOptions for the public schema.
        Expected: names_only_the_node is True for the bare node name only, and the options from the bare node name are
                  equal to the ones that name the public schema.
        """
        self.assertTrue(ShardOptions.from_alias('default').names_only_the_node)
        self.assertFalse(ShardOptions.from_alias('default|public').names_only_the_node)
        self.assertFalse(ShardOptions.from_alias(('default', 'public')).names_only_the_node)
        self.assertFalse(
            ShardOptions.from_alias(ShardOptions(node_name='default', schema_name='public')).names_only_the_node
        )
        self.assertEqual(ShardOptions.from_alias('default'), ShardOptions.from_alias('default|public'))

    def test_from_alias_invalid(self):
        """
        Case: Get the ShardOptions from an alias that is invalid
        Expected: ValueError raised
        """
        alias = ('default',)
        with self.assertRaisesMessage(ValueError, '{} is an invalid connection alias.'.format(alias)):
            ShardOptions.from_alias(alias)

    def test_from_alias_does_not_look_up_the_shard_class_for_a_string_or_a_tuple(self):
        """
        Case: Get the ShardOptions from a bare node name, from a node name and schema name separated by a pipe, and from
              a tuple of a node name and schema name.
        Expected: The shard class is not looked up, because none of these aliases can be a shard.
        """
        with mock.patch('djanquiltdb.options.get_shard_class', return_value=Shard) as mock_get_shard_class:
            ShardOptions.from_alias('default')
            ShardOptions.from_alias('default|test_schema')
            ShardOptions.from_alias(('default', 'test_schema'))

        mock_get_shard_class.assert_not_called()

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

    def test_lock_keys_mapping_value_zero(self):
        """
        Case: Get the lock keys from a ShardOptions instance that has a shard id and the mapping value 0
        Expected: List with `shard_<SHARD_ID>` and `mapping_0` returned, because 0 is a mapping value like any other
        """
        shard_options = ShardOptions(
            node_name=self.shard.node_name, schema_name=self.shard.schema_name, shard_id=self.shard.id, mapping_value=0
        )
        self.assertEqual(shard_options.lock_keys, ['shard_{}'.format(self.shard.id), 'mapping_0'])

    def test_lock_keys_mapping_value_model_instance(self):
        """
        Case: Get the lock keys from a ShardOptions instance whose mapping value is a model instance, which
              use_shard_for() accepts when the mapping field is a foreign key.
        Expected: List with `shard_<SHARD_ID>` and `mapping_<PRIMARY_KEY>` returned. This is the lock that
                  move_data_to_shard takes for the mapping value.
        """
        shard_options = ShardOptions(
            node_name=self.shard.node_name,
            schema_name=self.shard.schema_name,
            shard_id=self.shard.id,
            mapping_value=Organization(pk=42),
        )
        self.assertEqual(shard_options.lock_keys, ['shard_{}'.format(self.shard.id), 'mapping_42'])

    @isolate_apps('djanquiltdb')
    def test_lock_keys_mapping_value_model_instance_for_a_foreign_key_to_another_field(self):
        """
        Case: Get the lock keys from a ShardOptions instance whose mapping value is a MirroredUser, while the mapping
              field of the mapping model is a foreign key to the email address of MirroredUser.
        Expected: List with `shard_<SHARD_ID>` and `mapping_<EMAIL_ADDRESS>` returned. The email address is the value
                  that the mapping field stores, so use_shard_for() locks the same key for the user and for the email
                  address of the user.
        """

        class MirroredUserShard(models.Model):
            shard = models.ForeignKey(Shard, on_delete=models.CASCADE, related_name='+')
            user = models.ForeignKey(MirroredUser, to_field='email', on_delete=models.CASCADE, related_name='+')
            state = models.CharField(choices=STATES, max_length=1, default=State.ACTIVE)

            mapping_field = 'user'

            class Meta:
                app_label = 'djanquiltdb'

        with mock.patch('djanquiltdb.utils.get_mapping_class', return_value=MirroredUserShard):
            shard_options = ShardOptions(
                node_name=self.shard.node_name,
                schema_name=self.shard.schema_name,
                shard_id=self.shard.id,
                mapping_value=MirroredUser(pk=42, email='luke@layton.l5'),
            )
            self.assertEqual(shard_options.lock_keys, ['shard_{}'.format(self.shard.id), 'mapping_luke@layton.l5'])

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

    @mock.patch('djanquiltdb.options.use_shard_for')
    def test_use_mapping_value_zero(self, mock_use_shard_for):
        """
        Case: Call ShardOptions's use() method while having the mapping value 0 set
        Expected: Return a use_shard_for instance with the mapping value 0, because 0 is a mapping value like any other
        """
        _use_shard_for = mock.MagicMock()
        mock_use_shard_for.return_value = _use_shard_for

        shard_options = ShardOptions(
            node_name=self.shard.node_name, schema_name=self.shard.schema_name, shard_id=self.shard.id, mapping_value=0
        )

        self.assertEqual(shard_options.use(), _use_shard_for)

        mock_use_shard_for.assert_called_once_with(0)

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
            OrganizationShard.objects.create(shard=self.shard, organization_id=organization.id, slug='foo')

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

    def test_use_shard_id_without_a_node_integration(self):
        """
        Case: Call ShardOptions's use() method on options that have a shard id but no node name and no schema name, like
              the options of a target that identifies a shard by its id only.
        Expected: Looks up the shard and switches the connection to it
        """
        shard_options = ShardOptions(node_name=None, schema_name=None, shard_id=self.shard.id)

        with shard_options.use():
            self.assertEqual(connection.alias, '{}|{}'.format(self.shard.node_name, self.shard.schema_name))

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


class ShardOptionsUseInsideEnteredUseShardTestCase(ShardingTransactionTestCase):
    """
    Each test has two active shards on the default node, ``self.shard`` with the mapping value 1 and
    ``self.other_shard``, and runs outside a transaction.
    """

    def setUp(self):
        super().setUp()

        create_template_schema()
        self.shard = Shard.objects.create(
            alias='test', schema_name='test_schema', node_name='default', state=State.ACTIVE
        )
        self.other_shard = Shard.objects.create(
            alias='other', schema_name='test_other_schema', node_name='default', state=State.ACTIVE
        )
        OrganizationShard.objects.create(shard=self.shard, organization_id=1, slug='test')

    def put_shard_into_maintenance(self):
        Shard.objects.filter(id=self.shard.id).update(state=State.MAINTENANCE)
        # The flush after the test fails on a shard in maintenance, so the cleanup makes the shard active again.
        self.addCleanup(Shard.objects.filter(id=self.shard.id).update, state=State.ACTIVE)

    def test_use_runs_no_statements(self):
        """
        Case: Call use() on the options of a mapping value while a use_shard_for of that mapping value is entered and
              another shard is active.
        Expected: The options become the active connection without running a statement, because the entered
                  use_shard_for holds the locks of the shard and of the mapping value.
        """
        with use_shard_for(1):
            shard_options = get_active_connection()

            with use_shard(self.other_shard):
                with CaptureQueriesContext(connections['default']) as captured:
                    with shard_options.use():
                        self.assertEqual(get_active_connection(), shard_options)

        self.assertEqual(captured.captured_queries, [])

    def test_use_does_not_check_the_state_again(self):
        """
        Case: Put a shard into maintenance while a use_shard of that shard is entered, enter another shard, and call
              use() on the options of the first shard.
        Expected: No StateException, because the outer use_shard checks the state when it enters the shard, and its
                  locks make the move commands wait until it is left.
        """
        with use_shard(self.shard):
            shard_options = get_active_connection()
            self.put_shard_into_maintenance()

            with use_shard(self.other_shard):
                with shard_options.use():
                    self.assertEqual(get_active_connection(), shard_options)

    def test_use_checks_the_state_without_an_entered_use_shard(self):
        """
        Case: Call use() on the options of a shard in maintenance, while no use_shard of that shard is entered.
        Expected: A StateException, because use() looks up the shard and checks its state.
        """
        shard_options = ShardOptions(
            node_name=self.shard.node_name, schema_name=self.shard.schema_name, shard_id=self.shard.id
        )
        self.put_shard_into_maintenance()

        with self.assertRaises(StateException):
            with shard_options.use():
                pass

    def test_use_entered_after_the_entered_use_shard_is_left(self):
        """
        Case: Call use() on the options of a shard while a use_shard of that shard is entered, leave that use_shard, and
              then enter what use() returned.
        Expected: The shard is entered and its lock is counted, because what use() returned takes the lock of the shard
                  and reads the shard again, as the use_shard that is left no longer holds the lock.
        """
        with use_shard(self.shard):
            shard_options = get_active_connection()
            context = shard_options.use()

        with context:
            self.assertEqual(get_active_connection(), shard_options)
            self.assertEqual(
                connections['default'].session_locks_of_entered_use_shards['shard_{}'.format(self.shard.id)], 1
            )

    def test_use_entered_after_the_entered_use_shard_is_left_for_a_moved_shard(self):
        """
        Case: Call use() on the options of a shard while a use_shard of that shard is entered, leave that use_shard,
              move the shard to another schema, and then enter what use() returned.
        Expected: A StateException, because what use() returned reads the shard again once it holds the lock, and the
                  shard is no longer in the schema of the options.
        """
        with use_shard(self.shard):
            context = get_active_connection().use()

        Shard.objects.filter(id=self.shard.id).update(schema_name='test_other_schema')
        self.addCleanup(Shard.objects.filter(id=self.shard.id).update, schema_name='test_schema')

        with self.assertRaisesMessage(StateException, 'which differs from the shard'):
            with context:
                pass
