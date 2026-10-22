import json
import uuid
from unittest import mock

from django.db import models
from django.test import SimpleTestCase, override_settings
from django.test.utils import isolate_apps
from djanquiltdb import State
from djanquiltdb.decorators import override_sharding_setting
from djanquiltdb.options import ShardOptions
from djanquiltdb.router import get_active_connection
from djanquiltdb.utils import StateException, get_mapping_class, use_shard, use_shard_for

from djanquiltdb_plugin_celery.targets import (
    TARGET_HEADER_NAME,
    TaskShardError,
    accepted_shard_kwargs,
    describe_missing_node,
    describe_missing_shard,
    node_target_from_task_kwargs,
    shard_target_from_active_connection,
    shard_target_from_task_kwargs,
    use_target,
)
from example.models import Shard, Widget
from plugin_tests.base import PluginTestCase


def define_a_mapping_model_with_a_foreign_key_to_an_email_address():
    """
    Defines a Customer model and a CustomerShard mapping model whose mapping field is a foreign key to the email address
    of a Customer, and returns both. Call it inside isolate_apps('example'), so that the models stay out of the app
    registry.
    """

    class Customer(models.Model):
        email = models.EmailField(unique=True)

        class Meta:
            app_label = 'example'

    class CustomerShard(models.Model):
        shard = models.ForeignKey(Shard, on_delete=models.CASCADE, related_name='+')
        customer = models.ForeignKey(Customer, to_field='email', on_delete=models.CASCADE, related_name='+')

        mapping_field = 'customer'

        class Meta:
            app_label = 'example'

    return Customer, CustomerShard


class ShardTargetFromTaskKwargsTestCase(SimpleTestCase):
    def test_a_shard_id_names_a_shard_by_its_primary_key(self):
        """
        Case: The keyword arguments of a task have a shard_id.
        Expected: A target that identifies the shard by its primary key.
        """
        self.assertEqual(shard_target_from_task_kwargs({'shard_id': 3, 'name': 'x'}), {'shard_id': 3})

    def test_the_mapping_field_names_a_shard_by_its_mapping_value(self):
        """
        Case: The keyword arguments of a task have a tenant_id, the mapping field of the project's mapping model.
        Expected: A target that identifies the shard by its mapping value.
        """
        self.assertEqual(shard_target_from_task_kwargs({'tenant_id': 7}), {'mapping_value': 7})

    def test_a_node_name_does_not_name_a_shard(self):
        """
        Case: The keyword arguments of a task have a node_name and no shard.
        Expected: None, because a node_name identifies a node, and not a shard.
        """
        self.assertIsNone(shard_target_from_task_kwargs({'node_name': 'other'}))

    def test_keyword_arguments_without_a_shard_give_none(self):
        """
        Case: The keyword arguments of a task have none of the keyword arguments that identify a shard.
        Expected: None.
        """
        self.assertIsNone(shard_target_from_task_kwargs({'name': 'x'}))

    def test_a_keyword_argument_that_is_none_names_no_shard(self):
        """
        Case: The keyword arguments of a task have tenant_id=None, as a task with an optional tenant has.
        Expected: None, and not a target with the mapping value 'None'.
        """
        self.assertIsNone(shard_target_from_task_kwargs({'tenant_id': None}))

    def test_two_shards_in_the_keyword_arguments_are_an_error(self):
        """
        Case: The keyword arguments of a task have both a shard_id and a tenant_id.
        Expected: TaskShardError whose message includes both keyword arguments, because they could identify two
                  different shards.
        """
        with self.assertRaisesMessage(TaskShardError, 'shard_id, tenant_id'):
            shard_target_from_task_kwargs({'shard_id': 3, 'tenant_id': 7})

    @override_settings(QUILT_CELERY={'MAPPING_KWARG': 'customer_id'})
    def test_the_mapping_kwarg_setting_replaces_the_mapping_field(self):
        """
        Case: QUILT_CELERY['MAPPING_KWARG'] is customer_id.
        Expected: A customer_id identifies a shard by its mapping value, and a tenant_id does not identify a shard.
        """
        self.assertEqual(shard_target_from_task_kwargs({'customer_id': 7}), {'mapping_value': 7})
        self.assertIsNone(shard_target_from_task_kwargs({'tenant_id': 7}))

    @override_sharding_setting('MAPPING_MODEL')
    def test_without_a_mapping_model_only_a_shard_id_names_a_shard(self):
        """
        Case: The project has no MAPPING_MODEL.
        Expected: A tenant_id does not identify a shard, and shard_id is the only accepted keyword argument.
        """
        self.assertIsNone(shard_target_from_task_kwargs({'tenant_id': 7}))
        self.assertEqual(accepted_shard_kwargs(), ['shard_id'])

    def test_the_accepted_keyword_arguments_include_the_mapping_field(self):
        """
        Case: accepted_shard_kwargs() is called in a project with a mapping model.
        Expected: shard_id and tenant_id.
        """
        self.assertEqual(accepted_shard_kwargs(), ['shard_id', 'tenant_id'])

    def test_a_value_that_json_cannot_encode_is_turned_into_a_string(self):
        """
        Case: The shard_id of a task is a UUID.
        Expected: The target has the UUID as a string, so that it can be encoded as JSON in a message header.
        """
        shard_id = uuid.uuid4()

        target = shard_target_from_task_kwargs({'shard_id': shard_id})

        self.assertEqual(target, {'shard_id': str(shard_id)})
        self.assertEqual(json.loads(json.dumps(target)), target)

    def test_a_model_instance_is_turned_into_its_primary_key(self):
        """
        Case: The tenant_id of a task is a model instance with the primary key 2, as use_shard_for() takes for a mapping
              field that is a foreign key.
        Expected: The target has the primary key 2, which the worker uses to look up the mapping, and not the display
                  string of the instance. The mapping field tenant_id is not a foreign key to another field, so the
                  primary key is the value that it stores.
        """
        self.assertEqual(shard_target_from_task_kwargs({'tenant_id': Widget(pk=2)}), {'mapping_value': 2})

    @isolate_apps('example')
    def test_a_model_instance_is_turned_into_the_value_that_the_mapping_field_stores(self):
        """
        Case: The customer of a task is a Customer with the primary key 42, while the mapping field of the mapping model
              is a foreign key to the email address of a Customer.
        Expected: The target has the email address, which the mapping field stores, so that the worker finds the mapping
                  and locks the key that use_shard_for() and the move commands lock for the customer.
        """
        customer_model, mapping_model = define_a_mapping_model_with_a_foreign_key_to_an_email_address()

        with (
            mock.patch('djanquiltdb.utils.get_mapping_class', return_value=mapping_model),
            override_settings(QUILT_CELERY={'MAPPING_KWARG': 'customer'}),
        ):
            target = shard_target_from_task_kwargs({'customer': customer_model(pk=42, email='luke@layton.l5')})

        self.assertEqual(target, {'mapping_value': 'luke@layton.l5'})


class NodeTargetFromTaskKwargsTestCase(SimpleTestCase):
    def test_a_node_name_names_the_public_schema_of_that_node(self):
        """
        Case: The keyword arguments of a task have a node_name.
        Expected: A target that identifies the public schema of that node.
        """
        self.assertEqual(
            node_target_from_task_kwargs({'node_name': 'other', 'name': 'x'}),
            {'node_name': 'other', 'schema_name': 'public'},
        )

    def test_a_shard_id_does_not_name_a_node(self):
        """
        Case: The keyword arguments of a task have a shard_id and no node_name.
        Expected: None, because a shard_id identifies a shard, and not a node.
        """
        self.assertIsNone(node_target_from_task_kwargs({'shard_id': 3}))

    def test_a_node_name_that_is_none_names_no_node(self):
        """
        Case: The keyword arguments of a task have node_name=None.
        Expected: None, and not a target with the node 'None'.
        """
        self.assertIsNone(node_target_from_task_kwargs({'node_name': None}))


class MissingTargetMessageTestCase(SimpleTestCase):
    def test_the_message_for_a_missing_shard_names_the_task_and_the_keyword_arguments(self):
        """
        Case: describe_missing_shard() is called for a task whose keyword arguments identify nothing.
        Expected: The message includes the task, every keyword argument that can identify a shard, and the header, and
                  does not mention shared_task_for_node.
        """
        message = describe_missing_shard('example.tasks.record_target', {'name': 'x'})

        self.assertIn('example.tasks.record_target has no shard', message)
        self.assertIn('shard_id or tenant_id', message)
        self.assertIn(TARGET_HEADER_NAME, message)
        self.assertNotIn('shared_task_for_node', message)

    @override_sharding_setting('MAPPING_MODEL')
    def test_without_a_mapping_model_the_message_for_a_missing_shard_names_shard_id_only(self):
        """
        Case: describe_missing_shard() is called in a project without a MAPPING_MODEL.
        Expected: The message asks for the keyword argument shard_id.
        """
        message = describe_missing_shard('example.tasks.record_target', {})

        self.assertIn('Pass the keyword argument shard_id,', message)

    def test_the_message_for_a_missing_shard_points_a_node_name_at_shared_task_for_node(self):
        """
        Case: describe_missing_shard() is called for a task whose keyword arguments have a node_name.
        Expected: The message says that node_name identifies a node, and to decorate a task that runs on a node with
                  shared_task_for_node.
        """
        message = describe_missing_shard('example.tasks.record_target', {'node_name': 'other'})

        self.assertIn('node_name identifies a node', message)
        self.assertIn('Decorate a task that runs on a node with shared_task_for_node', message)

    def test_the_message_for_a_missing_node_names_the_task_and_node_name(self):
        """
        Case: describe_missing_node() is called for a task whose keyword arguments identify nothing.
        Expected: The message includes the task, node_name and the header, and does not mention shared_task_for_shard.
        """
        message = describe_missing_node('example.tasks.record_node', {})

        self.assertIn('example.tasks.record_node has no node', message)
        self.assertIn('node_name', message)
        self.assertIn(TARGET_HEADER_NAME, message)
        self.assertNotIn('shared_task_for_shard', message)

    def test_the_message_for_a_missing_node_points_a_shard_at_shared_task_for_shard(self):
        """
        Case: describe_missing_node() is called for a task whose keyword arguments have a tenant_id.
        Expected: The message says that tenant_id identifies a shard, and to decorate a task that runs in a shard with
                  shared_task_for_shard.
        """
        message = describe_missing_node('example.tasks.record_node', {'tenant_id': 2})

        self.assertIn('tenant_id identifies a shard', message)
        self.assertIn('Decorate a task that runs in a shard with shared_task_for_shard', message)


class MappingKwargCacheTestCase(SimpleTestCase):
    def test_the_mapping_model_is_imported_once_for_two_sends(self):
        """
        Case: accepted_shard_kwargs() is called twice, as sending a task twice calls it twice. Overriding QUILT_CELERY
              with the default empty dict clears the cache before the first call.
        Expected: The mapping model is imported once.
        """
        with override_settings(QUILT_CELERY={}):
            with mock.patch('djanquiltdb.utils.get_mapping_class', wraps=get_mapping_class) as wrapped:
                accepted_shard_kwargs()
                accepted_shard_kwargs()

        self.assertEqual(wrapped.call_count, 1)

    def test_a_change_to_the_quilt_celery_setting_resets_the_cached_keyword_argument(self):
        """
        Case: Call accepted_shard_kwargs(), call it again while QUILT_CELERY['MAPPING_KWARG'] is customer_id, and call
              it again after that override ends.
        Expected: tenant_id, then customer_id, then tenant_id again.
        """
        self.assertIn('tenant_id', accepted_shard_kwargs())

        with override_settings(QUILT_CELERY={'MAPPING_KWARG': 'customer_id'}):
            self.assertEqual(accepted_shard_kwargs(), ['shard_id', 'customer_id'])

        self.assertEqual(accepted_shard_kwargs(), ['shard_id', 'tenant_id'])

    def test_a_change_to_the_quilt_db_setting_resets_the_cached_keyword_argument(self):
        """
        Case: Call accepted_shard_kwargs(), call it again while the project has no MAPPING_MODEL, and call it again
              after that override ends.
        Expected: tenant_id, then no keyword argument for a mapping value, then tenant_id again.
        """
        self.assertIn('tenant_id', accepted_shard_kwargs())

        with override_sharding_setting('MAPPING_MODEL'):
            self.assertEqual(accepted_shard_kwargs(), ['shard_id'])

        self.assertEqual(accepted_shard_kwargs(), ['shard_id', 'tenant_id'])


class ShardTargetFromActiveConnectionTestCase(PluginTestCase):
    def test_inside_use_shard_the_target_is_the_shard(self):
        """
        Case: shard_target_from_active_connection() is called inside use_shard() for the shard remote.
        Expected: A target that identifies remote by its primary key.
        """
        with use_shard(self.remote):
            self.assertEqual(shard_target_from_active_connection(), {'shard_id': self.remote.id})

    def test_inside_use_shard_for_the_target_is_the_mapping_value(self):
        """
        Case: shard_target_from_active_connection() is called inside use_shard_for() for tenant 2.
        Expected: A target that identifies the shard by the mapping value 2, so that the task checks the state of the
                  mapping too.
        """
        with use_shard_for(2):
            self.assertEqual(shard_target_from_active_connection(), {'mapping_value': 2})

    @isolate_apps('example')
    def test_a_mapping_value_that_is_a_model_instance_is_the_value_that_the_mapping_field_stores(self):
        """
        Case: shard_target_from_active_connection() is called while the options of first with a Customer as the mapping
              value are active, and the mapping field of the mapping model is a foreign key to the email address of a
              Customer.
        Expected: A target that identifies the shard by the email address, which the mapping field stores.
        """
        customer_model, mapping_model = define_a_mapping_model_with_a_foreign_key_to_an_email_address()
        options = ShardOptions(
            node_name='default',
            schema_name='first_schema',
            shard_id=self.first.id,
            mapping_value=customer_model(pk=42, email='luke@layton.l5'),
        )

        with (
            mock.patch('djanquiltdb.utils.get_mapping_class', return_value=mapping_model),
            mock.patch('djanquiltdb.router.get_active_connection', return_value=options),
        ):
            target = shard_target_from_active_connection()

        self.assertEqual(target, {'mapping_value': 'luke@layton.l5'})

    def test_inside_the_public_schema_of_a_node_there_is_no_shard(self):
        """
        Case: shard_target_from_active_connection() is called inside use_shard() for the public schema of the other
              node.
        Expected: None, because the public schema of a node is not a shard.
        """
        with use_shard(node_name='other', schema_name='public'):
            self.assertIsNone(shard_target_from_active_connection())

    def test_outside_use_shard_there_is_no_shard(self):
        """
        Case: shard_target_from_active_connection() is called while no shard is active.
        Expected: None.
        """
        self.assertIsNone(shard_target_from_active_connection())


class UseTargetTestCase(PluginTestCase):
    def test_a_shard_target_enters_the_shard(self):
        """
        Case: use_target() is entered for a target that identifies the shard remote, on the other node.
        Expected: The schema of remote on the other node is active inside the block.
        """
        with use_target({'shard_id': self.remote.id}):
            active_connection = get_active_connection()

        self.assertEqual((active_connection.node_name, active_connection.schema_name), ('other', 'remote_schema'))

    def test_a_shard_target_is_read_from_the_shard_table_of_the_active_node(self):
        """
        Case: use_target() is entered for a target that identifies first, while remote, on the other node, is active.
        Expected: The schema of first is active inside the block, because the Shard table is mirrored, so the other node
                  has first too.
        """
        with use_shard(self.remote):
            with use_target({'shard_id': self.first.id}):
                self.assertEqual(get_active_connection().schema_name, 'first_schema')

    def test_a_shard_target_with_the_primary_key_as_a_string_enters_the_shard(self):
        """
        Case: use_target() is entered for a target that has the primary key of remote as a string, as a header has it
              when the primary key of a shard is a UUID.
        Expected: The schema of remote is active inside the block.
        """
        with use_target({'shard_id': str(self.remote.id)}):
            self.assertEqual(get_active_connection().schema_name, 'remote_schema')

    def test_a_mapping_target_enters_the_shard_of_the_mapping(self):
        """
        Case: use_target() is entered for a target that identifies tenant 2 by its mapping value.
        Expected: The schema of remote is active inside the block, entered for the mapping value 2.
        """
        with use_target({'mapping_value': 2}):
            active_connection = get_active_connection()

        self.assertEqual((active_connection.schema_name, active_connection.mapping_value), ('remote_schema', 2))

    def test_a_node_target_enters_the_public_schema_of_the_node(self):
        """
        Case: use_target() is entered for a target that identifies the public schema of the other node.
        Expected: The public schema of the other node is active inside the block.
        """
        with use_target({'node_name': 'other', 'schema_name': 'public'}):
            active_connection = get_active_connection()

        self.assertEqual((active_connection.node_name, active_connection.schema_name), ('other', 'public'))

    def test_a_node_target_is_entered_without_a_lock(self):
        """
        Case: use_target() is entered with lock=True for a target that identifies the public schema of the other node.
        Expected: The public schema of the other node is active inside the block, without a lock, because a public
                  schema has none.
        """
        with use_target({'node_name': 'other', 'schema_name': 'public'}, lock=True):
            active_connection = get_active_connection()

        self.assertEqual((active_connection.schema_name, active_connection.lock), ('public', False))

    def test_a_shard_in_maintenance_raises_a_state_exception(self):
        """
        Case: use_target() is entered for a target that identifies the shard maintained, which is in maintenance.
        Expected: StateException, which a task retries.
        """
        with self.assertRaises(StateException):
            with use_target({'shard_id': self.maintained.id}):
                pass  # pragma: no cover

    def test_a_shard_put_in_maintenance_while_waiting_for_its_lock_raises_a_state_exception(self):
        """
        Case: use_target() is entered for a target that identifies first, which is put in maintenance while use_shard()
              waits for the lock of first, as move_shard_to_node does while it holds the exclusive lock.
        Expected: StateException, which a task retries, and not a task that runs in the shard during the move.
        """

        def put_first_in_maintenance(env):
            Shard.objects.filter(pk=self.first.pk).update(state=State.MAINTENANCE)

        # The flush after the test enters every shard, so first has to be active again by then.
        self.addCleanup(self.make_shard_active, self.first)

        with mock.patch.object(use_shard, 'acquire_lock', autospec=True, side_effect=put_first_in_maintenance):
            with self.assertRaises(StateException):
                with use_target({'shard_id': self.first.id}):
                    pass  # pragma: no cover

    def test_a_shard_that_does_not_exist_raises_does_not_exist(self):
        """
        Case: use_target() is entered for a target whose primary key belongs to no shard.
        Expected: Shard.DoesNotExist.
        """
        with self.assertRaises(Shard.DoesNotExist):
            with use_target({'shard_id': 0}):
                pass  # pragma: no cover

    def test_the_lock_can_be_turned_off(self):
        """
        Case: use_target() is entered for a target that identifies the shard first, without lock and then with
              lock=False.
        Expected: The shard is locked by default, as use_shard() locks it, and not with lock=False.
        """
        with use_target({'shard_id': self.first.id}):
            self.assertTrue(get_active_connection().lock)

        with use_target({'shard_id': self.first.id}, lock=False):
            self.assertFalse(get_active_connection().lock)
