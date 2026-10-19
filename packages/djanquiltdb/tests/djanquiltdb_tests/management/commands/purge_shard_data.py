import re
from types import SimpleNamespace
from unittest import mock

from django.contrib.admin.utils import NestedObjects
from django.core.management import CommandError, call_command
from django.db import DatabaseError
from django.db.models.signals import post_delete, pre_delete

from djanquiltdb.management.commands.purge_shard_data import Command
from djanquiltdb.utils import State, create_template_schema, use_shard
from djanquiltdb_tests import ShardingTestCase
from djanquiltdb_tests.plugin_hooks import plugin_installed, recording_plugin
from example.models import (
    Cake,
    Note,
    Organization,
    OrganizationShard,
    Shard,
    Statement,
    Suborganization,
    SuperType,
    Type,
    User,
)


class PurgeShardDataTransactionTestCase(ShardingTestCase):
    maxDiff = None

    def setUp(self):
        super().setUp()

        create_template_schema()

        self.source_shard = Shard.objects.create(
            alias='CuriousVillage', node_name='default', schema_name='test', state=State.ACTIVE
        )

        with use_shard(self.source_shard):
            self.super = SuperType.objects.create(name='Character')

            self.type_1 = Type.objects.create(name='Professor', super=self.super)
            self.type_2 = Type.objects.create(name='Child', super=self.super)

            self.organization_1 = Organization.objects.create(name='Layton inc.')
            self.organization_2 = Organization.objects.create(name='Curious Village')
            self.suborganization = Suborganization.objects.create(parent=self.organization_1, child=self.organization_2)

            self.user_1 = User.objects.create(
                name='Layton', email='professor@layton.l5', organization=self.organization_1, type=self.type_1
            )
            self.user_2 = User.objects.create(
                name='Luke', email='luke@layton.l5', organization=self.organization_1, type=self.type_2
            )
            self.user_3 = User.objects.create(
                name='Flora', email='f@reinhold.cap', organization=self.organization_2, type=self.type_2
            )

            self.statement_1 = Statement.objects.create(content="'Luke'!", user=self.user_1, offset=1)
            self.statement_2 = Statement.objects.create(
                content='Try to; solve this "puzzle."', user=self.user_1, offset=2
            )
            self.statement_3 = Statement.objects.create(content='Do you see the sun?', user=self.user_3, offset=3)

            self.organization_shard1 = OrganizationShard.objects.create(
                shard=self.source_shard, organization_id=self.organization_1.id, state=State.ACTIVE
            )
            self.organization_shard2 = OrganizationShard.objects.create(
                shard=self.source_shard, organization_id=self.organization_2.id, state=State.ACTIVE
            )

            self.type_3 = Type.objects.create(name='Attorney', super=self.super)
            self.organization_3 = Organization.objects.create(
                name='Ace',
            )
            self.user_4 = User.objects.create(
                name='Phoenix Wright', email='p@wright.cap', organization=self.organization_3, type=self.type_3
            )
            self.statement_4 = Statement.objects.create(content='Objection!', user=self.user_4, offset=4)
            self.statement_5 = Statement.objects.create(content='discrepancy', user=self.user_4, offset=5)

            self.organization_shard3 = OrganizationShard.objects.create(
                shard=self.source_shard, organization_id=self.organization_3.id, state=State.ACTIVE
            )

            # Some many-to-many models
            self.cake_1 = Cake.objects.create(name='Butter cake')
            self.cake_2 = Cake.objects.create(name='Chocolate cake')
            self.cake_3 = Cake.objects.create(name='Sponge cake')
            self.cake_4 = Cake.objects.create(name='Coffee cake')

            self.user_1.cake.add(self.cake_1)
            self.user_1.cake.add(self.cake_2)

            self.user_2.cake.add(self.cake_3)

            self.user_3.cake.add(self.cake_4)

            self.user_cake_model = User.cake.through  # Auto-created model
            self.user_cake_1 = self.user_cake_model.objects.get(cake=self.cake_1, user=self.user_1)
            self.user_cake_2 = self.user_cake_model.objects.get(cake=self.cake_2, user=self.user_1)
            self.user_cake_3 = self.user_cake_model.objects.get(cake=self.cake_3, user=self.user_2)

            self.user_cake_4 = self.user_cake_model.objects.get(cake=self.cake_4, user=self.user_3)

        self.expected_data = {
            Organization: {self.organization_1},
            Suborganization: {self.suborganization},
            User: {self.user_1, self.user_2},
            Statement: {self.statement_1, self.statement_2},
            self.user_cake_model: {self.user_cake_1, self.user_cake_2, self.user_cake_3},
        }

        self.leftover_data = {
            Organization: {
                self.organization_2,
                self.organization_3,
            },
            Suborganization: {},
            User: {
                self.user_3,
                self.user_4,
            },
            Statement: {
                self.statement_3,
                self.statement_4,
                self.statement_5,
            },
            self.user_cake_model: {
                self.user_cake_4,
            },
        }

        # Used for unit tests
        self.command = Command()
        self.command.shard = self.source_shard
        self.command.options['shard_alias'] = self.source_shard.alias
        self.command.options['model_name'] = 'example.Organization'
        self.command.options['object_value'] = str(self.organization_1.pk)
        self.command.options['object_field'] = 'id'
        self.command.options['simple_collector'] = True
        self.command.options['verbosity'] = 0
        self.command.options['interactive'] = False

        # Used for system tests with `call_command`
        self.options = {
            'shard_alias': self.command.options['shard_alias'],
            'model_name': self.command.options['model_name'],
            'object_value': self.command.options['object_value'],
            'object_field': self.command.options['object_field'],
            'simple_collector': self.command.options['simple_collector'],
            'verbosity': self.command.options['verbosity'],
            'interactive': self.command.options['interactive'],
        }

    def _should_check_constraints(self, connection):
        # Some of the test artifacts will be constraint non compliant. Do not check for constraints during teardown.
        return False

    def test_the_hooks_of_a_plugin_run_around_the_delete(self):
        """
        Case: The data of an organization is purged while a plugin has every hook of purge_shard_data.
        Expected: post_collect runs first and receives the collected rows. Then purge_context is entered, pre_delete
                  runs before the delete and post_delete after it, and purge_context is exited last. The other plain
                  hooks receive the primary keys of the purged rows.
        """
        calls = []

        def record(hook, command, rows_by_model=None):
            organization_exists = (
                Organization.objects.using(self.source_shard).filter(pk=self.organization_1.pk).exists()
            )
            calls.append(
                (
                    hook,
                    organization_exists,
                    rows_by_model and [getattr(row, 'pk', row) for row in rows_by_model[Organization]],
                )
            )

        plugin = recording_plugin(
            'purge_shard_data', 'purge_context', ['post_collect', 'pre_delete', 'post_delete'], record
        )

        with plugin_installed(plugin):
            call_command('purge_shard_data', **self.options)

        purged = [self.organization_1.pk]

        self.assertEqual(
            calls,
            [
                ('post_collect', True, purged),
                ('enter purge_context', True, None),
                ('pre_delete', True, purged),
                ('post_delete', False, purged),
                ('exit purge_context', False, None),
            ],
        )

    def test_a_row_that_a_hook_leaves_out_is_not_purged(self):
        """
        Case: The organization has a Note, which the simple collector finds through its foreign key. Its data is purged
              with the simple collector while a plugin's post_collect hook removes the Note from the collected rows.
        Expected: The organization is purged, and the Note stays in the shard.
        """
        with use_shard(self.source_shard):
            note = Note.objects.create(organization=self.organization_1, text='left out')

        plugin = SimpleNamespace(
            purge_shard_data=SimpleNamespace(post_collect=lambda command, data: data[Note].clear())
        )

        with plugin_installed(plugin):
            call_command('purge_shard_data', **self.options)

        self.assertFalse(Organization.objects.using(self.source_shard).filter(pk=self.organization_1.pk).exists())
        self.assertTrue(Note.objects.using(self.source_shard).filter(pk=note.pk).exists())

    def test_a_purge_whose_post_delete_hook_raises_changes_nothing(self):
        """
        Case: The data of an organization is purged while a plugin's post_delete hook raises an error.
        Expected: The error is raised, and the rows of the organization are still in the shard, because the delete and
                  the hooks run in one transaction.
        """

        def fail(command, pk_set):
            raise DatabaseError('The hook failed.')

        plugin = SimpleNamespace(purge_shard_data=SimpleNamespace(post_delete=fail))

        with plugin_installed(plugin), self.assertRaisesMessage(DatabaseError, 'The hook failed.'):
            call_command('purge_shard_data', **self.options)

        self.assertTrue(Organization.objects.using(self.source_shard).filter(pk=self.organization_1.pk).exists())
        self.assertTrue(User.objects.using(self.source_shard).filter(pk=self.user_1.pk).exists())

    def test_the_delete_hooks_of_a_plugin_run_with_the_delete_signals_disconnected(self):
        """
        Case: A pre_delete and a post_delete receiver are connected for User. The data of an organization is purged
              while a plugin's post_delete hook deletes a User of another organization through the ORM.
        Expected: The hook deletes the User, and neither receiver is called.
        """

        def delete_a_user(command, pk_set):
            User.objects.using(command.shard).filter(pk=self.user_4.pk).delete()

        receiver = mock.Mock()
        pre_delete.connect(receiver, sender=User)
        post_delete.connect(receiver, sender=User)
        self.addCleanup(pre_delete.disconnect, receiver, sender=User)
        self.addCleanup(post_delete.disconnect, receiver, sender=User)

        plugin = SimpleNamespace(purge_shard_data=SimpleNamespace(post_delete=delete_a_user))

        with plugin_installed(plugin):
            call_command('purge_shard_data', **self.options)

        self.assertFalse(User.objects.using(self.source_shard).filter(pk=self.user_4.pk).exists())
        receiver.assert_not_called()

    def test_get_objects(self):
        """
        Case: Call get_objects.
        Expected: The correct objects to be returned.
        """
        self.assertEqual(self.command.get_objects(), [self.organization_1])

    def test_get_objects_that_are_not_sharded(self):
        """
        Case: Call get_objects for model that is not sharded.
        Expected: CommandError raised.
        """
        self.command.options['model_name'] = 'example.type'
        self.command.options['object_value'] = str(self.type_1.id)

        msg = "'example.type' is not a sharded model."

        with self.assertRaisesRegex(CommandError, '^{}$'.format(re.escape(msg))):
            self.command.get_objects()

    def test_get_objects_object_field_does_not_exist(self):
        """
        Case: Call get_objects() with an object field that does not exist.
        Expected: CommandError is raised with a specific message.
        """
        self.command.options['object_field'] = 'dummy'

        msg = "Object field '{}' is not valid for model '{}'.".format(
            self.command.options['object_field'],
            self.command.options['model_name'],
        )

        with self.assertRaisesRegex(CommandError, '^{}$'.format(re.escape(msg))):
            self.command.get_objects()

    def test_get_objects_object_value_does_not_exist(self):
        """
        Case: Call get_objects() with an object value that does not exist.
        Expected: CommandError is raised with a specific message.
        """
        self.command.options['object_value'] = 0

        msg = "No object could be found with object field '{}' and object value '{}' for model '{}'.".format(
            self.command.options['object_field'],
            self.command.options['object_value'],
            self.command.options['model_name'],
        )

        with self.assertRaisesRegex(CommandError, '^{}$'.format(re.escape(msg))):
            self.command.get_objects()

    def test_get_objects_object_value_multiple_objects_exist(self):
        """
        Case: Call get_objects() with an object value that returns multiple objects.
        Expected: CommandError is raised with a specific message.
        """
        self.organization_1.name = 'Test'
        self.organization_1.save(update_fields=['name'])

        self.organization_2.name = 'Test'
        self.organization_2.save(update_fields=['name'])

        self.command.options['object_field'] = 'name'
        self.command.options['object_value'] = 'Test'

        msg = "Multiple objects found with object field '{}' and object value '{}' for model '{}'.".format(
            self.command.options['object_field'],
            self.command.options['object_value'],
            self.command.options['model_name'],
        )

        with self.assertRaisesRegex(CommandError, '^{}$'.format(re.escape(msg))):
            self.command.get_objects()

    def test_get_objects_object_value_invalid_type(self):
        """
        Case: Call get_objects() with an object value that is an invalid type for the object field.
        Expected: CommandError is raised with a specific message.
        """
        self.command.options['field'] = 'id'
        self.command.options['object_value'] = 'dummy'

        msg = "Provided object value '{}' is invalid for object field '{}' for model '{}'.".format(
            self.command.options['object_value'],
            self.command.options['object_field'],
            self.command.options['model_name'],
        )

        with self.assertRaisesRegex(CommandError, '^{}$'.format(re.escape(msg))):
            self.command.get_objects()

    def test_get_shard(self):
        """
        Case: Call get_shard.
        Expected: The correct shard object to be returned.
        """
        self.assertEqual(self.command.get_shard(alias='CuriousVillage'), self.source_shard)

    def test_get_shard_for_nonexistent_model(self):
        """
        Case: Call get_shard with an nonexistent alias.
        Expected: CommandError to be raised.
        """
        with self.assertRaises(CommandError):
            self.command.get_shard(alias='void')

    @mock.patch('djanquiltdb.management.commands.purge_shard_data.SimpleCollector.collect')
    def test_get_data_collector(self, mock_collect):
        """
        Case: Call get_data_collector using the simple collector.
        Expected: SimpleCollector.collect() called.
        """
        self.command.get_data_collector(objects=[self.organization_1])
        mock_collect.assert_called_once_with([self.organization_1])

    def test_get_data_collector_result(self):
        """
        Case: Call get_data_collector using the simple collector and check the data attribute
        Expected: A dict with the correct data to be returned, with only sharded models and no mirrored models
        """
        collector = self.command.get_data_collector(objects=[self.organization_1])
        self.assertEqual(collector.data, self.expected_data)

    def test_get_data_collector_on_a_maintenance_shard(self):
        """
        Case: Call get_data_collector using the simple collector while the shard is in maintenance, which is exactly the
              state a shard being purged tends to be in.
        Expected: The collector is built and collects, instead of raising a StateException.
        """
        self.source_shard.state = State.MAINTENANCE
        self.source_shard.save(update_fields=['state'])
        self.command.shard = self.source_shard

        collector = self.command.get_data_collector(objects=self.command.get_objects())
        # Compare by primary key: comparing instances would re-enter the shard through the active-state
        # options the setUp instances were loaded under, tripping over the maintenance state again.
        self.assertEqual(
            {model: {obj.pk for obj in objs} for model, objs in collector.data.items()},
            {model: {obj.pk for obj in objs} for model, objs in self.expected_data.items()},
        )

    @mock.patch('djanquiltdb.management.commands.purge_shard_data.NestedObjects.collect')
    def test_get_data_collector_nested_collector(self, mock_collect):
        """
        Case: Call get_data, with use_original_collector set to True.
        Expected: NestedObjects.collect() called
        """
        self.command.get_data_collector(objects=[self.organization_1], use_original_collector=True)

        mock_collect.assert_called_once_with([self.organization_1])

    def test_get_data_collector_nested_collector_result(self):
        """
        Case: Call get_data_collector using Django's NestedCollector and check the data attribute
        Expected: A dict with the collected data to be returned, with only sharded models and no mirrored models,
                  but most likely also missing a lot of data as well.
        """
        collector = self.command.get_data_collector(objects=[self.organization_1], use_original_collector=True)
        self.assertEqual(collector.data, {Organization: {self.organization_1}, Suborganization: {self.suborganization}})

    @mock.patch('djanquiltdb.management.commands.purge_shard_data.disable_signals')
    def test_delete_data(self, mock_disable_signals):
        """
        Case: Call delete_data.
        Expected: The delete method of the collector will be called within a disable_signals context manager.
        """
        mock_collector = mock.Mock(data={})

        self.command.shard = self.source_shard
        self.command.delete_data(collector=mock_collector)

        mock_collector.delete.assert_called_once_with()

        mock_disable_signals.assert_called_once_with([pre_delete, post_delete])

    def test_disconnect_delete_signals(self):
        """
        Case: Call the handle while the re are pre_delete and post_delete signals on models that will be collected.
        Expected: Objects are deleted, but no delete signals are called.
        """
        fake_pre_delete_signal = mock.Mock()
        fake_post_delete_signal = mock.Mock()
        pre_delete.connect(fake_pre_delete_signal, sender='example.Organization')
        post_delete.connect(fake_post_delete_signal, sender='example.Organization')

        self.command.handle(**self.options)

        self.assertEqual(fake_pre_delete_signal.call_count, 0)
        self.assertEqual(fake_post_delete_signal.call_count, 0)

        with use_shard(self.source_shard):
            self.assertFalse(Organization.objects.filter(id=self.organization_1.id).exists())

    @mock.patch(
        'djanquiltdb.management.commands.purge_shard_data.NestedObjects.collect', mock.Mock(side_effect=DatabaseError)
    )
    @mock.patch('djanquiltdb.management.commands.purge_shard_data.Command.delete_data')
    def test_no_change_on_failure_get_data_collector(self, mock_delete_data):
        """
        Case: Call the handle while NestedObjects.collect() will raise an exception.
        Expected: `delete_data` is not called and data is not deleted.
        """
        self.options['simple_collector'] = False
        with self.assertRaises(DatabaseError):
            self.command.handle(**self.options)

        self.organization_shard1.refresh_from_db()
        self.assertEqual(self.organization_shard1.shard, self.source_shard)
        self.assertEqual(mock_delete_data.call_count, 0)

    @mock.patch('djanquiltdb.management.commands.purge_shard_data.Command.delete_data', side_effect=DatabaseError)
    def test_no_change_on_failure_delete_data(self, mock_delete_data):
        """
        Case: Call the handle while `delete_data` will raise an exception.
        Expected: `delete_data` is called, but data is not deleted.
        """
        with self.assertRaises(DatabaseError):
            self.command.handle(**self.options)

        self.organization_shard1.refresh_from_db()
        self.assertEqual(self.organization_shard1.shard, self.source_shard)
        self.assertEqual(mock_delete_data.call_count, 1)

    @mock.patch('djanquiltdb.management.commands.purge_shard_data.NestedObjects')
    def test_get_data_collector_mirrored_models(self, mock_collector):
        """
        Case: Have the collector return mirrored models.
        Expected: CommandError is raised with a specific message.
        """

        class FakeCollector(NestedObjects):
            def collect(self, objs, *args, **kwargs):
                obj = objs[0]
                self.data = {User: {obj}, Type: {obj.type}}

        mock_collector.side_effect = FakeCollector

        msg = (
            'There might be something wrong with your data structure, because the collector '
            'collected mirrored models. Check your data points closely to see if no unexpected '
            'model instances are collected.'
        )

        with self.assertRaisesRegex(CommandError, '^{}$'.format(re.escape(msg))):
            self.command.get_data_collector(objects=[self.user_1], use_original_collector=True)

    def test_move_no_delete_and_purge(self):
        """
        Case: Call `move_data_to_shard` with the --no-delete option and call the `purge_shard_data` command after.
        Expected: The object's data is deleted on the source shard and (still) exists on the target shard. The leftover
                  data is untouched and remains on the source shard.
        """
        self.target_shard = Shard.objects.create(
            alias='Court', node_name='default', schema_name='test_target', state=State.ACTIVE
        )

        call_command(
            'move_data_to_shard',
            '--source-shard-alias=' + self.options['shard_alias'],
            '--target-shard-alias=' + self.target_shard.alias,
            '--root-object-id=' + self.options['object_value'],
            '--model-name=' + self.options['model_name'],
            '--no-input',
            '--quiet',
            '--no-delete',
        )

        # Check if all the initial data is still on the source shard
        for model, instances in self.expected_data.items():
            self.assertEqual(
                list(model.objects.using(self.source_shard).all()), list(instances) + list(self.leftover_data[model])
            )

        call_command('purge_shard_data', **self.options)

        # Check that only the leftover data remains on the source shard
        for model, instances in self.expected_data.items():
            self.assertEqual(list(model.objects.using(self.source_shard).all()), list(self.leftover_data[model]))

        # Check if all the data is still on the target shard
        for model, instances in self.expected_data.items():
            self.assertEqual(list(model.objects.using(self.target_shard).all()), list(self.expected_data[model]))

    @mock.patch('djanquiltdb.management.commands.purge_shard_data.Command.confirm')
    @mock.patch('djanquiltdb.management.commands.purge_shard_data.Command.log')
    @mock.patch('djanquiltdb.management.commands.purge_shard_data.Command.delete_data')
    def test_confirm_not_yes_cancels_operation(self, mock_delete_data, mock_log, mock_confirm):
        """
        Case: Call command and do not confirm.
        Expected: A message is shown and `delete_data` is not called.
        """
        mock_confirm.return_value = False

        call_command('purge_shard_data', **self.options)

        mock_log.assert_any_call('\nOperation cancelled.', level=1)

        self.assertFalse(mock_delete_data.called)
