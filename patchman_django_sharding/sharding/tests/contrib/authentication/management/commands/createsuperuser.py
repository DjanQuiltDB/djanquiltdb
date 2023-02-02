import sys
from unittest import mock

from django.db.utils import IntegrityError
from django.core.management import call_command, CommandError
from django.test import override_settings

from example.models import Shard, User, MirroredUser, DefaultUser
from sharding import State
from sharding.tests import ShardingTransactionTestCase, ShardingTestCase, OverrideMirroredRoutingMixin
from sharding.utils import create_template_schema, use_shard


class CreateSuperUserTestCaseMixin:
    @staticmethod
    def command(*args, **kwargs):
        return call_command(
            'createsuperuser',
            '--noinput',
            '--verbosity', '0',
            *args,
            **kwargs
        )


@override_settings(AUTH_USER_MODEL='example.User')
class CreateSuperUserShardedUserModelTestCase(CreateSuperUserTestCaseMixin, ShardingTestCase):
    """
    Test cases for usage of the `createsuperuser` command in combination with a user model that's a sharded model.
    """
    available_apps = None  # We do want all apps installed

    def setUp(self):
        super().setUp()

        create_template_schema()
        create_template_schema('other')

        self.shard = Shard.objects.create(alias='sadala', schema_name='sadala_schema', node_name='default',
                                          state=State.ACTIVE)

    def test(self):
        """
        Case: Create a superuser on a specific shard
        Expected: Superuser created on that shard
        """
        self.command(
            '--database', 'default',
            '--schema-name', 'sadala_schema',
            '--email', 'vegeta@saiyans.zgt',
        )

        with self.shard.use():
            self.assertTrue(User.objects.filter(email='vegeta@saiyans.zgt').exists())

    def test_unknown_database(self):
        """
        Case: Create a superuser on a database that does not exists
        Expected: CommandError raised
        """
        expected_message = "Error: argument --database: invalid choice: 'foo' (choose from 'default', 'other')"
        with self.assertRaisesMessage(CommandError, expected_message):
            self.command(
                '--database', 'foo',
            )

    def test_unknown_schema(self):
        """
        Case: Create a superuser on a shard that does not exists
        Expected: CommandError raised
        """
        with self.assertRaisesMessage(CommandError, 'The shard you provided (default|namek_schema) does not exist'):
            self.command(
                '--database', 'default',
                '--schema-name', 'namek_schema',
            )

    def test_no_database(self):
        """
        Case: Create a superuser while not providing a database
        Expected: CommandError raised
        """
        with self.assertRaisesMessage(CommandError, 'Error: the following arguments are required: --database'):
            self.command(
                '--schema-name', 'sadala_schema',
            )

    def test_no_schema_name(self):
        """
        Case: Create a superuser while not providing a schema name
        Expected: CommandError raised
        """
        with self.assertRaisesMessage(CommandError, 'Error: the following arguments are required: --schema-name'):
            self.command(
                '--database', 'default',
            )


@override_settings(AUTH_USER_MODEL='example.MirroredUser')
class CreateSuperUserMirroredUserModelTestCase(OverrideMirroredRoutingMixin, CreateSuperUserTestCaseMixin,
                                               ShardingTransactionTestCase):
    """
    Test cases for usage of the `createsuperuser` command in combination with a user model that's a mirrored model.
    """
    available_apps = None  # We do want all apps installed

    def test_single_database(self):
        """
        Case: Create a superuser on a specific database
        Expected: Superuser created on that database
        """
        self.command(
            '--database', 'default',
            '--email', 'kakarot@saiyans.zgt',
        )

        with use_shard(node_name='default', schema_name='public'):
            self.assertTrue(MirroredUser.objects.filter(email='kakarot@saiyans.zgt').exists())

        with use_shard(node_name='other', schema_name='public'):
            self.assertFalse(MirroredUser.objects.filter(email='kakarot@saiyans.zgt').exists())

    def test_all_databases(self):
        """
        Case: Create a superuser on all databases
        Expected: Superuser created on all the databases
        """
        self.command(
            '--database', 'all',
            '--email', 'broly@saiyans.zgt',
        )

        with use_shard(node_name='default', schema_name='public'):
            self.assertTrue(MirroredUser.objects.filter(email='broly@saiyans.zgt').exists())

        with use_shard(node_name='other', schema_name='public'):
            self.assertTrue(MirroredUser.objects.filter(email='broly@saiyans.zgt').exists())

    def test_unknown_database(self):
        """
        Case: Create a superuser on a database that does not exists
        Expected: CommandError raised
        """
        expected_message = "Error: argument --database: invalid choice: 'foo' (choose from 'all', 'default', 'other')"
        with self.assertRaisesMessage(CommandError, expected_message):
            self.command(
                '--database', 'foo',
            )

    def test_no_database(self):
        """
        Case: Create a superuser while not providing the database
        Expected: Defaults to all databases, so superuser created on all the databases
        """
        self.command(
            '--email', 'raditz@saiyans.zgt',
        )

        with use_shard(node_name='default', schema_name='public'):
            self.assertTrue(MirroredUser.objects.filter(email='raditz@saiyans.zgt').exists())

        with use_shard(node_name='other', schema_name='public'):
            self.assertTrue(MirroredUser.objects.filter(email='raditz@saiyans.zgt').exists())

    def test_already_exists_on_one_database(self):
        """
        Case: Create a superuser while the user already exists on one database
        Expected: IntegrityError raised and no new user created on the other databases
        """
        with use_shard(node_name='other', schema_name='public'):
            MirroredUser.objects.create(email='bardock@saiyans.zgt')

        with self.assertRaises((CommandError, IntegrityError)):
            self.command(
                '--database', 'all',
                '--email', 'bardock@saiyans.zgt',
            )

        with use_shard(node_name='default', schema_name='public'):
            self.assertFalse(MirroredUser.objects.filter(email='bardock@saiyans.zgt').exists())

    @mock.patch('sharding.contrib.authentication.management.commands.createsuperuser.Command.get_input_data')
    def test_already_exists_on_one_database_interactive(self, mock_get_input_data):
        """
        Case: Create a superuser while the user already exists on one database, while using the interactive mode
        Expected: Error written to stderr that the email address is already taken
        """
        # The username input runs in a while loop, so we need to stop that in someway. We do that to keep track of the
        # fact we returned something already or not by saving that on a class instance the `get_input_data` method can
        # access and modify. If it already returned something, we raise a TestError, which we assert.
        class TestError(Exception):
            pass

        class Sentinel:
            def __init__(self, returned=False):
                self.returned = returned
        obj = Sentinel()

        def get_input_data(*args, **kwargs):
            if obj.returned:
                raise TestError()
            else:
                obj.returned = True
                return 'nappa@saiyans.zgt'

        mock_get_input_data.side_effect = get_input_data

        class MockTTY:
            """ Need to trick the command that we are actually in tty """
            def isatty(self):
                return True

        stderr = mock.MagicMock(spec=sys.stderr)  # And this is what we actually want to know

        # Now create the mirrored user with email 'nappa@saiyans.zgt' on the 'other' database
        with use_shard(node_name='other', schema_name='public'):
            MirroredUser.objects.create(email='nappa@saiyans.zgt')

        with self.assertRaises(TestError):  # Artificial error, only raised to be able to test the command
            call_command('createsuperuser', '--verbosity', '0', stdin=MockTTY(), stderr=stderr)

        stderr.write.assert_called_once_with('Error: That email address is already taken.\n')

        with use_shard(node_name='other', schema_name='public'):
            self.assertFalse(MirroredUser.objects.filter(email='bardock@saiyans.zgt').exists())


@override_settings(AUTH_USER_MODEL='example.DefaultUser')
class CreateSuperUserNoShardingModeTestCase(CreateSuperUserTestCaseMixin, ShardingTransactionTestCase):
    """
    Test cases for usage of the `createsuperuser` command in combination with a user model that's not sharded nor
    mirrored.
    """
    available_apps = None  # We do want all apps installed

    def test(self):
        """
        Case: Create a superuser
        Expected: Superuser created on the default database only, since that's the only database the DefaultUser table
                  exists on
        """
        self.command(
            '--email', 'paragus@saiyans.zgt',
        )

        with use_shard(node_name='default', schema_name='public'):
            self.assertTrue(DefaultUser.objects.filter(email='paragus@saiyans.zgt').exists())
