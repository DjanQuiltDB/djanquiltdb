from datetime import timedelta
from io import StringIO

from django.conf import settings
from django.core.management import call_command, get_commands
from django.test import SimpleTestCase, override_settings
from django.utils import timezone
from djanquiltdb.utils import use_shard, use_shard_for
from oauth2_provider.models import get_access_token_model

from djanquiltdb_plugin_oauth_toolkit.identifiers import generate_token_for_selector
from djanquiltdb_plugin_oauth_toolkit.utils import clear_expired_for_all_shards
from plugin_tests.base import PluginTestCase


class CleartokensCommandRegistrationTestCase(SimpleTestCase):
    def test_the_plugin_provides_the_cleartokens_command(self):
        """
        Case: The test project lists djanquiltdb_plugin_oauth_toolkit before oauth2_provider in INSTALLED_APPS, and both
              apps have a cleartokens command.
        Expected: Django runs the cleartokens command of the plugin.
        """
        self.assertEqual(get_commands()['cleartokens'], 'djanquiltdb_plugin_oauth_toolkit')


class ClearExpiredForAllShardsTestCase(PluginTestCase):
    def setUp(self):
        super().setUp()

        for organization_id in (1, 2, 3):
            self.create_tokens(organization_id)

        self.put_shard_in_maintenance(self.maintained)

    def create_tokens(self, organization_id):
        """
        Creates an expired and a live access token in the shard of ``organization_id``.
        """
        application = self.create_application(organization_id)

        with use_shard_for(organization_id):
            for expires in (timezone.now() - timedelta(days=1), timezone.now() + timedelta(days=1)):
                get_access_token_model().objects.create(
                    token=generate_token_for_selector(organization_id),
                    application=application,
                    expires=expires,
                    scope='read',
                )

    def assertLiveTokensOnly(self, shard):
        with use_shard(shard, active_only_schemas=False):
            expires = list(get_access_token_model().objects.values_list('expires', flat=True))

        self.assertEqual(len(expires), 1)
        self.assertGreater(expires[0], timezone.now())

    def assertExpiredTokenKept(self, shard):
        with use_shard(shard, active_only_schemas=False):
            self.assertEqual(get_access_token_model().objects.count(), 2)

    def test_the_expired_tokens_of_every_active_shard_are_deleted(self):
        """
        Case: The shards of organizations 1 and 2, on two nodes, and the shard of organization 3, which is in
              maintenance, each have an expired and a live access token. clear_expired_for_all_shards() is called.
        Expected: The active shards keep their live token only, the shard in maintenance keeps both tokens, and the
                  two active shards are returned as cleared and the shard in maintenance as skipped.
        """
        cleared_shards, skipped_shards = clear_expired_for_all_shards()

        self.assertLiveTokensOnly(self.first)
        self.assertLiveTokensOnly(self.remote)
        self.assertExpiredTokenKept(self.maintained)
        self.assertEqual(sorted(shard.alias for shard in cleared_shards), ['first', 'remote'])
        self.assertEqual([shard.alias for shard in skipped_shards], ['maintained'])

    def test_ignore_maintenance_also_clears_the_shard_in_maintenance(self):
        """
        Case: The shards of organizations 1 and 2, on two nodes, and the shard of organization 3, which is in
              maintenance, each have an expired and a live access token. clear_expired_for_all_shards() is called with
              ignore_maintenance=True.
        Expected: Every shard keeps its live token only, all three shards are returned as cleared, and none as skipped.
        """
        cleared_shards, skipped_shards = clear_expired_for_all_shards(ignore_maintenance=True)

        self.assertLiveTokensOnly(self.first)
        self.assertLiveTokensOnly(self.remote)
        self.assertLiveTokensOnly(self.maintained)
        self.assertEqual(sorted(shard.alias for shard in cleared_shards), ['first', 'maintained', 'remote'])
        self.assertEqual(skipped_shards, [])

    def test_the_command_clears_every_active_shard_and_names_the_skipped_shard(self):
        """
        Case: cleartokens is run without REFRESH_TOKEN_EXPIRE_SECONDS.
        Expected: The active shards keep their live token only, and the shard in maintenance keeps both tokens. The
                  output names each active shard as cleared and the shard in maintenance as skipped, and the command's
                  warning about REFRESH_TOKEN_EXPIRE_SECONDS is written to stderr.
        """
        stdout, stderr = StringIO(), StringIO()

        call_command('cleartokens', stdout=stdout, stderr=stderr)

        self.assertLiveTokensOnly(self.first)
        self.assertLiveTokensOnly(self.remote)
        self.assertExpiredTokenKept(self.maintained)
        self.assertIn('Deleted the expired tokens of shard first.', stdout.getvalue())
        self.assertIn('Deleted the expired tokens of shard remote.', stdout.getvalue())
        self.assertIn('Skipped shard maintained, which is in maintenance.', stdout.getvalue())
        self.assertIn('REFRESH_TOKEN_EXPIRE_SECONDS', stderr.getvalue())

    def test_the_command_with_ignore_maintenance_clears_the_shard_in_maintenance(self):
        """
        Case: cleartokens is run with --ignore-maintenance.
        Expected: Every shard keeps its live token only, and the output names the shard in maintenance as cleared and
                  skips no shard.
        """
        stdout = StringIO()

        call_command('cleartokens', '--ignore-maintenance', stdout=stdout, stderr=StringIO())

        self.assertLiveTokensOnly(self.first)
        self.assertLiveTokensOnly(self.remote)
        self.assertLiveTokensOnly(self.maintained)
        self.assertIn('Deleted the expired tokens of shard maintained.', stdout.getvalue())
        self.assertNotIn('Skipped', stdout.getvalue())

    @override_settings(OAUTH2_PROVIDER={**settings.OAUTH2_PROVIDER, 'REFRESH_TOKEN_EXPIRE_SECONDS': 3600})
    def test_the_command_does_not_warn_with_a_refresh_token_expiry(self):
        """
        Case: cleartokens is run with REFRESH_TOKEN_EXPIRE_SECONDS, and with verbosity 0.
        Expected: Nothing is written to stdout or to stderr.
        """
        stdout, stderr = StringIO(), StringIO()

        call_command('cleartokens', verbosity=0, stdout=stdout, stderr=stderr)

        self.assertEqual(stdout.getvalue(), '')
        self.assertEqual(stderr.getvalue(), '')
