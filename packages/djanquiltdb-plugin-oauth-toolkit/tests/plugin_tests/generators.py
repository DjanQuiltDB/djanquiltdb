from djanquiltdb.decorators import override_sharding_setting
from djanquiltdb.utils import use_shard, use_shard_for
from oauth2_provider.models import get_application_model
from oauthlib.common import Request

from djanquiltdb_plugin_oauth_toolkit.generators import (
    ShardSelectorClientIdGenerator,
    ShardSelectorError,
    active_shard_selector,
    generate_access_token,
    generate_refresh_token,
    refuse_foreign_client_id,
)
from djanquiltdb_plugin_oauth_toolkit.identifiers import generate_client_id_for_selector, parse_client_id, parse_token
from example.models import OrganizationShard
from plugin_tests.base import PluginTestCase


def token_request(application):
    """
    Returns an oauthlib request like the one that the toolkit passes to a token generator, after the toolkit has
    authenticated ``application`` as the client of the request.
    """
    request = Request('https://testserver/o/token/')
    request.client = application

    return request


class ActiveShardSelectorTestCase(PluginTestCase):
    def test_a_shard_that_is_entered_by_mapping_value_has_that_value_as_its_selector(self):
        """
        Case: The shard of organization 1 is entered with use_shard_for(1).
        Expected: The shard selector '1'.
        """
        with use_shard_for(1):
            self.assertEqual(active_shard_selector(), '1')

    def test_a_shard_that_is_entered_with_use_shard_has_the_value_of_its_mapping_row_as_its_selector(self):
        """
        Case: The shard first, which organization 1 maps onto, is entered with use_shard(first).
        Expected: The shard selector '1', read from the mapping row of the shard.
        """
        with use_shard(self.first):
            self.assertEqual(active_shard_selector(), '1')

    def test_a_shard_with_two_mapping_rows_has_no_selector(self):
        """
        Case: Organizations 1 and 4 both map onto the shard first, which is entered with use_shard(first).
        Expected: ShardSelectorError that names the shard and its two mapping rows.
        """
        OrganizationShard.objects.create(shard=self.first, organization_id=4)

        with use_shard(self.first):
            with self.assertRaisesMessage(ShardSelectorError, '2 mapping rows') as raised:
                active_shard_selector()

        self.assertIn('first', str(raised.exception))

    def test_no_selector_outside_a_shard(self):
        """
        Case: The shard selector is read outside any shard, and inside the public schema of a node.
        Expected: ShardSelectorError each time, which says to enter the shard with use_shard_for().
        """
        with self.assertRaisesMessage(ShardSelectorError, 'use_shard_for('):
            active_shard_selector()

        with use_shard(node_name='other', schema_name='public'):
            with self.assertRaisesMessage(ShardSelectorError, 'use_shard_for('):
                active_shard_selector()

    def test_no_selector_without_a_mapping_model(self):
        """
        Case: The shard first is entered with use_shard(first) in a project without a MAPPING_MODEL.
        Expected: ShardSelectorError that names MAPPING_MODEL.
        """
        with override_sharding_setting('MAPPING_MODEL'):
            with use_shard(self.first):
                with self.assertRaisesMessage(ShardSelectorError, 'MAPPING_MODEL'):
                    active_shard_selector()


class ShardSelectorClientIdGeneratorTestCase(PluginTestCase):
    def test_a_client_id_has_the_selector_of_the_active_shard(self):
        """
        Case: hash() is called without arguments inside use_shard_for(1), as the toolkit calls it.
        Expected: A client id that parses to '1'.
        """
        with use_shard_for(1):
            client_id = ShardSelectorClientIdGenerator().hash()

        self.assertEqual(parse_client_id(client_id), '1')

    def test_an_application_gets_a_client_id_of_its_shard(self):
        """
        Case: An application is created inside the shard of organization 2, on the other node, without a client id.
        Expected: Its client id parses to '2', and it is stored in the shard remote only.
        """
        application = self.create_application(2)

        self.assertEqual(parse_client_id(application.client_id), '2')

        with use_shard(self.remote):
            self.assertTrue(get_application_model().objects.filter(pk=application.pk).exists())

        with use_shard(self.first):
            self.assertFalse(get_application_model().objects.filter(client_id=application.client_id).exists())

    def test_an_application_cannot_be_built_outside_a_shard(self):
        """
        Case: An application is instantiated outside any shard, so the default of its client id is generated outside
              any shard.
        Expected: ShardSelectorError at instantiation, which says to enter the shard with use_shard_for().
        """
        with self.assertRaisesMessage(ShardSelectorError, 'use_shard_for('):
            get_application_model()(name='Nowhere')


class TokenGeneratorTestCase(PluginTestCase):
    def test_both_generators_give_distinct_tokens_of_the_active_shard(self):
        """
        Case: Inside use_shard_for(1), both generators are called for a token request whose client is an application
              of organization 1.
        Expected: Two different tokens, which both parse to '1'.
        """
        request = token_request(self.create_application(1))

        with use_shard_for(1):
            access_token = generate_access_token(request)
            refresh_token = generate_refresh_token(request)

        self.assertNotEqual(access_token, refresh_token)
        self.assertEqual(parse_token(access_token), '1')
        self.assertEqual(parse_token(refresh_token), '1')

    def test_a_client_of_another_shard_gets_no_token(self):
        """
        Case: Inside use_shard_for(1), the access token generator is called for a token request whose client is an
              application of organization 2.
        Expected: ShardSelectorError that names both shard selectors, because the token would name another shard than
                  its client.
        """
        request = token_request(self.create_application(2))

        with use_shard_for(1):
            with self.assertRaisesMessage(ShardSelectorError, "'2'") as raised:
                generate_access_token(request)

        self.assertIn("'1'", str(raised.exception))

    def test_a_request_without_a_client_gets_no_token(self):
        """
        Case: A token request without a client, inside use_shard_for(1).
        Expected: ShardSelectorError.
        """
        with use_shard_for(1):
            with self.assertRaisesMessage(ShardSelectorError, 'client'):
                generate_refresh_token(Request('https://testserver/o/token/'))


class RefuseForeignClientIdTestCase(PluginTestCase):
    def test_a_client_id_of_another_shard_is_refused(self):
        """
        Case: Inside the shard of organization 1, an application is saved with a client id of organization 2, as the
              admin and createapplication --client-id allow.
        Expected: ShardSelectorError that names the client id, and the application is not saved.
        """
        foreign_client_id = generate_client_id_for_selector(2)

        with self.assertRaisesMessage(ShardSelectorError, foreign_client_id):
            self.create_application(1, client_id=foreign_client_id)

        with use_shard_for(1):
            self.assertFalse(get_application_model().objects.exists())

    def test_a_client_id_without_a_selector_is_refused(self):
        """
        Case: Inside the shard of organization 1, an application is saved with a client id that has no shard selector.
        Expected: ShardSelectorError, because no request could name the shard of that client id.
        """
        with self.assertRaisesMessage(ShardSelectorError, 'hand-picked'):
            self.create_application(1, client_id='hand-picked')

    def test_a_client_id_of_the_active_shard_is_kept(self):
        """
        Case: Inside the shard of organization 1, an application is saved with a client id of organization 1 that is
              set by hand.
        Expected: It is saved with that client id.
        """
        own_client_id = generate_client_id_for_selector(1)

        application = self.create_application(1, client_id=own_client_id)

        self.assertEqual(application.client_id, own_client_id)

    def test_a_raw_save_is_not_checked(self):
        """
        Case: Outside any shard, the receiver is called for a raw save, such as one from loaddata.
        Expected: No error, because a fixture is loaded unchanged.
        """
        refuse_foreign_client_id(sender=get_application_model(), instance=None, raw=True)
