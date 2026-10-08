from unittest import mock

from django.db import connections
from django.test.utils import CaptureQueriesContext

from djanquiltdb import State
from djanquiltdb.contrib.quilt_admin.utils import CrossShardMappingUserProxy, CrossShardUserProxy
from djanquiltdb.utils import StateException, create_template_schema, use_shard, use_shard_for
from djanquiltdb_tests import ShardingTransactionTestCase
from example.models import OrganizationShard, Shard, User


class CrossShardUserMethodsTestCase(ShardingTransactionTestCase):
    """
    Method calls on request.user while the admin shows a shard that is not the home shard of the user. The admin makes
    such calls many times on every page, such as the permission checks for each registered model and the names in the
    page header.

    Each test has a home shard with the mapping value 1 and a user, and a shown shard with the mapping value 2. The
    tests enter the home shard and then the shown shard outside a transaction, as the middleware does for a request.
    """

    def setUp(self):
        super().setUp()
        create_template_schema()

        self.home_shard = Shard.objects.create(
            alias='home', schema_name='home_schema', node_name='default', state=State.ACTIVE
        )
        self.shown_shard = Shard.objects.create(
            alias='shown', schema_name='shown_schema', node_name='default', state=State.ACTIVE
        )
        OrganizationShard.objects.create(shard=self.home_shard, organization_id=1, slug='home')
        OrganizationShard.objects.create(shard=self.shown_shard, organization_id=2, slug='shown')

        with use_shard(self.home_shard):
            User.objects.create(name='Admin', email='admin@home.test', is_staff=True)

    def call_user_methods(self, user):
        for _ in range(10):
            user.get_username()
            user.get_short_name()
            user.get_full_name()

    def test_repeated_method_calls_run_no_statements(self):
        """
        Case: Enter the home shard and then the shown shard, and call three methods of a proxy of the user ten times
              each, in two rounds.
        Expected: The second round runs no statements, because the methods need no query, and the entered use_shard_for
                  of the home shard holds the locks that the methods need to enter the home shard.
        """
        with use_shard_for(1):
            user = CrossShardMappingUserProxy(User.objects.get(email='admin@home.test'), 1)

            with use_shard_for(2, active_only_schemas=False):
                with CaptureQueriesContext(connections['default']) as first_round:
                    self.call_user_methods(user)

                with CaptureQueriesContext(connections['default']) as second_round:
                    self.call_user_methods(user)

        self.assertEqual(
            len(second_round.captured_queries),
            0,
            'First round: {} statements, second round: {} statements:\n{}'.format(
                len(first_round.captured_queries),
                len(second_round.captured_queries),
                '\n'.join(query['sql'] for query in second_round.captured_queries),
            ),
        )

    def test_method_call_with_the_shard_id_from_the_session(self):
        """
        Case: Enter the home shard and then the shown shard, and call a method of a user whose proxy is constructed with
              the id of the home shard as the session stores it, which is a string.
        Expected: The method runs in the home shard and returns the user's value.
        """
        with use_shard(self.home_shard):
            user = CrossShardUserProxy(User.objects.get(email='admin@home.test'), str(self.home_shard.id))

            with use_shard(self.shown_shard):
                self.assertEqual(user.get_username(), 'admin@home.test')

    def test_method_call_with_the_shard_id_inside_a_shard_without_the_public_schema(self):
        """
        Case: Enter the home shard, and then the shown shard with include_public set to False, and call a method of a
              user whose proxy is constructed with the id of the home shard as the session stores it.
        Expected: The method runs in the home shard and returns the user's value, because the proxy reads the Shard
                  table in the public schema of the active node, which the search_path of the shown shard does not
                  have.
        """
        with use_shard(self.home_shard):
            user = CrossShardUserProxy(User.objects.get(email='admin@home.test'), str(self.home_shard.id))

            with use_shard(self.shown_shard, include_public=False):
                self.assertEqual(user.get_username(), 'admin@home.test')

    def test_method_call_after_the_mapping_value_of_the_user_moves_to_another_shard(self):
        """
        Case: Enter the shown shard, and call a method of a user whose proxy has the mapping value 1, while
              move_data_to_shard moves the mapping value 1 to the shown shard during the wait for the lock of the
              mapping value.
        Expected: A StateException, because the user is no longer in the home shard that the proxy looked up. The
                  proxy does not read the user from the home shard.
        """
        with use_shard(self.home_shard):
            user = CrossShardMappingUserProxy(User.objects.get(email='admin@home.test'), 1)

        acquire_lock = use_shard.acquire_lock

        def acquire_the_lock_while_the_mapping_value_moves(env):
            acquire_lock(env)
            OrganizationShard.objects.filter(organization_id=1).update(shard=self.shown_shard)

        with use_shard_for(2):
            with mock.patch.object(
                use_shard, 'acquire_lock', autospec=True, side_effect=acquire_the_lock_while_the_mapping_value_moves
            ):
                with self.assertRaisesMessage(StateException, 'which differs from the shard'):
                    user.get_username()
