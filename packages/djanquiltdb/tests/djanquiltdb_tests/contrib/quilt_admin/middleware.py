from unittest import mock

from django.core.exceptions import PermissionDenied
from django.db import DatabaseError, OperationalError
from django.http import HttpResponse
from django.test import SimpleTestCase, override_settings
from django.test.client import RequestFactory

from djanquiltdb.contrib.quilt_admin.middleware import (
    MappingValueAdminOverrideMiddleware,
    ShardIdAdminOverrideMiddleware,
    _check_maintenance_status,
    _is_logout_request,
    _is_shard_switching_request,
)
from djanquiltdb.contrib.quilt_admin.shard_selector import AdminShardSelector
from djanquiltdb.contrib.quilt_admin.utils import CrossShardMappingUserProxy, CrossShardUserProxy
from djanquiltdb.db import connection
from djanquiltdb.decorators import override_sharding_setting
from djanquiltdb.postgresql_backend.base import PUBLIC_SCHEMA_NAME
from djanquiltdb.utils import State, create_template_schema, use_shard
from djanquiltdb_tests import ShardingTestCase
from djanquiltdb_tests.contrib.quilt_admin.shard_selectors import (
    EvenAliasShardSelector,
    EvenOrganizationsShardSelector,
    QuerysetIgnoringShardSelector,
    RecordingShardSelector,
)
from example.models import OrganizationShard, Shard


class CheckMaintenanceStatusTestCase(ShardingTestCase):
    def setUp(self):
        super().setUp()
        create_template_schema()

    def test_a_maintenance_shard_blocks(self):
        """
        Case: The session override points at a shard that is in maintenance.
        Expected: The check reports maintenance and stores the message on the request.
        """
        shard = Shard.objects.create(
            alias='sleepy', schema_name='sleepy_schema', node_name='default', state=State.MAINTENANCE
        )
        request = mock.Mock()

        self.assertTrue(_check_maintenance_status(request, shard_id=shard.id))
        self.assertTrue(request._shard_maintenance_mode)

    def test_a_stale_shard_id_is_not_maintenance(self):
        """
        Case: The session still holds an override for a shard that no longer exists.
        Expected: The admin keeps working so the stale override can be cleared.
        """
        request = mock.Mock()

        self.assertFalse(_check_maintenance_status(request, shard_id=999999))
        self.assertFalse(request._shard_maintenance_mode)

    def test_a_stale_mapping_value_is_not_maintenance(self):
        """
        Case: The session still holds an override for a mapping value that the mapping table no longer has.
        Expected: The admin keeps working so the stale override can be cleared.
        """
        request = mock.Mock()

        self.assertFalse(_check_maintenance_status(request, mapping_value=999999))
        self.assertFalse(request._shard_maintenance_mode)

    def test_a_mapping_lookup_error_surfaces(self):
        """
        Case: The mapping lookup during the maintenance check fails with a database error.
        Expected: The error surfaces.
        """
        mapping_class = mock.Mock()
        mapping_class.mapping_field = 'organization_id'
        mapping_class.objects.using.side_effect = DatabaseError('boom')

        request = mock.Mock()

        with mock.patch('djanquiltdb.contrib.quilt_admin.middleware.get_mapping_class', return_value=mapping_class):
            with self.assertRaises(DatabaseError):
                _check_maintenance_status(request, mapping_value=1)

    def test_a_shard_in_maintenance_is_read_where_the_router_reads_the_shard_model(self):
        """
        Case: The session override points at a shard that is in maintenance in the shard registry of the other node,
              which is active, while the registry of the default node, which is the primary node, does not have it.
        Expected: The check reports maintenance, because it reads the shard registry where the router reads the Shard
                  model, which is the active node.
        """
        request = mock.Mock()

        with use_shard(node_name='other', schema_name=PUBLIC_SCHEMA_NAME) as env:
            env.connection.cursor().execute(
                'INSERT INTO "{}" (id, alias, schema_name, node_name, state) VALUES (%s, %s, %s, %s, %s)'.format(
                    Shard._meta.db_table
                ),
                [9001, 'sleepy', 'sleepy_schema', 'other', State.MAINTENANCE],
            )

            self.assertTrue(_check_maintenance_status(request, shard_id=9001))

    def test_a_mapping_in_maintenance_is_read_where_the_router_reads_the_mapping_model(self):
        """
        Case: The session override points at a mapping value that is in maintenance in the mapping table of the default
              node, which is active, while PRIMARY_DB_ALIAS is the other node and the mapping model sets
              route_to_primary_db to False.
        Expected: The check reports maintenance, because it reads the mapping table where the router reads the mapping
                  model, which is the active node.
        """
        shard = Shard.objects.create(alias='awake', schema_name='awake_schema', node_name='default', state=State.ACTIVE)
        OrganizationShard.objects.create(organization_id=1, shard=shard, state=State.MAINTENANCE)
        request = mock.Mock()

        with (
            override_sharding_setting('PRIMARY_DB_ALIAS', 'other'),
            mock.patch.object(OrganizationShard, 'route_to_primary_db', False),
            use_shard(node_name='default', schema_name=PUBLIC_SCHEMA_NAME),
        ):
            self.assertTrue(_check_maintenance_status(request, mapping_value=1))

    def test_an_active_mapping_whose_shard_is_in_maintenance_blocks(self):
        """
        Case: The session override points at a mapping value whose mapping is active, while the shard of the mapping
              is in maintenance.
        Expected: The check reports maintenance and stores the message of a shard in maintenance on the request.
        """
        shard = Shard.objects.create(
            alias='sleepy', schema_name='sleepy_schema', node_name='default', state=State.MAINTENANCE
        )
        OrganizationShard.objects.create(organization_id=1, shard=shard, state=State.ACTIVE)
        request = mock.Mock()

        self.assertTrue(_check_maintenance_status(request, mapping_value=1))
        self.assertEqual(request._shard_maintenance_message, 'This shard is currently in maintenance mode.')


ADMIN_URLCONF = 'djanquiltdb_tests.test_urlconfs.admin_urlconf'


@override_settings(ROOT_URLCONF=ADMIN_URLCONF)
class ShardSwitchingAndLogoutRequestTestCase(SimpleTestCase):
    def test_request_for_the_shard_switcher_url_is_a_shard_switching_request(self):
        """
        Case: A POST to /admin/switch-shard/, with the shard switcher URLs and the admin URLs in the order that the
              docs give.
        Expected: The request is a shard-switching request.
        """
        self.assertTrue(_is_shard_switching_request(RequestFactory().post('/admin/switch-shard/')))

    def test_request_for_another_admin_url_is_not_a_shard_switching_request(self):
        """
        Case: A GET of /admin/, with the shard switcher URLs and the admin URLs in the order that the docs give.
        Expected: The request is not a shard-switching request.
        """
        self.assertFalse(_is_shard_switching_request(RequestFactory().get('/admin/')))

    def test_request_for_the_admin_logout_url_is_a_logout_request(self):
        """
        Case: A POST to /admin/logout/, with the shard switcher URLs and the admin URLs in the order that the docs
              give.
        Expected: The request is a logout request, because the URL resolves to the logout view of the admin.
        """
        self.assertTrue(_is_logout_request(RequestFactory().post('/admin/logout/')))


class AdminOverrideUseShardMiddlewareTestCase(ShardingTestCase):
    def setUp(self):
        super().setUp()

        create_template_schema('default')
        self.entered_shard = Shard.objects.create(
            alias='entered_shard', schema_name='test_entered_schema', node_name='default', state=State.ACTIVE
        )
        self.override_shard = Shard.objects.create(
            alias='override_shard', schema_name='test_override_schema', node_name='default', state=State.ACTIVE
        )

    def test_enable_shard_inside_a_shard_without_the_public_schema(self):
        """
        Case: Enter the shard that the session override points at, while another shard is entered without the public
              schema.
        Expected: The middleware reads the shard from the shard registry, and enters it.
        """
        request = RequestFactory().get('/admin/')
        request.session = _Session()
        request.user = mock.Mock(is_authenticated=True)
        middleware = ShardIdAdminOverrideMiddleware(lambda request: None)

        with use_shard(self.entered_shard, include_public=False):
            middleware._enable_shard(request, self.override_shard.id)
            self.assertEqual(connection.schema_name, 'test_override_schema')
            middleware.process_response(request, HttpResponse())


class _Session(dict):
    """A session with the attribute shard_selector of djanquiltdb.sessions.SessionStore."""

    shard_selector = None


def _build_admin_request_with_session_override(override_value, method='get', path='/admin/'):
    request = getattr(RequestFactory(), method)(path)
    request.session = _Session({AdminShardSelector.override_shard_selector_key: override_value})
    request.user = mock.Mock(is_authenticated=True)
    request._messages = mock.Mock()
    return request


class AdminOverrideThatNoLongerExistsTestCase(ShardingTestCase):
    """The admin override middlewares treat an override that points nowhere as no override."""

    def test_shard_id_override_of_a_shard_that_the_registry_does_not_have(self):
        """
        Case: ShardIdAdminOverrideMiddleware processes an admin request whose session overrides the shard with the id
              of a shard that the shard registry does not have, as the override of a shard that was deleted.
        Expected: No exception, and the request goes on as without an override: no shard is entered, and request.user
                  is the user itself, so that the override can be cleared through the shard switcher.
        """
        self.assertFalse(Shard.objects.filter(id=999).exists())
        request = _build_admin_request_with_session_override(999)
        user = request.user
        middleware = ShardIdAdminOverrideMiddleware(lambda request: None)

        self.assertIsNone(middleware.process_request(request))

        self.assertIsNone(middleware.get_shard_context_manager(request))
        self.assertIs(request.user, user)
        self.assertEqual(connection.schema_name, PUBLIC_SCHEMA_NAME)

    def test_mapping_value_override_that_the_mapping_table_does_not_have(self):
        """
        Case: MappingValueAdminOverrideMiddleware processes an admin request whose session overrides the shard with a
              mapping value that the mapping table does not have, as the override of a mapping that was deleted.
        Expected: No exception, and the request goes on as without an override: no shard is entered, and request.user
                  is the user itself, so that the override can be cleared through the shard switcher.
        """
        self.assertFalse(OrganizationShard.objects.filter(organization_id=999).exists())
        request = _build_admin_request_with_session_override(999)
        user = request.user
        middleware = MappingValueAdminOverrideMiddleware(lambda request: None)

        self.assertIsNone(middleware.process_request(request))

        self.assertIsNone(middleware.get_shard_context_manager(request))
        self.assertIs(request.user, user)
        self.assertEqual(connection.schema_name, PUBLIC_SCHEMA_NAME)


class MappingValueFromTheUrlAdminOverrideMiddleware(MappingValueAdminOverrideMiddleware):
    """
    Takes the mapping value from the URL /admin/organizations/<organization id>/, standing in for a project that
    overrides get_mapping_value().
    """

    def get_mapping_value(self, request):
        return int(request.path.removeprefix('/admin/organizations/').strip('/'))


class MappingValueAdminOverrideSwitchableMappingsTestCase(ShardingTestCase):
    def setUp(self):
        super().setUp()
        create_template_schema('default')
        for organization_id in (1, 2):
            shard = Shard.objects.create(
                alias=f'shard_{organization_id}',
                schema_name=f'schema_{organization_id}',
                node_name='default',
                state=State.ACTIVE,
            )
            OrganizationShard.objects.create(organization_id=organization_id, shard=shard)

    def _process_request_with_selector(self, middleware, request, selector_class=EvenOrganizationsShardSelector):
        with mock.patch('djanquiltdb.contrib.quilt_admin.apps.ADMIN_SHARD_SELECTOR_CLASS', selector_class):
            return middleware.process_request(request)

    def test_session_override_of_a_switchable_mapping_enters_its_shard(self):
        """
        Case: MappingValueAdminOverrideMiddleware processes an admin request whose session overrides the shard with
              organization 2, which switchable_mappings() of the shard selector class returns.
        Expected: The middleware enters the shard of organization 2, and request.user is the cross-shard proxy.
        """
        request = _build_admin_request_with_session_override(2)
        middleware = MappingValueAdminOverrideMiddleware(lambda request: None)

        self.assertIsNone(self._process_request_with_selector(middleware, request))

        self.assertEqual(connection.schema_name, 'schema_2')
        self.assertIsInstance(request.user, CrossShardMappingUserProxy)
        middleware.process_response(request, HttpResponse())

    def test_post_with_a_session_override_that_is_not_switchable_is_forbidden(self):
        """
        Case: MappingValueAdminOverrideMiddleware processes a POST to a change form whose session overrides the shard
              with organization 1, which has a mapping row that switchable_mappings() of the shard selector class does
              not return, as when the user loses access to organization 1 after opening the form.
        Expected: The middleware raises PermissionDenied, which Django turns into a 403 response, without entering a
                  shard, removes the override from the session, and adds a message that says why.
        """
        request = _build_admin_request_with_session_override(
            1, method='post', path='/admin/example/organization/5/change/'
        )
        middleware = MappingValueAdminOverrideMiddleware(lambda request: None)

        with self.assertRaises(PermissionDenied):
            self._process_request_with_selector(middleware, request)

        self.assertIsNone(middleware.get_shard_context_manager(request))
        self.assertEqual(connection.schema_name, PUBLIC_SCHEMA_NAME)
        self.assertEqual(request.session, {})
        request._messages.add.assert_called_once()
        self.assertEqual(
            request._messages.add.call_args.args[1],
            'Cleared the override, because you do not have access to the selected mapping value.',
        )

    def test_session_override_with_a_hook_that_ignores_its_queryset_is_forbidden(self):
        """
        Case: MappingValueAdminOverrideMiddleware processes an admin request whose session overrides the shard with
              organization 1, while switchable_mappings() of the shard selector class returns a queryset of
              organization 2 that it builds itself instead of filtering the queryset that it receives.
        Expected: The middleware raises PermissionDenied without entering a shard, because the queryset that the hook
                  returns does not have organization 1.
        """
        request = _build_admin_request_with_session_override(1)
        middleware = MappingValueAdminOverrideMiddleware(lambda request: None)

        with self.assertRaises(PermissionDenied):
            self._process_request_with_selector(middleware, request, QuerysetIgnoringShardSelector)

        self.assertEqual(connection.schema_name, PUBLIC_SCHEMA_NAME)

    def test_mapping_value_from_the_url_that_is_not_switchable_is_forbidden(self):
        """
        Case: A subclass of MappingValueAdminOverrideMiddleware whose get_mapping_value() reads the URL processes a
              request for /admin/organizations/1/, and switchable_mappings() of the shard selector class does not
              return organization 1.
        Expected: The middleware raises PermissionDenied without entering a shard, and adds no message, because the
                  session has no override to clear.
        """
        request = RequestFactory().get('/admin/organizations/1/')
        request.session = _Session()
        request.user = mock.Mock(is_authenticated=True)
        request._messages = mock.Mock()
        middleware = MappingValueFromTheUrlAdminOverrideMiddleware(lambda request: None)

        with self.assertRaises(PermissionDenied):
            self._process_request_with_selector(middleware, request)

        self.assertIsNone(middleware.get_shard_context_manager(request))
        self.assertEqual(connection.schema_name, PUBLIC_SCHEMA_NAME)
        request._messages.add.assert_not_called()

    @override_settings(ROOT_URLCONF=ADMIN_URLCONF)
    def test_shard_switching_request_with_an_override_that_is_not_switchable_goes_on(self):
        """
        Case: MappingValueAdminOverrideMiddleware processes the shard-switching request of a user whose session
              overrides the shard with organization 1, which switchable_mappings() of the shard selector class does not
              return.
        Expected: The request goes on without the override, so that the user can switch to another mapping: no shard is
                  entered and request.user is the user itself.
        """
        request = _build_admin_request_with_session_override(1, method='post', path='/admin/switch-shard/')
        user = request.user
        middleware = MappingValueAdminOverrideMiddleware(lambda request: None)

        self.assertIsNone(self._process_request_with_selector(middleware, request))

        self.assertIsNone(middleware.get_shard_context_manager(request))
        self.assertIs(request.user, user)
        self.assertEqual(connection.schema_name, PUBLIC_SCHEMA_NAME)

    def test_logout_request_with_an_override_that_is_not_switchable_goes_on(self):
        """
        Case: MappingValueAdminOverrideMiddleware processes the logout request of a user whose session overrides the
              shard with organization 1, which switchable_mappings() of the shard selector class does not return.
        Expected: The request goes on without the override, so that the user can log out: no shard is entered.
        """
        request = _build_admin_request_with_session_override(1, method='post', path='/admin/logout/')
        middleware = MappingValueAdminOverrideMiddleware(lambda request: None)

        self.assertIsNone(self._process_request_with_selector(middleware, request))

        self.assertIsNone(middleware.get_shard_context_manager(request))
        self.assertEqual(connection.schema_name, PUBLIC_SCHEMA_NAME)

    def test_user_who_is_not_logged_in_goes_on_without_the_override(self):
        """
        Case: MappingValueAdminOverrideMiddleware processes an admin request of a user who is not logged in, whose
              session overrides the shard with organization 2, as after the user was made inactive.
        Expected: The request goes on without the override and without calling switchable_mappings() of the shard
                  selector class, so that the admin sends the user to the login page, and the session keeps the
                  override.
        """
        request = _build_admin_request_with_session_override(2)
        request.user = mock.Mock(is_authenticated=False)
        middleware = MappingValueAdminOverrideMiddleware(lambda request: None)

        with mock.patch.object(RecordingShardSelector, 'calls', []):
            self.assertIsNone(self._process_request_with_selector(middleware, request, RecordingShardSelector))
            calls = RecordingShardSelector.calls

        self.assertEqual(calls, [])
        self.assertIsNone(middleware.get_shard_context_manager(request))
        self.assertEqual(connection.schema_name, PUBLIC_SCHEMA_NAME)
        self.assertEqual(request.session, {AdminShardSelector.override_shard_selector_key: 2})

    def test_connection_error_during_the_lookup_gives_the_connection_error_response(self):
        """
        Case: MappingValueAdminOverrideMiddleware processes an admin request whose session overrides the shard with
              organization 2, and the lookup of the mapping raises OperationalError.
        Expected: The middleware returns the 503 response of a connection error, as for any other lookup of the
                  shard of the request.
        """
        request = _build_admin_request_with_session_override(2)
        middleware = MappingValueAdminOverrideMiddleware(lambda request: None)

        with mock.patch(
            'djanquiltdb.contrib.quilt_admin.middleware.find_switchable_mapping', side_effect=OperationalError('boom')
        ):
            response = self._process_request_with_selector(middleware, request)

        self.assertEqual(response.status_code, 503)

    def test_switchable_mappings_receives_the_user_and_not_the_cross_shard_proxy(self):
        """
        Case: MappingValueAdminOverrideMiddleware processes an admin request whose session overrides the shard with
              organization 2, with a shard selector class whose switchable_mappings() records the user.
        Expected: switchable_mappings() receives the request while request.user is the user itself and not the
                  cross-shard proxy.
        """
        request = _build_admin_request_with_session_override(2)
        user = request.user
        middleware = MappingValueAdminOverrideMiddleware(lambda request: None)

        with mock.patch.object(RecordingShardSelector, 'calls', []):
            self._process_request_with_selector(middleware, request, RecordingShardSelector)
            calls = RecordingShardSelector.calls
        middleware.process_response(request, HttpResponse())

        self.assertEqual([recorded_user for recorded_user, _ in calls], [user])

    def test_request_whose_user_is_already_the_cross_shard_proxy_gets_one_proxy(self):
        """
        Case: MappingValueAdminOverrideMiddleware processes an admin request whose session overrides the shard with
              organization 2, while request.user is already the cross-shard proxy of the user.
        Expected: The middleware enters the shard of organization 2, and request.user is a cross-shard proxy of the
                  user itself, not a proxy of the proxy.
        """
        request = _build_admin_request_with_session_override(2)
        user = request.user
        request.user = CrossShardMappingUserProxy(user, 2)
        middleware = MappingValueAdminOverrideMiddleware(lambda request: None)

        self.assertIsNone(self._process_request_with_selector(middleware, request))

        self.assertEqual(connection.schema_name, 'schema_2')
        self.assertIsInstance(request.user, CrossShardMappingUserProxy)
        self.assertIs(request.user._user, user)
        middleware.process_response(request, HttpResponse())

    def test_post_with_a_session_override_of_a_mapping_in_maintenance_is_refused(self):
        """
        Case: MappingValueAdminOverrideMiddleware processes a POST to a change form whose session overrides the shard
              with organization 2, whose mapping is in maintenance.
        Expected: The middleware returns a 503 response that says that the mapping entry is in maintenance and that
                  changes are not allowed.
        """
        OrganizationShard.objects.filter(organization_id=2).update(state=State.MAINTENANCE)
        request = _build_admin_request_with_session_override(
            2, method='post', path='/admin/example/organization/5/change/'
        )
        middleware = MappingValueAdminOverrideMiddleware(lambda request: None)

        response = self._process_request_with_selector(middleware, request)
        middleware.process_response(request, response)

        self.assertEqual(response.status_code, 503)
        self.assertEqual(
            response.content, b'This mapping entry is currently in maintenance mode. Changes are not allowed.'
        )

    def test_request_outside_the_admin_goes_on_without_the_override(self):
        """
        Case: MappingValueAdminOverrideMiddleware processes a request for /orders/, which is outside the admin, whose
              session overrides the shard with organization 2.
        Expected: The middleware leaves the request alone: no shard is entered and request.user is the user itself.
        """
        request = _build_admin_request_with_session_override(2, path='/orders/')
        user = request.user
        middleware = MappingValueAdminOverrideMiddleware(lambda request: None)

        self.assertIsNone(self._process_request_with_selector(middleware, request))

        self.assertIsNone(middleware.get_shard_context_manager(request))
        self.assertIs(request.user, user)
        self.assertEqual(connection.schema_name, PUBLIC_SCHEMA_NAME)


@override_sharding_setting('MAPPING_MODEL')
class ShardIdAdminOverrideSwitchableShardsTestCase(ShardingTestCase):
    def setUp(self):
        super().setUp()
        create_template_schema('default')
        self.shard_1 = Shard.objects.create(
            alias='shard_1', schema_name='schema_1', node_name='default', state=State.ACTIVE
        )
        self.shard_2 = Shard.objects.create(
            alias='shard_2', schema_name='schema_2', node_name='default', state=State.ACTIVE
        )

    def _process_request_with_selector(self, middleware, request, selector_class=EvenAliasShardSelector):
        with mock.patch('djanquiltdb.contrib.quilt_admin.apps.ADMIN_SHARD_SELECTOR_CLASS', selector_class):
            return middleware.process_request(request)

    def test_session_override_of_a_switchable_shard_enters_it(self):
        """
        Case: ShardIdAdminOverrideMiddleware processes an admin request whose session overrides the shard with the id
              of shard_2, which switchable_shards() of the shard selector class returns.
        Expected: The middleware enters shard_2, and request.user is the cross-shard proxy.
        """
        request = _build_admin_request_with_session_override(self.shard_2.id)
        middleware = ShardIdAdminOverrideMiddleware(lambda request: None)

        self.assertIsNone(self._process_request_with_selector(middleware, request))

        self.assertEqual(connection.schema_name, 'schema_2')
        self.assertIsInstance(request.user, CrossShardUserProxy)
        middleware.process_response(request, HttpResponse())

    def test_post_with_a_session_override_that_is_not_switchable_is_forbidden(self):
        """
        Case: ShardIdAdminOverrideMiddleware processes a POST to a change form whose session overrides the shard with
              the id of shard_1, which the shard registry has but switchable_shards() of the shard selector class does
              not return.
        Expected: The middleware raises PermissionDenied, which Django turns into a 403 response, without entering a
                  shard, removes the override from the session, and adds a message that says why.
        """
        request = _build_admin_request_with_session_override(
            self.shard_1.id, method='post', path='/admin/example/organization/5/change/'
        )
        middleware = ShardIdAdminOverrideMiddleware(lambda request: None)

        with self.assertRaises(PermissionDenied):
            self._process_request_with_selector(middleware, request)

        self.assertIsNone(middleware.get_shard_context_manager(request))
        self.assertEqual(connection.schema_name, PUBLIC_SCHEMA_NAME)
        self.assertEqual(request.session, {})
        request._messages.add.assert_called_once()
        self.assertEqual(
            request._messages.add.call_args.args[1],
            'Cleared the override, because you do not have access to the selected shard.',
        )

    def test_session_override_with_a_hook_that_ignores_its_queryset_is_forbidden(self):
        """
        Case: ShardIdAdminOverrideMiddleware processes an admin request whose session overrides the shard with the id
              of shard_1, while switchable_shards() of the shard selector class returns a queryset of shard_2 that it
              builds itself instead of filtering the queryset that it receives.
        Expected: The middleware raises PermissionDenied without entering a shard, because the queryset that the hook
                  returns does not have shard_1.
        """
        request = _build_admin_request_with_session_override(self.shard_1.id)
        middleware = ShardIdAdminOverrideMiddleware(lambda request: None)

        with self.assertRaises(PermissionDenied):
            self._process_request_with_selector(middleware, request, QuerysetIgnoringShardSelector)

        self.assertEqual(connection.schema_name, PUBLIC_SCHEMA_NAME)

    @override_settings(ROOT_URLCONF=ADMIN_URLCONF)
    def test_shard_switching_request_with_an_override_that_is_not_switchable_goes_on(self):
        """
        Case: ShardIdAdminOverrideMiddleware processes the shard-switching request of a user whose session overrides
              the shard with the id of shard_1, which switchable_shards() of the shard selector class does not return.
        Expected: The request goes on without the override, so that the user can switch to another shard: no shard is
                  entered, request.user is the user itself, and the session keeps the override until the switch
                  replaces it.
        """
        request = _build_admin_request_with_session_override(
            self.shard_1.id, method='post', path='/admin/switch-shard/'
        )
        user = request.user
        middleware = ShardIdAdminOverrideMiddleware(lambda request: None)

        self.assertIsNone(self._process_request_with_selector(middleware, request))

        self.assertIsNone(middleware.get_shard_context_manager(request))
        self.assertIs(request.user, user)
        self.assertEqual(connection.schema_name, PUBLIC_SCHEMA_NAME)
        self.assertEqual(request.session, {AdminShardSelector.override_shard_selector_key: self.shard_1.id})

    def test_logout_request_with_an_override_that_is_not_switchable_goes_on(self):
        """
        Case: ShardIdAdminOverrideMiddleware processes the logout request of a user whose session overrides the shard
              with the id of shard_1, which switchable_shards() of the shard selector class does not return.
        Expected: The request goes on without the override, so that the user can log out: no shard is entered.
        """
        request = _build_admin_request_with_session_override(self.shard_1.id, method='post', path='/admin/logout/')
        middleware = ShardIdAdminOverrideMiddleware(lambda request: None)

        self.assertIsNone(self._process_request_with_selector(middleware, request))

        self.assertIsNone(middleware.get_shard_context_manager(request))
        self.assertEqual(connection.schema_name, PUBLIC_SCHEMA_NAME)

    def test_user_who_is_not_logged_in_goes_on_without_the_override(self):
        """
        Case: ShardIdAdminOverrideMiddleware processes an admin request of a user who is not logged in, whose session
              overrides the shard with the id of shard_2.
        Expected: The request goes on without the override and without calling switchable_shards() of the shard
                  selector class, and the session keeps the override.
        """
        request = _build_admin_request_with_session_override(self.shard_2.id)
        request.user = mock.Mock(is_authenticated=False)
        middleware = ShardIdAdminOverrideMiddleware(lambda request: None)

        with mock.patch.object(RecordingShardSelector, 'calls', []):
            self.assertIsNone(self._process_request_with_selector(middleware, request, RecordingShardSelector))
            calls = RecordingShardSelector.calls

        self.assertEqual(calls, [])
        self.assertEqual(connection.schema_name, PUBLIC_SCHEMA_NAME)
        self.assertEqual(request.session, {AdminShardSelector.override_shard_selector_key: self.shard_2.id})

    def test_connection_error_during_the_lookup_gives_the_connection_error_response(self):
        """
        Case: ShardIdAdminOverrideMiddleware processes an admin request whose session overrides the shard with the id
              of shard_2, and the lookup of the shard raises OperationalError.
        Expected: The middleware returns the 503 response of a connection error, as for any other lookup of the
                  shard of the request.
        """
        request = _build_admin_request_with_session_override(self.shard_2.id)
        middleware = ShardIdAdminOverrideMiddleware(lambda request: None)

        with mock.patch(
            'djanquiltdb.contrib.quilt_admin.middleware.find_switchable_shard', side_effect=OperationalError('boom')
        ):
            response = self._process_request_with_selector(middleware, request)

        self.assertEqual(response.status_code, 503)

    def test_request_whose_user_is_already_the_cross_shard_proxy_gets_one_proxy(self):
        """
        Case: ShardIdAdminOverrideMiddleware processes an admin request whose session overrides the shard with the id
              of shard_2, while request.user is already the cross-shard proxy of the user.
        Expected: The middleware enters shard_2, and request.user is a cross-shard proxy of the user itself, not a
                  proxy of the proxy.
        """
        request = _build_admin_request_with_session_override(self.shard_2.id)
        user = request.user
        request.user = CrossShardUserProxy(user, self.shard_2.id)
        middleware = ShardIdAdminOverrideMiddleware(lambda request: None)

        self.assertIsNone(self._process_request_with_selector(middleware, request))

        self.assertEqual(connection.schema_name, 'schema_2')
        self.assertIsInstance(request.user, CrossShardUserProxy)
        self.assertIs(request.user._user, user)
        middleware.process_response(request, HttpResponse())

    def test_post_with_a_session_override_of_a_shard_in_maintenance_is_refused(self):
        """
        Case: ShardIdAdminOverrideMiddleware processes a POST to a change form whose session overrides the shard with
              the id of shard_2, which is in maintenance.
        Expected: The middleware returns a 503 response that says that the shard is in maintenance and that changes
                  are not allowed.
        """
        Shard.objects.filter(id=self.shard_2.id).update(state=State.MAINTENANCE)
        request = _build_admin_request_with_session_override(
            self.shard_2.id, method='post', path='/admin/example/organization/5/change/'
        )
        middleware = ShardIdAdminOverrideMiddleware(lambda request: None)

        response = self._process_request_with_selector(middleware, request)
        middleware.process_response(request, response)

        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.content, b'This shard is currently in maintenance mode. Changes are not allowed.')

    def test_request_outside_the_admin_goes_on_without_the_override(self):
        """
        Case: ShardIdAdminOverrideMiddleware processes a request for /orders/, which is outside the admin, whose
              session overrides the shard with the id of shard_2.
        Expected: The middleware leaves the request alone: no shard is entered and request.user is the user itself.
        """
        request = _build_admin_request_with_session_override(self.shard_2.id, path='/orders/')
        user = request.user
        middleware = ShardIdAdminOverrideMiddleware(lambda request: None)

        self.assertIsNone(self._process_request_with_selector(middleware, request))

        self.assertIsNone(middleware.get_shard_context_manager(request))
        self.assertIs(request.user, user)
        self.assertEqual(connection.schema_name, PUBLIC_SCHEMA_NAME)
