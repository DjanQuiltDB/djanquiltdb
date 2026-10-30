"""
Tests for djanquiltdb.sessions.SessionStore

Basic test to verify sessions are stored in the appropriate shard.
"""

from datetime import timedelta
from unittest import mock

from django.conf import settings
from django.contrib.auth import BACKEND_SESSION_KEY, HASH_SESSION_KEY, SESSION_KEY
from django.core import signing
from django.core.exceptions import ImproperlyConfigured
from django.test import SimpleTestCase, override_settings
from django.utils import timezone

from djanquiltdb.sessions import SessionStore
from djanquiltdb.utils import State, create_template_schema, use_shard
from djanquiltdb_tests import ShardingTestCase
from example.models import Organization, OrganizationShard, QuiltSession, Shard, User


@override_settings(
    QUILT_DB={
        'SHARD_CLASS': 'example.models.Shard',
        'MAPPING_MODEL': 'example.models.OrganizationShard',
    },
    QUILT_SESSIONS={
        'SESSION_MODEL': 'example.models.QuiltSession',
    },
)
class SessionStoreShardRoutingTestCase(ShardingTestCase):
    """Test that sessions are stored in the correct shard"""

    def setUp(self):
        super().setUp()
        create_template_schema()

        # Create a shard
        self.shard = Shard.objects.create(
            alias='test_shard', schema_name='test_schema', node_name='default', state=State.ACTIVE
        )

        # Create an organization and mapping for use_shard_for to work
        with use_shard(self.shard):
            self.org = Organization.objects.create(name='Test Org')
        OrganizationShard.objects.create(organization_id=self.org.id, shard=self.shard)

    def test_clear_expired_clears_expired_sessions_on_shards(self):
        """
        Case: A shard holds one expired and one live session. Call SessionStore.clear_expired (simulating Django's
              clearsessions command).
        Expected: The expired session is removed, and nothing else.
        """
        expired = SessionStore(shard_selector=self.org.id)
        expired.create()
        live = SessionStore(shard_selector=self.org.id)
        live.create()

        with use_shard(self.shard):
            QuiltSession.objects.filter(session_key=expired.session_key).update(
                expire_date=timezone.now() - timedelta(days=1)
            )

        SessionStore.clear_expired()

        with use_shard(self.shard):
            remaining = set(QuiltSession.objects.values_list('session_key', flat=True))
        self.assertEqual(remaining, {live.session_key})

    def test_session_stored_in_correct_shard(self):
        """
        Case: Create and save a session with a shard selector
        Expected: Session is stored in the correct shard
        """
        # Create a session store with organization_id as shard_selector
        store = SessionStore(shard_selector=self.org.id)
        store.create()
        store['test_key'] = 'test_value'
        store.save()

        # Verify the session exists in the correct shard
        with use_shard(self.shard):
            session_exists = QuiltSession.objects.filter(session_key=store.session_key).exists()
            self.assertTrue(session_exists, 'Session should exist in the correct shard')

        # Verify we can load the session
        store2 = SessionStore(session_key=store.session_key, shard_selector=self.org.id)
        store2.load()
        self.assertEqual(store2.get('test_key'), 'test_value')

    def create_session_of_removed_mapping(self):
        """
        Saves a session in the shard of the organization, then deletes the mapping of the organization, and returns a
        store for the key of that session that knows only the key.
        """
        store = SessionStore(shard_selector=self.org.id)
        store['test_key'] = 'test_value'
        store.save()
        OrganizationShard.objects.filter(organization_id=self.org.id).delete()
        return SessionStore(session_key=store.session_key)

    def test_load_reads_a_session_whose_mapping_is_gone_as_empty(self):
        """
        Case: Load a session whose shard selector the mapping table no longer has.
        Expected: The session is empty, and the store drops its session key and its shard selector, as Django's database
                  backend drops a key that it does not find.
        """
        store = self.create_session_of_removed_mapping()

        self.assertEqual(store.load(), {})
        self.assertIsNone(store.session_key)
        self.assertIsNone(store.shard_selector)

    def test_load_reads_a_session_key_without_a_shard_selector_as_empty_without_queries(self):
        """
        Case: Load a session whose key is signed correctly, but holds no shard selector that matches
              SHARD_SELECTOR_REGEX, as a key that was signed before SHARD_SELECTOR_REGEX changed.
        Expected: The session is empty and the store drops its session key, and no query runs, because without a
                  selector there is no mapping value to look up.
        """
        store = SessionStore(session_key=sign_session_key('SacmeKrandompart'))

        with self.assertNumQueries(0):
            self.assertEqual(store.load(), {})

        self.assertIsNone(store.session_key)

    def test_delete_of_a_session_whose_mapping_is_gone_does_nothing(self):
        """
        Case: Delete a session whose shard selector the mapping table no longer has, as logout() does.
        Expected: No exception, and the session stays in the shard, where clearsessions deletes it once it expires.
        """
        store = self.create_session_of_removed_mapping()
        session_key = store.session_key

        store.delete()

        with use_shard(self.shard):
            self.assertTrue(QuiltSession.objects.filter(session_key=session_key).exists())

    def test_save_after_loading_a_session_whose_mapping_is_gone_needs_a_selector(self):
        """
        Case: Load a session whose shard selector the mapping table no longer has, write to it, and save it.
        Expected: The save raises the DoesNotExist of the mapping model, as the save of any session without a selector
                  does, instead of saving the session under the selector that the mapping table does not have.
        """
        store = self.create_session_of_removed_mapping()
        store.load()
        store['test_key'] = 'other_value'

        with self.assertRaises(OrganizationShard.DoesNotExist):
            store.save()

    def test_load_and_delete_without_a_mapping_model_report_the_missing_setting(self):
        """
        Case: Load and delete a session while QUILT_DB has no MAPPING_MODEL.
        Expected: The ImproperlyConfigured that use_shard_for raises without a MAPPING_MODEL, and no AttributeError.
        """
        store = SessionStore(shard_selector=self.org.id)

        with override_settings(QUILT_DB={'SHARD_CLASS': 'example.models.Shard'}):
            with self.assertRaisesMessage(ImproperlyConfigured, 'MAPPING_MODEL'):
                store.load()
            with self.assertRaisesMessage(ImproperlyConfigured, 'MAPPING_MODEL'):
                store.delete()


@override_settings(
    QUILT_DB={
        'SHARD_CLASS': 'example.models.Shard',
        'MAPPING_MODEL': 'example.models.OrganizationShard',
    },
    QUILT_SESSIONS={
        'SESSION_MODEL': 'example.models.QuiltSession',
        'SESSION_KEY_DELIMITER': 'Z',
    },
)
class SessionStoreDelimiterTestCase(SimpleTestCase):
    """New session keys must use the configured SESSION_KEY_DELIMITER, not a hardcoded one."""

    def test_configured_delimiter_round_trips(self):
        """
        Case: SESSION_KEY_DELIMITER is set to a non-default value and a new session key is minted.
        Expected: a store constructed from just that key parses the shard selector back out of it.
        """
        store = SessionStore(shard_selector=42)
        with mock.patch.object(SessionStore, 'exists', return_value=False):
            session_key = store._get_new_session_key()
        self.assertEqual(SessionStore(session_key=session_key).shard_selector, '42')


@override_settings(
    QUILT_DB={
        'SHARD_CLASS': 'example.models.Shard',
        'MAPPING_MODEL': 'example.models.OrganizationShard',
        'SHARD_SELECTOR_REGEX': '[a-z]+',
    },
    QUILT_SESSIONS={
        'SESSION_MODEL': 'example.models.QuiltSession',
    },
)
class SessionStoreShardSelectorRegexTestCase(SimpleTestCase):
    """The shard selector of a session key matches QUILT_DB['SHARD_SELECTOR_REGEX']."""

    def test_a_selector_of_the_quilt_db_regex_round_trips(self):
        """
        Case: QUILT_DB['SHARD_SELECTOR_REGEX'] is '[a-z]+', and a new session key is minted for the selector 'abc'.
        Expected: A store constructed from just that key parses 'abc' back out of it.
        """
        store = SessionStore(shard_selector='abc')
        with mock.patch.object(SessionStore, 'exists', return_value=False):
            session_key = store._get_new_session_key()
        self.assertEqual(SessionStore(session_key=session_key).shard_selector, 'abc')

    def test_a_selector_outside_the_quilt_db_regex_is_refused(self):
        """
        Case: QUILT_DB['SHARD_SELECTOR_REGEX'] is '[a-z]+', and the selector 42 is set on a store.
        Expected: ImproperlyConfigured that names the regex.
        """
        with self.assertRaisesMessage(ImproperlyConfigured, '[a-z]+'):
            SessionStore().shard_selector = 42


@override_settings(
    QUILT_DB={
        'SHARD_CLASS': 'example.models.Shard',
        'MAPPING_MODEL': 'example.models.OrganizationShard',
    },
    QUILT_SESSIONS={
        'SESSION_MODEL': 'example.models.QuiltSession',
    },
)
class SessionSettingChangeTestCase(SimpleTestCase):
    def assert_new_session_key_uses_delimiter(self, delimiter):
        store = SessionStore(shard_selector=42)
        with mock.patch.object(SessionStore, 'exists', return_value=False):
            session_key = store._get_new_session_key()

        signed_value = signing.loads(session_key, serializer=store.serializer, salt=store.salt)
        self.assertTrue(signed_value.startswith(f'S42{delimiter}'), signed_value)
        self.assertEqual(SessionStore(session_key=session_key).shard_selector, '42')

    def test_a_change_to_the_delimiter_setting_reaches_the_session_keys(self):
        """
        Case: A session key is created with the default SESSION_KEY_DELIMITER K, another while the delimiter is Z, and
              another after that override ends.
        Expected: The keys use K, then Z, then K again, and SessionStore reads the shard selector 42 from each key.
        """
        self.assert_new_session_key_uses_delimiter('K')

        with override_settings(
            QUILT_SESSIONS={'SESSION_MODEL': 'example.models.QuiltSession', 'SESSION_KEY_DELIMITER': 'Z'}
        ):
            self.assert_new_session_key_uses_delimiter('Z')

        self.assert_new_session_key_uses_delimiter('K')


@override_settings(
    QUILT_DB={
        'SHARD_CLASS': 'example.models.Shard',
        'MAPPING_MODEL': 'example.models.OrganizationShard',
    },
    QUILT_SESSIONS={
        'SESSION_MODEL': 'example.models.QuiltSession',
    },
)
class SessionStoreShardSelectorSignatureTestCase(SimpleTestCase):
    """SessionStore.shard_selector must not raise on expired or tampered session keys."""

    # Any value long enough to pass SessionBase's minimum-length validation; the
    # actual content is irrelevant because signing.loads is mocked to fail.
    INVALID_SESSION_KEY = 'x' * 32

    def test_shard_selector_returns_none_when_signature_expired(self):
        """
        Case: session cookie is older than SESSION_COOKIE_AGE so signing.loads raises SignatureExpired.
        Expected: shard_selector returns None, session_key is cleared, no exception.
        """
        store = SessionStore(session_key=self.INVALID_SESSION_KEY)
        with mock.patch.object(
            signing, 'loads', side_effect=signing.SignatureExpired('Signature age 1211225 > 1209600 seconds')
        ):
            self.assertIsNone(store.shard_selector)
        self.assertIsNone(store.session_key)

    def test_shard_selector_returns_none_when_signature_invalid(self):
        """
        Case: session cookie signature is tampered so signing.loads raises BadSignature.
        Expected: shard_selector returns None, session_key is cleared, no exception.
        """
        store = SessionStore(session_key=self.INVALID_SESSION_KEY)
        with mock.patch.object(signing, 'loads', side_effect=signing.BadSignature('bad signature')):
            self.assertIsNone(store.shard_selector)
        self.assertIsNone(store.session_key)

    def test_shard_selector_returns_none_when_key_holds_no_selector(self):
        """
        Case: The session key is signed correctly, but the value it signs has no shard selector that matches
              SHARD_SELECTOR_REGEX, as the keys made before a change to SHARD_SELECTOR_REGEX or SESSION_KEY_DELIMITER.
        Expected: shard_selector returns None, session_key is cleared, no exception.
        """
        store = SessionStore(session_key=sign_session_key('SacmeKrandompart'))

        self.assertIsNone(store.shard_selector)
        self.assertIsNone(store.session_key)


@override_settings(
    QUILT_DB={
        'SHARD_CLASS': 'example.models.Shard',
        'MAPPING_MODEL': 'example.models.OrganizationShard',
    },
    QUILT_SESSIONS={
        'SESSION_MODEL': 'example.models.QuiltSession',
    },
)
class SessionStoreAsyncTestCase(SimpleTestCase):
    """The async methods of SessionStore refuse, since they would read and write the session outside its shard."""

    async def test_async_methods_raise_not_implemented_error(self):
        """
        Case: Call each async method of SessionStore that reaches the database, and aget() and aflush(), which reach
              them through Django's SessionBase.
        Expected: NotImplementedError from each, instead of a query outside the shard of the session.
        """
        calls = {
            'aload': lambda store: store.aload(),
            'aexists': lambda store: store.aexists(store.session_key),
            'acreate': lambda store: store.acreate(),
            'asave': lambda store: store.asave(),
            'adelete': lambda store: store.adelete(),
            'aclear_expired': lambda store: SessionStore.aclear_expired(),
            'aget': lambda store: store.aget('test_key'),
            'aflush': lambda store: store.aflush(),
        }

        for name, call in calls.items():
            with self.assertRaises(NotImplementedError, msg=name):
                await call(SessionStore(session_key='x' * 32))


def sign_session_key(value):
    """
    Signs ``value`` as SessionStore signs the session keys it makes.
    """
    return signing.dumps(value, compress=True, salt=SessionStore.salt, serializer=SessionStore().serializer)


@override_settings(
    QUILT_DB={
        'SHARD_CLASS': 'example.models.Shard',
        'MAPPING_MODEL': 'example.models.OrganizationShard',
    },
    QUILT_SESSIONS={
        'SESSION_MODEL': 'example.models.QuiltSession',
    },
    SESSION_ENGINE='djanquiltdb.sessions',
    MIDDLEWARE=[
        'django.contrib.sessions.middleware.SessionMiddleware',
        'djanquiltdb.middleware.UseShardForMiddleware',
        'django.contrib.auth.middleware.AuthenticationMiddleware',
    ],
    ROOT_URLCONF='djanquiltdb_tests.test_urlconfs.session_urlconf',
)
class SessionStoreRequestTestCase(ShardingTestCase):
    """A request whose session cookie names no session the backend can read goes on as an anonymous visitor."""

    def setUp(self):
        super().setUp()
        create_template_schema()

        self.shard = Shard.objects.create(
            alias='test_shard', schema_name='test_schema', node_name='default', state=State.ACTIVE
        )
        with use_shard(self.shard):
            self.org = Organization.objects.create(name='Test Org')
            self.user = User.objects.create_user(email='bob@example.com', password='password', organization=self.org)
        OrganizationShard.objects.create(organization_id=self.org.id, shard=self.shard)

    def create_logged_in_session(self):
        """
        Saves a session in which the user is logged in, and returns its key.
        """
        store = SessionStore(shard_selector=self.org.id)
        store[SESSION_KEY] = str(self.user.pk)
        store[BACKEND_SESSION_KEY] = 'django.contrib.auth.backends.ModelBackend'
        store[HASH_SESSION_KEY] = self.user.get_session_auth_hash()
        store.save()
        return store.session_key

    def get_with_session_cookie(self, session_key):
        self.client.cookies[settings.SESSION_COOKIE_NAME] = session_key
        return self.client.get('/')

    def assert_anonymous_and_cookie_deleted(self, response):
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, b'anonymous')
        self.assertEqual(response.cookies[settings.SESSION_COOKIE_NAME].value, '')
        self.assertEqual(response.cookies[settings.SESSION_COOKIE_NAME]['max-age'], 0)

    def test_session_cookie_of_a_logged_in_user(self):
        """
        Case: A request carries the session cookie of a session in which the user is logged in.
        Expected: The view sees the user as logged in, and the response keeps the cookie.
        """
        response = self.get_with_session_cookie(self.create_logged_in_session())

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, b'authenticated')
        self.assertNotIn(settings.SESSION_COOKIE_NAME, response.cookies)

    def test_session_cookie_with_a_bad_signature(self):
        """
        Case: A request carries a session cookie whose signature is wrong.
        Expected: The view sees an anonymous visitor, and the response deletes the cookie.
        """
        response = self.get_with_session_cookie(self.create_logged_in_session()[:-4] + 'XXXX')

        self.assert_anonymous_and_cookie_deleted(response)

    def test_session_cookie_without_a_shard_selector(self):
        """
        Case: A request carries a session cookie that is signed correctly, but holds no shard selector that matches
              SHARD_SELECTOR_REGEX.
        Expected: The view sees an anonymous visitor, and the response deletes the cookie.
        """
        response = self.get_with_session_cookie(sign_session_key('SacmeKrandompart'))

        self.assert_anonymous_and_cookie_deleted(response)

    def test_session_cookie_whose_mapping_is_gone(self):
        """
        Case: A request carries the session cookie of a logged-in user whose mapping value the mapping table no longer
              has.
        Expected: The view sees an anonymous visitor, and the response deletes the cookie.
        """
        session_key = self.create_logged_in_session()
        OrganizationShard.objects.filter(organization_id=self.org.id).delete()

        response = self.get_with_session_cookie(session_key)

        self.assert_anonymous_and_cookie_deleted(response)
