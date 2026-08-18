"""
Tests for djanquiltdb.sessions.SessionStore

Basic test to verify sessions are stored in the appropriate shard.
"""

from unittest import mock

from django.core import signing
from django.test import SimpleTestCase, override_settings

from djanquiltdb.sessions import SessionStore, _get_delimiter, _get_session_key_regex, _get_shard_selector_regex
from djanquiltdb.utils import State, create_template_schema, use_shard
from djanquiltdb_tests import ShardingTestCase
from example.models import Organization, OrganizationShard, QuiltSession, Shard


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

    def setUp(self):
        super().setUp()
        self._clear_setting_caches()
        self.addCleanup(self._clear_setting_caches)

    @staticmethod
    def _clear_setting_caches():
        _get_shard_selector_regex.cache_clear()
        _get_delimiter.cache_clear()
        _get_session_key_regex.cache_clear()

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
