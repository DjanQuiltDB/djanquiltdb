"""
Tests for djanquiltdb.sessions.SessionStore

Basic test to verify sessions are stored in the appropriate shard.
"""

from django.test import override_settings
from example.models import Organization, OrganizationShard, QuiltSession, Shard

from djanquiltdb.sessions import SessionStore
from djanquiltdb.tests import ShardingTestCase
from djanquiltdb.utils import State, create_template_schema, use_shard


@override_settings(
    SHARDING={
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
