from djanquiltdb import State
from djanquiltdb.testing import ShardingTransactionTestCase
from djanquiltdb.utils import create_template_schema, use_shard_for
from oauth2_provider.models import get_application_model

from example.models import OrganizationShard, Shard, User

#: The raw client secret of every application that create_application() creates. The toolkit stores a hash of it.
CLIENT_SECRET = 'client-secret'

USER_PASSWORD = 'user-password'


class PluginTestCase(ShardingTransactionTestCase):
    """
    Base case for the tests that create shards for real. ``available_apps`` restricts the migration graph, so it lists
    every app of the project.

    setUp creates the shard first on the primary node, the shard remote on the other node, and the shard maintained on
    the primary node, in maintenance. Organization 1 maps onto first, organization 2 onto remote and organization 3 onto
    maintained.
    """

    available_apps = [
        'django.contrib.auth',
        'django.contrib.contenttypes',
        'djanquiltdb',
        'oauth2_provider',
        'example',
    ]

    def setUp(self):
        super().setUp()
        create_template_schema('default')
        create_template_schema('other')
        self.first = Shard.objects.create(
            alias='first', node_name='default', schema_name='first_schema', state=State.ACTIVE
        )
        self.remote = Shard.objects.create(
            alias='remote', node_name='other', schema_name='remote_schema', state=State.ACTIVE
        )
        self.maintained = Shard.objects.create(
            alias='maintained', node_name='default', schema_name='maintained_schema', state=State.ACTIVE
        )

        OrganizationShard.objects.create(shard=self.first, organization_id=1)
        OrganizationShard.objects.create(shard=self.remote, organization_id=2)
        OrganizationShard.objects.create(shard=self.maintained, organization_id=3)

    def put_shard_in_maintenance(self, shard):
        """
        Puts ``shard`` in maintenance until the end of the test. The flush after the test enters every shard, so the
        shard is made active again before then.
        """
        shard.state = State.MAINTENANCE
        shard.save(update_fields=['state'])
        self.addCleanup(self.make_shard_active, shard)

    @staticmethod
    def make_shard_active(shard):
        shard.state = State.ACTIVE
        shard.save(update_fields=['state'])

    @staticmethod
    def create_user(organization_id, email='user@example.com'):
        with use_shard_for(organization_id):
            return User.objects.create_user(email=email, password=USER_PASSWORD)

    @staticmethod
    def create_application(organization_id, **fields):
        """
        Creates an application in the shard of ``organization_id``. Its client secret is CLIENT_SECRET.
        """
        fields = {
            'name': 'Client of organization {}'.format(organization_id),
            'client_type': 'confidential',
            'authorization_grant_type': 'client-credentials',
            'client_secret': CLIENT_SECRET,
            **fields,
        }

        with use_shard_for(organization_id):
            return get_application_model().objects.create(**fields)
