from django.contrib.admin import ModelAdmin
from django.contrib.admin.models import LogEntry
from django.contrib.admin.sites import AdminSite
from django.test import RequestFactory
from example.models import Organization, Shard, User

from djanquiltdb import State
from djanquiltdb.contrib.quilt_admin.utils import CrossShardUserProxy
from djanquiltdb_tests import ShardingTransactionTestCase
from djanquiltdb.utils import create_template_schema, use_shard


class AdminCrossShardLoggingTestCase(ShardingTransactionTestCase):
    """
    Regression test for the IntegrityError raised when an admin, switched to another shard, saved a change.

    Django's admin logging wrote a LogEntry into the *viewed* shard's django_admin_log, whose user_id FK points at
    that shard's user table - where the admin user does not exist - so the FK failed on write/commit. The log write
    must instead land in the admin's *home* shard, where the user_id FK resolves.

    A TransactionTestCase is used so the LogEntry write actually commits (a plain TestCase keeps everything inside one
    rolled-back transaction, hiding a deferred-FK failure that only fires at COMMIT).
    """

    # django_admin_log is sharded (it FKs to the sharded user table), so it must exist per schema. That only happens
    # if django.contrib.admin is part of the migration graph while the template/shards are migrated. The base class
    # restricts the registry to ['djanquiltdb', 'example'], which would drop admin (and its deps) from the graph
    # during the test, so widen available_apps here.
    available_apps = [
        'djanquiltdb',
        'example',
        'django.contrib.contenttypes',
        'django.contrib.auth',
        'django.contrib.sessions',
        'django.contrib.admin',
    ]

    def setUp(self):
        super().setUp()
        create_template_schema()

        # The admin user lives on their home shard; the customer's data lives on a separate shard. Both shards are
        # cloned from the template, which carries django_admin_log because admin is in available_apps above.
        self.home_shard = Shard.objects.create(
            alias='home', schema_name='home_schema', node_name='default', state=State.ACTIVE
        )
        self.customer_shard = Shard.objects.create(
            alias='customer', schema_name='customer_schema', node_name='default', state=State.ACTIVE
        )

        with use_shard(self.home_shard):
            self.admin_user = User.objects.create(name='Admin', email='admin@home.test', is_staff=True)

        with use_shard(self.customer_shard):
            self.organization = Organization.objects.create(name='Customer Org')

        self.model_admin = ModelAdmin(Organization, AdminSite())

    def test_log_change_writes_to_home_shard_without_integrity_error(self):
        """
        Case: A proxied admin (switched to the customer shard) logs a change.
        Expected: No IntegrityError; the LogEntry lands in the admin's home shard, not the customer shard.
        """
        request = RequestFactory().get('/')
        # While switched to the customer shard, the admin override middleware wraps request.user in this proxy.
        request.user = CrossShardUserProxy(self.admin_user, self.home_shard)

        with use_shard(self.customer_shard):
            self.model_admin.log_change(request, self.organization, 'changed something')

        # The LogEntry must have committed to the admin's home shard (where the user_id FK resolves)...
        with use_shard(self.home_shard):
            entries = LogEntry.objects.filter(user_id=self.admin_user.pk)
            self.assertEqual(entries.count(), 1)
            self.assertEqual(entries.first().object_id, str(self.organization.pk))

        # ...and must not have leaked into the customer shard.
        with use_shard(self.customer_shard):
            self.assertFalse(LogEntry.objects.exists())

    def test_log_for_regular_user_stays_on_active_shard(self):
        """
        Case: A regular (non-proxied) admin session logs an addition while on its own shard.
        Expected: The wrapper is a no-op and the LogEntry is written to the active shard as before.
        """
        request = RequestFactory().get('/')

        with use_shard(self.home_shard):
            request.user = User.objects.get(pk=self.admin_user.pk)
            self.model_admin.log_addition(request, self.organization, 'added')

            self.assertEqual(LogEntry.objects.filter(user_id=request.user.pk).count(), 1)
