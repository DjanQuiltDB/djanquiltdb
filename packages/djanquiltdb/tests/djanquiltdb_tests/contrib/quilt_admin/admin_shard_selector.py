from types import SimpleNamespace

from django.test import SimpleTestCase, override_settings

from djanquiltdb.contrib.quilt_admin.shard_selector import AdminShardSelector
from example.models import Shard


class AdminShardSelectorOverrideKeyTestCase(SimpleTestCase):
    @override_settings(QUILT_ADMIN={'OVERRIDE_SHARD_SELECTOR_KEY': 'tenant_override'})
    def test_setting_names_the_session_key_of_the_override(self):
        """
        Case: QUILT_ADMIN['OVERRIDE_SHARD_SELECTOR_KEY'] is 'tenant_override', and the shard selector stores the
              override 'orders'.
        Expected: The session key of the override is 'tenant_override', and the session has the override under it.
        """
        request = SimpleNamespace(session={})

        AdminShardSelector.set_override_value(request, 'orders')

        self.assertEqual(AdminShardSelector.override_shard_selector_key, 'tenant_override')
        self.assertEqual(request.session, {'tenant_override': 'orders'})


class AdminShardSelectorLabelTestCase(SimpleTestCase):
    def test_label_of_a_shard_without_a_value_names_the_shard(self):
        """
        Case: Format the label of the shard orders_shard without a value.
        Expected: The alias of the shard, followed by its node name and schema name.
        """
        shard = Shard(alias='orders_shard', node_name='default', schema_name='orders_schema')

        self.assertEqual(
            AdminShardSelector.format_override_label(value=None, shard=shard), 'orders_shard (default|orders_schema)'
        )

    def test_label_of_a_value_without_a_shard_is_the_value(self):
        """
        Case: Format the label of the value 'orders' without a shard.
        Expected: The value itself.
        """
        self.assertEqual(AdminShardSelector.format_override_label(value='orders', shard=None), 'orders')
