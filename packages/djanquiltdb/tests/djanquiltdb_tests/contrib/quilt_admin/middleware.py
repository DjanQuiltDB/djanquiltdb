from unittest import mock

from django.db import DatabaseError

from djanquiltdb.contrib.quilt_admin.middleware import _check_maintenance_status
from djanquiltdb.utils import State, create_template_schema
from djanquiltdb_tests import ShardingTestCase
from example.models import Shard


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
