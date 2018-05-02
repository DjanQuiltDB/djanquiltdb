from unittest import mock

from django.test import TestCase

from example.models import Shard, Organization, OrganizationShards
from sharding import State
from sharding.exceptions import ShardingError
from sharding.options import InstanceShardOptions, get_shard_from_instance_options, connection_has_same_shard_options, \
    use_shard_from_instance_options
from sharding.utils import create_template_schema, use_shard


class GetShardFromInstanceOptionsTestCase(TestCase):
    def setUp(self):
        create_template_schema()

    def test(self):
        """
        Case: Have an InstanceShardOptions with a shard id set
        Expected: Get the correct shard from get_shard_from_instance_options
        """
        shard = Shard.objects.create(alias='zeus', node_name='default', schema_name='test_zeus', state=State.ACTIVE)

        shard_options = InstanceShardOptions(
            schema_name=shard.schema_name,
            node_name=shard.node_name,
            id_=shard.id,
            mapping_value=None,
            active_only_schemas=None
        )

        self.assertEqual(get_shard_from_instance_options(shard_options), shard)

    def test_no_shard_id(self):
        """
        Case: Have an InstanceShardOptions with no shard id set (happens when you enter a shard with node_name and
              schema_name set)
        Expected: ShardingError raised
        """
        shard_options = InstanceShardOptions(
            schema_name='test_hera',
            node_name='default',
            id_=None,
            mapping_value=None,
            active_only_schemas=None
        )

        with self.assertRaisesMessage(ShardingError, 'Shard ID is not known for this instance'):
            get_shard_from_instance_options(shard_options)


class ConnectionHasSameShardOptionsTestCase(TestCase):
    def setUp(self):
        super().setUp()

        create_template_schema()

        self.shard = Shard.objects.create(alias='apollo', node_name='default', schema_name='test_apollo',
                                          state=State.ACTIVE)

        self.shard_options = InstanceShardOptions(
            schema_name=self.shard.schema_name,
            node_name=self.shard.node_name,
            id_=self.shard.id,
            mapping_value=None,
            active_only_schemas=True
        )

    def test_same_shard_options(self):
        """
        Case: Enter a shard with the same options as provided in self.shard_options
        Expected: connection_has_same_shard_options returns True
        """
        with use_shard(self.shard):
            self.assertTrue(connection_has_same_shard_options(self.shard_options))

    def test_not_same_shard_options(self):
        """
        Case: Enter a shard with different options than provided in self.shard_options
        Expected: connection_has_same_shard_options returns False
        """
        with use_shard(schema_name=self.shard.schema_name, node_name=self.shard.node_name):
            self.assertFalse(connection_has_same_shard_options(self.shard_options))


class UseShardFromInstanceOptions(TestCase):
    def setUp(self):
        super().setUp()

        create_template_schema()

        self.shard = Shard.objects.create(alias='tartaros', node_name='default', schema_name='test_tartaros',
                                          state=State.ACTIVE)

        with use_shard(self.shard):
            self.organization = Organization.objects.create(name='Troje')
            self.organization_shard = OrganizationShards.objects.create(shard=self.shard,
                                                                        organization_id=self.organization.id,
                                                                        state=State.ACTIVE)

    @mock.patch('sharding.options.use_shard_for')
    def test_mapping_value(self, mock_use_shard_for):
        """
        Case: Have sharding options with a mapping value provided
        Expected: use_shard_from_instance_options calls use_shard_for with the correct parameters
        """
        shard_options = InstanceShardOptions(
            schema_name=self.shard.schema_name,
            node_name=self.shard.node_name,
            id_=self.shard.id,
            mapping_value=self.organization.id,
            active_only_schemas=True
        )

        use_shard_from_instance_options(shard_options)

        mock_use_shard_for.assert_called_once_with(
            target_value=self.organization.id
        )

    @mock.patch('sharding.options.use_shard')
    def test_shard(self, mock_use_shard):
        """
        Case: Have sharding options with a shard id provided
        Expected: use_shard_from_instance_options calls use_shard with the correct parameters
        """
        shard_options = InstanceShardOptions(
            schema_name=self.shard.schema_name,
            node_name=self.shard.node_name,
            id_=self.shard.id,
            mapping_value=None,
            active_only_schemas=True
        )

        use_shard_from_instance_options(shard_options)

        mock_use_shard.assert_called_once_with(
            shard=self.shard,
            active_only_schemas=True
        )

        shard_options.active_only_schemas = False

        mock_use_shard.reset_mock()

        use_shard_from_instance_options(shard_options)

        mock_use_shard.assert_called_once_with(
            shard=self.shard,
            active_only_schemas=False
        )

    @mock.patch('sharding.options.use_shard')
    def test_schema_name(self, mock_use_shard):
        """
        Case: Have sharding options with only node name and schema name provided
        Expected: use_shard_from_instance_options calls use_shard with the correct parameters
        """
        shard_options = InstanceShardOptions(
            schema_name=self.shard.schema_name,
            node_name=self.shard.node_name,
            id_=None,
            mapping_value=None,
            active_only_schemas=True
        )

        use_shard_from_instance_options(shard_options)

        mock_use_shard.assert_called_once_with(
            node_name=self.shard.node_name,
            schema_name=self.shard.schema_name,
            active_only_schemas=True
        )

        shard_options.active_only_schemas = False

        mock_use_shard.reset_mock()

        use_shard_from_instance_options(shard_options)

        mock_use_shard.assert_called_once_with(
            node_name=self.shard.node_name,
            schema_name=self.shard.schema_name,
            active_only_schemas=False
        )
