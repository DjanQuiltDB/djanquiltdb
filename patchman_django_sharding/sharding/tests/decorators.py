from unittest import mock

from django.core.exceptions import ImproperlyConfigured
from django.db import models

from example.models import Shard
from sharding import ShardingMode, State, STATES
from sharding.decorators import sharded_model, shard_mapping_model, mirrored_model, _reset_shard_mapping_models, \
    class_method_use_shard_from_db_arg
from sharding.options import ShardOptions
from sharding.tests import ShardingTestCase
from sharding.utils import create_template_schema


class ModelTestCase(ShardingTestCase):
    def setUp(self):
        super().setUp()

        # Make sure we don't register these models, so they don't interfere with others tests. If we do not do this, the
        # Shard model will have related fields to the models created in these tests. Note that we cannot do this with a
        # decorator, because mock decorators on the class won't be copied to classes that inherit this class.
        self.addCleanup(mock.patch.stopall)
        mock.patch('django.apps.registry.Apps.register_model', mock.Mock()).start()


class ShardedModelDecoratorTestCase(ModelTestCase):
    def test_sharding_mode(self):
        """
        Case: Check if decorated model has sharding_mode set.
        Expected: 'S' to be returned.
        """

        @sharded_model()
        class TestShardedModel(models.Model):
            test_model = True

            class Meta:
                app_label = 'sharding'
                abstract = True

        self.assertEqual(getattr(TestShardedModel, '__sharding_mode'), ShardingMode.SHARDED)


class MirroredModelDecoratorTestCase(ModelTestCase):
    def test_mirrored_mode(self):
        """
        Case: Check if decorated model has sharding_mode set.
        Expected: 'M' to be returned.
        """

        @mirrored_model()
        class TestMirroredModel(models.Model):
            test_model = True

            class Meta:
                app_label = 'sharding'
                abstract = True

        self.assertEqual(getattr(TestMirroredModel, '__sharding_mode'), ShardingMode.MIRRORED)


class MappingModelDecoratorTestCase(ModelTestCase):
    def setUp(self):
        super().setUp()
        _reset_shard_mapping_models()

    def test_shard_mapping_model(self):
        """
        Case: Check if decorated model has shard_mapping_model set.
        Expected: It to not have one set.
        """
        @shard_mapping_model('map_field')
        class MappingDummyModel1(models.Model):
            test_model = True

            shard = models.ForeignKey('example.Shard', verbose_name='shard', on_delete=models.CASCADE)
            map_field = models.PositiveSmallIntegerField()
            state = models.CharField(choices=STATES, max_length=1, default=State.ACTIVE)

            class Meta:
                app_label = 'sharding'

        self.assertFalse(hasattr(MappingDummyModel1, '__sharding_mode'))

    def test_shard_mapping_model_no_shard_field(self):
        """
        Case: Use shard_mapping_model on a model that has no shard field.
        Expected: ImproperlyConfigured to be raised.
        """
        with self.assertRaises(ImproperlyConfigured):
            @shard_mapping_model('map_field')
            class MappingDummyModel2(models.Model):
                test_model = True

                map_field = models.PositiveSmallIntegerField()
                state = models.CharField(choices=STATES, max_length=1, default=State.ACTIVE)

                class Meta:
                    app_label = 'sharding'

    def test_shard_mapping_model_invalid_shard_field_type(self):
        """
        Case: Use shard_mapping_model on a model that has a shard field that is not a FKey
        Expected: ImproperlyConfigured to be raised.
        """
        with self.assertRaises(ImproperlyConfigured):
            @shard_mapping_model('map_field')
            class MappingDummyModel3(models.Model):
                test_model = True

                shard = models.CharField('name', max_length=100)
                map_field = models.PositiveSmallIntegerField()
                state = models.CharField(choices=STATES, max_length=1, default=State.ACTIVE)

                class Meta:
                    app_label = 'sharding'

    def test_shard_mapping_model_invalid_shard_relation(self):
        """
        Case: Use shard_mapping_model on a model that has a shard field that does not relate to the sharding model.
        Expected: ImproperlyConfigured to be raised.
        """
        with self.assertRaises(ImproperlyConfigured):
            @shard_mapping_model('map_field')
            class MappingDummyModel4(models.Model):
                test_model = True

                # NOOA (unresolved reference on purpose)
                shard = models.ForeignKey('Stars', verbose_name='shard', on_delete=models.CASCADE)
                map_field = models.PositiveSmallIntegerField()
                state = models.CharField(choices=STATES, max_length=1, default=State.ACTIVE)

                class Meta:
                    app_label = 'sharding'

    def test_shard_mapping_model_double_usage(self):
        """
        Case: Use shard_mapping_model on two models
        Expected: ImproperlyConfigured to be raised.
        """
        @shard_mapping_model('map_field')  # first time goes without error.
        class MappingDummyModel5(models.Model):
            test_model = True

            shard = models.ForeignKey('example.Shard', verbose_name='shard', on_delete=models.CASCADE)
            map_field = models.PositiveSmallIntegerField()
            state = models.CharField(choices=STATES, max_length=1, default=State.ACTIVE)

            class Meta:
                app_label = 'sharding'

        with self.assertRaises(ImproperlyConfigured):
            @shard_mapping_model('map_field')
            class MappingDummyModel6(models.Model):
                test_model = True

                shard = models.ForeignKey('example.Shard', verbose_name='shard', on_delete=models.CASCADE)
                map_field = models.PositiveSmallIntegerField()
                state = models.CharField(choices=STATES, max_length=1, default=State.ACTIVE)

                class Meta:
                    app_label = 'sharding'

    def test_shard_mapping_model_without_argument(self):
        """
        Case: Use shard_mapping_model without an argument
        Expected: TypeError to be raised.
        """
        with self.assertRaises(TypeError):
            @shard_mapping_model()
            class MappingDummyModel7(models.Model):
                test_model = True

                shard = models.ForeignKey('example.Shard', verbose_name='shard', on_delete=models.CASCADE)
                map_field = models.PositiveSmallIntegerField()
                state = models.CharField(choices=STATES, max_length=1, default=State.ACTIVE)

                class Meta:
                    app_label = 'sharding'
                    abstract = True

    def test_shard_mapping_model_with_invalid_argument(self):
        """
        Case: Use shard_mapping_model with an argument not pointing to an existing field
        Expected: ImproperlyConfigured to be raised.
        """
        with self.assertRaises(ImproperlyConfigured):
            @shard_mapping_model('no_field')
            class MappingDummyModel7(models.Model):
                test_model = True

                shard = models.ForeignKey('example.Shard', verbose_name='shard', on_delete=models.CASCADE)
                map_field = models.PositiveSmallIntegerField()
                state = models.CharField(choices=STATES, max_length=1, default=State.ACTIVE)

                class Meta:
                    app_label = 'sharding'

    def test_shard_mapping_model_no_state_field(self):
        """
        Case: Use shard_mapping_model on a model that has no state field.
        Expected: ImproperlyConfigured to be raised.
        """
        with self.assertRaises(ImproperlyConfigured):
            @shard_mapping_model('map_field')
            class MappingDummyModel9(models.Model):
                test_model = True

                shard = models.ForeignKey('example.Shard', verbose_name='shard', on_delete=models.CASCADE)
                map_field = models.PositiveSmallIntegerField()

                class Meta:
                    app_label = 'sharding'

    def test_shard_mapping_model_with_invalid_state_type(self):
        """
        Case: Use shard_mapping_model with a invalid state field TYPE
        Expected: ImproperlyConfigured to be raised.
        """
        with self.assertRaises(ImproperlyConfigured):
            @shard_mapping_model('map_field')
            class MappingDummyModel10(models.Model):
                test_model = True

                shard = models.ForeignKey('example.Shard', verbose_name='shard', on_delete=models.CASCADE)
                map_field = models.PositiveSmallIntegerField()
                state = models.PositiveSmallIntegerField()

                class Meta:
                    app_label = 'sharding'

    def test_shard_mapping_model_with_invalid_state_options(self):
        """
        Case: Use shard_mapping_model with a invalid state field OPTIONS
        Expected: ImproperlyConfigured to be raised.
        """
        with self.assertRaises(ImproperlyConfigured):
            @shard_mapping_model('map_field')
            class MappingDummyModel11(models.Model):
                test_model = True

                shard = models.ForeignKey('example.Shard', verbose_name='shard', on_delete=models.CASCADE)
                map_field = models.PositiveSmallIntegerField()
                state = models.CharField(choices=(('P', 'Pie'),), max_length=1, default='P')

                class Meta:
                    app_label = 'sharding'


class UseShardFromDbArgDecoratorTestCase(ShardingTestCase):
    def setUp(self):
        super().setUp()
        create_template_schema()

        self.shard = Shard.objects.create(alias='M84', schema_name='outro', node_name='default',
                                          state=State.ACTIVE)

    def test_decorated_with(self):
        """
        Case: Check if the function is decorator with a specific decorator
        Expected: The function is decorated and called with the expected argument
        """
        @class_method_use_shard_from_db_arg
        def test_function(db, arg):
            pass

        decorator, bound_arguments = test_function.__decorator__

        self.assertEqual(decorator, class_method_use_shard_from_db_arg)

    @mock.patch('sharding.decorators.get_active_connection')
    @mock.patch('sharding.options.ShardOptions.from_alias')
    def test_already_on_default_connection(self, mock_from_alias, mock_get_active_connection):
        """
        Case: Use class_method_use_shard_from_db_arg decorator on a function, call for the same connection we are
              already active on: default
        Expected: Functions the decorator uses are called, the retrieved shard_options not used, since we're already on
                  the correct connection
        """
        default_shard_options = ShardOptions(node_name='default', schema_name='public')
        mock_from_alias.return_value = default_shard_options
        mock_get_active_connection.return_value = default_shard_options

        with mock.patch.object(mock_from_alias.return_value, 'use') as mock_shardoptions_use:
            @class_method_use_shard_from_db_arg
            def test_function(db, arg):
                self.assertEqual(arg, 'cake')

            test_function(db='db_argument', arg='cake')

        mock_from_alias.assert_called_once_with('db_argument')
        self.assertEqual(mock_get_active_connection.call_count, 1)
        self.assertFalse(mock_shardoptions_use.called)

    @mock.patch('sharding.decorators.get_active_connection')
    @mock.patch('sharding.options.ShardOptions.from_alias')
    def test_not_yet_on_default_connection(self, mock_from_alias, mock_get_active_connection):
        """
        Case: Use class_method_use_shard_from_db_arg decorator on a function, call for the a different connection than
              we're currently active on: default
        Expected: Functions the decorator uses are called, the retrieved shard_options IS used, since we have to switch
                  connections
        """
        mock_from_alias.return_value = ShardOptions(node_name='default', schema_name='public')
        mock_get_active_connection.return_value = ShardOptions.from_shard(self.shard)

        with mock.patch.object(mock_from_alias.return_value, 'use') as mock_shardoptions_use:
            @class_method_use_shard_from_db_arg
            def test_function(db, arg):
                self.assertEqual(arg, 'cake')

            mock_from_alias.reset_mock()  # shard.use calls from_alias.
            test_function(db='db_argument', arg='cake')

        mock_from_alias.assert_called_once_with('db_argument')
        self.assertEqual(mock_get_active_connection.call_count, 1)
        self.assertTrue(mock_shardoptions_use.called)

    @mock.patch('sharding.decorators.get_active_connection')
    @mock.patch('sharding.options.ShardOptions.from_alias')
    def test_already_on_shard_connection(self, mock_from_alias, mock_get_active_connection):
        """
        Case: Use class_method_use_shard_from_db_arg decorator on a function, call for the same connection we are
              already active on: a shard connection
        Expected: Functions the decorator uses are called, the retrieved shard_options not used, since we're already on
                  the correct connection
        """
        default_shard_options = ShardOptions.from_shard(self.shard)
        mock_from_alias.return_value = default_shard_options
        mock_get_active_connection.return_value = default_shard_options

        with mock.patch.object(mock_from_alias.return_value, 'use') as mock_shardoptions_use:
            @class_method_use_shard_from_db_arg
            def test_function(db, arg):
                self.assertEqual(arg, 'cake')

            test_function(db='db_argument', arg='cake')

        mock_from_alias.assert_called_once_with('db_argument')
        self.assertEqual(mock_get_active_connection.call_count, 1)
        self.assertFalse(mock_shardoptions_use.called)

    @mock.patch('sharding.decorators.get_active_connection')
    @mock.patch('sharding.options.ShardOptions.from_alias')
    def test_not_yet_on_shard_connection(self, mock_from_alias, mock_get_active_connection):
        """
        Case: Use class_method_use_shard_from_db_arg decorator on a function, call for the a different connection than
              we're currently active on: a shard connection
        Expected: Functions the decorator uses are called, the retrieved shard_options IS used, since we have to switch
                  connections
        """
        mock_from_alias.return_value = ShardOptions.from_shard(self.shard)
        mock_get_active_connection.return_value = ShardOptions(node_name='default', schema_name='public')

        with mock.patch.object(mock_from_alias.return_value, 'use') as mock_shardoptions_use:
            @class_method_use_shard_from_db_arg
            def test_function(db, arg):
                self.assertEqual(arg, 'cake')

            with self.shard.use():
                mock_from_alias.reset_mock()  # shard.use calls from_alias.
                test_function(db='db_argument', arg='cake')

        mock_from_alias.assert_called_once_with('db_argument')
        self.assertEqual(mock_get_active_connection.call_count, 1)
        self.assertTrue(mock_shardoptions_use.called)
