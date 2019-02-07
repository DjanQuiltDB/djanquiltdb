from pprint import pprint
from unittest import mock

from django.core.exceptions import ImproperlyConfigured
from django.db import models
from django.test import SimpleTestCase

from sharding import ShardingMode, State, STATES
from sharding.decorators import sharded_model, shard_mapping_model, mirrored_model, _reset_shard_mapping_models, \
    class_method_use_shard_from_db
from sharding.options import ShardOptions
from sharding.tests import ShardingTestCase, DecoratorTestCaseMixin
from sharding.utils import create_template_schema, get_all_sharded_models, use_shard


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

        self.assertEqual(TestShardedModel.sharding_mode, ShardingMode.SHARDED)


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

        self.assertEqual(TestMirroredModel.sharding_mode, ShardingMode.MIRRORED)


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

            shard = models.ForeignKey('example.Shard', verbose_name='shard')
            map_field = models.PositiveSmallIntegerField()
            state = models.CharField(choices=STATES, max_length=1, default=State.ACTIVE)

            class Meta:
                app_label = 'sharding'

        self.assertFalse(hasattr(MappingDummyModel1, 'sharding_mode'))

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

                shard = models.ForeignKey('Stars', verbose_name='shard')  # NOOA (unresolved reference on purpose)
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

            shard = models.ForeignKey('example.Shard', verbose_name='shard')
            map_field = models.PositiveSmallIntegerField()
            state = models.CharField(choices=STATES, max_length=1, default=State.ACTIVE)

            class Meta:
                app_label = 'sharding'

        with self.assertRaises(ImproperlyConfigured):
            @shard_mapping_model('map_field')
            class MappingDummyModel6(models.Model):
                test_model = True

                shard = models.ForeignKey('example.Shard', verbose_name='shard')
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

                shard = models.ForeignKey('example.Shard', verbose_name='shard')
                map_field = models.PositiveSmallIntegerField()
                state = models.CharField(choices=STATES, max_length=1, default=State.ACTIVE)

                class Meta:
                    app_label = 'sharding'

    def test_shard_mapping_model_with_invalid_argument(self):
        """
        Case: Use shard_mapping_model with an argument not pointing to an existing field
        Expected: ImproperlyConfigured to be raised.
        """
        with self.assertRaises(ImproperlyConfigured):
            @shard_mapping_model('no_field')
            class MappingDummyModel7(models.Model):
                test_model = True

                shard = models.ForeignKey('example.Shard', verbose_name='shard')
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

                shard = models.ForeignKey('example.Shard', verbose_name='shard')
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

                shard = models.ForeignKey('example.Shard', verbose_name='shard')
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

                shard = models.ForeignKey('example.Shard', verbose_name='shard')
                map_field = models.PositiveSmallIntegerField()
                state = models.CharField(choices=(('P', 'Pie'),), max_length=1, default='P')

                class Meta:
                    app_label = 'sharding'


class ClassMethodUseShardFromDbTestCase(DecoratorTestCaseMixin, SimpleTestCase):

    def test(self):
        """
        Case: Create a function and decorate it with class_method_use_shard_from_db; Check that the decoration works.
        Expected: The use_shard context manager is open and closed at the right point.
        """
        mock_db = mock.Mock()
        mock_field_names = mock.Mock()
        mock_values = mock.Mock()

        with mock.patch.object(ShardOptions, 'from_alias') as mock_shard_options:
            mock_shard_options.return_value.use = mock_use = mock.MagicMock(spec=use_shard)

            def func(db, field_names, values):
                self.assertEqual(db, mock_db)
                self.assertEqual(field_names, mock_field_names)
                self.assertEqual(values, mock_values)

                # We are inside the function, so we know that the use_shard context manager is opened, but not closed
                mock_use().__enter__.assert_called_once_with()
                self.assertFalse(mock_use().__exit__.called)

            class_method_use_shard_from_db(func)(mock_db, mock_field_names, mock_values)

        # The function call is finished. We knew earlier that the use_shard context manager opened, but we also know it
        # closed at this point.
        mock_use().__enter__.assert_called_once_with()
        mock_use().__exit__.assert_called_once_with(None, None, None)

    def test_decorated_with(self):
        """
        Case: For all sharded models, check that from_db is decorated with `class_method_use_shard_from_db`
        Expected: The `from_db` method from all sharded models is decorated with `class_method_use_shard_from_db`
        """
        for model in get_all_sharded_models():
            self.assertDecoratedWith(model.from_db, class_method_use_shard_from_db)
