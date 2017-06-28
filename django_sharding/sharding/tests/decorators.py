from django.core.exceptions import ImproperlyConfigured
from django.db import models
from django.test import TestCase

from sharding.decorators import sharded_model, defining_shard_model, mirrored_model, _reset_defining_shard_models
from sharding.tests.utils import test_model
from sharding.utils import ShardingMode


class ShardedModelDecoratorTestCase(TestCase):
    def test_sharding_mode(self):
        """
        Case: Check if decorated model has sharding_mode set.
        Expected: 'S' to be returned.
        """

        @sharded_model()
        @test_model()
        class ShardedDummyModel(models.Model):
            class Meta:
                app_label = 'sharding'

        self.assertEqual(ShardedDummyModel.sharding_mode, ShardingMode.SHARDED)


class MirroredModelDecoratorTestCase(TestCase):
    def test_mirrored_mode(self):
        """
        Case: Check if decorated model has sharding_mode set.
        Expected: 'M' to be returned.
        """

        @mirrored_model()
        @test_model()
        class MirroredDummyModel(models.Model):
            class Meta:
                app_label = 'sharding'

        self.assertEqual(MirroredDummyModel.sharding_mode, ShardingMode.MIRRORED)


class DefiningShardModelDecoratorTestCase(TestCase):
    def setUp(self):
        super().setUp()
        _reset_defining_shard_models()

    def test_defining_shard_model(self):
        """
        Case: Check if decorated model has defining_shard_model set.
        Expected: 'D' to be returned.
        """
        @defining_shard_model()
        @test_model()
        class DefiningDummyModel1(models.Model):
            shard = models.ForeignKey('shardingtest.Shard', verbose_name='shard')

            class Meta:
                app_label = 'sharding'

        self.assertEqual(DefiningDummyModel1.sharding_mode, ShardingMode.DEFINING)

    def test_defining_shard_model_no_shard_field(self):
        """
        Case: Use defining_shard_model on a model that has no shard field.
        Expected: ImproperlyConfigured to be raised.
        """
        with self.assertRaises(ImproperlyConfigured):
            @test_model()
            @defining_shard_model()
            class DefiningDummyModel2(models.Model):

                class Meta:
                    app_label = 'sharding'

    def test_defining_shard_model_invalid_shard_field_type(self):
        """
        Case: Use defining_shard_model on a model that has a shard field that is not a FKey
        Expected: ImproperlyConfigured to be raised.
        """
        with self.assertRaises(ImproperlyConfigured):
            @defining_shard_model()
            @test_model()
            class DefiningDummyModel3(models.Model):
                shard = models.CharField('name', max_length=100)

                class Meta:
                    app_label = 'sharding'

    def test_defining_shard_model_invalid_shard_relation(self):
        """
        Case: Use defining_shard_model on a model that has a shard field that does not relate to the sharding model.
        Expected: ImproperlyConfigured to be raised.
        """
        with self.assertRaises(ImproperlyConfigured):
            @defining_shard_model()
            @test_model()
            class DefiningDummyModel4(models.Model):
                shard = models.ForeignKey('Stars', verbose_name='shard')

                class Meta:
                    app_label = 'sharding'

    def test_defining_shard_model_double_usage(self):
        """
        Case: Use defining_shard_model on two models
        Expected: ImproperlyConfigured to be raised.
        """
        @defining_shard_model()  # first time goes without error.
        @test_model()
        class DefiningDummyModel5(models.Model):
            shard = models.ForeignKey('shardingtest.Shard', verbose_name='shard')

            class Meta:
                app_label = 'sharding'

        with self.assertRaises(ImproperlyConfigured):
            @defining_shard_model()
            @test_model()
            class DefiningDummyModel6(models.Model):
                shard = models.ForeignKey('shardingtest.Shard', verbose_name='shard')

                class Meta:
                    app_label = 'sharding'
