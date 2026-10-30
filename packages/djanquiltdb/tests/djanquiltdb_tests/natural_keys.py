from unittest import mock

from django.apps import apps
from django.contrib.sessions.models import Session
from django.db import models
from django.db.migrations.state import ModelState
from django.test import SimpleTestCase
from django.test.utils import isolate_apps

from djanquiltdb import ShardingMode, single_node_unsharded
from djanquiltdb.apps import _validate_public_models
from djanquiltdb.natural_keys import add_natural_keys_to_public_session_models, make_the_primary_key_the_natural_key


class MakeThePrimaryKeyTheNaturalKeyTestCase(SimpleTestCase):
    @isolate_apps('djanquiltdb')
    def test_the_natural_key_is_the_primary_key(self):
        """
        Case: A model with a string primary key and no natural key is given its primary key as the natural key.
        Expected: natural_key() returns the primary key, which is also the first entry of unique_together.
        """

        class Wafer(models.Model):
            code = models.CharField(max_length=40, primary_key=True)

            class Meta:
                app_label = 'djanquiltdb'

        make_the_primary_key_the_natural_key(Wafer)

        self.assertEqual(Wafer(code='w1').natural_key(), ('w1',))
        self.assertEqual(Wafer._meta.unique_together, (('code',),))

    @isolate_apps('djanquiltdb')
    def test_the_managers_look_a_row_up_by_its_primary_key(self):
        """
        Case: A model with two managers of its own is given its primary key as the natural key, and the cache of the app
              registry is then cleared, which replaces the copies of the managers that the model hands out.
        Expected: Every manager has get_by_natural_key(), which looks the row up by its primary key.
        """

        class Wafer(models.Model):
            code = models.CharField(max_length=40, primary_key=True)

            objects = models.Manager()
            crisp = models.Manager()

            class Meta:
                app_label = 'djanquiltdb'

        make_the_primary_key_the_natural_key(Wafer)
        Wafer._meta._expire_cache()
        Wafer._meta.apps.clear_cache()

        for manager in Wafer._meta.managers:
            with self.subTest(manager=manager.name):
                with mock.patch.object(type(manager), 'get') as get:
                    manager.get_by_natural_key('w1')

                get.assert_called_once_with(pk='w1')

    @isolate_apps('djanquiltdb')
    def test_what_the_model_declares_itself_is_kept(self):
        """
        Case: A model that declares a unique_together and a natural_key() of its own is given its primary key as the
              natural key.
        Expected: The primary key goes first in unique_together, before the declared entry, and the declared
                  natural_key() is kept.
        """

        class Wafer(models.Model):
            flavour = models.CharField(max_length=40)
            size = models.IntegerField()

            class Meta:
                app_label = 'djanquiltdb'
                unique_together = (('flavour', 'size'),)

            def natural_key(self):
                return (self.flavour, self.size)

        make_the_primary_key_the_natural_key(Wafer)

        self.assertEqual(Wafer._meta.unique_together, (('id',), ('flavour', 'size')))
        self.assertEqual(Wafer(flavour='vanilla', size=3).natural_key(), ('vanilla', 3))

    @isolate_apps('djanquiltdb')
    def test_the_unique_together_is_not_seen_by_the_migrations(self):
        """
        Case: The migration state of a model is built, as makemigrations does, after the model is given its primary key
              as the natural key.
        Expected: The state has no unique_together, so the app of the model does not need a new migration.
        """

        class Wafer(models.Model):
            class Meta:
                app_label = 'djanquiltdb'

        make_the_primary_key_the_natural_key(Wafer)

        self.assertEqual(Wafer._meta.unique_together, (('id',),))
        self.assertNotIn('unique_together', ModelState.from_model(Wafer).options)

    @isolate_apps('djanquiltdb')
    def test_giving_the_natural_key_again_changes_nothing(self):
        """
        Case: A model is given its primary key as the natural key twice, as happens when AppConfig.ready() runs again.
        Expected: unique_together and the class of each manager are the same after the second time as after the first.
        """

        class Wafer(models.Model):
            class Meta:
                app_label = 'djanquiltdb'

        make_the_primary_key_the_natural_key(Wafer)
        unique_together = Wafer._meta.unique_together
        manager_classes = [type(manager) for manager in Wafer._meta.local_managers]

        make_the_primary_key_the_natural_key(Wafer)

        self.assertEqual(Wafer._meta.unique_together, unique_together)
        self.assertEqual([type(manager) for manager in Wafer._meta.local_managers], manager_classes)

    @isolate_apps('djanquiltdb')
    def test_the_model_passes_the_validation_of_public_models(self):
        """
        Case: A public model without a natural key is given its primary key as the natural key, and DjanQuiltDB
              validates the public models.
        Expected: No ImproperlyConfigured.
        """

        class Wafer(models.Model):
            class Meta:
                app_label = 'djanquiltdb'

        make_the_primary_key_the_natural_key(Wafer)
        Wafer._meta._expire_cache()

        with mock.patch('djanquiltdb.apps.get_all_public_models', return_value=[Wafer]):
            _validate_public_models()


class PublicSessionModelNaturalKeyTestCase(SimpleTestCase):
    def keep_the_session_model_as_it_is(self):
        """
        The tests below change the Session model of django.contrib.sessions itself, which every other test shares, so
        each change is undone when the test ends.
        """
        unique_together = Session._meta.unique_together
        manager_classes = {manager: type(manager) for manager in Session._meta.local_managers}

        def restore():
            Session._meta.unique_together = unique_together
            if 'natural_key' in Session.__dict__:
                del Session.natural_key
            for manager, manager_class in manager_classes.items():
                manager.__class__ = manager_class
            Session._meta._expire_cache()

        self.addCleanup(restore)

    def test_djanquiltdb_starts_with_a_public_session_model(self):
        """
        Case: DjanQuiltDB gets ready while the sessions app is public, which the test project's settings only stand in
              for, as they keep the app on a single node for Django's database session backend.
        Expected: No ImproperlyConfigured: the Session model has its session key as the natural key, and its migration
                  state has no unique_together, so the sessions app does not need a new migration.
        """
        self.keep_the_session_model_as_it_is()
        sharding_app = apps.get_app_config(app_label='djanquiltdb')

        with mock.patch('djanquiltdb.natural_keys.get_model_sharding_mode', return_value=ShardingMode.PUBLIC):
            with mock.patch('djanquiltdb.apps.get_all_public_models', return_value=[Session]):
                sharding_app.ready()

        self.assertEqual(Session(session_key='k1').natural_key(), ('k1',))
        self.assertTrue(hasattr(Session.objects, 'get_by_natural_key'))
        self.assertNotIn('unique_together', ModelState.from_model(Session).options)

    def test_a_session_model_that_is_not_public_is_left_unchanged(self):
        """
        Case: The natural keys of the public session models are added while the sessions app is sharded, mirrored or on
              a single node, which need no natural key.
        Expected: The Session model is not given one.
        """
        for mode in [ShardingMode.SHARDED, ShardingMode.MIRRORED, single_node_unsharded(node='default')]:
            with self.subTest(mode=mode):
                with mock.patch('djanquiltdb.natural_keys.get_model_sharding_mode', return_value=mode):
                    with mock.patch('djanquiltdb.natural_keys.make_the_primary_key_the_natural_key') as make:
                        add_natural_keys_to_public_session_models()

                make.assert_not_called()

    def test_nothing_is_added_without_the_sessions_app(self):
        """
        Case: The natural keys of the public session models are added while django.contrib.sessions is not installed.
        Expected: It returns without looking up the sessions app, which does not exist without the app.
        """
        with mock.patch.object(apps, 'is_installed', return_value=False):
            with mock.patch.object(apps, 'get_app_config') as get_app_config:
                add_natural_keys_to_public_session_models()

        get_app_config.assert_not_called()
