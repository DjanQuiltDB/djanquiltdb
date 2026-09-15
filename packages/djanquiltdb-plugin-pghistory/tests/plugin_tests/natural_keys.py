import uuid
from unittest import mock

from django.apps import apps
from django.db.migrations.state import ModelState
from django.test import SimpleTestCase
from djanquiltdb import State
from djanquiltdb.apps import _validate_public_models
from djanquiltdb.testing import ShardingTransactionTestCase
from djanquiltdb.utils import create_template_schema, use_shard
from pghistory.models import Context, Events, MiddlewareEvents

from djanquiltdb_plugin_pghistory.natural_keys import add_natural_keys_to_pghistory_models
from example.models import Shard


class PghistoryModelNaturalKeyTestCase(SimpleTestCase):
    def test_the_natural_key_of_a_context_is_its_id(self):
        """
        Case: natural_key() is called on a Context.
        Expected: It returns a tuple with the id of the context, a UUID that is unique on every node.
        """
        context_id = uuid.uuid4()

        self.assertEqual(Context(id=context_id).natural_key(), (context_id,))

    def test_the_natural_key_of_an_aggregated_event_is_its_slug(self):
        """
        Case: natural_key() is called on an Events row.
        Expected: It returns a tuple with the pgh_slug of the event, which names the event model and the id of the event.
        """
        self.assertEqual(Events(pgh_slug='example.CakeEvent:1').natural_key(), ('example.CakeEvent:1',))

    def test_the_models_of_the_pghistory_app_pass_the_validation_of_public_models(self):
        """
        Case: DjanQuiltDB validates the models of the pghistory app as public models, which need unique_together,
              natural_key() and a default manager with get_by_natural_key(). Like DjanQuiltDB, the test leaves out the
              proxy model MiddlewareEvents.
        Expected: No ImproperlyConfigured.
        """
        with mock.patch('djanquiltdb.apps.get_all_public_models', return_value=[Context, Events]):
            _validate_public_models()

    def test_a_proxy_model_inherits_the_natural_key_of_its_concrete_model(self):
        """
        Case: natural_key() is called on a MiddlewareEvents row, a proxy of Events that has no unique_together of its
              own.
        Expected: It returns the pgh_slug of the event, like Events does.
        """
        self.assertEqual(MiddlewareEvents(pgh_slug='example.CakeEvent:1').natural_key(), ('example.CakeEvent:1',))
        self.assertEqual(MiddlewareEvents._meta.unique_together, ())

    def test_the_unique_together_of_the_pghistory_models_is_not_seen_by_the_migrations(self):
        """
        Case: The migration state of Context and Events is built, as makemigrations does.
        Expected: Neither has a unique_together, so the pghistory app does not need a new migration.
        """
        self.assertEqual(Context._meta.unique_together, (('id',),))
        self.assertEqual(Events._meta.unique_together, (('pgh_slug',),))

        for model in [Context, Events]:
            with self.subTest(model=model.__name__):
                self.assertNotIn('unique_together', ModelState.from_model(model).options)

    def test_every_manager_keeps_get_by_natural_key_when_the_app_registry_cache_is_cleared(self):
        """
        Case: The cache of the app registry is cleared, as registering a model does. This replaces the copies of the
              managers that the models hand out.
        Expected: Every manager of Context, Events and MiddlewareEvents still has get_by_natural_key().
        """
        apps.clear_cache()

        for model in [Context, Events, MiddlewareEvents]:
            for manager in model._meta.managers:
                with self.subTest(model=model.__name__, manager=manager.name):
                    self.assertTrue(hasattr(manager, 'get_by_natural_key'))

    def test_adding_the_natural_keys_again_keeps_the_manager_classes(self):
        """
        Case: add_natural_keys_to_pghistory_models() runs a second time, as it does when AppConfig.ready() runs again.
        Expected: The class of each manager is unchanged.
        """
        manager_classes = [type(manager) for manager in Events._meta.managers]

        add_natural_keys_to_pghistory_models()

        self.assertEqual([type(manager) for manager in Events._meta.managers], manager_classes)

    def test_nothing_is_added_without_the_pghistory_app(self):
        """
        Case: add_natural_keys_to_pghistory_models() runs while the pghistory app is not installed.
        Expected: It returns without looking up the pghistory app, which does not exist without the app.
        """
        with mock.patch.object(apps, 'is_installed', return_value=False):
            with mock.patch.object(apps, 'get_app_config') as get_app_config:
                add_natural_keys_to_pghistory_models()

        get_app_config.assert_not_called()


class ContextLookupByNaturalKeyTestCase(ShardingTransactionTestCase):
    available_apps = ['djanquiltdb', 'example', 'pghistory', 'django.contrib.auth', 'django.contrib.contenttypes']

    def setUp(self):
        super().setUp()
        create_template_schema()

        self.shard = Shard.objects.create(
            alias='one', schema_name='one_schema', node_name='default', state=State.ACTIVE
        )

    def test_a_context_is_found_by_its_natural_key(self):
        """
        Case: A Context row is created inside a shard, which reaches the Context table whether it is sharded or public,
              and is looked up with get_by_natural_key().
        Expected: The lookup returns that row.
        """
        with use_shard(self.shard):
            context = Context.objects.create(metadata={'baked_by': 'ann'})

            self.assertEqual(Context.objects.get_by_natural_key(context.id), context)
