from django.conf import settings
from django.contrib.auth.models import Group
from django.core.exceptions import ImproperlyConfigured
from django.db import models
from django.test import SimpleTestCase, override_settings
from django.test.utils import isolate_apps
from djanquiltdb import ShardingMode
from pghistory.models import Event

from djanquiltdb_plugin_pghistory import MirroredEvent, PublicEvent, ShardedEvent, create_event_model
from example.models import Bakery, Cake, CakeType, GroupProxy


class BaseModelInferenceTestCase(SimpleTestCase):
    def test_sharded_model_gets_a_sharded_event_base(self):
        """
        Case: Call create_event_model on Cake, which is decorated with @sharded_model(), without an explicit base_model.
        Expected: The generated event model inherits the plugin's ShardedEvent base.
        """
        event_model = create_event_model(Cake, abstract=True)

        self.assertTrue(issubclass(event_model, ShardedEvent))

    def test_mirrored_model_gets_a_mirrored_event_base(self):
        """
        Case: Call create_event_model on Bakery, which is decorated with @mirrored_model(), without an explicit
              base_model.
        Expected: The generated event model inherits the plugin's MirroredEvent base.
        """
        event_model = create_event_model(Bakery, abstract=True)

        self.assertTrue(issubclass(event_model, MirroredEvent))

    def test_public_model_gets_a_public_event_base(self):
        """
        Case: Call create_event_model on CakeType, which is decorated with @public_model(), without an explicit
              base_model.
        Expected: The generated event model inherits the plugin's PublicEvent base.
        """
        event_model = create_event_model(CakeType, abstract=True)

        self.assertTrue(issubclass(event_model, PublicEvent))

    def test_the_mode_comes_from_override_sharding_mode(self):
        """
        Case: Call create_event_model on Group, a third-party model that has no mode decorator, while
              QUILT_DB['OVERRIDE_SHARDING_MODE'] marks its app.
        Expected: The mode comes from the settings entry. The settings mark auth as mirrored, so the event model
                  inherits MirroredEvent.
        """
        event_model = create_event_model(Group, app_label='example', abstract=True)

        self.assertTrue(issubclass(event_model, MirroredEvent))

    def test_an_override_key_with_a_capitalized_model_name_matches_before_ready(self):
        """
        Case: Call create_event_model on Group while the OVERRIDE_SHARDING_MODE key is ('auth', 'Group'), with the
              casing of the settings file. Decorators run before AppConfig.ready() lowercases the keys.
        Expected: The entry matches.
        """
        quilt_db = {**settings.QUILT_DB, 'OVERRIDE_SHARDING_MODE': {('auth', 'Group'): ShardingMode.MIRRORED}}

        with override_settings(QUILT_DB=quilt_db):
            event_model = create_event_model(Group, app_label='example', abstract=True)

        self.assertTrue(issubclass(event_model, MirroredEvent))

    def test_an_auto_created_model_gets_the_mode_of_the_model_that_caused_its_creation(self):
        """
        Case: Call create_event_model on the through model of Cake.toppings, which Django creates automatically without
              a mode of its own.
        Expected: The mode comes from the model that declares the many-to-many field. Cake is sharded, so the event
                  model inherits ShardedEvent.
        """
        event_model = create_event_model(Cake.toppings.through, app_label='example', abstract=True)

        self.assertTrue(issubclass(event_model, ShardedEvent))

    def test_a_proxy_gets_the_mode_of_its_concrete_model(self):
        """
        Case: Call create_event_model on GroupProxy, a proxy in the example app, which has no mode. Its concrete model
              Group is in auth, which has an OVERRIDE_SHARDING_MODE entry.
        Expected: The mode comes from the concrete model, so the event model inherits MirroredEvent.
        """
        event_model = create_event_model(GroupProxy, abstract=True)

        self.assertTrue(issubclass(event_model, MirroredEvent))

    @isolate_apps('example')
    def test_a_model_without_a_mode_raises_improperly_configured(self):
        """
        Case: Call create_event_model on a model with no mode decorator, no OVERRIDE_SHARDING_MODE entry and no explicit
              base_model.
        Expected: ImproperlyConfigured, which names the model and the three ways to give it a mode: a mode decorator, an
                  OVERRIDE_SHARDING_MODE entry and base_model.
        """

        class ModelWithoutMode(models.Model):
            class Meta:
                app_label = 'example'

        with self.assertRaisesMessage(ImproperlyConfigured, 'ModelWithoutMode') as raised:
            create_event_model(ModelWithoutMode, abstract=True)

        self.assertIn('@sharded_model()', str(raised.exception))
        self.assertIn('OVERRIDE_SHARDING_MODE', str(raised.exception))
        self.assertIn('base_model', str(raised.exception))

    def test_explicit_base_model_must_inherit_a_plugin_base(self):
        """
        Case: Call create_event_model with a base_model that inherits pghistory's Event, but none of the plugin's bases
              that have a sharding mode.
        Expected: ImproperlyConfigured. Such an event model would have no sharding mode, and its queryset would break
                  under the sharded queryset wrapper.
        """

        class BaseWithoutShardingMode(Event):
            class Meta:
                abstract = True

        with self.assertRaises(ImproperlyConfigured):
            create_event_model(Cake, base_model=BaseWithoutShardingMode, abstract=True)

    def test_explicit_base_model_is_checked_against_the_mode_of_the_model(self):
        """
        Case: Call create_event_model on Bakery, which is mirrored, with a base_model that inherits ShardedEvent.
        Expected: ImproperlyConfigured. The events would be stored in the shards, while Bakery rows are stored in the
                  public schema of every node.
        """

        class ShardedEventBaseForAMirroredModel(ShardedEvent):
            class Meta:
                abstract = True

        with self.assertRaises(ImproperlyConfigured):
            create_event_model(Bakery, base_model=ShardedEventBaseForAMirroredModel, abstract=True)

    def test_an_explicit_base_model_that_matches_the_mode_is_used(self):
        """
        Case: Call create_event_model on Cake, which is decorated with @sharded_model(), with a base_model that inherits
              ShardedEvent.
        Expected: The event model inherits the given base.
        """

        class ShardedEventBaseOfTheProject(ShardedEvent):
            class Meta:
                abstract = True

        event_model = create_event_model(Cake, base_model=ShardedEventBaseOfTheProject, abstract=True)

        self.assertTrue(issubclass(event_model, ShardedEventBaseOfTheProject))

    def test_a_unique_together_that_the_caller_passes_for_a_public_model_is_kept(self):
        """
        Case: Call create_event_model on CakeType, a public model, with a unique_together that the caller passes in
              meta.
        Expected: The caller's value is kept. The pgh_id unique_together is only added when meta has none.
        """
        event_model = create_event_model(CakeType, abstract=True, meta={'unique_together': [['pgh_id', 'pgh_label']]})

        self.assertEqual(event_model._meta.unique_together, (('pgh_id', 'pgh_label'),))

    def test_explicit_base_model_is_accepted_for_a_model_without_a_mode(self):
        """
        Case: Call create_event_model with an explicit MirroredEvent base, on a model that has no mode.
        Expected: The base is used as given, so the explicit tracking functions work on models without a mode.
        """
        with isolate_apps('example'):

            class ModelWithoutMode(models.Model):
                class Meta:
                    app_label = 'example'

            event_model = create_event_model(ModelWithoutMode, base_model=MirroredEvent, abstract=True)

        self.assertTrue(issubclass(event_model, MirroredEvent))
