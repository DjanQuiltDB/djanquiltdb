import sys

import pghistory
from django.conf import settings
from django.contrib.auth.models import Group
from django.core.exceptions import ImproperlyConfigured
from django.db import models
from django.db.migrations.state import ModelState
from django.test import SimpleTestCase, override_settings
from django.test.utils import isolate_apps
from djanquiltdb import PRIMARY_NODE, ShardingMode
from djanquiltdb.decorators import override_sharding_setting, single_node_unsharded_model
from djanquiltdb.utils import get_declared_placement, get_model_single_node_alias
from pghistory.models import Event

from djanquiltdb_plugin_pghistory import (
    MirroredEvent,
    PublicEvent,
    ShardedEvent,
    SingleNodeEvent,
    create_event_model,
)
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

    @isolate_apps('example')
    def test_a_unique_together_that_the_caller_passes_for_a_public_model_follows_the_natural_key(self):
        """
        Case: Call create_event_model with the PublicEvent base and a unique_together that the caller passes in meta,
              and build the migration state of the generated model, as makemigrations does. The model is concrete,
              because only a concrete model gets the natural key, and it tracks a model of its own with a manual
              tracker, so that no trigger and no tracker label is registered for the models of the example app.
        Expected: The pgh_id natural key comes first in unique_together, where DjanQuiltDB reads the natural key, and
                  the caller's value follows it. Only the caller's value is in the migration state, so only that one
                  becomes a constraint.
        """

        class LabelledRecipe(models.Model):
            class Meta:
                app_label = 'example'

        event_model = create_event_model(
            LabelledRecipe,
            pghistory.ManualEvent('labelled_recipe.noted'),
            base_model=PublicEvent,
            model_name='LabelledRecipeEvent',
            abstract=False,
            meta={'unique_together': [['pgh_id', 'pgh_label']]},
        )
        # pghistory sets a concrete event model on the models module of its app.
        self.addCleanup(delattr, sys.modules['example.models'], 'LabelledRecipeEvent')

        self.assertEqual(event_model._meta.unique_together, (('pgh_id',), ('pgh_id', 'pgh_label')))
        self.assertEqual(ModelState.from_model(event_model).options['unique_together'], {('pgh_id', 'pgh_label')})

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


class SingleNodeEventModelTestCase(SimpleTestCase):
    def model_on_a_single_node(self, node):
        """
        Returns a model on the single node ``node``, defined in an isolated app registry.
        """
        with isolate_apps('example'):

            @single_node_unsharded_model(node=node)
            class LegacyLedger(models.Model):
                class Meta:
                    app_label = 'example'

        return LegacyLedger

    def test_a_model_on_a_single_node_gets_its_events_on_its_node(self):
        """
        Case: Call create_event_model on a model on the node 'other', without an explicit base_model.
        Expected: The event model inherits SingleNodeEvent, and is on the node 'other', as the tracked model is.
        """
        event_model = create_event_model(self.model_on_a_single_node('other'), abstract=True)

        self.assertTrue(issubclass(event_model, SingleNodeEvent))
        self.assertEqual(get_declared_placement(event_model), (ShardingMode.SINGLE_NODE_UNSHARDED, 'other'))
        self.assertEqual(get_model_single_node_alias(event_model), 'other')

    def test_the_primary_node_of_the_tracked_model_is_the_node_of_its_events(self):
        """
        Case: Call create_event_model on a model on PRIMARY_NODE, while PRIMARY_DB_ALIAS is 'other'.
        Expected: The event model is on PRIMARY_NODE as declared, so its node resolves to 'other', as the node of the
                  tracked model does.
        """
        tracked_model = self.model_on_a_single_node(PRIMARY_NODE)
        event_model = create_event_model(tracked_model, abstract=True)

        self.assertEqual(get_declared_placement(event_model), (ShardingMode.SINGLE_NODE_UNSHARDED, PRIMARY_NODE))

        with override_sharding_setting('PRIMARY_DB_ALIAS', 'other'):
            self.assertEqual(get_model_single_node_alias(event_model), 'other')
            self.assertEqual(get_model_single_node_alias(tracked_model), 'other')

    def test_the_attrs_of_the_caller_are_kept(self):
        """
        Case: Call create_event_model on a model on the node 'other', with attrs of the caller.
        Expected: The event model has the attrs of the caller, and is on the node 'other'.
        """
        event_model = create_event_model(
            self.model_on_a_single_node('other'), attrs={'description': 'ledger history'}, abstract=True
        )

        self.assertEqual(event_model.description, 'ledger history')
        self.assertEqual(get_model_single_node_alias(event_model), 'other')

    def test_an_explicit_single_node_base_model_gets_the_node_of_the_tracked_model(self):
        """
        Case: Call create_event_model on a model on the node 'other', with a base_model that inherits SingleNodeEvent.
        Expected: The event model inherits the given base, and is on the node 'other'.
        """

        class SingleNodeEventBaseOfTheProject(SingleNodeEvent):
            class Meta:
                abstract = True

        event_model = create_event_model(
            self.model_on_a_single_node('other'), base_model=SingleNodeEventBaseOfTheProject, abstract=True
        )

        self.assertTrue(issubclass(event_model, SingleNodeEventBaseOfTheProject))
        self.assertEqual(get_model_single_node_alias(event_model), 'other')

    def test_an_explicit_single_node_base_model_is_refused_for_a_model_that_is_not_on_a_single_node(self):
        """
        Case: Call create_event_model with base_model=SingleNodeEvent on Cake, which is sharded, and on a model without
              a mode.
        Expected: ImproperlyConfigured for both: Cake has another mode, and the model without a mode has no node for
                  its events.
        """
        with isolate_apps('example'):

            class ModelWithoutMode(models.Model):
                class Meta:
                    app_label = 'example'

        with self.assertRaisesMessage(ImproperlyConfigured, 'example.Cake is SHARDED'):
            create_event_model(Cake, base_model=SingleNodeEvent, abstract=True)

        with self.assertRaisesMessage(ImproperlyConfigured, 'is not on a single node'):
            create_event_model(ModelWithoutMode, base_model=SingleNodeEvent, abstract=True)
