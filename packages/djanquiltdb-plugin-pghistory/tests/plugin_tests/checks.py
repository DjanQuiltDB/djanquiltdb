from unittest import mock

import pghistory
from django.apps import apps
from django.conf import settings
from django.core.checks.registry import registry
from django.db import models
from django.test import SimpleTestCase, modify_settings, override_settings
from django.test.utils import isolate_apps
from djanquiltdb import ShardingMode
from djanquiltdb.decorators import mirrored_model

from djanquiltdb_plugin_pghistory import MirroredEvent, ShardedEvent, create_event_model
from djanquiltdb_plugin_pghistory.checks import (
    check_event_bases_match_tracked_modes,
    check_event_models_are_placed_where_their_base_expects,
    check_pghistory_sharding_configuration,
)
from example.models import Bakery, Cake, CakeEvent, CakeType, WaffleEvent

#: Event models whose tables are outside the shards, and that have a foreign key to the Context table. They are
#: abstract, because they cannot reach the Context table in the shards of the test settings. The check only sees them
#: through a patched registry.
PublicContextEvent = create_event_model(
    CakeType,
    pghistory.InsertEvent('caketype.context_insert'),
    context_field=pghistory.ContextForeignKey(),
    model_name='PublicContextEvent',
    abstract=True,
)

MirroredContextEvent = create_event_model(
    Bakery,
    pghistory.InsertEvent('bakery.context_insert'),
    context_field=pghistory.ContextForeignKey(),
    model_name='MirroredContextEvent',
    abstract=True,
)

#: An event model without a context (context_field=None), which does not limit where the Context table can be placed.
NoContextEvent = create_event_model(
    Cake,
    pghistory.InsertEvent('cake.contextless_insert'),
    context_field=None,
    model_name='NoContextEvent',
    abstract=True,
)

#: The events of a mirrored model, generated into the pghistory app, which OVERRIDE_SHARDING_MODE marks as sharded.
MisplacedEvent = create_event_model(
    Bakery,
    pghistory.InsertEvent('bakery.misplaced_insert'),
    app_label='pghistory',
    model_name='MisplacedEvent',
    abstract=True,
)


def configuration_check_ids():
    return [message.id for message in check_pghistory_sharding_configuration(None)]


def configuration_check_ids_for_event_models(event_models):
    with mock.patch.object(apps, 'get_models', return_value=list(event_models)):
        return configuration_check_ids()


def tracked_mode_check_ids(event_models):
    with mock.patch.object(apps, 'get_models', return_value=list(event_models)):
        return [error.id for error in check_event_bases_match_tracked_modes(None)]


def quilt_db_with_app_sharding_mode(app_label, mode):
    override = {**settings.QUILT_DB['OVERRIDE_SHARDING_MODE'], (app_label,): mode}

    return {**settings.QUILT_DB, 'OVERRIDE_SHARDING_MODE': override}


class PghistoryShardingConfigurationCheckTestCase(SimpleTestCase):
    def test_the_sharding_configuration_check_is_registered(self):
        """
        Case: The plugin's install() has run.
        Expected: check_pghistory_sharding_configuration is part of Django's check registry.
        """
        self.assertIn(check_pghistory_sharding_configuration, registry.get_checks())

    def test_the_settings_of_the_test_project_report_no_messages(self):
        """
        Case: The test project uses the denormalized context for the whole project, and places pghistory's Context model
              through an OVERRIDE_SHARDING_MODE entry.
        Expected: No messages.
        """
        self.assertEqual(configuration_check_ids(), [])

    def test_the_sharding_configuration_check_does_nothing_without_the_pghistory_app(self):
        """
        Case: The pghistory app is not installed, whatever the other settings are.
        Expected: No messages. Without the app, there are no event models and no Context model to place.
        """
        with modify_settings(INSTALLED_APPS={'remove': 'pghistory'}):
            with override_settings(PGHISTORY_CONTEXT_FIELD=pghistory.ContextForeignKey()):
                self.assertEqual(configuration_check_ids(), [])

    def test_every_placement_of_the_context_model_reports_the_expected_messages(self):
        """
        Case: The Context model is placed in each of the three modes, for event models that denormalize their context,
              for normalized event models that are all sharded, and for normalized event models of different modes.
              Each case also has an event model that records no context.
        Expected: Each combination reports exactly the expected messages, and no message twice for the same event model.
        """
        denormalized_event_models = [CakeEvent, NoContextEvent]
        normalized_sharded_event_models = [WaffleEvent, NoContextEvent]
        normalized_event_models_of_different_modes = [WaffleEvent, PublicContextEvent, MirroredContextEvent]

        cases = [
            # No event model reads the table.
            (ShardingMode.PUBLIC, denormalized_event_models, []),
            (ShardingMode.MIRRORED, denormalized_event_models, ['djanquiltdb_plugin_pghistory.W001']),
            (ShardingMode.SHARDED, denormalized_event_models, ['djanquiltdb_plugin_pghistory.W001']),
            # The events have a foreign key to the table.
            (ShardingMode.PUBLIC, normalized_sharded_event_models, []),
            (ShardingMode.PUBLIC, normalized_event_models_of_different_modes, []),
            # Mirrored works, but only if the project keeps this table in sync itself.
            (ShardingMode.MIRRORED, normalized_sharded_event_models, ['djanquiltdb_plugin_pghistory.W003']),
            (ShardingMode.MIRRORED, normalized_event_models_of_different_modes, ['djanquiltdb_plugin_pghistory.W003']),
            # Sharded works only when every event model that has a foreign key to the table is sharded.
            (ShardingMode.SHARDED, normalized_sharded_event_models, []),
            (
                ShardingMode.SHARDED,
                normalized_event_models_of_different_modes,
                ['djanquiltdb_plugin_pghistory.E002', 'djanquiltdb_plugin_pghistory.E002'],
            ),
        ]

        for context_mode, event_models, expected in cases:
            with self.subTest(context=context_mode.name, events=[model.__name__ for model in event_models]):
                with override_settings(QUILT_DB=quilt_db_with_app_sharding_mode('pghistory', context_mode)):
                    self.assertEqual(configuration_check_ids_for_event_models(event_models), expected)

    def test_the_error_names_each_event_model_that_cannot_reach_the_context_table(self):
        """
        Case: Two event models with different modes have a foreign key to a sharded Context table.
        Expected: One error for each event model, which names the model and its mode.
        """
        with mock.patch.object(apps, 'get_models', return_value=[PublicContextEvent, MirroredContextEvent]):
            errors = check_pghistory_sharding_configuration(None)

        self.assertEqual([error.obj for error in errors], [PublicContextEvent, MirroredContextEvent])
        self.assertIn('PUBLIC', errors[0].msg)
        self.assertIn('MIRRORED', errors[1].msg)

    def test_a_context_model_without_a_sharding_mode_is_reported(self):
        """
        Case: The pghistory app has no OVERRIDE_SHARDING_MODE entry, so its Context model has no sharding mode.
        Expected: The W002 warning.
        """
        quilt_db = {
            **settings.QUILT_DB,
            'OVERRIDE_SHARDING_MODE': {
                ('auth',): ShardingMode.MIRRORED,
                ('contenttypes',): ShardingMode.MIRRORED,
            },
        }

        with override_settings(QUILT_DB=quilt_db):
            self.assertEqual(configuration_check_ids(), ['djanquiltdb_plugin_pghistory.W002'])


class EventModelPlacementCheckTestCase(SimpleTestCase):
    def base_placement_check_ids(self, event_models):
        with mock.patch.object(apps, 'get_models', return_value=list(event_models)):
            return [error.id for error in check_event_models_are_placed_where_their_base_expects(None)]

    def test_the_event_model_placement_check_is_registered(self):
        """
        Case: The plugin's install() has run.
        Expected: check_event_models_are_placed_where_their_base_expects is part of Django's check registry.
        """
        self.assertIn(check_event_models_are_placed_where_their_base_expects, registry.get_checks())

    def test_the_event_models_of_the_example_app_are_placed_as_their_bases_expect(self):
        """
        Case: The check runs over the real registry. All of its event models are generated into the example app, which
              has no OVERRIDE_SHARDING_MODE entry.
        Expected: No errors. The mode of each event model comes from the marker of its base.
        """
        self.assertEqual(check_event_models_are_placed_where_their_base_expects(None), [])

    def test_an_event_model_generated_into_an_app_with_a_different_mode_is_reported(self):
        """
        Case: The events of a mirrored model are generated into the pghistory app, which the settings mark as sharded.
        Expected: The E003 error.
        """
        self.assertEqual(self.base_placement_check_ids([MisplacedEvent]), ['djanquiltdb_plugin_pghistory.E003'])

    def test_the_event_model_placement_check_does_nothing_without_the_pghistory_app(self):
        """
        Case: The pghistory app is not installed.
        Expected: No errors, because the check returns before it looks at any model.
        """
        with modify_settings(INSTALLED_APPS={'remove': 'pghistory'}):
            self.assertEqual(self.base_placement_check_ids([MisplacedEvent]), [])


class EventBaseModeCheckTestCase(SimpleTestCase):
    def test_the_tracked_mode_check_is_registered(self):
        """
        Case: The plugin's install() has run.
        Expected: check_event_bases_match_tracked_modes is part of Django's check registry.
        """
        self.assertIn(check_event_bases_match_tracked_modes, registry.get_checks())

    def test_the_event_models_of_the_example_app_match_their_tracked_models(self):
        """
        Case: The check runs over the real registry, which has Bakery with its mode decorator above track_mirrored().
        Expected: No errors.
        """
        self.assertEqual(check_event_bases_match_tracked_modes(None), [])

    @isolate_apps('example')
    def test_a_mirrored_mode_applied_after_a_sharded_event_model_is_created_is_reported(self):
        """
        Case: An event model with the sharded base is created for a model before the model has a mode, and the mirrored
              mode decorator is applied afterwards, as happens when the mode decorator is above the tracking decorator.
        Expected: The E001 error, once the registry has the finished model.
        """

        class MirroredAfterTracking(models.Model):
            class Meta:
                app_label = 'example'

        event_model = create_event_model(MirroredAfterTracking, base_model=ShardedEvent, abstract=True)
        mirrored_model()(MirroredAfterTracking)

        self.assertEqual(tracked_mode_check_ids([event_model]), ['djanquiltdb_plugin_pghistory.E001'])

    @isolate_apps('example')
    def test_a_tracked_model_without_a_mode_is_not_reported(self):
        """
        Case: An event model is created with the mirrored base for a model that has no declared mode.
        Expected: No error, because there is no mode that the base could contradict.
        """

        class ModelWithoutMode(models.Model):
            class Meta:
                app_label = 'example'

        self.assertEqual(
            tracked_mode_check_ids([create_event_model(ModelWithoutMode, base_model=MirroredEvent, abstract=True)]), []
        )

    def test_an_event_model_that_tracks_no_model_is_not_reported(self):
        """
        Case: An abstract event model that is declared by hand as a subclass of ShardedEvent, and that tracks no model, is
              checked.
        Expected: No error, because there is no tracked model whose mode could be different.
        """

        class EventModelWithoutTrackedModel(ShardedEvent):
            class Meta:
                abstract = True

        self.assertEqual(tracked_mode_check_ids([EventModelWithoutTrackedModel]), [])

    def test_the_tracked_mode_check_does_nothing_without_the_pghistory_app(self):
        """
        Case: The pghistory app is not installed.
        Expected: No errors, because the check returns before it looks at any model.
        """
        with modify_settings(INSTALLED_APPS={'remove': 'pghistory'}):
            self.assertEqual(check_event_bases_match_tracked_modes(None), [])
