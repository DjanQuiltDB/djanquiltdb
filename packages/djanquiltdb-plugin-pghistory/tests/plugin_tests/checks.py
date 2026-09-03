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
from example.models import Bakery

#: The events of a mirrored model, generated into the pghistory app, which OVERRIDE_SHARDING_MODE marks as sharded.
MisplacedEvent = create_event_model(
    Bakery,
    pghistory.InsertEvent('bakery.misplaced_insert'),
    app_label='pghistory',
    model_name='MisplacedEvent',
    abstract=True,
)


def configuration_check_ids():
    return [warning.id for warning in check_pghistory_sharding_configuration(None)]


def tracked_mode_check_ids(event_models):
    with mock.patch.object(apps, 'get_models', return_value=list(event_models)):
        return [error.id for error in check_event_bases_match_tracked_modes(None)]


class PghistoryShardingConfigurationCheckTestCase(SimpleTestCase):
    def test_check_is_registered(self):
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

    def test_a_foreign_key_context_field_beside_sharded_events_is_warned_about(self):
        """
        Case: PGHISTORY_CONTEXT_FIELD is left a foreign key, pghistory's default, while sharded event models
              exist.
        Expected: The context-field warning: an event table inside a shard cannot hold a foreign key to a context
                  table outside it.
        """
        with override_settings(PGHISTORY_CONTEXT_FIELD=pghistory.ContextForeignKey()):
            self.assertEqual(configuration_check_ids(), ['djanquiltdb_plugin_pghistory.W001'])

    def test_an_unplaced_context_model_is_warned_about(self):
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
