from unittest import mock

import pghistory
from django.apps import apps
from django.conf import settings
from django.core.checks.registry import registry
from django.db import models
from django.test import SimpleTestCase, modify_settings, override_settings
from django.test.utils import isolate_apps
from djanquiltdb import ShardingMode, single_node_unsharded
from djanquiltdb.decorators import mirrored_model, single_node_unsharded_model

from djanquiltdb_plugin_pghistory import MirroredEvent, ShardedEvent, create_event_model
from djanquiltdb_plugin_pghistory.checks import (
    check_event_bases_match_tracked_modes,
    check_event_models_are_placed_where_their_base_expects,
    check_pghistory_sharding_configuration,
)
from example.models import Bakery, Cake, CakeEvent, CakeType, Croissant, WaffleEvent
from plugin_tests.context_placement import context_table_placement, run_only_when_the_context_table_is

#: Event models whose tables are outside the shards, and that have a foreign key to the Context table. They are
#: abstract, because the Context table is sharded in the run with config.settings, so they cannot reach it.
#: The check only sees them through a patched registry.
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

#: Event models that plain pghistory generates, as a third-party app does, so they inherit PGHISTORY_BASE_MODEL and none
#: of the plugin's event bases. One has a foreign key to the Context table, and the other has no context.
PlainContextEvent = pghistory.create_event_model(
    Cake,
    pghistory.InsertEvent('cake.plain_context_insert'),
    context_field=pghistory.ContextForeignKey(),
    model_name='PlainContextEvent',
    abstract=True,
)

PlainNoContextEvent = pghistory.create_event_model(
    Cake,
    pghistory.InsertEvent('cake.plain_contextless_insert'),
    context_field=None,
    model_name='PlainNoContextEvent',
    abstract=True,
)

#: The events of a mirrored model, generated into the pghistory app. The test that uses it makes the app sharded.
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

    def test_the_settings_of_the_test_project_report_the_warning_for_its_normalized_context(self):
        """
        Case: The test project uses the denormalized context for the whole project, but the call sites of Waffle, Danish
              and Strudel, and in the runs with a public or mirrored Context table those of Kitchen and Recipe, ask for
              a context foreign key.
        Expected: Only the warning about a normalized context for the sharding mode of the Context table in this run.
                  The statement-level triggers of Proofer and Grain report no error, because the Context table is not
                  sharded in the runs that install them.
        """
        expected_by_context_mode = {
            ShardingMode.SHARDED: ['djanquiltdb_plugin_pghistory.W005'],
            ShardingMode.PUBLIC: ['djanquiltdb_plugin_pghistory.W004'],
            ShardingMode.MIRRORED: ['djanquiltdb_plugin_pghistory.W003'],
        }

        self.assertEqual(configuration_check_ids(), expected_by_context_mode[context_table_placement()])

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
              for normalized event models that are sharded, public, mirrored, or of different modes, and for normalized
              event models that plain pghistory generates. Each case also has an event model that records no context.
        Expected: Each combination reports exactly the expected messages, and no message twice for the same event model.
                  Only a public Context table with a denormalized context reports nothing.
        """
        denormalized_event_models = [CakeEvent, NoContextEvent]
        normalized_sharded_event_models = [WaffleEvent, NoContextEvent]
        normalized_public_event_models = [PublicContextEvent, NoContextEvent]
        normalized_mirrored_event_models = [MirroredContextEvent, NoContextEvent]
        normalized_event_models_of_different_modes = [WaffleEvent, PublicContextEvent, MirroredContextEvent]
        normalized_plain_pghistory_event_models = [PlainContextEvent, PlainNoContextEvent]

        cases = [
            # An event model that plain pghistory generates counts like one that the plugin generates.
            (ShardingMode.SHARDED, normalized_plain_pghistory_event_models, ['djanquiltdb_plugin_pghistory.W005']),
            (ShardingMode.PUBLIC, normalized_plain_pghistory_event_models, ['djanquiltdb_plugin_pghistory.W004']),
            (ShardingMode.MIRRORED, normalized_plain_pghistory_event_models, ['djanquiltdb_plugin_pghistory.W003']),
            # No event model has a foreign key to the table.
            (ShardingMode.PUBLIC, denormalized_event_models, []),
            (ShardingMode.MIRRORED, denormalized_event_models, ['djanquiltdb_plugin_pghistory.W001']),
            (ShardingMode.SHARDED, denormalized_event_models, ['djanquiltdb_plugin_pghistory.W001']),
            # A public table works for every event model, but stores the context outside the shards.
            (ShardingMode.PUBLIC, normalized_sharded_event_models, ['djanquiltdb_plugin_pghistory.W004']),
            (ShardingMode.PUBLIC, normalized_public_event_models, ['djanquiltdb_plugin_pghistory.W004']),
            (ShardingMode.PUBLIC, normalized_mirrored_event_models, ['djanquiltdb_plugin_pghistory.W004']),
            (ShardingMode.PUBLIC, normalized_event_models_of_different_modes, ['djanquiltdb_plugin_pghistory.W004']),
            # A mirrored table works, but only if the project keeps this table in sync itself.
            (ShardingMode.MIRRORED, normalized_sharded_event_models, ['djanquiltdb_plugin_pghistory.W003']),
            (ShardingMode.MIRRORED, normalized_public_event_models, ['djanquiltdb_plugin_pghistory.W003']),
            (ShardingMode.MIRRORED, normalized_mirrored_event_models, ['djanquiltdb_plugin_pghistory.W003']),
            (ShardingMode.MIRRORED, normalized_event_models_of_different_modes, ['djanquiltdb_plugin_pghistory.W003']),
            # A sharded table works only when every event model that has a foreign key to the table is sharded.
            (ShardingMode.SHARDED, normalized_sharded_event_models, ['djanquiltdb_plugin_pghistory.W005']),
            (ShardingMode.SHARDED, normalized_public_event_models, ['djanquiltdb_plugin_pghistory.E002']),
            (ShardingMode.SHARDED, normalized_mirrored_event_models, ['djanquiltdb_plugin_pghistory.E002']),
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
        with override_settings(QUILT_DB=quilt_db_with_app_sharding_mode('pghistory', ShardingMode.SHARDED)):
            with mock.patch.object(apps, 'get_models', return_value=[PublicContextEvent, MirroredContextEvent]):
                errors = check_pghistory_sharding_configuration(None)

        self.assertEqual([error.obj for error in errors], [PublicContextEvent, MirroredContextEvent])
        self.assertIn('PUBLIC', errors[0].msg)
        self.assertIn('MIRRORED', errors[1].msg)

    def test_a_context_model_on_a_single_node_reports_the_event_models_that_cannot_reach_it(self):
        """
        Case: The pghistory app is on the single node 'default'. The event models denormalize their context, or have a
              foreign key to the Context table: WaffleEvent, which is sharded, and event models on the nodes 'default'
              and 'other'.
        Expected: Nothing for a denormalized context and for the event model on 'default'. djanquiltdb_plugin_pghistory
                  .E005 for WaffleEvent and for the event model on 'other', which names the node of the Context table.
        """
        with isolate_apps('example'):

            @single_node_unsharded_model(node='default')
            class LedgerOnDefault(models.Model):
                class Meta:
                    app_label = 'example'

            @single_node_unsharded_model(node='other')
            class LedgerOnOther(models.Model):
                class Meta:
                    app_label = 'example'

        event_model_on_default = create_event_model(
            LedgerOnDefault,
            pghistory.InsertEvent('ledger_on_default.context_insert'),
            context_field=pghistory.ContextForeignKey(),
            abstract=True,
        )
        event_model_on_other = create_event_model(
            LedgerOnOther,
            pghistory.InsertEvent('ledger_on_other.context_insert'),
            context_field=pghistory.ContextForeignKey(),
            abstract=True,
        )
        cases = [
            ([CakeEvent, NoContextEvent], []),
            ([event_model_on_default], []),
            ([WaffleEvent], ['djanquiltdb_plugin_pghistory.E005']),
            ([event_model_on_other], ['djanquiltdb_plugin_pghistory.E005']),
        ]

        quilt_db = quilt_db_with_app_sharding_mode('pghistory', single_node_unsharded(node='default'))

        for event_models, expected in cases:
            with self.subTest(events=[model.__name__ for model in event_models]):
                with override_settings(QUILT_DB=quilt_db):
                    self.assertEqual(configuration_check_ids_for_event_models(event_models), expected)

        with override_settings(QUILT_DB=quilt_db), mock.patch.object(apps, 'get_models', return_value=[WaffleEvent]):
            [error] = check_pghistory_sharding_configuration(None)

        self.assertIn('example.WaffleEvent has a foreign key to the Context model', error.msg)
        self.assertIn("not on the node 'default'", error.msg)

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

    def test_each_warning_names_the_sharding_mode_and_says_what_to_change(self):
        """
        Case: The warning for each placement of the Context table is read, with an event model that denormalizes its
              context, and with WaffleEvent, which has a foreign key to the table.
        Expected: Each message names the one sharding mode that the Context table has, and the warnings for a
                  normalized context name WaffleEvent. Each hint says to denormalize the context when an event model
                  has a foreign key to the table, and to make the app public when it is not.
        """
        cases = [
            (ShardingMode.SHARDED, [CakeEvent], 'djanquiltdb_plugin_pghistory.W001'),
            (ShardingMode.MIRRORED, [CakeEvent], 'djanquiltdb_plugin_pghistory.W001'),
            (ShardingMode.SHARDED, [WaffleEvent], 'djanquiltdb_plugin_pghistory.W005'),
            (ShardingMode.MIRRORED, [WaffleEvent], 'djanquiltdb_plugin_pghistory.W003'),
            (ShardingMode.PUBLIC, [WaffleEvent], 'djanquiltdb_plugin_pghistory.W004'),
        ]

        for context_mode, event_models, expected_id in cases:
            with self.subTest(context=context_mode.name, events=[model.__name__ for model in event_models]):
                with override_settings(QUILT_DB=quilt_db_with_app_sharding_mode('pghistory', context_mode)):
                    with mock.patch.object(apps, 'get_models', return_value=event_models):
                        [warning] = check_pghistory_sharding_configuration(None)

                self.assertEqual(warning.id, expected_id)
                self.assertIn(f'The pghistory Context model is {context_mode.name}', warning.msg)

                for other_mode in {ShardingMode.SHARDED, ShardingMode.MIRRORED, ShardingMode.PUBLIC} - {context_mode}:
                    self.assertNotIn(other_mode.name, warning.msg)

                if event_models == [WaffleEvent]:
                    self.assertIn('example.WaffleEvent', warning.msg)
                    self.assertIn('PGHISTORY_CONTEXT_FIELD = pghistory.ContextJSONField()', warning.hint)

                if context_mode is not ShardingMode.PUBLIC:
                    self.assertIn("{('pghistory',): ShardingMode.PUBLIC}", warning.hint)


@run_only_when_the_context_table_is(ShardingMode.PUBLIC, ShardingMode.MIRRORED)
class TriggersOutsideTheShardsCheckTestCase(SimpleTestCase):
    """
    The public and mirrored tracked models of unsharded_context_example are only installed in the runs in which the
    Context table is not sharded, so these tests make it sharded through override_settings.
    """

    def test_every_tracked_model_and_context_placement_reports_the_expected_errors(self):
        """
        Case: The check runs for each placement of the Context table, over a sharded, a mirrored and a public model that
              are tracked at row level with a denormalized context (Cake, Bakery, CakeType), at row level with a
              context foreign key (Kitchen, Recipe), and at statement level (Croissant, Proofer, Grain).
        Expected: Only a sharded Context table reports E004, once for each public or mirrored model whose triggers call
                  _pgh_attach_context(): Kitchen, Recipe, Proofer and Grain. The hint says to make the app public, or
                  to track the model at row level with a denormalized context, which also applies to Kitchen and
                  Recipe, which are already tracked at row level.
        """
        from unsharded_context_example.models import Grain, Kitchen, Proofer, Recipe

        tracked_models = [Cake, Bakery, CakeType, Kitchen, Recipe, Croissant, Proofer, Grain]

        for context_mode in [ShardingMode.SHARDED, ShardingMode.MIRRORED, ShardingMode.PUBLIC]:
            with self.subTest(context=context_mode.name):
                with override_settings(QUILT_DB=quilt_db_with_app_sharding_mode('pghistory', context_mode)):
                    with mock.patch.object(apps, 'get_models', return_value=tracked_models):
                        errors = [
                            message
                            for message in check_pghistory_sharding_configuration(None)
                            if message.id == 'djanquiltdb_plugin_pghistory.E004'
                        ]

                expected = [Kitchen, Recipe, Proofer, Grain] if context_mode is ShardingMode.SHARDED else []

                self.assertEqual([error.obj for error in errors], expected)

                for error in errors:
                    self.assertIn("{('pghistory',): ShardingMode.PUBLIC}", error.hint)
                    self.assertIn('at row level with a denormalized context', error.hint)


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
        with override_settings(QUILT_DB=quilt_db_with_app_sharding_mode('pghistory', ShardingMode.SHARDED)):
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
        Case: An abstract event model that is declared by hand as a subclass of ShardedEvent, and that tracks no model,
              is checked.
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
