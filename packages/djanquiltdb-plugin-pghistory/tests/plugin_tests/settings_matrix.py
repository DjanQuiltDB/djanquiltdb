import pghistory
import pgtrigger
from django.core.exceptions import FieldDoesNotExist
from django.db import models
from django.test import SimpleTestCase, override_settings

from djanquiltdb_plugin_pghistory import MirroredEvent, PublicEvent, ShardedEvent, create_event_model
from example.models import Bakery, Cake, CakeType


class SettingsReachTheGeneratedModelTestCase(SimpleTestCase):
    """
    Covers the pghistory settings that the rest of the suite does not use. Each one has to reach the generated model as
    it does without the plugin.
    """

    def create_abstract_event_model(self, tracked_model=Cake, **kwargs):
        return create_event_model(tracked_model, abstract=True, **kwargs)

    def test_default_trackers_decide_which_events_exist(self):
        """
        Case: PGHISTORY_DEFAULT_TRACKERS names a delete tracker next to the insert and update trackers, and a call site
              names no trackers of its own.
        Expected: The generated model has all three trackers, and is based on ShardedEvent, the base for the mode of
                  Cake.
        """
        trackers = [pghistory.InsertEvent(), pghistory.UpdateEvent(), pghistory.DeleteEvent()]

        with override_settings(PGHISTORY_DEFAULT_TRACKERS=trackers):
            event_model = self.create_abstract_event_model()

        self.assertEqual({tracker.label for tracker in event_model.pgh_trackers}, {'insert', 'update', 'delete'})
        self.assertTrue(issubclass(event_model, ShardedEvent))

    def test_append_only_protects_the_event_table(self):
        """
        Case: PGHISTORY_APPEND_ONLY is on, so pghistory adds a trigger that prevents updates and deletes of event rows.
        Expected: The generated model has the append_only protect trigger in its meta.
        """
        with override_settings(PGHISTORY_APPEND_ONLY=True):
            event_model = self.create_abstract_event_model()

        triggers = [trigger for trigger in event_model._meta.triggers if isinstance(trigger, pgtrigger.Protect)]

        self.assertEqual([trigger.name for trigger in triggers], ['append_only'])

    def test_the_obj_field_setting_names_the_reverse_accessor(self):
        """
        Case: PGHISTORY_OBJ_FIELD has a related_name, which the pgh_obj of every generated model should use.
        Expected: The generated foreign key uses it.
        """
        with override_settings(PGHISTORY_OBJ_FIELD=pghistory.ObjForeignKey(related_name='history')):
            event_model = self.create_abstract_event_model()

        self.assertEqual(event_model._meta.get_field('pgh_obj').remote_field.related_name, 'history')

    def test_the_context_id_field_can_be_turned_off_at_a_call_site(self):
        """
        Case: A call site passes context_id_field=None, because the setting does not accept None.
        Expected: The generated model has no pgh_context_id, and keeps the denormalized context.
        """
        event_model = self.create_abstract_event_model(context_id_field=None)

        with self.assertRaises(FieldDoesNotExist):
            event_model._meta.get_field('pgh_context_id')

        self.assertIsNotNone(event_model._meta.get_field('pgh_context'))
        self.assertTrue(issubclass(event_model, ShardedEvent))

    def test_the_foreign_key_field_setting_reaches_the_generated_relations(self):
        """
        Case: PGHISTORY_FOREIGN_KEY_FIELD asks for real database constraints, which pghistory does not create by
              default.
        Expected: pgh_obj is generated with a constraint. This is safe here, because an event model and the model that
                  it tracks are always placed together, so the constraint stays inside one schema.
        """
        with override_settings(PGHISTORY_FOREIGN_KEY_FIELD=pghistory.ForeignKey(db_constraint=True)):
            event_model = self.create_abstract_event_model()

        self.assertTrue(event_model._meta.get_field('pgh_obj').db_constraint)

    def test_the_field_setting_reaches_the_copied_columns(self):
        """
        Case: PGHISTORY_FIELD asks for an index on every copied column, where pghistory by default removes the indexes
              from the event table.
        Expected: The column that is copied from the tracked model is generated with an index.
        """
        with override_settings(PGHISTORY_FIELD=pghistory.Field(db_index=True)):
            event_model = self.create_abstract_event_model()

        self.assertTrue(event_model._meta.get_field('name').db_index)

    def test_excluded_field_kwargs_are_dropped_from_the_copied_columns(self):
        """
        Case: PGHISTORY_EXCLUDE_FIELD_KWARGS drops max_length when a CharField is copied.
        Expected: The generated column has no max_length.
        """
        with override_settings(PGHISTORY_EXCLUDE_FIELD_KWARGS={models.CharField: ['max_length']}):
            event_model = self.create_abstract_event_model()

        self.assertIsNone(event_model._meta.get_field('name').max_length)

    def test_the_created_at_function_is_read_whenever_the_trigger_sql_is_rendered(self):
        """
        Case: The trigger SQL of a tracked model is rendered with the default setting, and with
              PGHISTORY_CREATED_AT_FUNCTION='clock_timestamp()'.
        Expected: The rendered SQL follows the setting each time. The installed trigger is the one that a migration
                  recorded, so NoPendingModelMigrationsTestCase fails when the setting and the migrations differ.
        """
        default_trigger_sql = Cake._meta.triggers[0].get_func(Cake).func

        with override_settings(PGHISTORY_CREATED_AT_FUNCTION='clock_timestamp()'):
            overridden_trigger_sql = Cake._meta.triggers[0].get_func(Cake).func

        self.assertIn('NOW()', default_trigger_sql)
        self.assertIn('clock_timestamp()', overridden_trigger_sql)

    def test_each_mode_gets_its_base_while_append_only_is_on(self):
        """
        Case: PGHISTORY_APPEND_ONLY, a setting that changes every generated model, is on while models of all three modes
              are tracked.
        Expected: Each one gets the base for its own mode.
        """
        with override_settings(PGHISTORY_APPEND_ONLY=True):
            self.assertTrue(issubclass(self.create_abstract_event_model(Cake), ShardedEvent))
            self.assertTrue(issubclass(self.create_abstract_event_model(Bakery), MirroredEvent))
            self.assertTrue(issubclass(self.create_abstract_event_model(CakeType), PublicEvent))


class BaseModelIsReadAtImportTestCase(SimpleTestCase):
    def test_overriding_the_base_model_setting_does_not_change_the_event_bases(self):
        """
        Case: PGHISTORY_BASE_MODEL is overridden after the plugin's event bases are imported.
        Expected: A generated model still has pgh_from_batch from BatchFlagEvent, because the event bases read the
                  setting once, when they are imported.
        """
        with override_settings(PGHISTORY_BASE_MODEL='pghistory.models.Event'):
            event_model = create_event_model(Cake, abstract=True)

        self.assertIsNotNone(event_model._meta.get_field('pgh_from_batch'))
