from io import StringIO

from django.core.management import call_command
from django.core.management.base import SystemCheckError
from django.test import SimpleTestCase, override_settings
from djanquiltdb.checks import check_trigger_writes_stay_in_their_sharding_mode

from antipattern.models import TaskType


class CrossModeTriggerCheckTestCase(SimpleTestCase):
    def test_the_triggers_on_a_mirrored_model_are_reported(self):
        """
        Case: DjanQuiltDB's check for triggers that write across sharding modes runs for the triggers of this project.
        Expected: One djanquiltdb.E001 for the cascade trigger on TaskType and one for its fill trigger, and nothing for
                  the set triggers or for the triggers of the example app.
        """
        messages = check_trigger_writes_stay_in_their_sharding_mode(None)

        self.assertEqual(
            [(message.id, message.obj) for message in messages],
            [('djanquiltdb.E001', TaskType), ('djanquiltdb.E001', TaskType)],
        )

        for message, trigger_name in zip(
            messages, ['denormalize_cascade_antipattern_typedtask', 'denormalize_fill_antipattern_typedtask']
        ):
            with self.subTest(trigger_name=trigger_name):
                self.assertIn(f'antipattern.TaskType:{trigger_name}', message.msg)
                self.assertIn('antipattern.TypedTask', message.msg)

    def test_the_check_command_fails_unless_the_error_is_silenced(self):
        """
        Case: `manage.py check` runs without the SILENCED_SYSTEM_CHECKS of the test settings.
        Expected: SystemCheckError with djanquiltdb.E001, so a project with this lookup path does not start until it
                  silences the error.
        """
        with override_settings(SILENCED_SYSTEM_CHECKS=[]):
            with self.assertRaisesMessage(SystemCheckError, 'djanquiltdb.E001'):
                call_command('check', stdout=StringIO(), stderr=StringIO())
