from io import StringIO
from unittest import mock

from django.core.checks import run_checks
from django.core.management import call_command
from django.core.management.base import SystemCheckError
from django.db import connections
from django.db.backends.postgresql.psycopg_any import IsolationLevel
from django.test import SimpleTestCase, override_settings
from djanquiltdb.checks import check_trigger_writes_stay_in_their_sharding_mode
from postgres_denormalize.checks import TAG, check_isolation_level

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


def reported_nodes():
    return [
        (message.id, alias)
        for message in run_checks(tags=[TAG])
        if message.id.endswith('303')
        for alias in connections
        if f'database {alias!r}' in message.msg
    ]


class IsolationCheckTestCase(SimpleTestCase):
    def set_isolation_level(self, alias, isolation_level):
        options = {**connections[alias].settings_dict.get('OPTIONS', {}), 'isolation_level': isolation_level}
        patcher = mock.patch.dict(connections[alias].settings_dict, {'OPTIONS': options})
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_every_node_below_serializable_is_reported(self):
        """
        Case: The system checks run with the default ISOLATION_CHECK_LEVEL, while both nodes run transactions at READ
              COMMITTED.
        Expected: One postgres_denormalize.W303 for each node, because the shards of both nodes store copies, and
                  DjanQuiltDB's router answers the checks for the whole node.
        """
        self.assertEqual(
            reported_nodes(), [('postgres_denormalize.W303', 'default'), ('postgres_denormalize.W303', 'other')]
        )

    def test_the_check_reports_no_node_outside_the_system_checks(self):
        """
        Case: django-postgres-denormalize's isolation check is called directly, outside Django's run_checks.
        Expected: Nothing, because outside the system checks DjanQuiltDB's router answers a node name for the node's
                  public schema, where the sharded leaves have no table.
        """
        self.assertEqual(check_isolation_level(None), [])

    @override_settings(POSTGRES_DENORMALIZE={'ISOLATION_CHECK_LEVEL': 'error'})
    def test_the_error_level_stops_the_check_command(self):
        """
        Case: ISOLATION_CHECK_LEVEL is 'error', and `manage.py check --tag postgres_denormalize` runs.
        Expected: One postgres_denormalize.E303 for each node, and the command fails with them.
        """
        self.assertEqual(
            reported_nodes(), [('postgres_denormalize.E303', 'default'), ('postgres_denormalize.E303', 'other')]
        )
        with self.assertRaisesMessage(SystemCheckError, 'postgres_denormalize.E303'):
            call_command('check', tags=[TAG], stdout=StringIO(), stderr=StringIO())

    @override_settings(POSTGRES_DENORMALIZE={'ISOLATION_CHECK_LEVEL': 'off'})
    def test_the_off_level_reports_nothing(self):
        """
        Case: ISOLATION_CHECK_LEVEL is 'off'.
        Expected: Nothing.
        """
        self.assertEqual(reported_nodes(), [])

    def test_a_node_that_runs_serializable_is_not_reported(self):
        """
        Case: The node other runs transactions at SERIALIZABLE.
        Expected: Only default is reported.
        """
        self.set_isolation_level('other', IsolationLevel.SERIALIZABLE)

        self.assertEqual(reported_nodes(), [('postgres_denormalize.W303', 'default')])

    @override_settings(
        POSTGRES_DENORMALIZE={'EXCLUDE_MODELS': ['example.Comment', 'example.Task', 'antipattern.TypedTask']}
    )
    def test_a_project_without_leaves_is_not_reported(self):
        """
        Case: Every model with a denormalized field is listed in EXCLUDE_MODELS.
        Expected: Nothing, because no shard stores copies.
        """
        self.assertEqual(reported_nodes(), [])
