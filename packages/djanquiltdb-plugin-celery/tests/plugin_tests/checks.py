from io import StringIO

from django.core.management import call_command
from django.core.management.base import SystemCheckError
from django.test import SimpleTestCase, override_settings
from djanquiltdb.decorators import override_sharding_setting

from djanquiltdb_plugin_celery.checks import check_quilt_celery_setting


def message_ids(messages):
    return [message.id for message in messages]


class QuiltCelerySettingCheckTestCase(SimpleTestCase):
    def test_a_project_without_the_setting_passes(self):
        """
        Case: The check runs for the test settings, which have no QUILT_CELERY.
        Expected: No messages.
        """
        self.assertEqual(check_quilt_celery_setting(None), [])

    @override_settings(
        QUILT_CELERY={
            'MAPPING_KWARG': 'customer_id',
            'MAX_RETRIES': None,
            'RETRY_BACKOFF': True,
            'RETRY_BACKOFF_MAX': 120,
            'RETRY_JITTER': True,
        }
    )
    def test_every_key_with_a_value_of_its_type_passes(self):
        """
        Case: QUILT_CELERY sets every key to a value of its type, with None for MAX_RETRIES, which Celery reads as
              retrying without a limit.
        Expected: No messages.
        """
        self.assertEqual(check_quilt_celery_setting(None), [])

    @override_settings(QUILT_CELERY={'MAX_RETRIE': 3})
    def test_an_unknown_key_is_reported(self):
        """
        Case: QUILT_CELERY has the key MAX_RETRIE, which the plugin does not read.
        Expected: djanquiltdb_plugin_celery.E001, whose message includes the key, and whose hint includes the keys that
                  the plugin reads.
        """
        messages = check_quilt_celery_setting(None)

        self.assertEqual(message_ids(messages), ['djanquiltdb_plugin_celery.E001'])
        self.assertIn("'MAX_RETRIE'", messages[0].msg)
        self.assertIn('MAX_RETRIES', messages[0].hint)

    def test_a_mapping_kwarg_that_names_another_target_is_reported(self):
        """
        Case: QUILT_CELERY['MAPPING_KWARG'] is set to shard_id, and then to node_name, which each already identify a
              target.
        Expected: djanquiltdb_plugin_celery.E002 for each, whose message includes the value.
        """
        for name in ['shard_id', 'node_name']:
            with self.subTest(name=name), self.settings(QUILT_CELERY={'MAPPING_KWARG': name}):
                messages = check_quilt_celery_setting(None)

                self.assertEqual(message_ids(messages), ['djanquiltdb_plugin_celery.E002'])
                self.assertIn("QUILT_CELERY['MAPPING_KWARG'] is '{}'".format(name), messages[0].msg)

    @override_sharding_setting('MAPPING_MODEL')
    @override_settings(QUILT_CELERY={'MAPPING_KWARG': 'customer_id'})
    def test_a_mapping_kwarg_without_a_mapping_model_is_reported(self):
        """
        Case: QUILT_CELERY['MAPPING_KWARG'] is customer_id in a project without a MAPPING_MODEL.
        Expected: djanquiltdb_plugin_celery.E002, whose message includes the value and MAPPING_MODEL, because a task
                  that is sent with customer_id cannot find its shard.
        """
        messages = check_quilt_celery_setting(None)

        self.assertEqual(message_ids(messages), ['djanquiltdb_plugin_celery.E002'])
        self.assertIn("QUILT_CELERY['MAPPING_KWARG'] is 'customer_id'", messages[0].msg)
        self.assertIn('MAPPING_MODEL', messages[0].msg)

    @override_settings(QUILT_CELERY={'RETRY_BACKOFF_MAX': 'a minute', 'MAPPING_KWARG': 3})
    def test_a_value_of_the_wrong_type_is_reported(self):
        """
        Case: QUILT_CELERY sets RETRY_BACKOFF_MAX to a string and MAPPING_KWARG to an integer.
        Expected: djanquiltdb_plugin_celery.E002 for each, whose message includes the key and the type that the key
                  accepts.
        """
        messages = check_quilt_celery_setting(None)

        self.assertEqual(message_ids(messages), ['djanquiltdb_plugin_celery.E002'] * 2)
        self.assertIn("QUILT_CELERY['MAPPING_KWARG']", messages[0].msg)
        self.assertIn('a string', messages[0].msg)
        self.assertIn("QUILT_CELERY['RETRY_BACKOFF_MAX']", messages[1].msg)
        self.assertIn('an integer', messages[1].msg)

    @override_settings(QUILT_CELERY=['MAX_RETRIES'])
    def test_a_setting_that_is_not_a_dict_is_reported(self):
        """
        Case: QUILT_CELERY is a list.
        Expected: djanquiltdb_plugin_celery.E002, whose message says that the setting is not a dict.
        """
        messages = check_quilt_celery_setting(None)

        self.assertEqual(message_ids(messages), ['djanquiltdb_plugin_celery.E002'])
        self.assertIn('dict', messages[0].msg)

    @override_settings(QUILT_CELERY={'MAX_RETRIE': 3})
    def test_the_check_command_fails_on_an_unknown_key(self):
        """
        Case: `manage.py check` runs while QUILT_CELERY has an unknown key.
        Expected: SystemCheckError with djanquiltdb_plugin_celery.E001, because the plugin registers the check when it
                  is installed.
        """
        with self.assertRaisesMessage(SystemCheckError, 'djanquiltdb_plugin_celery.E001'):
            call_command('check', stdout=StringIO(), stderr=StringIO())
