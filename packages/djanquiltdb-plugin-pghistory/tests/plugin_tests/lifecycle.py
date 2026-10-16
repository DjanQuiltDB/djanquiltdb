import json
import os
import tempfile
from io import StringIO
from unittest import mock

from django.core.management import call_command
from djanquiltdb.testing import ShardingTestCase

from example.models import CakeType, CakeTypeEvent


class PublicEventFixtureTestCase(ShardingTestCase):
    available_apps = ['djanquiltdb', 'example', 'pghistory', 'django.contrib.auth', 'django.contrib.contenttypes']

    def dump_public_events_by_natural_key(self):
        output = StringIO()
        call_command('dumpdata', 'example.CakeTypeEvent', '--natural-primary', '--natural-foreign', stdout=output)

        return output.getvalue()

    def load_fixture_from_string(self, fixture):
        """
        Writes the fixture to a temporary file and loads that file, because loaddata takes file paths and not streams.
        """
        handle, path = tempfile.mkstemp(suffix='.json')

        try:
            with os.fdopen(handle, 'w') as fixture_file:
                fixture_file.write(fixture)

            call_command('loaddata', path, verbosity=0)
        finally:
            os.unlink(path)

    def test_public_events_are_dumped_by_natural_key_and_not_by_primary_key(self):
        """
        Case: The events of a tracked public model are dumped with natural primary keys.
        Expected: The records have no pk.
        """
        CakeType.objects.create(name='layered')

        records = json.loads(self.dump_public_events_by_natural_key())

        self.assertEqual([record['model'] for record in records], ['example.caketypeevent'])
        self.assertNotIn('pk', records[0])

    def test_dumped_public_events_load_again_with_what_they_recorded(self):
        """
        Case: The dumped events are loaded again after the event rows are deleted, and the loaded event is read.
        Expected: One event is loaded. loaddata looks each record up through get_by_natural_key of the event manager,
                  finds no row, and inserts the event. The event has the primary key of the CakeType row that it
                  describes, its label and the values that it recorded.
        """
        cake_type = CakeType.objects.create(name='layered')
        fixture = self.dump_public_events_by_natural_key()

        CakeTypeEvent.objects.all().delete()
        self.assertEqual(CakeTypeEvent.objects.count(), 0)

        event_manager_class = type(CakeTypeEvent.objects)

        with mock.patch.object(
            event_manager_class,
            'get_by_natural_key',
            autospec=True,
            side_effect=event_manager_class.get_by_natural_key,
        ) as get_by_natural_key:
            self.load_fixture_from_string(fixture)

        get_by_natural_key.assert_called()

        event = CakeTypeEvent.objects.get()

        self.assertEqual(event.pgh_obj_id, cake_type.pk)
        self.assertEqual(event.pgh_label, 'insert')
        self.assertEqual(event.name, 'layered')

    def test_reloading_a_natural_key_fixture_duplicates_the_events(self):
        """
        Case: The natural-primary fixture is loaded again while the events that it describes still exist.
        Expected: Two events and not one. --natural-primary omits the primary key, which is also the natural key, so the
                  existing row is never found. An event has no other value that could identify it, so this is accepted.
        """
        CakeType.objects.create(name='layered')
        fixture = self.dump_public_events_by_natural_key()

        self.load_fixture_from_string(fixture)

        self.assertEqual(CakeTypeEvent.objects.count(), 2)

    def test_reloading_a_fixture_that_keeps_the_primary_key_updates_in_place(self):
        """
        Case: The events of a CakeType are dumped without --natural-primary, so each record has its pgh_id, and the
              fixture is loaded twice.
        Expected: There is one event the whole time. The fixture has the primary keys, so loading it again updates the
                  existing rows in place.
        """
        CakeType.objects.create(name='layered')

        output = StringIO()
        call_command('dumpdata', 'example.CakeTypeEvent', stdout=output)
        fixture = output.getvalue()

        self.load_fixture_from_string(fixture)
        self.load_fixture_from_string(fixture)

        self.assertEqual(CakeTypeEvent.objects.count(), 1)
