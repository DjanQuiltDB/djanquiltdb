from django.db import connection
from django.db.utils import IntegrityError
from django.test import TestCase

from sharding.utils import use_shard, create_schema


class UseShardTestCase(TestCase):
    def setUp(self):
        super().setUp()
        self.addCleanup(self.clean_up)

    def clean_up(self):
        # drop test_schema if it is created to make the state clean for the next test
        if connection.get_ps_schema('test_schema'):
            connection.cursor().execute("DROP SCHEMA {};".format('test_schema'))

    def test_use_shard(self):
        create_schema('test_schema')

        cursor = connection.cursor()
        cursor.execute("SHOW search_path;")
        self.assertFalse('test_schema' in cursor.fetchall()[0][0])  # search_path is likely to be "$user, public"

        with use_shard('test_schema'):
            cursor = connection.cursor()
            cursor.execute("SHOW search_path;")
            self.assertEqual(cursor.fetchall()[0][0], 'test_schema')  # we're in test_schema now

        cursor = connection.cursor()
        cursor.execute("SHOW search_path;")
        self.assertEqual(cursor.fetchall()[0][0], 'public')  # back in public

    def test_use_unexisting_shard(self):
        cursor = connection.cursor()

        cursor.execute("SHOW search_path;")
        self.assertFalse('test_schema' in cursor.fetchall()[0][0])  # search_path is likely "$user, public"

        with self.assertRaises(IntegrityError):
            with use_shard('test_schema'):
                cursor = connection.cursor()
                cursor.execute("SHOW search_path;")

        cursor = connection.cursor()
        cursor.execute("SHOW search_path;")
        self.assertEqual(cursor.fetchall()[0][0], 'public')  # back in public


class CreateSchemaTestCase(TestCase):
    def test_save(self):
        """
        Case: call create_schema with a name
        Expected: a new PostgreSQL schema is made
        """
        cursor = connection.cursor()
        # first: check if schema does not exist yet.
        self.assertFalse(connection.get_ps_shard('test_schema'))

        create_schema('test_schema')

        # check if it exists now
        self.assertTrue(connection.get_ps_shard('test_schema'))
