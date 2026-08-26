from djanquiltdb import State
from djanquiltdb.utils import create_template_schema, get_template_name
from example.models import Shard
from pgtrigger_tests.tests.base import (
    IGNORE_FUNC_NAME,
    PgtriggerTestCase,
    functions_in_schema,
    triggers_in_schema,
)

PUBLIC_SCHEMA = 'public'


class TriggerPlacementTestCase(PgtriggerTestCase):
    def setUp(self):
        super().setUp()
        # Migrating the template is what puts the sharded model's trigger there; the shards then clone it.
        create_template_schema()
        self.template = get_template_name()

        self.shards = [
            Shard.objects.create(
                alias=alias, schema_name='{}_schema'.format(alias), node_name='default', state=State.ACTIVE
            )
            for alias in ('first', 'second')
        ]
        self.shard_schemas = [shard.schema_name for shard in self.shards]

    def test_sharded_trigger_reaches_the_template_and_every_shard(self):
        """
        Case: A sharded model declares a trigger through django-pgtrigger, and the project has been migrated.
        Expected: The trigger exists in the template and in each shard, and not in the public schema.
        """
        expected = (self.sharded_table, self.sharded_trigger)

        for schema_name in [self.template, *self.shard_schemas]:
            with self.subTest(schema_name=schema_name):
                self.assertIn(expected, triggers_in_schema(schema_name))

        self.assertNotIn(expected, triggers_in_schema(PUBLIC_SCHEMA))

    def test_public_trigger_stays_in_the_public_schema(self):
        """
        Case: A public model declares a trigger through django-pgtrigger.
        Expected: The trigger exists in the public schema only, and reaches neither the template nor a shard.
        """
        expected = (self.public_table, self.public_trigger)

        self.assertIn(expected, triggers_in_schema(PUBLIC_SCHEMA))

        for schema_name in [self.template, *self.shard_schemas]:
            with self.subTest(schema_name=schema_name):
                self.assertNotIn(expected, triggers_in_schema(schema_name))

    def test_trigger_functions_are_created_per_schema(self):
        """
        Case: pgtrigger creates the function backing a trigger unqualified, so it lands wherever the connection points.
        Expected: The sharded model's function sits in the template and in every shard, next to the table it serves,
                  and the public model's function sits in the public schema.
        """
        for schema_name in [self.template, *self.shard_schemas]:
            with self.subTest(schema_name=schema_name):
                self.assertIn(self.sharded_trigger, functions_in_schema(schema_name))

        self.assertNotIn(self.sharded_trigger, functions_in_schema(PUBLIC_SCHEMA))
        self.assertIn(self.public_trigger, functions_in_schema(PUBLIC_SCHEMA))

    def test_the_ignore_helper_is_shared_from_the_public_schema(self):
        """
        Case: Every trigger pgtrigger renders calls a helper to decide whether `pgtrigger.ignore` is in effect, and
              pgtrigger qualifies that helper into the public schema.
        Expected: The helper exists once, in the public schema, and is not copied into the template or a shard.
        """
        self.assertIn(IGNORE_FUNC_NAME, functions_in_schema(PUBLIC_SCHEMA))

        for schema_name in [self.template, *self.shard_schemas]:
            with self.subTest(schema_name=schema_name):
                self.assertNotIn(IGNORE_FUNC_NAME, functions_in_schema(schema_name))
