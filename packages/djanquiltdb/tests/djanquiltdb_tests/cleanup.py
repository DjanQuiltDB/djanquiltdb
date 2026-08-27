"""
The cleanup the test cases themselves provide, which the rest of the suite leans on to leave no schemas behind.
"""

from djanquiltdb.db import connection
from djanquiltdb_tests import ShardingTransactionTestCase

SCHEMA_NAME = 'cleanup_mixin_schema'


class ForgetfulSetUpTestCase(ShardingTransactionTestCase):
    """
    A case that overrides setUp without calling super(), the way two of these once did.

    Schemas created by a test are dropped by comparing what exists afterwards against a baseline, and that baseline
    used to be recorded in setUp. A subclass like this one left none behind, the teardown skipped its work without
    saying so, and every schema the test created survived into whichever test ran next, which is how a leak here
    surfaced three modules away as tables missing from a cloned schema. Recording the baseline before setUp runs at
    all is what makes that impossible.
    """

    def setUp(self):
        pass

    def test_the_baseline_is_recorded_without_a_cooperating_setup(self):
        """
        Case: Create a schema from a case whose setUp never called super().
        Expected: A baseline to compare against, without the new schema in it, so the teardown drops it. Were it
                  missing, this test would leave the schema behind for the next one to trip over.
        """
        connection.create_schema(SCHEMA_NAME)

        for db_name in self._databases_names():
            self.assertIn(db_name, self._initial_schemas)
            self.assertNotIn(SCHEMA_NAME, self._initial_schemas[db_name])
