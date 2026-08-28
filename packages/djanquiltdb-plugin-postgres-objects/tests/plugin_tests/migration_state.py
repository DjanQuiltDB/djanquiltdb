from django.test import SimpleTestCase
from djanquiltdb.testing import pending_migration_changes
from postgres_objects.operations import DatabaseObjectOperation, RecalculateGeneratedField

# The apps this project maintains migrations for. postgres_objects is left out: it is the dependency, not ours.
OWNED_APPS = frozenset({'djanquiltdb', 'example', 'declared'})

# What the autodetector contributes for declared objects rather than for models. Add/Alter/Remove of a function or a
# view and a materialized-view refresh all descend from DatabaseObjectOperation; a recalculation does not, so it is
# named beside it.
DECLARED_OBJECT_OPERATIONS = (DatabaseObjectOperation, RecalculateGeneratedField)


class NoPendingModelMigrationsTestCase(SimpleTestCase):
    def test_no_model_change_is_left_unmigrated(self):
        """
        Case: The autodetector compares this project's models against its migrations, with the operations for
              declared objects set aside.
        Expected: Nothing left pending.

        Those declared objects are permanently pending on purpose (see example/functions.py, example/db_views.py) so
        `makemigrations --check` can never pass here. This test is our failsafe that a field that drifted from its
        migration would otherwise sit unnoticed among the many entries that are supposed to be there.
        """
        self.assertEqual(pending_migration_changes(OWNED_APPS, ignored_operations=DECLARED_OBJECT_OPERATIONS), {})
