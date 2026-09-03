from django.test import SimpleTestCase
from djanquiltdb.testing import pending_migration_changes

# The apps whose migrations this project maintains. pghistory and pgtrigger are dependencies, so their migrations are
# not maintained here.
OWNED_APPS = frozenset({'djanquiltdb', 'example'})


class NoPendingModelMigrationsTestCase(SimpleTestCase):
    def test_every_model_change_has_a_migration(self):
        """
        Case: The autodetector compares the models of this project, including the generated event models, with its
              migrations.
        Expected: Nothing is pending, so a model or tracking change without a matching migration cannot go unnoticed.
        """
        self.assertEqual(pending_migration_changes(OWNED_APPS), {})
