from django.apps import apps
from django.db.migrations.autodetector import MigrationAutodetector
from django.db.migrations.loader import MigrationLoader
from django.db.migrations.questioner import NonInteractiveMigrationQuestioner
from django.db.migrations.state import ProjectState
from django.test import SimpleTestCase
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
        loader = MigrationLoader(None, ignore_no_migrations=True)
        autodetector = MigrationAutodetector(
            loader.project_state(),
            ProjectState.from_apps(apps),
            # The questioner writes through a logger it is only given by makemigrations itself, and asking about a
            # field that turned non-nullable is enough to reach it. Passing a no-op keeps a regression here a
            # readable assertion failure rather than a TypeError raised from inside the autodetector.
            NonInteractiveMigrationQuestioner(dry_run=True, log=lambda message: None),
        )
        changes = autodetector.changes(graph=loader.graph)

        model_changes = {}
        for app_label in changes.keys() & OWNED_APPS:
            operations = [
                operation
                for migration in changes[app_label]
                for operation in migration.operations
                if not isinstance(operation, DECLARED_OBJECT_OPERATIONS)
            ]
            if operations:
                model_changes[app_label] = operations

        self.assertEqual(model_changes, {})
