from django.apps import apps
from django.db.migrations.autodetector import MigrationAutodetector
from django.db.migrations.loader import MigrationLoader
from django.db.migrations.questioner import NonInteractiveMigrationQuestioner
from django.db.migrations.state import ProjectState
from django.test import SimpleTestCase

# The apps whose migrations this repository maintains. Contrib apps are deliberately left out: their migrations are
# not ours to keep in step, and they show up as pending here anyway, because the cases that swap AUTH_USER_MODEL
# (djanquiltdb_tests/contrib/quilt_auth/management/commands/createsuperuser.py, djanquiltdb_tests/app_config.py)
# leave admin's LogEntry.user re-rendered against whichever user model was last installed. That residue is global to
# the worker process, so including admin would make this pass or fail on test ordering alone.
OWNED_APPS = frozenset({'djanquiltdb', 'example', 'migration_tests', 'pgtrigger_tests'})


class NoPendingMigrationsTestCase(SimpleTestCase):
    def test_no_app_has_a_change_left_unmigrated(self):
        """
        Case: The autodetector compares the models of every app this repository maintains migrations for against
              those migrations, as `makemigrations --check` would.
        Expected: Nothing pending.

        These migrations are hand-maintained, so a field added to a model without the matching edit is easy to miss. It
        stays missed, too: the schema every other test reads is built from the migrations, while `SyncDbTestCase` builds
        tables from the models, so the two halves of the suite quietly disagree rather than failing. A drifting field
        also makes the unmigrated-changes notice of `migrate` fire on a fully migrated database, where it means nothing.
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

        self.assertEqual({app_label: changes[app_label] for app_label in changes.keys() & OWNED_APPS}, {})
