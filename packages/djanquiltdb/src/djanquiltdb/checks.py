"""
Checks DjanQuiltDB registers with Django's system check framework.

They are registered from ``DjanQuiltDBConfig.ready()``, so they only run when the app is installed.
"""

from django.apps import apps
from django.conf import settings
from django.core import checks

# The QUILT_DB key that silences the pgtrigger compatibility check.
MUTE_SETTING = 'MUTE_PGTRIGGER_COMPATIBILITY_WARNING'

# The dotted name of the django-pgtrigger app, which is what is looked up rather than its label: a project is free to
# point INSTALLED_APPS at an AppConfig of its own and relabel it.
PGTRIGGER_APP_NAME = 'pgtrigger'

MUTE_HINT = "Set QUILT_DB['{}'] = True to silence this check.".format(MUTE_SETTING)


@checks.register(checks.Tags.compatibility)
def check_pgtrigger_compatibility(app_configs, **kwargs):
    """
    Warn when django-pgtrigger is installed but configured to bypass migrations.

    DjanQuiltDB spreads a migration across the public schema, the template schema and every existing shard. A trigger
    that never enters a migration never makes that trip: it lands in whichever single schema the connection points at
    and the other schemas, plus every shard cloned from the template afterwards, are left without it. See the trigger
    documentation for the whole story.
    """
    quilt_db = getattr(settings, 'QUILT_DB', None) or {}

    if quilt_db.get(MUTE_SETTING, False):
        return []

    if not apps.is_installed(PGTRIGGER_APP_NAME):
        return []

    warnings = []

    # The defaults are pgtrigger's own, read from the settings directly so the check does not lean on its internals.
    if not getattr(settings, 'PGTRIGGER_MIGRATIONS', True):
        warnings.append(
            checks.Warning(
                'PGTRIGGER_MIGRATIONS is off, which is incompatible with DjanQuiltDB.',
                hint=(
                    'With it off, django-pgtrigger never writes a trigger into a migration, so the shard-aware '
                    'migrate cannot apply it to the public schema, the template schema and every existing shard. '
                    'Set PGTRIGGER_MIGRATIONS = True, its default. ' + MUTE_HINT
                ),
                id='djanquiltdb.W001',
            )
        )

    if getattr(settings, 'PGTRIGGER_INSTALL_ON_MIGRATE', False):
        warnings.append(
            checks.Warning(
                'PGTRIGGER_INSTALL_ON_MIGRATE is on, which is incompatible with DjanQuiltDB.',
                hint=(
                    'It installs triggers straight into the database at the end of migrate instead of through a '
                    'migration, so they reach only the schema the connection points at and leave the template and '
                    'every other shard without them. Set PGTRIGGER_INSTALL_ON_MIGRATE = False, its default. '
                    + MUTE_HINT
                ),
                id='djanquiltdb.W002',
            )
        )

    return warnings
