"""
Checks DjanQuiltDB registers with Django's system check framework.

They are registered from ``DjanQuiltDBConfig.ready()``, so they only run when the app is installed.
"""

import re

from django.apps import apps
from django.conf import settings
from django.core import checks

from djanquiltdb import ShardingMode
from djanquiltdb.utils import get_declared_sharding_mode

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


# The parts of a function body that are not SQL: comments, string literals and dollar-quoted strings, which are blanked
# out before the body is searched. A quoted identifier is matched too, so that a quote inside it does not start a string
# literal, and it is kept as it is. They are matched in one pass from left to right, so that "--" inside a string
# literal does not start a comment. In an escape string, E'...', a backslash escapes the character after it, so \' does
# not end the string. In a string literal without E, a backslash is an ordinary character.
_NOT_SQL = re.compile(
    r'(?P<identifier>"(?:[^"]|"")*")'
    r'|--[^\n]*'
    r'|/\*.*?\*/'
    r"|(?<!\w)[eE]'(?:[^'\\]|\\.|'')*'"
    r"|'(?:[^']|'')*'"
    r'|\$(?P<tag>\w*)\$.*?\$(?P=tag)\$',
    re.DOTALL,
)

# A name, quoted or not, optionally qualified with a schema.
_NAME = r'(?:"(?:[^"]|"")+"|\w+)'

# The statements that write rows to a table, followed by the name of that table.
_WRITE_TARGET = re.compile(
    r'\b(?P<keyword>INSERT\s+INTO|MERGE\s+INTO|DELETE\s+FROM(?:\s+ONLY)?|TRUNCATE(?:\s+TABLE)?(?:\s+ONLY)?'
    r'|UPDATE(?:\s+ONLY)?)\s+(?P<name>{name}(?:\s*\.\s*{name})?)'.format(name=_NAME),
    re.IGNORECASE,
)

# A word before UPDATE that makes it a lock (FOR UPDATE, FOR NO KEY UPDATE), the action of an upsert
# (ON CONFLICT DO UPDATE) or the action of a MERGE (WHEN MATCHED THEN UPDATE), and not a statement that writes a table.
_NOT_A_WRITE_BEFORE_UPDATE = {'for', 'key', 'do', 'then'}

# The pairs of sharding modes that a trigger must not write across: the mode of the table that the trigger is on, the
# mode of the table that it writes, and the check message for that pair.
_CROSS_MODE_WRITES = {
    (ShardingMode.PUBLIC, ShardingMode.SHARDED): (checks.Error, 'djanquiltdb.E001'),
    (ShardingMode.MIRRORED, ShardingMode.SHARDED): (checks.Error, 'djanquiltdb.E001'),
    (ShardingMode.PUBLIC, ShardingMode.MIRRORED): (checks.Error, 'djanquiltdb.E002'),
    (ShardingMode.SHARDED, ShardingMode.MIRRORED): (checks.Error, 'djanquiltdb.E002'),
    (ShardingMode.MIRRORED, ShardingMode.PUBLIC): (checks.Warning, 'djanquiltdb.W003'),
}

_CROSS_MODE_HINTS = {
    'djanquiltdb.E001': (
        'Every shard has its own copy of a sharded table, and the trigger writes one of them only: the copy in the '
        'shard whose schema the name is qualified with, or else the copy that PostgreSQL finds through the search path '
        'of the session that fires the trigger. Outside a shard, a name without a schema finds no table, and the write '
        'fails.'
    ),
    'djanquiltdb.E002': (
        'A mirrored table has the same rows on every node. The trigger writes the mirrored table of the node that it '
        'runs on, so the nodes no longer have the same rows.'
    ),
    'djanquiltdb.W003': (
        'A write to a mirrored table goes to the primary node, so the trigger writes the public table of that node '
        'only. The public tables of the other nodes do not get the write.'
    ),
}

_SILENCE_HINT = (
    ' DjanQuiltDB has no setting that allows such a trigger, because the relation that it follows does not work across '
    'sharding modes. See the DjanQuiltDB documentation on triggers.'
)


def _unquoted(name):
    """
    Returns the table name of a name in SQL: the last part of a schema-qualified name, without its quotes, and in
    lowercase when it was not quoted, as PostgreSQL folds it.
    """
    last = re.findall(_NAME, name)[-1]

    if last.startswith('"'):
        return last[1:-1].replace('""', '"')

    return last.lower()


def written_tables(body):
    """
    Returns the names of the tables that the statements in a trigger function's body write, in the order in which they
    appear. ``SELECT ... INTO`` assigns a variable and writes no table, and so does a lock with ``FOR UPDATE``.
    """
    sql = _NOT_SQL.sub(lambda match: match.group('identifier') or ' ', body)
    tables = []

    for match in _WRITE_TARGET.finditer(sql):
        if match.group('keyword').split()[0].lower() == 'update':
            words_before = sql[: match.start()].split()

            if words_before and words_before[-1].lower() in _NOT_A_WRITE_BEFORE_UPDATE:
                continue

        table = _unquoted(match.group('name'))

        if table not in tables:
            tables.append(table)

    return tables


def cross_mode_trigger_messages(triggers):
    """
    Returns a check message for each table that a trigger writes in another sharding mode than the table that the
    trigger is on, when that pair of modes is in ``_CROSS_MODE_WRITES``. ``triggers`` lists ``(model, trigger)`` pairs,
    as ``pgtrigger.registered()`` returns them.

    The tables are found in the trigger's function body. Writes that the body leaves to a function that it calls, or
    that it runs with a dynamic EXECUTE, are not found.
    """
    # A proxy model or an unmanaged model can name the table of another model, so only the models whose migrations
    # create a table are looked up by their table.
    models_by_table = {
        model._meta.db_table: model
        for model in apps.get_models(include_auto_created=True)
        if model._meta.managed and not model._meta.proxy
    }
    messages = []

    for model, trigger in triggers:
        mode = get_declared_sharding_mode(model)

        if mode is None:
            continue

        for table in written_tables(trigger.render_func(model)):
            target = models_by_table.get(table)
            target_mode = None if target is None else get_declared_sharding_mode(target)
            level_and_id = _CROSS_MODE_WRITES.get((mode, target_mode))

            if level_and_id is None:
                continue

            level, check_id = level_and_id
            messages.append(
                level(
                    'The trigger {uri} on the {mode} model {model} writes the table of the {target_mode} model '
                    '{target}.'.format(
                        uri=trigger.get_uri(model),
                        mode=_mode_name(mode),
                        model=model._meta.label,
                        target_mode=_mode_name(target_mode),
                        target=target._meta.label,
                    ),
                    hint=_CROSS_MODE_HINTS[check_id] + _SILENCE_HINT,
                    obj=model,
                    id=check_id,
                )
            )

    return messages


def _mode_name(mode):
    return mode.name.lower()


@checks.register(checks.Tags.models)
def check_trigger_writes_stay_in_their_sharding_mode(app_configs, **kwargs):
    """
    Report each django-pgtrigger trigger that writes a table in another sharding mode than the table that it is on, as
    ``_CROSS_MODE_WRITES`` lists the pairs. Such a trigger writes the wrong copy of the table, or none, depending on the
    node and the shard of the session that fires it. When ``app_configs`` is given, only the triggers on the models of
    those apps are checked.
    """
    if not apps.is_installed(PGTRIGGER_APP_NAME):
        return []

    import pgtrigger

    triggers = pgtrigger.registered()

    if app_configs is not None:
        app_labels = {app_config.label for app_config in app_configs}
        triggers = [(model, trigger) for model, trigger in triggers if model._meta.app_label in app_labels]

    return cross_mode_trigger_messages(triggers)
