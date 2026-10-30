"""
Checks DjanQuiltDB registers with Django's system check framework.

They are registered from ``DjanQuiltDBConfig.ready()``, so they only run when the app is installed.
"""

import functools
import inspect
import re

from django.apps import apps
from django.conf import settings
from django.core import checks
from django.urls import URLResolver, get_resolver
from django.utils.module_loading import import_string

from djanquiltdb import ShardingMode
from djanquiltdb.utils import (
    get_declared_sharding_mode,
    get_mapping_class,
    get_model_single_node_alias,
    get_shard_class,
    relation_with_a_model_on_a_single_node_is_allowed,
)

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


@checks.register(checks.Tags.compatibility)
def check_shard_selector_regex_setting(app_configs, **kwargs):
    """
    Report a warning when the deprecated ``QUILT_SESSIONS['SHARD_SELECTOR_REGEX']`` is set, and an error when
    ``QUILT_DB['SHARD_SELECTOR_REGEX']`` is set to another regex. With two regexes, the session backend uses the regex
    of ``QUILT_DB``, and the session keys in use may not match it.
    """
    session_regex = (getattr(settings, 'QUILT_SESSIONS', None) or {}).get('SHARD_SELECTOR_REGEX')

    if session_regex is None:
        return []

    quilt_db_regex = (getattr(settings, 'QUILT_DB', None) or {}).get('SHARD_SELECTOR_REGEX')

    if quilt_db_regex is not None and quilt_db_regex != session_regex:
        return [
            checks.Error(
                "QUILT_DB['SHARD_SELECTOR_REGEX'] is {!r} and QUILT_SESSIONS['SHARD_SELECTOR_REGEX'] is {!r}.".format(
                    quilt_db_regex, session_regex
                ),
                hint=(
                    'DjanQuiltDB uses the regex of QUILT_DB. Check that the session keys in use match it, and then '
                    'remove SHARD_SELECTOR_REGEX from QUILT_SESSIONS.'
                ),
                id='djanquiltdb.E010',
            )
        ]

    return [
        checks.Warning(
            "QUILT_SESSIONS['SHARD_SELECTOR_REGEX'] is deprecated and will be removed in djanquiltdb 5.0.",
            hint=(
                "Set QUILT_DB['SHARD_SELECTOR_REGEX'] = {!r} and remove SHARD_SELECTOR_REGEX from QUILT_SESSIONS."
            ).format(session_regex),
            id='djanquiltdb.W005',
        )
    ]


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


def _concrete_models(app_configs, include_auto_created=False):
    """
    Returns the models that have a table of their own, so no proxy models, of the apps in ``app_configs``, or of every
    app when it is None.
    """
    app_labels = None if app_configs is None else {app_config.label for app_config in app_configs}

    return [
        model
        for model in apps.get_models(include_auto_created=include_auto_created)
        if not model._meta.proxy and (app_labels is None or model._meta.app_label in app_labels)
    ]


_NO_SHARDING_MODE_HINT = (
    "Its table is on the node named 'default' only, and its queries go to the node that is active, so they fail while "
    'another node is active. Decorate the model with @sharded_model(), @mirrored_model() or @public_model(), or give it '
    "or its app an entry in QUILT_DB['OVERRIDE_SHARDING_MODE']. To keep the model on one node at your own risk, use "
    '@single_node_unsharded_model(node), or single_node_unsharded(node) in OVERRIDE_SHARDING_MODE.'
)

_SINGLE_NODE_HINT = (
    'A model on a single node is strongly discouraged: only a model on the same node can refer to it, it can only refer '
    'to a model that its node has, and its rows are on one node only. Silence this warning with '
    "SILENCED_SYSTEM_CHECKS = ['djanquiltdb.W004']."
)

_RELATION_TO_ANOTHER_NODE_HINT = (
    'A model on a single node has its table in the public schema of that node only. A model on another node, or in '
    'another sharding mode, cannot refer to it, and it can only refer to a public model, a mirrored model or a model on '
    'the same node.'
)


@checks.register(checks.Tags.models)
def check_models_have_a_sharding_mode(app_configs, **kwargs):
    """
    Report each model without a sharding mode. When ``app_configs`` is given, only the models of those apps are
    checked.
    """
    return [
        checks.Error(
            'The model {} has no sharding mode.'.format(model._meta.label),
            hint=_NO_SHARDING_MODE_HINT,
            obj=model,
            id='djanquiltdb.E004',
        )
        for model in _concrete_models(app_configs)
        if get_declared_sharding_mode(model) is None
    ]


@checks.register(checks.Tags.models)
def check_models_on_a_single_node(app_configs, **kwargs):
    """
    Warn about each model on a single node, report one whose node is not in ``DATABASES``, and report each relation that
    goes to or from a model on a single node and does not stay on that node. When ``app_configs`` is given, only the
    models of those apps, and the relations from them, are checked.
    """
    messages = []

    for model in _concrete_models(app_configs):
        single_node_alias = get_model_single_node_alias(model)

        if single_node_alias is None:
            continue

        messages.append(
            checks.Warning(
                "The model {} is on the single node '{}'.".format(model._meta.label, single_node_alias),
                hint=_SINGLE_NODE_HINT,
                obj=model,
                id='djanquiltdb.W004',
            )
        )

        if single_node_alias not in settings.DATABASES:
            messages.append(
                checks.Error(
                    "The model {} is on the node '{}', which is not in DATABASES.".format(
                        model._meta.label, single_node_alias
                    ),
                    obj=model,
                    id='djanquiltdb.E006',
                )
            )

    for model in _concrete_models(app_configs, include_auto_created=True):
        for field in model._meta.local_fields:
            if not field.is_relation or field.related_model is None:
                continue

            message = _relation_across_nodes_message(model, field)
            if message is not None:
                messages.append(message)

    return messages


def _placement_description(model):
    """
    Describes where a model is, for the messages of the relation check: "a sharded model", or "a model on the node
    'default'".
    """
    single_node_alias = get_model_single_node_alias(model)

    if single_node_alias is not None:
        return "a model on the node '{}'".format(single_node_alias)

    return 'a {} model'.format(_mode_name(get_declared_sharding_mode(model)))


def _relation_across_nodes_message(model, field):
    """
    Returns the error for a relation from ``model`` through ``field`` that does not stay on the node of a model on a
    single node, or None when the relation is allowed or when either model has no sharding mode, which E004 reports.
    """
    target = field.related_model
    source_mode = get_declared_sharding_mode(model)
    target_mode = get_declared_sharding_mode(target)

    if source_mode is None or target_mode is None:
        return None

    allowed = relation_with_a_model_on_a_single_node_is_allowed(
        referred_mode=target_mode,
        referred_node=get_model_single_node_alias(target),
        referring_mode=source_mode,
        referring_node=get_model_single_node_alias(model),
    )

    if allowed is None or allowed:
        return None

    return checks.Error(
        'The field {field} of {model}, {source}, refers to {target_model}, {target}.'.format(
            field=field.name,
            source=_placement_description(model),
            model=model._meta.label,
            target=_placement_description(target),
            target_model=target._meta.label,
        ),
        hint=_RELATION_TO_ANOTHER_NODE_HINT,
        obj=model,
        id='djanquiltdb.E005',
    )


_NOT_MIRRORED_HINT = (
    'DjanQuiltDB reads the {table} in the public schema of the node that the router picks, which is the node that is '
    'active for a model that is not routed to the primary node, so every node needs a copy of it. Decorate the model '
    'with @mirrored_model(), and keep the copies the same on every node, as for every mirrored model. If the project '
    'has more than one node, also add a migration with CreateMissingMirroredTable, as "Making an existing model '
    'mirrored" in the migrations guide describes.'
)


@checks.register(checks.Tags.models)
def check_shard_and_mapping_models_are_mirrored(app_configs, **kwargs):
    """
    Report the Shard model, and the mapping model when the project has one, when it is not mirrored. When
    ``app_configs`` is given, only the models of those apps are checked.
    """
    models_to_check = [
        (get_shard_class(), 'Shard model', 'shard registry'),
        (get_mapping_class(), 'mapping model', 'mapping table'),
    ]
    app_labels = None if app_configs is None else {app_config.label for app_config in app_configs}
    errors = []

    for model, kind, table in models_to_check:
        if model is None or (app_labels is not None and model._meta.app_label not in app_labels):
            continue

        if get_declared_sharding_mode(model) is not ShardingMode.MIRRORED:
            errors.append(
                checks.Error(
                    'The {} {} is not mirrored.'.format(kind, model._meta.label),
                    hint=_NOT_MIRRORED_HINT.format(table=table),
                    obj=model,
                    id='djanquiltdb.E003',
                )
            )

    return errors


_NOT_ASYNC_SAFE_HINT = (
    'DjanQuiltDB is not async-safe: it keeps the active shard per thread, and async code runs its queries in another '
    'thread than the one it entered the shard in. {} Enter shards only inside sync_to_async or database_sync_to_async, '
    'and once the async code of the project does so, acknowledge this error with SILENCED_SYSTEM_CHECKS = '
    '[\'{}\']. The "Async code" guide of the DjanQuiltDB documentation describes what is safe.'
)


def _url_patterns_with_routes(patterns, prefix=''):
    """
    Yields each URLPattern below ``patterns``, with the route that leads to it, through every include().
    """
    for pattern in patterns:
        route = prefix + str(pattern.pattern)
        if isinstance(pattern, URLResolver):
            yield from _url_patterns_with_routes(pattern.url_patterns, route)
        else:
            yield pattern, route


def _dotted_path(view):
    """
    Returns the dotted path of a view: of the class of a class-based view, or of the function that functools.partial()
    wraps, which inspect.iscoroutinefunction() sees through too. A view without a qualified name, such as an instance
    of a class, is named by its repr.
    """
    view = getattr(view, 'view_class', view)

    while isinstance(view, functools.partial):
        view = view.func

    if not hasattr(view, '__qualname__'):
        return repr(view)

    return '{}.{}'.format(view.__module__, view.__qualname__)


@checks.register(checks.Tags.urls)
def check_views_are_sync(app_configs, **kwargs):
    """
    Report each async view of the URL configuration: an async function, or a class-based view whose handlers are async,
    which Django marks as a coroutine function too. A project without ROOT_URLCONF has no views, and passes.
    """
    if not getattr(settings, 'ROOT_URLCONF', None):
        return []

    return [
        checks.Error(
            "The view {} at '{}' is async.".format(_dotted_path(pattern.callback), route),
            hint=_NOT_ASYNC_SAFE_HINT.format('An async view runs on the event loop.', 'djanquiltdb.E007'),
            obj=pattern,
            id='djanquiltdb.E007',
        )
        for pattern, route in _url_patterns_with_routes(get_resolver().url_patterns)
        if inspect.iscoroutinefunction(pattern.callback)
    ]


def _middleware_that_imports():
    """
    Yields the dotted path and the imported middleware of each entry of MIDDLEWARE that imports. Django reports an entry
    that does not import when it loads the request handler, and a process that does not serve requests, such as one
    that runs migrate, may not have the packages of every middleware installed, so such an entry is left out.
    """
    for middleware_path in settings.MIDDLEWARE:
        try:
            yield middleware_path, import_string(middleware_path)
        except ImportError:
            continue


@checks.register(checks.Tags.compatibility)
def check_middleware_is_sync_capable(app_configs, **kwargs):
    """
    Report each middleware in MIDDLEWARE that is async-only. Django runs the hooks of a middleware that is sync capable
    in the sync thread of the request, also under ASGI, so those are left alone.
    """
    return [
        checks.Error(
            'The middleware {} is async-only.'.format(middleware_path),
            hint=_NOT_ASYNC_SAFE_HINT.format('An async-only middleware runs on the event loop.', 'djanquiltdb.E008'),
            id='djanquiltdb.E008',
        )
        for middleware_path, middleware in _middleware_that_imports()
        if not getattr(middleware, 'sync_capable', True)
    ]


@checks.register(checks.Tags.compatibility)
def check_no_asgi_application(app_configs, **kwargs):
    """
    Report an ASGI_APPLICATION setting, through which Channels and Daphne serve consumers that run on the event loop.
    """
    asgi_application = getattr(settings, 'ASGI_APPLICATION', None)
    if not asgi_application:
        return []

    return [
        checks.Error(
            'ASGI_APPLICATION is set to {}.'.format(asgi_application),
            hint=_NOT_ASYNC_SAFE_HINT.format(
                'The consumers of an ASGI application, such as those of Channels, run on the event loop.',
                'djanquiltdb.E009',
            ),
            id='djanquiltdb.E009',
        )
    ]
