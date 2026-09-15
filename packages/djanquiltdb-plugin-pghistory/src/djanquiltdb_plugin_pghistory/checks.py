from django.apps import apps
from django.core import checks
from django.core.exceptions import FieldDoesNotExist
from django.db import models
from djanquiltdb import ShardingMode, public_modes

PGHISTORY_APP_NAME = 'pghistory'

_DENORMALIZE_THE_CONTEXT = (
    'Denormalize the context with PGHISTORY_CONTEXT_FIELD = pghistory.ContextJSONField(), and do not pass '
    'context_field=pghistory.ContextForeignKey() to a tracking call'
)

_MAKE_THE_APP_PUBLIC = (
    "make the pghistory app PUBLIC by adding {('pghistory',): ShardingMode.PUBLIC} to "
    "QUILT_DB['OVERRIDE_SHARDING_MODE']"
)

_UNUSED_CONTEXT_TABLE_BY_SHARDING_MODE = {
    ShardingMode.SHARDED: 'A sharded Context table is created in the template schema and in every shard.',
    ShardingMode.MIRRORED: 'Nothing reads the table, so it does not need to be mirrored.',
}


def _start_a_sentence(text):
    return text[:1].upper() + text[1:]


def _find_event_models_with_context_foreign_key():
    """
    Reads the generated models and not PGHISTORY_CONTEXT_FIELD, because a call site can pass its own context_field.
    Every subclass of pghistory's Event counts, so an event model that a third-party app generates with plain
    pghistory.track() counts too.
    """
    import pghistory.models

    event_models = []

    for model in apps.get_models():
        if not issubclass(model, pghistory.models.Event):
            continue

        try:
            field = model._meta.get_field('pgh_context')
        except FieldDoesNotExist:
            continue

        if isinstance(field, models.ForeignKey):
            event_models.append(model)

    return event_models


@checks.register(checks.Tags.compatibility)
def check_pghistory_sharding_configuration(app_configs, **kwargs):
    """
    Reports django-pghistory configuration that does not work with sharding.
    """
    from djanquiltdb.utils import get_declared_sharding_mode, get_sharding_mode

    if not apps.is_installed(PGHISTORY_APP_NAME):
        return []

    context_mode = get_sharding_mode(PGHISTORY_APP_NAME, 'context')

    if not context_mode:
        return [
            checks.Warning(
                "The pghistory app's Context model has no sharding mode.",
                hint=(
                    'Migrations need to know where to create its table, also when no event model uses it. Add an '
                    "OVERRIDE_SHARDING_MODE entry for the app to QUILT_DB, such as {('pghistory',): "
                    'ShardingMode.PUBLIC}.'
                ),
                id='djanquiltdb_plugin_pghistory.W002',
            )
        ]

    messages = []

    if context_mode is ShardingMode.SHARDED:
        messages.extend(_check_triggers_outside_the_shards_reach_the_context_function())

    event_models_with_context_foreign_key = _find_event_models_with_context_foreign_key()

    if not event_models_with_context_foreign_key:
        if context_mode is not ShardingMode.PUBLIC:
            messages.append(
                checks.Warning(
                    'The pghistory Context model is {mode}, but no event model has a foreign key to it.'.format(
                        mode=context_mode.name
                    ),
                    hint='{reason} {make_public}.'.format(
                        reason=_UNUSED_CONTEXT_TABLE_BY_SHARDING_MODE[context_mode],
                        make_public=_start_a_sentence(_MAKE_THE_APP_PUBLIC),
                    ),
                    id='djanquiltdb_plugin_pghistory.W001',
                )
            )

        return messages

    labels = ', '.join(model._meta.label for model in event_models_with_context_foreign_key)

    if context_mode is ShardingMode.SHARDED:
        # A table outside the shards cannot have a foreign key to a table in a shard.
        unreachable_event_models = [
            model
            for model in event_models_with_context_foreign_key
            if get_declared_sharding_mode(model) in public_modes
        ]

        for model in unreachable_event_models:
            messages.append(
                checks.Error(
                    '{event} has a foreign key to the Context model, but its events are {mode} while the pghistory '
                    'app is SHARDED, so the events cannot reach the Context table.'.format(
                        event=model._meta.label, mode=get_declared_sharding_mode(model).name
                    ),
                    hint='{denormalize}, and {make_public}.'.format(
                        denormalize=_DENORMALIZE_THE_CONTEXT, make_public=_MAKE_THE_APP_PUBLIC
                    ),
                    obj=model,
                    id='djanquiltdb_plugin_pghistory.E002',
                )
            )

        if not unreachable_event_models:
            messages.append(
                checks.Warning(
                    'The pghistory Context model is SHARDED, and these event models have a foreign key to it: '
                    '{labels}.'.format(labels=labels),
                    hint=(
                        'This works while every event model that has a foreign key to the table is sharded, but the '
                        'events of a public or mirrored model cannot have one, and a pghistory.context() block that '
                        'spans two shards writes a Context row in each of them. {denormalize}, and {make_public}.'
                    ).format(denormalize=_DENORMALIZE_THE_CONTEXT, make_public=_MAKE_THE_APP_PUBLIC),
                    id='djanquiltdb_plugin_pghistory.W005',
                )
            )
    elif context_mode is ShardingMode.MIRRORED:
        messages.append(
            checks.Warning(
                'The pghistory Context model is MIRRORED, and these event models have a foreign key to it: '
                '{labels}.'.format(labels=labels),
                hint=(
                    'The event triggers write Context rows on the node where the tracked write happened, so the copies '
                    'on the nodes stop matching unless the project keeps this table in sync itself. {denormalize}, and '
                    '{make_public}.'
                ).format(denormalize=_DENORMALIZE_THE_CONTEXT, make_public=_MAKE_THE_APP_PUBLIC),
                id='djanquiltdb_plugin_pghistory.W003',
            )
        )
    else:
        messages.append(
            checks.Warning(
                'The pghistory Context model is PUBLIC, and these event models have a foreign key to it: '
                '{labels}.'.format(labels=labels),
                hint=(
                    'One Context table cannot follow the events of every sharding mode: the context of sharded events '
                    'is stored outside their shards, and the Context rows of mirrored events are only written on the '
                    'node where the tracked write happened. {denormalize}.'
                ).format(denormalize=_DENORMALIZE_THE_CONTEXT),
                id='djanquiltdb_plugin_pghistory.W004',
            )
        )

    return messages


def _check_triggers_outside_the_shards_reach_the_context_function():
    """
    Reports each public or mirrored model that has a pghistory trigger whose SQL calls _pgh_attach_context(), while the
    pghistory app is SHARDED. A statement-level trigger always calls the function, and a row-level trigger calls it
    when its event model has a foreign key to the Context model. The migrations of a sharded app only create the
    function in the template schema and the shards, so a write to the model outside a shard fails.
    """
    import pghistory.trigger
    from djanquiltdb.utils import get_declared_sharding_mode

    errors = []

    for model in apps.get_models():
        mode = get_declared_sharding_mode(model)

        if mode not in public_modes:
            continue

        if any(
            isinstance(trigger, pghistory.trigger.Event) and '_pgh_attach_context()' in trigger.render_func(model)
            for trigger in getattr(model._meta, 'triggers', [])
        ):
            errors.append(
                checks.Error(
                    '{model} is {mode}, and its pghistory triggers call _pgh_attach_context(), but the pghistory app '
                    'is SHARDED, so the function only exists in the shards, and a write to {model} outside a shard '
                    'fails.'.format(model=model._meta.label, mode=mode.name),
                    hint=(
                        '{make_public}, so that the function is in the public schema, which is on the search path of '
                        'every shard too. Or track {model} at row level with a denormalized context, the only setup '
                        'whose trigger does not call the function.'
                    ).format(make_public=_start_a_sentence(_MAKE_THE_APP_PUBLIC), model=model._meta.label),
                    obj=model,
                    id='djanquiltdb_plugin_pghistory.E004',
                )
            )

    return errors


@checks.register(checks.Tags.models)
def check_event_models_are_placed_where_their_base_expects(app_configs, **kwargs):
    """
    Reports an error when OVERRIDE_SHARDING_MODE gives an event model a different sharding mode than its base, as
    happens when a call site passes app_label= to generate the event model into an app that has another mode.
    """
    if not apps.is_installed(PGHISTORY_APP_NAME):
        return []

    from djanquiltdb.utils import get_declared_sharding_mode

    from djanquiltdb_plugin_pghistory.core import _EVENT_BASE_BY_SHARDING_MODE

    errors = []

    for model in apps.get_models():
        for base_mode, event_base in _EVENT_BASE_BY_SHARDING_MODE.items():
            if not issubclass(model, event_base):
                continue

            mode = get_declared_sharding_mode(model)

            if mode is not None and mode != base_mode:
                errors.append(
                    checks.Error(
                        '{event} is based on the {base_mode} event base, but OVERRIDE_SHARDING_MODE places it as '
                        '{mode}, so migrations create its table under a different mode than its base.'.format(
                            event=model._meta.label,
                            base_mode=base_mode.name,
                            mode=mode.name,
                        ),
                        hint=(
                            'Generate the event model into an app whose sharding mode matches its base, or give the '
                            'event model its own OVERRIDE_SHARDING_MODE entry with the mode of its base.'
                        ),
                        obj=model,
                        id='djanquiltdb_plugin_pghistory.E003',
                    )
                )

    return errors


@checks.register(checks.Tags.models)
def check_event_bases_match_tracked_modes(app_configs, **kwargs):
    """
    Reports an error when the base of an event model has a different sharding mode than the model that it tracks. The
    tracking functions cannot see this mismatch when the mode decorator is applied above them.
    """
    if not apps.is_installed(PGHISTORY_APP_NAME):
        return []

    from djanquiltdb.utils import get_declared_sharding_mode

    from djanquiltdb_plugin_pghistory.core import _EVENT_BASE_BY_SHARDING_MODE

    errors = []

    for model in apps.get_models():
        for base_mode, event_base in _EVENT_BASE_BY_SHARDING_MODE.items():
            if not issubclass(model, event_base) or getattr(model, 'pgh_tracked_model', None) is None:
                continue

            mode = get_declared_sharding_mode(model.pgh_tracked_model)

            if mode is not None and mode != base_mode:
                errors.append(
                    checks.Error(
                        '{event} places its events as {base_mode}, but its tracked model {tracked} is {mode}, so the '
                        'events would be stored in a different place than the rows that they describe.'.format(
                            event=model._meta.label,
                            base_mode=base_mode.name,
                            tracked=model.pgh_tracked_model._meta.label,
                            mode=mode.name,
                        ),
                        hint=(
                            'Move the mode decorator below the tracking decorator, so the tracking decorator raises '
                            'an error for the mismatch, or change the tracking function or base_model to match the '
                            'mode.'
                        ),
                        obj=model,
                        id='djanquiltdb_plugin_pghistory.E001',
                    )
                )

    return errors
