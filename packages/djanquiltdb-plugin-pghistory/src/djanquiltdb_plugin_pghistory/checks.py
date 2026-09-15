from django.apps import apps
from django.core import checks
from django.core.exceptions import FieldDoesNotExist
from django.db import models
from djanquiltdb import ShardingMode, public_modes

PGHISTORY_APP_NAME = 'pghistory'


def _find_event_models_with_context_foreign_key():
    """
    Reads the generated models and not PGHISTORY_CONTEXT_FIELD, because a call site can pass its own context_field.
    """
    from djanquiltdb_plugin_pghistory.core import _EVENT_BASE_BY_SHARDING_MODE

    event_models = []

    for model in apps.get_models():
        if not any(issubclass(model, event_base) for event_base in _EVENT_BASE_BY_SHARDING_MODE.values()):
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
    event_models_with_context_foreign_key = _find_event_models_with_context_foreign_key()

    if not event_models_with_context_foreign_key:
        if context_mode is not ShardingMode.PUBLIC:
            messages.append(
                checks.Warning(
                    'The pghistory Context model is sharded or mirrored, but no event model reads it.',
                    hint=(
                        'With the pghistory app as PUBLIC, the project has one unused Context table, and not an unused '
                        'copy in every shard or on every node. Every placement works, so silence this check if you '
                        'want to keep the current placement.'
                    ),
                    id='djanquiltdb_plugin_pghistory.W001',
                )
            )
    elif context_mode is ShardingMode.SHARDED:
        # A table outside the shards cannot have a foreign key to a table in a shard.
        for model in event_models_with_context_foreign_key:
            mode = get_declared_sharding_mode(model)

            if mode in public_modes:
                messages.append(
                    checks.Error(
                        '{event} has a foreign key to the Context model, but its events are {mode} while the '
                        'pghistory app is SHARDED, so the events cannot reach the Context table.'.format(
                            event=model._meta.label, mode=mode.name
                        ),
                        hint=(
                            'Place the pghistory app in the public schema, or denormalize the context of this event '
                            'model by passing context_field=pghistory.ContextJSONField() to its tracking call. The '
                            'app can stay sharded only if every event model that has a foreign key to it is sharded '
                            'too.'
                        ),
                        obj=model,
                        id='djanquiltdb_plugin_pghistory.E002',
                    )
                )
    elif context_mode is ShardingMode.MIRRORED:
        messages.append(
            checks.Warning(
                'The pghistory Context model is mirrored, and event models have a foreign key to it.',
                hint=(
                    'The event triggers write Context rows on the node where the tracked write happened, so the copies '
                    'on the nodes stop matching unless the project keeps this table in sync itself. Place the '
                    'pghistory app as PUBLIC, or silence this check once the table is kept in sync.'
                ),
                id='djanquiltdb_plugin_pghistory.W003',
            )
        )

    return messages


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
