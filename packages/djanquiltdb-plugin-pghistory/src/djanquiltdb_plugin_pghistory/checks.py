import pghistory
from django.apps import apps
from django.core import checks
from pghistory import config

PGHISTORY_APP_NAME = 'pghistory'


def _sharded_event_models():
    from djanquiltdb_plugin_pghistory import sharded

    return [model for model in apps.get_models() if issubclass(model, sharded.Event)]


@checks.register(checks.Tags.compatibility)
def check_pghistory_sharding_configuration(app_configs, **kwargs):
    """
    Warn about django-pghistory configuration that does not survive sharding.

    The context table an event's foreign key would point at lives outside the shard holding the event table, so sharded
    events need the denormalized context field. pghistory's own Context model is placed by nothing unless the settings
    say where it goes; since we do not have a control to disable that table from getting created at all, we need to
    make sure it can be created in the least costly way (public) where it will happily sit unused.
    """
    from djanquiltdb.utils import get_sharding_mode

    if not apps.is_installed(PGHISTORY_APP_NAME):
        return []

    warnings = []

    if isinstance(config.context_field(), pghistory.ContextForeignKey) and _sharded_event_models():
        warnings.append(
            checks.Warning(
                'PGHISTORY_CONTEXT_FIELD is a foreign key while sharded event models exist.',
                hint=(
                    'An event table inside a shard cannot hold a foreign key to the context table outside it. Set '
                    'PGHISTORY_CONTEXT_FIELD = pghistory.ContextJSONField() to denormalize the context into the event '
                    'rows.'
                ),
                id='djanquiltdb_plugin_pghistory.W001',
            )
        )

    if not get_sharding_mode(PGHISTORY_APP_NAME, 'context'):
        warnings.append(
            checks.Warning(
                "Nothing places the pghistory app's Context model.",
                hint=(
                    'The app carries a concrete model, so migrations need to know where it goes. Add an '
                    "OVERRIDE_SHARDING_MODE entry for it to QUILT_DB, such as {('pghistory',): ShardingMode.SHARDED}. "
                    'This has to be configured explicitly even if you are not using the normalized context table.'
                ),
                id='djanquiltdb_plugin_pghistory.W002',
            )
        )

    return warnings


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
