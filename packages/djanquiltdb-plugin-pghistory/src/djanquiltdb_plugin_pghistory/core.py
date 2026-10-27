import pghistory
from django.core.exceptions import ImproperlyConfigured
from djanquiltdb import ShardingMode
from djanquiltdb.utils import get_declared_placement, get_declared_sharding_mode

from djanquiltdb_plugin_pghistory import mirrored, public, sharded, single_node

_EVENT_BASE_BY_SHARDING_MODE = {
    ShardingMode.SHARDED: sharded.Event,
    ShardingMode.MIRRORED: mirrored.Event,
    ShardingMode.PUBLIC: public.Event,
    ShardingMode.SINGLE_NODE_UNSHARDED: single_node.Event,
}


def _resolve_base_model(tracked_model, base_model):
    """
    Returns the plugin base for the sharding mode of the tracked model, or base_model after checking it against the
    plugin bases and that mode.
    """
    mode = get_declared_sharding_mode(tracked_model)

    if base_model is None:
        if mode not in _EVENT_BASE_BY_SHARDING_MODE:
            raise ImproperlyConfigured(
                'Cannot determine where to place the events of {label}: apply @sharded_model(), @mirrored_model() '
                'or @public_model() below the tracking decorator, add an OVERRIDE_SHARDING_MODE entry for the '
                'model, or pass base_model= explicitly.'.format(label=tracked_model._meta.label)
            )

        return _EVENT_BASE_BY_SHARDING_MODE[mode]

    for base_mode, event_base in _EVENT_BASE_BY_SHARDING_MODE.items():
        if issubclass(base_model, event_base):
            if mode is not None and mode != base_mode:
                raise ImproperlyConfigured(
                    'base_model {base} places the events of {label} as {base_mode}, but {label} is {mode}, so its '
                    'events would be stored in a different place than the rows that they describe.'.format(
                        base=base_model.__name__,
                        label=tracked_model._meta.label,
                        base_mode=base_mode.name,
                        mode=mode.name,
                    )
                )

            return base_model

    raise ImproperlyConfigured(
        'base_model {base} does not inherit any of the plugin event bases (ShardedEvent, MirroredEvent, '
        'PublicEvent, SingleNodeEvent), so the events of {label} would have no sharding mode.'.format(
            base=base_model.__name__, label=tracked_model._meta.label
        )
    )


def _attrs_of_the_event_model(tracked_model, base_model, attrs):
    """
    Returns the ``attrs`` of the event model of ``tracked_model``. Events on a single node also get the node of the
    tracked model, which is the node where the trigger of the tracked model writes them.
    """
    if not issubclass(base_model, single_node.Event):
        return attrs

    node = get_declared_placement(tracked_model)[1]

    if node is None:
        raise ImproperlyConfigured(
            'base_model {base} stores the events on the node of {label}, but {label} is not on a single node.'.format(
                base=base_model.__name__, label=tracked_model._meta.label
            )
        )

    return {**(attrs or {}), '__single_node': node}


def create_event_model(tracked_model, *trackers, base_model=None, attrs=None, **kwargs):
    """
    Calls ``pghistory.create_event_model`` with the plugin base for the sharding mode of the tracked model, and not with
    ``PGHISTORY_BASE_MODEL``. Takes the same arguments. An explicit ``base_model`` must inherit one of the plugin bases.
    The events of a model on a single node are stored on the node of that model.
    """
    base_model = _resolve_base_model(tracked_model, base_model)
    attrs = _attrs_of_the_event_model(tracked_model, base_model, attrs)

    return pghistory.create_event_model(tracked_model, *trackers, base_model=base_model, attrs=attrs, **kwargs)


def track(*trackers, base_model=None, attrs=None, **kwargs):
    """
    Calls ``pghistory.track`` with a base model that matches the sharding mode of the tracked model.

    Takes the same arguments as ``pghistory.track``. Without an explicit ``base_model``, the mode is read from the
    decorated model, so the mode decorator has to be applied first, below this one. An explicit ``base_model`` must
    inherit one of the plugin bases, and is checked against the mode of the model whenever the model has one. The events
    of a model on a single node are stored on the node of that model.
    """

    def track_with_resolved_base_model(tracked_model):
        resolved_base_model = _resolve_base_model(tracked_model, base_model)
        attrs_of_the_event_model = _attrs_of_the_event_model(tracked_model, resolved_base_model, attrs)

        return pghistory.track(*trackers, base_model=resolved_base_model, attrs=attrs_of_the_event_model, **kwargs)(
            tracked_model
        )

    return track_with_resolved_base_model


def _base_model_for_explicit_tracking_function(base_model, event_base, function_name, event_base_name):
    if base_model is None:
        return event_base

    if not issubclass(base_model, event_base):
        raise ImproperlyConfigured(
            'The base_model that is passed to {function_name}() must inherit from {event_base_name}.'.format(
                function_name=function_name, event_base_name=event_base_name
            )
        )

    return base_model


def track_sharded(*trackers, base_model=None, **kwargs):
    """
    Tracks changes to a sharded model. Each event is stored in the same shard as the row that it describes.
    """
    return track(
        *trackers,
        base_model=_base_model_for_explicit_tracking_function(
            base_model, sharded.Event, 'track_sharded', 'ShardedEvent'
        ),
        **kwargs,
    )


def track_mirrored(*trackers, base_model=None, **kwargs):
    """
    Tracks changes to a mirrored model. The generated event model is mirrored too, so its table is in the public schema
    of every node, like the table of the tracked model.
    """
    return track(
        *trackers,
        base_model=_base_model_for_explicit_tracking_function(
            base_model, mirrored.Event, 'track_mirrored', 'MirroredEvent'
        ),
        **kwargs,
    )


def track_public(*trackers, base_model=None, **kwargs):
    """
    Tracks changes to a public model. The generated event model is public too, so its table is in the public schema.
    """
    return track(
        *trackers,
        base_model=_base_model_for_explicit_tracking_function(base_model, public.Event, 'track_public', 'PublicEvent'),
        **kwargs,
    )
