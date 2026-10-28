from django.apps import apps
from djanquiltdb import ShardingMode
from djanquiltdb.natural_keys import make_the_primary_key_the_natural_key
from djanquiltdb.utils import get_model_sharding_mode


def add_natural_keys_to_public_pghistory_models():
    """
    Gives each public model of the pghistory app its primary key as the natural key. It is unique on every node: the id
    of Context is a UUID, and the pgh_slug of Events names the event model as well as the id of the event. A sharded or
    mirrored model needs no natural key, so it is left unchanged.

    Like the natural key of the public event models, the primary key is also the natural key. A fixture of contexts
    that is dumped with --natural-primary therefore leaves the id out, and loading it creates each context again with a
    new id.
    """
    if not apps.is_installed('pghistory'):
        return

    for model in apps.get_app_config('pghistory').get_models():
        # DjanQuiltDB does not validate proxy models, such as MiddlewareEvents, and Django rejects a unique_together on
        # a field that a proxy inherits. A proxy inherits natural_key() and the managers of its concrete model.
        if model._meta.proxy or get_model_sharding_mode(model) is not ShardingMode.PUBLIC:
            continue

        make_the_primary_key_the_natural_key(model)

    # Every model, proxy models included, makes its copies of the managers again. apps.clear_cache() would not do this
    # while the app registry is still running the ready() of each app.
    for model in apps.get_app_config('pghistory').get_models():
        model._meta._expire_cache()
