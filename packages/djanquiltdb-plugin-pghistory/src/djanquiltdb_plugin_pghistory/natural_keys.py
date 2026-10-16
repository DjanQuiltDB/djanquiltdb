from django.apps import apps
from djanquiltdb import ShardingMode
from djanquiltdb.utils import get_model_sharding_mode


class NaturalKeyByPrimaryKeyManagerMixin:
    def get_by_natural_key(self, pk):
        return self.get(pk=pk)


def natural_key_by_primary_key(instance):
    return (instance.pk,)


def make_the_primary_key_the_natural_key(model):
    """
    Gives a model the primary key as the natural key that DjanQuiltDB requires of a public model, where the model has no
    other value that identifies a row on every node.

    * The primary key is put first in unique_together, where DjanQuiltDB reads the natural key. It is set on _meta only.
      The migration autodetector reads _meta.original_attrs, so the table gets no unique constraint next to its primary
      key, and a unique_together that the model declares itself is the only one in its migrations. Model validation
      reads _meta, so full_clean() of a new row checks its primary key twice, and syncdb would add a unique constraint
      to the table of an app without migrations, which the pghistory app is not.
    * natural_key() is added when the model has none.
    * get_by_natural_key() is added, when the managers have none, by changing the class of each manager that the model
      declares. The model hands out copies of those managers, and apps.clear_cache() replaces the copies, so a method
      that was set on a copy would be lost.
    """
    natural_key_fields = (model._meta.pk.name,)

    if model._meta.unique_together[:1] != (natural_key_fields,):
        model._meta.unique_together = (natural_key_fields, *model._meta.unique_together)

    if not hasattr(model, 'natural_key'):
        model.natural_key = natural_key_by_primary_key

    for manager in model._meta.local_managers:
        if not hasattr(manager, 'get_by_natural_key'):
            manager.__class__ = type(
                type(manager).__name__,
                (NaturalKeyByPrimaryKeyManagerMixin, type(manager)),
                {'__module__': __name__},
            )


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
