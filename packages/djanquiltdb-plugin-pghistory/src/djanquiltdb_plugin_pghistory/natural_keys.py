from django.apps import apps


class NaturalKeyByPrimaryKeyManagerMixin:
    def get_by_natural_key(self, pk):
        return self.get(pk=pk)


def natural_key_by_primary_key(instance):
    return (instance.pk,)


def add_natural_keys_to_pghistory_models():
    """
    Gives the models of the pghistory app the natural key that DjanQuiltDB requires of a public model: the primary key.
    It is unique on every node: the id of Context is a UUID, and the pgh_slug of Events names the event model as well
    as the id of the event.

    The models belong to a third-party app, so the plugin changes them after they are created:

    * unique_together is set on _meta only. The migration autodetector reads _meta.original_attrs, so the pghistory app
      does not need a new migration.
    * get_by_natural_key() is added by changing the class of each manager that a model declares. The model hands out
      copies of those managers, and apps.clear_cache() replaces the copies, so a method that was set on a copy would be
      lost.

    Like the natural key of the public event models, the primary key is also the natural key. A fixture of contexts
    that is dumped with --natural-primary therefore leaves the id out, and loading it creates each context again with a
    new id.
    """
    if not apps.is_installed('pghistory'):
        return

    for model in apps.get_app_config('pghistory').get_models():
        # DjanQuiltDB does not validate proxy models, such as MiddlewareEvents, and Django rejects a unique_together on
        # a field that a proxy inherits. A proxy inherits natural_key() and the managers of its concrete model.
        if model._meta.proxy:
            continue

        model._meta.unique_together = ((model._meta.pk.name,),)
        model.natural_key = natural_key_by_primary_key

        for manager in model._meta.local_managers:
            if not isinstance(manager, NaturalKeyByPrimaryKeyManagerMixin):
                manager.__class__ = type(
                    type(manager).__name__,
                    (NaturalKeyByPrimaryKeyManagerMixin, type(manager)),
                    {'__module__': __name__},
                )

    # Every model, proxy models included, makes its copies of the managers again. apps.clear_cache() would not do this
    # while the app registry is still running the ready() of each app.
    for model in apps.get_app_config('pghistory').get_models():
        model._meta._expire_cache()
