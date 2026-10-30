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
    other value that identifies a row on every node. This is for a model of a third-party app, which cannot declare a
    natural key itself.

    * The primary key is put first in unique_together, where DjanQuiltDB reads the natural key. It is set on _meta only.
      The migration autodetector reads _meta.original_attrs, so the table gets no unique constraint next to its primary
      key, and a unique_together that the model declares itself is the only one in its migrations. Model validation
      reads _meta, so full_clean() of a new row checks its primary key twice, and syncdb would add a unique constraint
      to the table of an app without migrations.
    * natural_key() is added when the model has none.
    * get_by_natural_key() is added, when the managers have none, by changing the class of each manager that the model
      declares. The model hands out copies of those managers, and apps.clear_cache() replaces the copies, so a method
      that was set on a copy would be lost.

    The copies that the model has already handed out are replaced once its ``_meta._expire_cache()`` runs. Call that
    after this, also for the proxy models of the model, because apps.clear_cache() does not reach them while the app
    registry is still running the ready() of each app.
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


def add_natural_keys_to_public_session_models():
    """
    Gives the Session model of ``django.contrib.sessions`` its session key as the natural key, when the app is public.

    The ``djanquiltdb.sessions`` backend does not use that model, but the app has the ``clearsessions`` command, and its
    table is best kept in the public schema of each node rather than in every shard (see the Sessions guide). The
    session key is the primary key, a random string that Django draws from 36 ** 32 values, so it is unique on every
    node. Nothing has a foreign key to a session, so no command looks a session up on another node by its natural key.

    When the app is sharded, mirrored or on a single node, DjanQuiltDB requires no natural key of its models, so they
    are left unchanged.
    """
    if not apps.is_installed('django.contrib.sessions'):
        return

    session_models = list(apps.get_app_config('sessions').get_models())

    for model in session_models:
        if model._meta.proxy or get_model_sharding_mode(model) is not ShardingMode.PUBLIC:
            continue

        make_the_primary_key_the_natural_key(model)

    for model in session_models:
        model._meta._expire_cache()
