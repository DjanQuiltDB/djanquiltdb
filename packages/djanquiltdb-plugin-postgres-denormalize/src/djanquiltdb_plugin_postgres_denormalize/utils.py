"""
Helpers that find and correct the copies of django-postgres-denormalize in the shards of a DjanQuiltDB project.
DjanQuiltDB serves this module as ``djanquiltdb.utils.postgres_denormalize``.

``backfill_denormalized()`` and ``drift_condition()`` work on the table of the active shard only. These helpers enter
each shard for you.

The modules that the helpers need are imported inside them, because DjanQuiltDB loads the plugin before the app registry
is ready.
"""


def sharded_leaves():
    """
    Returns every sharded model that declares a denormalized field, in the order of the app registry. It is the default
    list of models for the other helpers. A model that django-postgres-denormalize excludes, through ``EXCLUDE_MODELS``
    or ``EXCLUDE_MODELS_CALLBACK``, is left out.
    """
    from django.apps import apps
    from djanquiltdb import ShardingMode
    from djanquiltdb.utils import get_declared_sharding_mode
    from postgres_denormalize.fields import get_denormalized_fields

    return [
        model
        for model in apps.get_models()
        if get_denormalized_fields(model) and get_declared_sharding_mode(model) == ShardingMode.SHARDED
    ]


def stale_rows(shard, models=None):
    """
    Returns, for each of ``models`` that has rows in ``shard`` whose copies differ from their lookup paths, the number
    of those rows. A model whose copies are all correct is left out, so an empty dict means that nothing needs a repair.
    ``models`` defaults to :func:`sharded_leaves`.
    """
    from djanquiltdb.utils import use_shard
    from postgres_denormalize.fields import drift_condition

    counts = {}

    with use_shard(shard):
        for model in sharded_leaves() if models is None else models:
            count = model._base_manager.filter(drift_condition(model)).count()

            if count:
                counts[model] = count

    return counts


def repair_shard(shard, models=None):
    """
    Corrects every copy of ``models`` in ``shard`` that differs from its lookup path, with ``backfill_denormalized()``,
    in one transaction on the shard's connection. A row whose copies are correct is not written. ``models`` defaults to
    :func:`sharded_leaves`.

    ``backfill_denormalized()`` corrects a whole table, so the repair covers every row in the shard, also the rows of
    other root objects than the one that the shard was entered for.
    """
    from django.db import transaction
    from djanquiltdb.utils import use_shard
    from postgres_denormalize.operations import backfill_denormalized

    with use_shard(shard), transaction.atomic():
        for model in sharded_leaves() if models is None else models:
            backfill_denormalized(model)


def repair_every_shard(models=None):
    """
    Runs :func:`repair_shard` for every active shard, on every node, and returns the shards that it skipped because they
    are not active. A shard that is being created, moved or purged cannot be entered, so run this again for those once
    they are active. Each shard is repaired in its own transaction, so a failure in one shard leaves the shards before
    it repaired.
    """
    from djanquiltdb import State
    from djanquiltdb.utils import for_each_shard

    models = sharded_leaves() if models is None else models
    skipped = []

    def repair(shard):
        if shard.state != State.ACTIVE:
            skipped.append(shard)
            return

        repair_shard(shard, models)

    for_each_shard(repair)

    return skipped


__all__ = ['repair_every_shard', 'repair_shard', 'sharded_leaves', 'stale_rows']
