"""
The hooks that DjanQuiltDB calls while move_data_to_shard and purge_shard_data run.

The simple collector of the commands follows every foreign key to a collected row, also the foreign keys of events: the
pgh_obj foreign key to the row that an event describes, and the snapshot of each foreign key of the tracked model. So it
finds the events of each collected row, and also the events of other rows whose snapshot points at a collected row.

The modules that these hooks need are imported inside them, because DjanQuiltDB loads the plugin before the app registry
is ready.
"""


def keep_only_the_events_of_collected_rows(command, data):
    """
    Removes each collected event whose row was not collected, served as the post_collect hook of both commands. Such an
    event was found through a snapshot of a foreign key to a collected row, but it describes a row that was deleted
    before, or that belongs to another root object now. Its history stays with that row, in the shard.

    An event model without a pgh_obj foreign key, such as the event model of a tracked many-to-many through model, has
    no other link to the rows than its snapshots, so its events are kept.

    An event model whose events are all left out stays in the data with no events, because the collector orders the
    delete by the dependencies that it recorded between the collected models.
    """
    import pghistory.models

    collected_pks_by_model = {}

    for model, instances in data.items():
        collected_pks_by_model.setdefault(model._meta.concrete_model, set()).update(
            instance.pk for instance in instances
        )

    for model in list(data):
        if not issubclass(model, pghistory.models.Event) or not hasattr(model, 'pgh_obj'):
            continue

        tracked_model = model._meta.get_field('pgh_obj').related_model._meta.concrete_model
        collected_pks = collected_pks_by_model.get(tracked_model, set())
        data[model] = {event for event in data[model] if event.pgh_obj_id in collected_pks}


def delete_moved_events(command, pk_set):
    """
    Deletes the moved events from the source shard. The relation from an event to its row has on_delete=DO_NOTHING,
    so Django's collector, which deletes the moved rows, leaves the events behind.
    """
    import pghistory.models
    from djanquiltdb.utils import use_shard

    with use_shard(command.source_shard, active_only_schemas=False, lock=False):
        for model, pks in pk_set.items():
            if issubclass(model, pghistory.models.Event):
                model.objects.filter(pk__in=pks).delete()


def delete_events_of_purged_rows(command, pk_set):
    """
    Deletes the events of the purged rows, served as the post_delete hook of purge_shard_data. Django's collector does
    not follow the relations of events, which have on_delete=DO_NOTHING, and the simple collector has already deleted
    the events that it found, so this finds nothing then.

    An event is deleted when its pgh_obj foreign key points at a purged row. An event model without that foreign key
    has no other link to the rows than its snapshots, so its events are deleted when a snapshot of a foreign key points
    at a purged row.
    """
    import pghistory.models
    from django.apps import apps
    from djanquiltdb.utils import use_shard

    purged_pks_by_model = {model._meta.concrete_model: pks for model, pks in pk_set.items()}

    with use_shard(command.shard, active_only_schemas=False, lock=False):
        for model in apps.get_models():
            if not issubclass(model, pghistory.models.Event):
                continue

            if hasattr(model, 'pgh_obj'):
                fields = [model._meta.get_field('pgh_obj')]
            else:
                fields = [field for field in model._meta.concrete_fields if field.many_to_one]

            for field in fields:
                purged_pks = purged_pks_by_model.get(field.related_model._meta.concrete_model)

                if purged_pks:
                    model.objects.filter(**{f'{field.attname}__in': purged_pks}).delete()
