import progressbar

from django.db.models.deletion import get_candidate_relations_to_delete as get_candidate_relations_to_collect, Collector


class SimpleCollector(Collector):
    """
    Simple collector does basically the same as the default Django collector, but also follows the on delete SET NULL
    relations. We need this to move data from one shard to another.
    """
    def __init__(self, connection, verbose=False):
        super().__init__(using=connection.alias)

        self.connection = connection
        self.verbose = verbose
        self.data_points = 0

        if self.verbose:
            self.bar = progressbar.ProgressBar(
                max_value=progressbar.UnknownLength,
                widgets=[
                    progressbar.RotatingMarker(),
                    ' Collected ',
                    progressbar.Counter(),
                    ' datapoints; ',
                    progressbar.Timer()
                ]
            )

    def add(self, *args, **kwargs):
        new_objs = super().add(*args, **kwargs)

        if self.verbose:
            self.data_points += len(new_objs)
            self.bar.update(self.data_points)

        return new_objs

    def get_batches(self, objs, field):
        """
        Returns the objs in suitably sized batches for the used connection.
        """
        conn_batch_size = max(
            self.connection.ops.bulk_batch_size([field.name], objs), 1)
        if len(objs) > conn_batch_size:
            return [objs[i:i + conn_batch_size]
                    for i in range(0, len(objs), conn_batch_size)]
        else:
            return [objs]

    def finish_bar(self):
        if self.verbose:
            self.bar.finish()

    def collect(self, objs, source=None, nullable=False, collect_related=True, source_attr=None,
                reverse_dependency=False):
        """
        Kind of the same as the original collector, but now also follows the on delete SET NULL relations.
        """
        new_objs = self.add(objs, source, nullable, reverse_dependency=reverse_dependency)

        if not new_objs:
            self.finish_bar()
            return

        model = new_objs[0].__class__

        # Recursively collect concrete model's parent models, but not their
        # related objects. These will be found by meta.get_fields()
        concrete_model = model._meta.concrete_model
        for ptr in concrete_model._meta.parents:
            if ptr:
                # FIXME: This seems to be buggy and execute a query for each
                # parent object fetch. We have the parent data in the obj,
                # but we don't have a nice way to turn that data into parent
                # object instance.
                parent_objs = [getattr(obj, ptr.name) for obj in new_objs]
                # Django < 2.0
                remote_field = 'rel' if hasattr(ptr, 'rel') else 'remote_field'
                self.collect(parent_objs, source=model,
                             source_attr=getattr(ptr, remote_field).related_name,
                             collect_related=False)

        if collect_related:
            for related in get_candidate_relations_to_collect(model._meta):
                field = related.field
                batches = self.get_batches(new_objs, field)
                for batch in batches:
                    sub_objs = self.related_objects(related, batch)
                    if not sub_objs:
                        continue
                    self.collect(sub_objs,
                                 source=model,
                                 source_attr=field.name)
            # Django < 2.0
            private_fields = 'virtual_fields' if hasattr(model._meta, 'virtual_fields') else 'private_fields'
            for field in getattr(model._meta, private_fields):
                if hasattr(field, 'bulk_related_objects'):
                    # Its something like generic foreign key.
                    sub_objs = field.bulk_related_objects(new_objs, self.using)
                    # Django < 2.0
                    remote_field = 'rel' if hasattr(field, 'rel') else 'remote_field'
                    self.collect(sub_objs,
                                 source=model,
                                 source_attr=getattr(field, remote_field).related_name)
