"""
Copyright (c) Django Software Foundation and individual contributors.
All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

    1. Redistributions of source code must retain the above copyright notice,
       this list of conditions and the following disclaimer.

    2. Redistributions in binary form must reproduce the above copyright
       notice, this list of conditions and the following disclaimer in the
       documentation and/or other materials provided with the distribution.

    3. Neither the name of Django nor the names of its contributors may be used
       to endorse or promote products derived from this software without
       specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""

import progressbar

from django.db.models.deletion import get_candidate_relations_to_delete as get_candidate_relations_to_collect


class SimpleCollector(object):
    """
    SimpleCollector is a copy of Django's own delete Collector.
    It's altered to collect all related data of the given objects without looking at on_delete logic.
    Another alteration is that it only saves pks, and not whole objects.
    """
    def __init__(self, connection, verbose=False):
        self.connection = connection
        self.verbose = verbose
        self.data = {}
        self.data_points = 0

        self.bar = progressbar.ProgressBar(max_value=progressbar.UnknownLength)

    def add(self, objs, source=None):
        """
        Adds the pk 'objs' to the collection of objects to be deleted.
        If the call is the result of a cascade, 'source' should be the model that caused it.

        Returns a list of all objects that were not already collected.
        """
        if not objs:
            return []
        new_objs = []
        new_pks = []
        model = objs[0].__class__
        instances = self.data.setdefault(model, set())
        for obj in objs:
            if obj not in instances:
                new_pks.append(obj.pk)
                new_objs.append(obj)
        instances.update(new_pks)

        if self.verbose:
            self.data_points += len(new_pks)
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

    def collect(self, objs, source=None, collect_related=True, source_attr=None):
        """
        Adds the pk of 'objs' to the collection of objects given as well as all parent instances.
        'objs' must be a homogeneous iterable collection of model instances (e.g. a QuerySet).
        If 'collect_related' is True, all related objects (children) are collected as well.
        """
        new_objs = self.add(objs, source)
        if not new_objs:
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
                self.collect(parent_objs, source=model,
                             source_attr=ptr.rel.related_name,
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
            for field in model._meta.virtual_fields:
                if hasattr(field, 'bulk_related_objects'):
                    # Its something like generic foreign key.
                    sub_objs = field.bulk_related_objects(new_objs, self.connection)
                    self.collect(sub_objs,
                                 source=model,
                                 source_attr=field.rel.related_name)

    def related_objects(self, related, objs):
        """
        Gets a QuerySet of objects related to ``objs`` via the relation ``related``.

        """
        return related.related_model._base_manager.filter(
            **{"%s__in" % related.field.name: objs}
        )
