=============
Customization
=============

The plugin keeps pghistory's two ways to customize event models:

* ``PGHISTORY_BASE_MODEL`` adds fields and methods to **every** event model. The plugin's event bases inherit from the
  model that this setting names.
* A wrapper around ``track()`` adds fields, meta attributes and triggers to the event models of **the call sites that
  use the wrapper**. The tracking functions pass ``attrs`` and ``meta`` on unchanged, so the wrapper works exactly as a
  wrapper around django-pghistory's own ``pghistory.track()`` would.

The second way is needed because pghistory builds the ``Meta`` of each generated model from scratch, and does not
inherit the ``Meta`` of the base. A trigger or another meta attribute that is meant for every event model therefore
cannot be declared on the base class, and has to be passed in the ``meta`` of a wrapper.

A worked example
================

Suppose that every event should record whether a batch job wrote it, so that interactive edits can be told apart from
bulk imports. Batch jobs run inside ``pghistory.context(batch=...)``, and the flag is computed when the event is
written.

The field is declared on the base model for the whole project. It must allow ``NULL``, because the trigger that writes
the events only sets the snapshot and ``pgh_*`` columns::

    # myproject/events.py
    from django.db import models
    from pghistory.models import Event as BaseEvent


    class BatchFlagEvent(BaseEvent):
        pgh_from_batch = models.BooleanField(null=True)

        class Meta:
            abstract = True

::

    # settings.py
    import pghistory

    PGHISTORY_BASE_MODEL = 'myproject.events.BatchFlagEvent'
    PGHISTORY_CONTEXT_FIELD = pghistory.ContextJSONField()

The example uses a denormalized context, so the event row has a copy of the context metadata. A ``track()`` wrapper adds
the trigger that computes the field. The trigger reads the key directly from the context column of the event row that is
being written::

    # myproject/tracking.py
    import pgtrigger

    from djanquiltdb.decorators import pghistory


    def batch_flag_trigger():
        return pgtrigger.Trigger(
            name='set_pgh_from_batch',
            operation=pgtrigger.Insert,
            when=pgtrigger.Before,
            func="NEW.pgh_from_batch := (NEW.pgh_context ? 'batch'); RETURN NEW;",
        )


    def track(*trackers, **kwargs):
        meta = dict(kwargs.pop('meta', None) or {})
        meta['triggers'] = [*meta.get('triggers', []), batch_flag_trigger()]

        return pghistory.track(*trackers, meta=meta, **kwargs)

Call sites use the wrapper wherever they would use the plugin's ``track()``. The wrapper calls the plugin's ``track()``,
so the mode is still read from the model, and ``base_model=`` and the other arguments work in the same way. A project
that uses the explicit tracking functions wraps each of them in the same way::

    from djanquiltdb.decorators import sharded_model

    from myproject.tracking import track


    @track()
    @sharded_model()
    class Order(models.Model):
        ...

Events written inside ``pghistory.context(batch=True)`` have ``pgh_from_batch = True``, events written inside another
context have ``False``, and events written outside any context have ``NULL``.

Models that are tracked without the wrapper still have the column, because the base is used for the whole project, but
nothing fills it in. The two ways are independent: a project that only needs extra fields sets the base model, and a
project that only needs extra meta at some call sites wraps ``track()``.
