"""
The ``track()`` wrapper from the customization documentation, which adds the trigger that fills in pgh_from_batch.
"""

import pgtrigger

import djanquiltdb_plugin_pghistory


def batch_flag_trigger():
    """
    A trigger that sets pgh_from_batch to whether the context of the event has a batch key.
    """
    return pgtrigger.Trigger(
        name='set_pgh_from_batch',
        operation=pgtrigger.Insert,
        when=pgtrigger.Before,
        func="NEW.pgh_from_batch := (NEW.pgh_context ? 'batch'); RETURN NEW;",
    )


def track(*trackers, **kwargs):
    """
    The plugin's track(), with the batch flag trigger added to the generated event model.
    """
    meta = dict(kwargs.pop('meta', None) or {})
    meta['triggers'] = [*meta.get('triggers', []), batch_flag_trigger()]

    return djanquiltdb_plugin_pghistory.track(*trackers, meta=meta, **kwargs)
