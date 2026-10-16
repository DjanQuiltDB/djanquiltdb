"""
The base model that PGHISTORY_BASE_MODEL names in the test settings. :func:`example.tracking.track` adds the trigger
that fills in its field.
"""

from django.db import models
from pghistory.models import Event as BaseEvent


class BatchFlagEvent(BaseEvent):
    """
    An event base model that records whether the event was written by a batch job.
    """

    pgh_from_batch = models.BooleanField(null=True)

    class Meta:
        abstract = True
