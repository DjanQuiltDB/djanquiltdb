"""Development settings with django-pgtrigger installed, used by the core-pgtrigger tox environment."""

from .dev import *  # NOQA

INSTALLED_APPS = (*INSTALLED_APPS, 'pgtrigger')  # NOQA: F405
