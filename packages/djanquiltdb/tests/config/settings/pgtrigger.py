"""Development settings with django-pgtrigger installed, used by the core-pgtrigger tox environment."""

from .dev import *  # NOQA

# The suite in pgtrigger_tests declares triggers through django-pgtrigger, so its models and migrations only import
# in this environment, and it is only installed here.
INSTALLED_APPS = (*INSTALLED_APPS, 'pgtrigger', 'pgtrigger_tests')  # NOQA: F405
