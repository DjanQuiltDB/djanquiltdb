"""
Suite for django-pgtrigger under sharding, installed only by the ``core-pgtrigger-*`` tox environments.

The other environments do not install django-pgtrigger, but they do run ``manage.py test`` with no labels, so their
discovery walks this package all the same and would import :mod:`pgtrigger` through the models below. Raising
``SkipTest`` while the package itself is imported is what unittest discovery understands as "skip this whole tree",
so it stops before reaching any module that needs the dependency.

The check is ``find_spec`` rather than ``apps.is_installed``, which the rest of the suite uses: this module is also
imported while the app registry is being populated, and ``is_installed`` raises ``AppRegistryNotReady`` there.
"""

from importlib.util import find_spec
from unittest import SkipTest

if find_spec('pgtrigger') is None:
    raise SkipTest('django-pgtrigger is not installed in this environment')
