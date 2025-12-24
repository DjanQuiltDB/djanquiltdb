# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations


class Migration(migrations.Migration):
    replaces = [
        ('migration_tests', '3_auto'),
        ('migration_tests', '4_auto'),
        ('migration_tests', '5_auto'),
    ]

    dependencies = [('migration_tests', '2_auto')]

    operations = [migrations.RunPython(migrations.RunPython.noop)]
