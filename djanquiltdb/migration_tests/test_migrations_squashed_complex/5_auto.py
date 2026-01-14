# -*- coding: utf-8 -*-

from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [('migration_tests', '4_auto')]

    operations = [migrations.RunPython(migrations.RunPython.noop)]
