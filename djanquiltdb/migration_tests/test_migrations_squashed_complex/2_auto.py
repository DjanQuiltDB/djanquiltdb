# -*- coding: utf-8 -*-

from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [('migration_tests', '1_auto')]

    operations = [migrations.RunPython(migrations.RunPython.noop)]
