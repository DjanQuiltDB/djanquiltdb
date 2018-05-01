# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('migration_tests', '0002_alter'),
    ]

    operations = [
        migrations.DeleteModel(
            name='Void',
        ),
    ]
