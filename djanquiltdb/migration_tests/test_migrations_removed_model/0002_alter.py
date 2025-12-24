# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ('migration_tests', '0001_initial'),
    ]

    operations = [
        migrations.AddField(
            model_name='Void',
            name='color',
            field=models.CharField(max_length=255),
        ),
    ]
