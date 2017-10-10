# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('example', '0001_initial'),
    ]

    operations = [
        migrations.AlterField(
            model_name='organizationshards',
            name='state',
            field=models.CharField(max_length=1, default='A', choices=[('A', 'Active'), ('M', 'Maintenance')]),
        ),
    ]
