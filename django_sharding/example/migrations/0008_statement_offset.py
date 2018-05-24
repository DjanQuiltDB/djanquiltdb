# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('example', '0007_auto_20180521_0918'),
    ]

    operations = [
        migrations.AddField(
            model_name='statement',
            name='offset',
            field=models.PositiveIntegerField(verbose_name='offset', null=True, blank=True),
        ),
    ]
