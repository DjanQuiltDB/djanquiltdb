# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('example', '0006_cake'),
    ]

    operations = [
        migrations.CreateModel(
            name='ProxyCake',
            fields=[
            ],
            options={
                'proxy': True,
            },
            bases=('example.cake',),
        ),
        migrations.AddField(
            model_name='statement',
            name='type',
            field=models.ManyToManyField(verbose_name='types', to='example.Type'),
        ),
    ]
