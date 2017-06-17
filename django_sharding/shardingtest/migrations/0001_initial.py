# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Shard',
            fields=[
                ('id', models.AutoField(verbose_name='ID', primary_key=True, auto_created=True, serialize=False)),
                ('alias', models.CharField(max_length=128, db_index=True)),
                ('schema_name', models.CharField(max_length=64)),
                ('node_name', models.CharField(max_length=64)),
            ],
        ),
    ]
