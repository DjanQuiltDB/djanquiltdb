# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('example', '0002_auto_20171009_1502'),
    ]

    operations = [
        migrations.AlterField(
            model_name='user',
            name='type',
            field=models.ForeignKey(verbose_name='type', null=True, on_delete=django.db.models.deletion.DO_NOTHING,
                                    to='example.Type'),
        ),
        migrations.CreateModel(
            name='SuperType',
            fields=[
                ('id', models.AutoField(verbose_name='ID', primary_key=True, auto_created=True, serialize=False)),
                ('name', models.CharField(max_length=100, verbose_name='name')),
            ],
        ),
        migrations.AddField(
            model_name='type',
            name='super',
            field=models.ForeignKey(to='example.SuperType', verbose_name='super',
                                    on_delete=django.db.models.deletion.DO_NOTHING, null=True),
        ),
    ]
