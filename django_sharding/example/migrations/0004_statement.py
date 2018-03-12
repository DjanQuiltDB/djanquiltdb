# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations, models
from django.conf import settings


class Migration(migrations.Migration):

    dependencies = [
        ('example', '0003_auto_20171130_1440'),
    ]

    operations = [
        migrations.CreateModel(
            name='Statement',
            fields=[
                ('id', models.AutoField(verbose_name='ID', primary_key=True, auto_created=True, serialize=False)),
                ('content', models.CharField(max_length=300, verbose_name='content')),
                ('user', models.ForeignKey(to=settings.AUTH_USER_MODEL, verbose_name='user')),
            ],
        ),
    ]
