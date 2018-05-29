# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('example', '0008_statement_offset'),
    ]

    operations = [
        migrations.AddField(
            model_name='organizationshards',
            name='slug',
            field=models.SlugField(default=''),
            preserve_default=False,
        ),
    ]
