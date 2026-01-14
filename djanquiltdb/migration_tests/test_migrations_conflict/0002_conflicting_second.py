# -*- coding: utf-8 -*-

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [('migration_tests', '0001_initial')]

    operations = [
        migrations.CreateModel(
            'Something',
            [
                ('id', models.AutoField(primary_key=True)),
            ],
        )
    ]
