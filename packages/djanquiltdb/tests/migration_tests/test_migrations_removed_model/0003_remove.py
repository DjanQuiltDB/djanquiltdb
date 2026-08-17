# -*- coding: utf-8 -*-

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
