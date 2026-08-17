"""
The RunPython counterpart of 0001: the same hint, read from the same place, on the other operation that carries one.
"""

from django.db import migrations

from djanquiltdb import ShardingMode


def create_table(apps, schema_editor):
    schema_editor.execute('CREATE TABLE hinted_python (id serial PRIMARY KEY)')


def drop_table(apps, schema_editor):
    schema_editor.execute('DROP TABLE hinted_python')


class Migration(migrations.Migration):
    dependencies = [
        ('migration_tests', '0001_run_sql'),
    ]

    operations = [
        migrations.RunPython(
            create_table,
            reverse_code=drop_table,
            hints={'sharding_mode': ShardingMode.SHARDED},
        ),
    ]
