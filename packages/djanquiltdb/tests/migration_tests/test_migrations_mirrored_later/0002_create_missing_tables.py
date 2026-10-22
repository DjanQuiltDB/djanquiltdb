"""
The migration that a project adds after it decorates existing models with ``@mirrored_model()``.
"""

from django.db import migrations

from djanquiltdb.operations import CreateMissingMirroredTable


class Migration(migrations.Migration):
    dependencies = [
        ('migration_tests', '0001_migrations_test_init'),
    ]

    operations = [
        CreateMissingMirroredTable('SuperMirroredModel'),
        CreateMissingMirroredTable('MirroredModel'),
    ]
