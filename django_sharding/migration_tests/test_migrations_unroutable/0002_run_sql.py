from django.db import migrations, models


def a_function():
    pass


class Migration(migrations.Migration):
    dependencies = [
        ('migration_tests', '0001_run_python'),
    ]

    operations = [
        migrations.RunSQL('SET CONSTRAINTS ALL IMMEDIATE', reverse_sql=migrations.RunSQL.noop),
    ]
