from django.db import migrations, models

# One entry per run of record_apps: the connection alias of the schema, and the historical models registry that
# record_apps receives.
RECORD_APPS_RUNS = []


def record_apps(apps, schema_editor):
    RECORD_APPS_RUNS.append((schema_editor.connection.alias, apps))


class Migration(migrations.Migration):
    dependencies = [
        ('migration_tests', '0001_initial'),
    ]

    operations = [
        migrations.AddField('Author', 'pages', models.IntegerField(default=0)),
        migrations.RunPython(record_apps, migrations.RunPython.noop),
    ]
