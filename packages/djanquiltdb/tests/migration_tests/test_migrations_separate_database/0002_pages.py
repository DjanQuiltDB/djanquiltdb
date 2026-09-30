from django.db import migrations, models

# One entry per run of count_pages: the connection alias of the schema and the fields of the Author model it got.
COUNT_PAGES_RUNS = []


def count_pages(apps, schema_editor):
    author = apps.get_model('migration_tests', 'Author')
    COUNT_PAGES_RUNS.append((schema_editor.connection.alias, sorted(field.name for field in author._meta.get_fields())))
    author.objects.using(schema_editor.connection.alias).filter(pages=0).count()


class Migration(migrations.Migration):
    dependencies = [
        ('migration_tests', '0001_initial'),
    ]

    operations = [
        migrations.SeparateDatabaseAndState(
            database_operations=[
                migrations.AddField('Author', 'pages', models.IntegerField(default=0)),
                migrations.RunPython(count_pages, migrations.RunPython.noop),
            ],
            state_operations=[
                migrations.AddField('Author', 'pages', models.IntegerField(default=0)),
            ],
        ),
    ]
