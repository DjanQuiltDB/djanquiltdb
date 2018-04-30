from django.db import migrations, models


def a_function():
    pass


class Migration(migrations.Migration):
    dependencies = [
        ("migration_tests", "0001_initial"),
    ]

    operations = [
        # migrations.AddField("UnexistingModel", "some_field", models.IntegerField(default=0)),
        migrations.RunSQL('SET CONSTRAINTS ALL IMMEDIATE', reverse_sql=migrations.RunSQL.noop),
    ]
