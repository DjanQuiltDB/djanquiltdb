from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("migration_tests", "0001_initial"),
    ]

    operations = [

        migrations.RemoveField("Knights", "name"),

        migrations.AddField("Knights", "sir_name", models.CharField(max_length=255)),

    ]
