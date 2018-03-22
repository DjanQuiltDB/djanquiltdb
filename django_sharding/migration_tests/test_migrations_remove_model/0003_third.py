from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("migration_tests", "0002_second"),
    ]

    operations = [
        migrations.SeparateDatabaseAndState(
            state_operations=[
                migrations.DeleteModel("Knights"),
            ]
        )
    ]
