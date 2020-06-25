from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('migration_tests', '0001_initial'),
    ]

    operations = [
        migrations.DeleteModel('NonExistingModel'),
    ]
