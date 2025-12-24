from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("migration_tests", "0002_second"),
    ]

    operations = [
        migrations.CreateModel(
            name='Hometown',
            fields=[
                ('id', models.AutoField(verbose_name='ID', primary_key=True, auto_created=True, serialize=False)),
            ],
        ),
    ]
