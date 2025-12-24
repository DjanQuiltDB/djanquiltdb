from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    operations = [
        migrations.CreateModel(
            'Void',
            [
                ('id', models.AutoField(primary_key=True)),
                ('name', models.CharField(max_length=255)),
            ],
        ),
    ]
