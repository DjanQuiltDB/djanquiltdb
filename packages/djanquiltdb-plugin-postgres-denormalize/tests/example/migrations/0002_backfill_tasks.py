from django.db import migrations
from postgres_denormalize.operations import BackfillDenormalized


class Migration(migrations.Migration):
    dependencies = [
        ('example', '0001_initial'),
    ]

    operations = [
        BackfillDenormalized('task'),
    ]
