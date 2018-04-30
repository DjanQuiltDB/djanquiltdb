from django.db import migrations, models


def a_function():
    pass


class Migration(migrations.Migration):
    initial = True
    operations = [
        migrations.RunPython(a_function, reverse_code=migrations.RunPython.noop),
    ]
