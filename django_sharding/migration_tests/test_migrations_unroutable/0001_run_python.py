from django.db import migrations


class Migration(migrations.Migration):
    initial = True
    operations = [
        migrations.RunPython(lambda: None, reverse_code=migrations.RunPython.noop),
    ]
