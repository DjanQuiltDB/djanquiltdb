from django.db import migrations, models


class Migration(migrations.Migration):
    initial = True

    dependencies = []

    operations = [
        migrations.CreateModel(
            name='Shard',
            fields=[
                ('id', models.BigAutoField(auto_created=True, verbose_name='ID', serialize=False, primary_key=True)),
                ('alias', models.CharField(unique=True, db_index=True, max_length=128)),
                ('schema_name', models.CharField(max_length=64)),
                ('node_name', models.CharField(max_length=64)),
                ('state', models.CharField(choices=[('A', 'Active'), ('M', 'Maintenance')], default='M', max_length=1)),
            ],
        ),
        migrations.CreateModel(
            name='Cake',
            fields=[
                ('id', models.BigAutoField(auto_created=True, verbose_name='ID', serialize=False, primary_key=True)),
                ('name', models.CharField(verbose_name='name', max_length=128)),
            ],
        ),
        migrations.CreateModel(
            name='CakeType',
            fields=[
                ('id', models.BigAutoField(auto_created=True, verbose_name='ID', serialize=False, primary_key=True)),
                ('name', models.CharField(verbose_name='name', max_length=100)),
            ],
            options={
                'unique_together': {('name',)},
            },
        ),
    ]
