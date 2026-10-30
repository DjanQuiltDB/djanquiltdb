import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):
    initial = True

    dependencies = []

    operations = [
        migrations.CreateModel(
            name='User',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('password', models.CharField(max_length=128, verbose_name='password')),
                ('last_login', models.DateTimeField(blank=True, null=True, verbose_name='last login')),
                ('email', models.EmailField(max_length=254, unique=True, verbose_name='email address')),
                ('is_active', models.BooleanField(default=True, verbose_name='active')),
            ],
        ),
        migrations.CreateModel(
            name='QuiltSession',
            fields=[
                ('session_data', models.TextField(verbose_name='session data')),
                ('expire_date', models.DateTimeField(db_index=True, verbose_name='expire date')),
                ('session_key', models.CharField(max_length=255, primary_key=True, serialize=False)),
            ],
        ),
        migrations.CreateModel(
            name='Shard',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('alias', models.CharField(db_index=True, max_length=128, unique=True)),
                ('schema_name', models.CharField(max_length=64)),
                ('node_name', models.CharField(max_length=64)),
                ('state', models.CharField(choices=[('A', 'Active'), ('M', 'Maintenance')], default='M', max_length=1)),
            ],
        ),
        migrations.CreateModel(
            name='OrganizationShard',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('organization_id', models.PositiveIntegerField(unique=True)),
                ('state', models.CharField(choices=[('A', 'Active'), ('M', 'Maintenance')], default='A', max_length=1)),
                ('shard', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='example.shard')),
            ],
        ),
    ]
