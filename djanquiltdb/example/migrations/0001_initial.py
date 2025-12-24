# -*- coding: utf-8 -*-

import django.utils.timezone
from django.conf import settings
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = []

    operations = [
        migrations.CreateModel(
            name='User',
            fields=[
                ('id', models.BigAutoField(auto_created=True, verbose_name='ID', serialize=False, primary_key=True)),
                ('password', models.CharField(verbose_name='password', max_length=128)),
                ('last_login', models.DateTimeField(blank=True, verbose_name='last login', null=True)),
                ('name', models.CharField(verbose_name='name', max_length=100)),
                ('email', models.EmailField(unique=True, verbose_name='email address', max_length=254)),
                ('created_at', models.DateTimeField(verbose_name='date joined', default=django.utils.timezone.now)),
                (
                    'is_staff',
                    models.BooleanField(
                        verbose_name='staff status',
                        default=False,
                        help_text='Designates whether the user can log into this admin site.',
                    ),
                ),
                (
                    'is_active',
                    models.BooleanField(
                        verbose_name='active',
                        default=True,
                        help_text='Designates whether this user should be treated as active. Unselect this instead of deleting accounts.',
                    ),
                ),
            ],
        ),
        migrations.CreateModel(
            name='CakeType',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=128, verbose_name='name')),
            ],
        ),
        migrations.AlterUniqueTogether(
            name='caketype',
            unique_together=set([('name',)]),
        ),
        migrations.CreateModel(
            name='ProxyCakeType',
            fields=[],
            options={
                'proxy': True,
            },
            bases=('example.caketype',),
        ),
        migrations.CreateModel(
            name='CoatingType',
            fields=[
                ('id', models.BigAutoField(auto_created=True, verbose_name='ID', serialize=False, primary_key=True)),
                ('hash', models.CharField(verbose_name='hash', max_length=40)),
                (
                    'type',
                    models.ForeignKey(
                        null=True,
                        on_delete=django.db.models.deletion.DO_NOTHING,
                        to='example.CakeType',
                        verbose_name='type',
                    ),
                ),
            ],
        ),
        migrations.AlterUniqueTogether(
            name='coatingtype',
            unique_together=set([('hash',)]),
        ),
        migrations.CreateModel(
            name='Cake',
            fields=[
                ('id', models.BigAutoField(auto_created=True, verbose_name='ID', serialize=False, primary_key=True)),
                ('name', models.CharField(verbose_name='name', max_length=128)),
                (
                    'type',
                    models.ForeignKey(
                        null=True,
                        on_delete=django.db.models.deletion.DO_NOTHING,
                        to='example.CakeType',
                        verbose_name='type',
                    ),
                ),
                (
                    'coating_type',
                    models.ForeignKey(
                        null=True,
                        on_delete=django.db.models.deletion.DO_NOTHING,
                        to='example.CoatingType',
                        verbose_name='Coating Type',
                    ),
                ),
            ],
        ),
        migrations.CreateModel(
            name='DefaultUser',
            fields=[
                ('id', models.BigAutoField(auto_created=True, verbose_name='ID', serialize=False, primary_key=True)),
                ('password', models.CharField(verbose_name='password', max_length=128)),
                ('last_login', models.DateTimeField(blank=True, verbose_name='last login', null=True)),
                ('name', models.CharField(verbose_name='name', max_length=100)),
                ('email', models.EmailField(unique=True, verbose_name='email address', max_length=254)),
                ('created_at', models.DateTimeField(verbose_name='date joined', default=django.utils.timezone.now)),
                (
                    'is_staff',
                    models.BooleanField(
                        verbose_name='staff status',
                        default=False,
                        help_text='Designates whether the user can log into this admin site.',
                    ),
                ),
                (
                    'is_active',
                    models.BooleanField(
                        verbose_name='active',
                        default=True,
                        help_text='Designates whether this user should be treated as active. Unselect this instead of deleting accounts.',
                    ),
                ),
            ],
        ),
        migrations.CreateModel(
            name='MirroredUser',
            fields=[
                ('id', models.BigAutoField(auto_created=True, verbose_name='ID', serialize=False, primary_key=True)),
                ('password', models.CharField(verbose_name='password', max_length=128)),
                ('last_login', models.DateTimeField(blank=True, verbose_name='last login', null=True)),
                ('name', models.CharField(verbose_name='name', max_length=100)),
                ('email', models.EmailField(unique=True, verbose_name='email address', max_length=254)),
                ('created_at', models.DateTimeField(verbose_name='date joined', default=django.utils.timezone.now)),
                (
                    'is_staff',
                    models.BooleanField(
                        verbose_name='staff status',
                        default=False,
                        help_text='Designates whether the user can log into this admin site.',
                    ),
                ),
                (
                    'is_active',
                    models.BooleanField(
                        verbose_name='active',
                        default=True,
                        help_text='Designates whether this user should be treated as active. Unselect this instead of deleting accounts.',
                    ),
                ),
            ],
        ),
        migrations.CreateModel(
            name='ProxyMirroredUser',
            fields=[],
            options={
                'proxy': True,
                'indexes': [],
            },
            bases=('example.mirroreduser',),
        ),
        migrations.CreateModel(
            name='Organization',
            fields=[
                ('id', models.BigAutoField(auto_created=True, verbose_name='ID', serialize=False, primary_key=True)),
                ('name', models.CharField(verbose_name='name', max_length=100)),
                ('created_at', models.DateTimeField(verbose_name='created at', default=django.utils.timezone.now)),
            ],
        ),
        migrations.CreateModel(
            name='OrganizationShards',
            fields=[
                ('id', models.BigAutoField(auto_created=True, verbose_name='ID', serialize=False, primary_key=True)),
                ('organization_id', models.PositiveSmallIntegerField()),
                ('state', models.CharField(choices=[('A', 'Active'), ('M', 'Maintenance')], default='A', max_length=1)),
                ('slug', models.SlugField()),
            ],
        ),
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
            name='Statement',
            fields=[
                ('id', models.BigAutoField(auto_created=True, verbose_name='ID', serialize=False, primary_key=True)),
                ('content', models.CharField(verbose_name='content', max_length=300)),
                ('offset', models.PositiveIntegerField(blank=True, verbose_name='offset', null=True)),
            ],
        ),
        migrations.CreateModel(
            name='Suborganization',
            fields=[
                ('id', models.BigAutoField(auto_created=True, verbose_name='ID', serialize=False, primary_key=True)),
                (
                    'child',
                    models.OneToOneField(
                        to='example.Organization',
                        verbose_name='organization',
                        related_name='children',
                        on_delete=models.CASCADE,
                    ),
                ),
                (
                    'parent',
                    models.ForeignKey(
                        to='example.Organization',
                        related_name='parent',
                        verbose_name='organization',
                        on_delete=models.CASCADE,
                    ),
                ),
            ],
        ),
        migrations.CreateModel(
            name='SuperType',
            fields=[
                ('id', models.BigAutoField(auto_created=True, verbose_name='ID', serialize=False, primary_key=True)),
                ('name', models.CharField(verbose_name='name', max_length=100)),
            ],
        ),
        migrations.AlterUniqueTogether(
            name='supertype',
            unique_together=set([('name',)]),
        ),
        migrations.CreateModel(
            name='Type',
            fields=[
                ('id', models.BigAutoField(auto_created=True, verbose_name='ID', serialize=False, primary_key=True)),
                ('name', models.CharField(verbose_name='name', max_length=100)),
                (
                    'super',
                    models.ForeignKey(
                        to='example.SuperType',
                        on_delete=django.db.models.deletion.DO_NOTHING,
                        verbose_name='super',
                        null=True,
                    ),
                ),
            ],
        ),
        migrations.AddField(
            model_name='statement',
            name='type',
            field=models.ManyToManyField(to='example.Type', verbose_name='types'),
        ),
        migrations.AddField(
            model_name='statement',
            name='user',
            field=models.ForeignKey(to=settings.AUTH_USER_MODEL, verbose_name='user', on_delete=models.CASCADE),
        ),
        migrations.AddField(
            model_name='organizationshards',
            name='shard',
            field=models.ForeignKey(to='example.Shard', on_delete=models.CASCADE),
        ),
        migrations.AddField(
            model_name='user',
            name='cake',
            field=models.ManyToManyField(to='example.Cake', verbose_name='cakes'),
        ),
        migrations.AddField(
            model_name='user',
            name='organization',
            field=models.ForeignKey(
                to='example.Organization', verbose_name='organization', null=True, on_delete=models.SET_NULL
            ),
        ),
        migrations.AddField(
            model_name='user',
            name='type',
            field=models.ForeignKey(
                to='example.Type', on_delete=django.db.models.deletion.DO_NOTHING, verbose_name='type', null=True
            ),
        ),
        migrations.AddField(
            model_name='mirroreduser',
            name='type',
            field=models.ManyToManyField(to='example.Type', verbose_name='type'),
        ),
        migrations.AddField(
            model_name='coatingtype',
            name='super_type',
            field=models.ManyToManyField(to='example.SuperType', verbose_name='super type'),
        ),
        migrations.CreateModel(
            name='ProxyCake',
            fields=[],
            options={
                'proxy': True,
            },
            bases=('example.cake',),
        ),
        migrations.CreateModel(
            name='Unrelated',
            fields=[
                ('id', models.BigAutoField(auto_created=True, verbose_name='ID', serialize=False, primary_key=True)),
                ('name', models.CharField(verbose_name='name', max_length=64)),
            ],
        ),
        migrations.CreateModel(
            name='QuiltSession',
            fields=[
                ('session_key', models.CharField(max_length=255, primary_key=True, serialize=False)),
                ('session_data', models.TextField()),
                ('expire_date', models.DateTimeField(db_index=True)),
            ],
            options={
                'abstract': False,
            },
        ),
    ]
