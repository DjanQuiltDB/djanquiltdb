# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations, models
import django.db.models.deletion
from django.conf import settings
import django.utils.timezone


class Migration(migrations.Migration):

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='User',
            fields=[
                ('id', models.AutoField(auto_created=True, verbose_name='ID', serialize=False, primary_key=True)),
                ('password', models.CharField(max_length=128, verbose_name='password')),
                ('last_login', models.DateTimeField(blank=True, verbose_name='last login', null=True)),
                ('name', models.CharField(max_length=100, verbose_name='name')),
                ('email', models.EmailField(unique=True, max_length=254, verbose_name='email address')),
                ('created_at', models.DateTimeField(verbose_name='date joined', default=django.utils.timezone.now)),
                ('is_staff', models.BooleanField(verbose_name='staff status', default=False, help_text='Designates whether the user can log into this admin site.')),
                ('is_active', models.BooleanField(verbose_name='active', default=True, help_text='Designates whether this user should be treated as active. Unselect this instead of deleting accounts.')),
            ],
        ),
        migrations.CreateModel(
            name='Cake',
            fields=[
                ('id', models.AutoField(auto_created=True, verbose_name='ID', serialize=False, primary_key=True)),
                ('name', models.CharField(max_length=128, verbose_name='name')),
            ],
        ),
        migrations.CreateModel(
            name='MirroredUser',
            fields=[
                ('id', models.AutoField(auto_created=True, verbose_name='ID', serialize=False, primary_key=True)),
                ('password', models.CharField(max_length=128, verbose_name='password')),
                ('last_login', models.DateTimeField(blank=True, verbose_name='last login', null=True)),
                ('name', models.CharField(max_length=100, verbose_name='name')),
                ('email', models.EmailField(unique=True, max_length=254, verbose_name='email address')),
                ('created_at', models.DateTimeField(verbose_name='date joined', default=django.utils.timezone.now)),
                ('is_staff', models.BooleanField(verbose_name='staff status', default=False, help_text='Designates whether the user can log into this admin site.')),
                ('is_active', models.BooleanField(verbose_name='active', default=True, help_text='Designates whether this user should be treated as active. Unselect this instead of deleting accounts.')),
            ],
        ),
        migrations.CreateModel(
            name='Organization',
            fields=[
                ('id', models.AutoField(auto_created=True, verbose_name='ID', serialize=False, primary_key=True)),
                ('name', models.CharField(max_length=100, verbose_name='name')),
                ('created_at', models.DateTimeField(verbose_name='created at', default=django.utils.timezone.now)),
            ],
        ),
        migrations.CreateModel(
            name='OrganizationShards',
            fields=[
                ('id', models.AutoField(auto_created=True, verbose_name='ID', serialize=False, primary_key=True)),
                ('organization_id', models.PositiveSmallIntegerField()),
                ('state', models.CharField(max_length=1, default='A', choices=[('A', 'Active'), ('M', 'Maintenance')])),
                ('slug', models.SlugField()),
            ],
        ),
        migrations.CreateModel(
            name='Shard',
            fields=[
                ('id', models.AutoField(auto_created=True, verbose_name='ID', serialize=False, primary_key=True)),
                ('alias', models.CharField(unique=True, max_length=128, db_index=True)),
                ('schema_name', models.CharField(max_length=64)),
                ('node_name', models.CharField(max_length=64)),
                ('state', models.CharField(max_length=1, default='M', choices=[('A', 'Active'), ('M', 'Maintenance')])),
            ],
        ),
        migrations.CreateModel(
            name='Statement',
            fields=[
                ('id', models.AutoField(auto_created=True, verbose_name='ID', serialize=False, primary_key=True)),
                ('content', models.CharField(max_length=300, verbose_name='content')),
                ('offset', models.PositiveIntegerField(blank=True, verbose_name='offset', null=True)),
            ],
        ),
        migrations.CreateModel(
            name='Suborganization',
            fields=[
                ('id', models.AutoField(auto_created=True, verbose_name='ID', serialize=False, primary_key=True)),
                ('child', models.OneToOneField(to='example.Organization', verbose_name='organization', related_name='children')),
                ('parent', models.ForeignKey(to='example.Organization', verbose_name='organization', related_name='parent')),
            ],
        ),
        migrations.CreateModel(
            name='SuperType',
            fields=[
                ('id', models.AutoField(auto_created=True, verbose_name='ID', serialize=False, primary_key=True)),
                ('name', models.CharField(max_length=100, verbose_name='name')),
            ],
        ),
        migrations.CreateModel(
            name='Type',
            fields=[
                ('id', models.AutoField(auto_created=True, verbose_name='ID', serialize=False, primary_key=True)),
                ('name', models.CharField(max_length=100, verbose_name='name')),
                ('super', models.ForeignKey(on_delete=django.db.models.deletion.DO_NOTHING, to='example.SuperType', verbose_name='super', null=True)),
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
            field=models.ForeignKey(to=settings.AUTH_USER_MODEL, verbose_name='user'),
        ),
        migrations.AddField(
            model_name='organizationshards',
            name='shard',
            field=models.ForeignKey(to='example.Shard'),
        ),
        migrations.AddField(
            model_name='user',
            name='cake',
            field=models.ManyToManyField(to='example.Cake', verbose_name='cakes'),
        ),
        migrations.AddField(
            model_name='user',
            name='organization',
            field=models.ForeignKey(to='example.Organization', verbose_name='organization', null=True),
        ),
        migrations.AddField(
            model_name='user',
            name='type',
            field=models.ForeignKey(on_delete=django.db.models.deletion.DO_NOTHING, to='example.Type', verbose_name='type', null=True),
        ),
        migrations.CreateModel(
            name='ProxyCake',
            fields=[
            ],
            options={
                'proxy': True,
            },
            bases=('example.cake',),
        ),
    ]
