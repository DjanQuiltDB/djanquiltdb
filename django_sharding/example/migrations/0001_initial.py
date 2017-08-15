# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations, models
import django.contrib.auth.models
import django.utils.timezone


class Migration(migrations.Migration):

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Shard',
            fields=[
                ('id', models.AutoField(verbose_name='ID', primary_key=True, auto_created=True, serialize=False)),
                ('alias', models.CharField(max_length=128, db_index=True, unique=True, )),
                ('schema_name', models.CharField(max_length=64)),
                ('node_name', models.CharField(max_length=64)),
                ('state', models.CharField(default='M', choices=[('A', 'Active'), ('M', 'Maintenance')], max_length=1)),
            ],
        ),
        migrations.CreateModel(
            name='User',
            fields=[
                ('id', models.AutoField(primary_key=True, auto_created=True, verbose_name='ID', serialize=False)),
                ('password', models.CharField(verbose_name='password', max_length=128)),
                ('last_login', models.DateTimeField(blank=True, verbose_name='last login', null=True)),
                ('name', models.CharField(verbose_name='name', max_length=100)),
                ('email', models.EmailField(unique=True, verbose_name='email address', max_length=254)),
                ('created_at', models.DateTimeField(default=django.utils.timezone.now, verbose_name='date joined')),
            ],
            managers=[
                ('objects', django.contrib.auth.models.UserManager()),
            ],
        ),
        migrations.CreateModel(
            name='Organization',
            fields=[
                ('id', models.AutoField(primary_key=True, auto_created=True, verbose_name='ID', serialize=False)),
                ('name', models.CharField(verbose_name='name', max_length=100)),
                ('created_at', models.DateTimeField(default=django.utils.timezone.now, verbose_name='created at')),
            ],
        ),
        migrations.CreateModel(
            name='Type',
            fields=[
                ('id', models.AutoField(primary_key=True, auto_created=True, verbose_name='ID', serialize=False)),
                ('name', models.CharField(verbose_name='name', max_length=100)),
            ],
        ),
        migrations.AddField(
            model_name='user',
            name='organization',
            field=models.ForeignKey(to='example.Organization', verbose_name='organization'),
        ),
        migrations.AddField(
            model_name='user',
            name='type',
            field=models.ForeignKey(to='example.Type', verbose_name='type'),
        ),
        migrations.CreateModel(
            name='OrganizationShards',
            fields=[
                ('id', models.AutoField(auto_created=True, verbose_name='ID', serialize=False, primary_key=True)),
                ('organization_id', models.PositiveSmallIntegerField()),
                ('state', models.CharField(default='M', choices=[('A', 'Active'), ('M', 'Maintenance')], max_length=1)),
                ('shard', models.ForeignKey(to='example.Shard')),
            ],
        ),
    ]
