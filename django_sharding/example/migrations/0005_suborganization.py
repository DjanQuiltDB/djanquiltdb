# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('example', '0004_statement'),
    ]

    operations = [
        migrations.CreateModel(
            name='Cake',
            fields=[
                ('id', models.AutoField(serialize=False, primary_key=True, verbose_name='ID', auto_created=True)),
                ('name', models.CharField(verbose_name='name', max_length=128)),
            ],
        ),
        migrations.CreateModel(
            name='Suborganization',
            fields=[
                ('id', models.AutoField(serialize=False, primary_key=True, verbose_name='ID', auto_created=True)),
                ('child', models.OneToOneField(verbose_name='organization', to='example.Organization',
                                               related_name='children')),
                ('parent', models.ForeignKey(verbose_name='organization', to='example.Organization',
                                             related_name='parent')),
            ],
        ),
    ]
