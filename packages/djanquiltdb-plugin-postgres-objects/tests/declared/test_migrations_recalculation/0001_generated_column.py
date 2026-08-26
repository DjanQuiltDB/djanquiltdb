"""
A PUBLIC function and a sharded model whose stored generated column computes with it, as makemigrations would write
them: the definition spelled out, and the placement this plugin annotated recorded alongside it as hints.

Only ever imported when a test points MIGRATION_MODULES here. Test discovery does not reach it: it matches every
``*.py``, but skips a file whose name is not a valid module identifier.
"""

from django.db import migrations, models
from django.db.models import F, Func
from djanquiltdb import ShardingMode
from postgres_objects import GeneratedField
from postgres_objects.functions import FunctionDefinition
from postgres_objects.operations import AddFunction

UPPERCASE = FunctionDefinition(
    name='recalcuppercase',
    db_name='recalc_uppercase',
    arguments='input TEXT',
    returns='TEXT',
    body='BEGIN RETURN UPPER(input); END;',
    language='plpgsql',
    volatility='IMMUTABLE',
    strict=True,
    parallel='SAFE',
)


class Migration(migrations.Migration):
    initial = True

    operations = [
        # The function first, and PUBLIC, so that it exists by the time the column below is created on the shards:
        # migrate applies a migration to every node's public schema before it reaches the template and the shards.
        AddFunction(UPPERCASE, hints={'sharding_mode': ShardingMode.PUBLIC}),
        migrations.CreateModel(
            name='Crumb',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False)),
                ('name', models.TextField()),
                # Named unqualified, so each shard resolves it through its own search path to the public copy.
                (
                    'name_uppercased',
                    GeneratedField(
                        expression=Func(F('name'), function=UPPERCASE.db_name),
                        output_field=models.TextField(),
                        db_persist=True,
                    ),
                ),
            ],
        ),
    ]
