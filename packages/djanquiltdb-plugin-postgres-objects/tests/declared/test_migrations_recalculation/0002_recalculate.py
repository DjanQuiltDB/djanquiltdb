"""
The body change and the rewrite that follows it, as makemigrations writes the pair: the function is altered once, and
the column computed with it is recalculated wherever its table lives.

Only ever imported when a test points MIGRATION_MODULES here.
"""

from django.db import migrations
from djanquiltdb import ShardingMode
from postgres_objects.functions import FunctionDefinition
from postgres_objects.operations import AlterFunction, RecalculateGeneratedField


def definition(body):
    """
    The one function these migrations manage, under the body given.

    Spelled out here rather than imported from 0001: a migration module's name is not a valid identifier, and a real
    migration carries both definitions verbatim anyway, so that deleting the declaration never breaks history.
    """
    return FunctionDefinition(
        name='recalcuppercase',
        db_name='recalc_uppercase',
        arguments='input TEXT',
        returns='TEXT',
        body=body,
        language='plpgsql',
        volatility='IMMUTABLE',
        strict=True,
        parallel='SAFE',
    )


class Migration(migrations.Migration):
    dependencies = [('declared', '0001_generated_column')]

    operations = [
        AlterFunction(
            definition("BEGIN RETURN UPPER(input) || '!'; END;"),
            definition('BEGIN RETURN UPPER(input); END;'),
            hints={'sharding_mode': ShardingMode.PUBLIC},
        ),
        # Carries no hints of its own: it belongs to the model, and the router places it the way it places any model
        # migration.
        RecalculateGeneratedField('Crumb', 'name_uppercased'),
    ]
