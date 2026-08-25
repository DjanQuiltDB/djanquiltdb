"""
A migration for declared Postgres objects, as makemigrations writes one: the definition spelled out, and the placement
this plugin annotated recorded alongside it as hints.

Only ever imported when a test points MIGRATION_MODULES here. Test discovery does not reach it: it matches every
``*.py``, but skips a file whose name is not a valid module identifier.
"""

from django.db import migrations
from postgres_objects.functions import FunctionDefinition
from postgres_objects.operations import AddFunction, AddView
from postgres_objects.views import ViewDefinition

from djanquiltdb import ShardingMode


class Migration(migrations.Migration):
    initial = True

    operations = [
        AddFunction(
            FunctionDefinition(
                name='declaredpublic',
                db_name='declared_public',
                arguments='input TEXT',
                returns='TEXT',
                body='BEGIN RETURN UPPER(input); END;',
                language='plpgsql',
                volatility='IMMUTABLE',
                strict=True,
                parallel='SAFE',
            ),
            hints={'sharding_mode': ShardingMode.PUBLIC},
        ),
        AddFunction(
            FunctionDefinition(
                name='declaredsharded',
                db_name='declared_sharded',
                arguments='input TEXT',
                returns='TEXT',
                body='BEGIN RETURN LOWER(input); END;',
                language='plpgsql',
                volatility='IMMUTABLE',
                strict=True,
                parallel='SAFE',
            ),
            hints={'sharding_mode': ShardingMode.SHARDED},
        ),
        # Selecting from no table at all, so that where the view lands is the only thing under test and not which
        # schema happens to hold the tables it would otherwise read.
        AddView(
            ViewDefinition(
                name='declaredpublicview',
                db_name='declared_public_view',
                sql='SELECT 1 AS id',
                options={},
            ),
            hints={'sharding_mode': ShardingMode.PUBLIC},
        ),
        AddView(
            ViewDefinition(
                name='declaredshardedview',
                db_name='declared_sharded_view',
                sql='SELECT 1 AS id',
                options={},
            ),
            hints={'sharding_mode': ShardingMode.SHARDED},
        ),
    ]
