"""
A migration for a declared Postgres function as makemigrations wrote one *before* this plugin was installed: the
definition spelled out, and no hints recorded alongside it, because nothing was there yet to annotate a placement.

Only ever imported when a test points MIGRATION_MODULES here. Test discovery does not reach it: it matches every
``*.py``, but skips a file whose name is not a valid module identifier.
"""

from django.db import migrations
from postgres_objects.functions import FunctionDefinition
from postgres_objects.operations import AddFunction


class Migration(migrations.Migration):
    initial = True

    operations = [
        AddFunction(
            FunctionDefinition(
                name='declaredhintless',
                db_name='declared_hintless',
                arguments='input TEXT',
                returns='TEXT',
                body='BEGIN RETURN UPPER(input); END;',
                language='plpgsql',
                volatility='IMMUTABLE',
                strict=True,
                parallel='SAFE',
            ),
        ),
    ]
