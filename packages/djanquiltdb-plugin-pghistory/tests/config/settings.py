"""
Settings for the test project, a minimal sharded DjanQuiltDB project that uses django-pghistory. This is not a template
for a real deployment.
"""

import os

import dj_database_url
import pghistory
from djanquiltdb import ShardingMode

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

SECRET_KEY = 'test-project-only-do-not-use-this-anywhere-real'  # nosec B105

DEBUG = True

ALLOWED_HOSTS = []

INSTALLED_APPS = (
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'djanquiltdb',
    'pgtrigger',
    'pghistory',
    'example',
)

DATABASES = {
    'default': {
        **dj_database_url.parse(
            os.environ.get('DATABASE_URL', 'postgresql://postgres:postgres@localhost:5432/test_db')
        ),
        'ENGINE': 'djanquiltdb.postgresql_backend',
    },
    # A second node, so that the tests can cover writes to a mirrored model on a node other than the primary.
    'other': {
        **dj_database_url.parse(
            os.environ.get('DATABASE_URL2', 'postgresql://postgres:postgres@localhost:5432/test_db2')
        ),
        'ENGINE': 'djanquiltdb.postgresql_backend',
    },
}

DATABASE_ROUTERS = ['djanquiltdb.router.DynamicDbRouter']

QUILT_DB = {
    'SHARD_CLASS': 'example.models.Shard',
    'PRIMARY_DB_ALIAS': 'default',
    'SHARED_TEST_MIGRATION_STATES': True,
    'OVERRIDE_SHARDING_MODE': {
        ('auth',): ShardingMode.MIRRORED,
        ('contenttypes',): ShardingMode.MIRRORED,
        # Places pghistory's own Context model; the event models carry their mode through their base classes.
        ('pghistory',): ShardingMode.SHARDED,
    },
}

# Denormalized context for the whole project. The Waffle, Danish and Strudel call sites in the example app pass
# context_field=pghistory.ContextForeignKey() to cover the normalized context.
PGHISTORY_CONTEXT_FIELD = pghistory.ContextJSONField()

# The documented customization: a base model for the whole project adds a field to every generated event model.
PGHISTORY_BASE_MODEL = 'example.events.BatchFlagEvent'

DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'

USE_TZ = True
