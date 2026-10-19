"""
Settings for the test project, a minimal sharded DjanQuiltDB project that uses django-postgres-denormalize. This is not
a template for a real deployment.
"""

import os

import dj_database_url
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
    'postgres_denormalize',
    'example',
    'antipattern',
)

DATABASES = {
    'default': {
        **dj_database_url.parse(
            os.environ.get('DATABASE_URL', 'postgresql://postgres:postgres@localhost:5432/test_db')
        ),
        'ENGINE': 'djanquiltdb.postgresql_backend',
    },
    # A second node, so that the tests can cover a shard on a node other than the primary.
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
    },
}

# The antipattern app copies a column of a mirrored model on a sharded model, so that the tests can show that
# DjanQuiltDB's system check reports its cascade and fill triggers. Silencing the check is the only way to run with it,
# as the documentation describes. Both nodes run transactions at READ COMMITTED, which django-postgres-denormalize
# reports as postgres_denormalize.W303.
SILENCED_SYSTEM_CHECKS = ['djanquiltdb.E001', 'postgres_denormalize.W303']

DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'

USE_TZ = True
