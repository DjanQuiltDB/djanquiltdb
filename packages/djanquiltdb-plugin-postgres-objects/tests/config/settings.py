"""
Settings for the test project: a minimal sharded DjanQuiltDB project using django-postgres-objects.
This is not a template for a real deployment.
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
    'postgres_objects',
    'example',
    # Carries no models: only the fixture migrations the declared-objects placement cases point MIGRATION_MODULES at.
    'declared',
)

DATABASES = {
    'default': {
        **dj_database_url.parse(
            os.environ.get('DATABASE_URL', 'postgresql://postgres:postgres@localhost:5432/test_db')
        ),
        'ENGINE': 'djanquiltdb.postgresql_backend',
    },
    # A second node, so the mirrored cases can show an object reaching every node rather than only the default one.
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
    'OVERRIDE_SHARDING_MODE': {
        ('auth',): ShardingMode.MIRRORED,
        ('contenttypes',): ShardingMode.MIRRORED,
    },
}

POSTGRES_OBJECTS = {
    'FUNCTIONS_MODULE_PATH': 'functions',
    'VIEWS_MODULE_PATH': 'db_views',
}

DEFAULT_AUTO_FIELD = 'django.db.models.AutoField'

USE_TZ = True
