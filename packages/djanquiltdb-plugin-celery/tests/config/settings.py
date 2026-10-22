"""
Settings for the test project, a minimal sharded DjanQuiltDB project that runs Celery tasks. This is not a template for
a real deployment.
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
    'example',
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
    'MAPPING_MODEL': 'example.models.TenantShard',
    'PRIMARY_DB_ALIAS': 'default',
    'SHARED_TEST_MIGRATION_STATES': True,
    'OVERRIDE_SHARDING_MODE': {
        ('auth',): ShardingMode.MIRRORED,
        ('contenttypes',): ShardingMode.MIRRORED,
    },
}

# The broker and the result backend are in the memory of the test process, so that a worker in a thread of that process
# receives the messages that the tests send, and no service has to run beside the database. Tasks run eagerly unless a
# test turns off CELERY_TASK_ALWAYS_EAGER, as the tests that start a worker do.
CELERY_BROKER_URL = 'memory://'
CELERY_RESULT_BACKEND = 'cache+memory://'
CELERY_TASK_ALWAYS_EAGER = True
CELERY_TASK_EAGER_PROPAGATES = True
CELERY_TASK_STORE_EAGER_RESULT = True

DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'

USE_TZ = True
