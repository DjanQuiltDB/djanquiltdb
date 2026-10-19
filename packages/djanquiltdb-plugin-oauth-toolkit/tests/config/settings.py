"""
Settings for the test project, a minimal sharded DjanQuiltDB project that issues django-oauth-toolkit client ids and
tokens. This is not a template for a real deployment.
"""

import os

import dj_database_url
from djanquiltdb import ShardingMode
from jwcrypto import jwk

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

SECRET_KEY = 'test-project-only-do-not-use-this-anywhere-real'  # nosec B105

DEBUG = True

ALLOWED_HOSTS = ['testserver']

INSTALLED_APPS = (
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'djanquiltdb',
    'oauth2_provider',
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
    'MAPPING_MODEL': 'example.models.OrganizationShard',
    'PRIMARY_DB_ALIAS': 'default',
    'SHARED_TEST_MIGRATION_STATES': True,
    'OVERRIDE_SHARDING_MODE': {
        ('auth',): ShardingMode.MIRRORED,
        ('contenttypes',): ShardingMode.MIRRORED,
        # Every application, grant and token belongs to one organization, so the tables of oauth2_provider are sharded.
        ('oauth2_provider',): ShardingMode.SHARDED,
    },
}

AUTH_USER_MODEL = 'example.User'

OAUTH2_PROVIDER = {
    'CLIENT_ID_GENERATOR_CLASS': 'djanquiltdb.base.oauth_toolkit.ShardSelectorClientIdGenerator',
    'ACCESS_TOKEN_GENERATOR': 'djanquiltdb.utils.oauth_toolkit.generate_access_token',
    'REFRESH_TOKEN_GENERATOR': 'djanquiltdb.utils.oauth_toolkit.generate_refresh_token',
    'SCOPES': {'read': 'Read', 'write': 'Write', 'openid': 'OpenID Connect', 'introspection': 'Introspect tokens'},
    'OIDC_ENABLED': True,
    # A key of the test process only, so that no private key is kept in the repository.
    'OIDC_RSA_PRIVATE_KEY': jwk.JWK.generate(kty='RSA', size=2048).export_to_pem(private_key=True, password=None),
}

DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'

USE_TZ = True
