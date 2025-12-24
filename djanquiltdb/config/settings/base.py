import os
import json

import dj_database_url
import functools

from config.secret import get as get_secret
from djanquiltdb import ShardingMode

# Build paths inside the project like this: os.path.join(BASE_DIR, ...)
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

# Read secrets.json as JSON
try:
    # Build paths inside the project like this: os.path.join(BASE_DIR, ...)
    BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    with open(os.path.join(BASE_DIR, "secrets.json")) as f:
        secrets_from_file = json.loads(f.read())
except OSError:
    secrets_from_file = {}

# Set the defaults to those defined in secrets.json
get_secret = functools.partial(get_secret, fallback_dict=secrets_from_file)

# This is an example app, so we don't care what the SECRET_KEY is, as long as we have one. Note: don't ever do this in
# production, keep your secret key safe!
SECRET_KEY = '07hkbkb*dlw-%=%ivclp^h)scx9_b$f6rty4611iqk2=b=r*an'  # nosec B105

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = False

ALLOWED_HOSTS = []


# Application definition
INSTALLED_APPS = (
    'djanquiltdb.contrib.quilt_auth',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'djanquiltdb',
    'example',
    'migration_tests',
)

MIDDLEWARE = (
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'example.middleware.UseShardMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
)

ROOT_URLCONF = 'config.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [os.path.join(BASE_DIR, 'templates')],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'config.wsgi.application'


DATABASES = {'default': dj_database_url.parse(get_secret('DATABASE_URL'), engine='djanquiltdb.postgresql_backend'),
             'other': dj_database_url.parse(get_secret('DATABASE_URL2'), engine='djanquiltdb.postgresql_backend')}

DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'

SHARDING = {
    'SHARD_CLASS': 'example.models.Shard',
    'MAPPING_MODEL': 'example.models.OrganizationShards',
    'PRIMARY_DB_ALIAS': 'default',
    'NEW_SHARD_NODE': 'other',
    'OVERRIDE_SHARDING_MODE': {
        ('auth',): ShardingMode.MIRRORED,
        ('contenttypes',): ShardingMode.MIRRORED,
    }
}

DATABASE_ROUTERS = ['djanquiltdb.router.DynamicDbRouter']

# Password validation
# https://docs.djangoproject.com/en/1.11/ref/settings/#auth-password-validators

# Settings of the auth backend
AUTH_USER_MODEL = 'example.User'
LOGIN_URL = 'login'
LOGIN_REDIRECT_URL = '/'

AUTHENTICATION_BACKENDS = (
    'django.contrib.auth.backends.ModelBackend',  # this is default
)
AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]

TEST_RUNNER = 'config.utils.test.WildcardDiscoverRunner'

# Internationalization
# https://docs.djangoproject.com/en/1.11/topics/i18n/

LANGUAGE_CODE = 'en-us'

TIME_ZONE = 'UTC'

USE_I18N = True

USE_L10N = True

USE_TZ = True

STATIC_URL = '/static/'
