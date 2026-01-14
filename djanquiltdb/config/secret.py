import os

from django.core.exceptions import ImproperlyConfigured


def get(setting, fallback=None, fallback_dict=None):
    try:
        return os.environ[setting]
    except KeyError:
        try:
            return fallback_dict[setting]
        except KeyError:
            if fallback is not None:
                return fallback

            raise ImproperlyConfigured('The setting {0} was not found in your environment'.format(setting))
