"""
The ``QUILT_OAUTH_TOOLKIT`` setting, an optional dict whose keys override the defaults below. A shard selector matches
the core setting ``QUILT_DB['SHARD_SELECTOR_REGEX']``, as the shard selector of a session key does.
"""

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured

QUILT_OAUTH_TOOLKIT_DEFAULTS = {
    # The text at the start of every client id and token, before the shard selector.
    'IDENTIFIER_PREFIX': 'O',
    # The text after the shard selector of a client id, before its random part.
    'CLIENT_ID_DELIMITER': 'I',
    # The text after the shard selector of an access or refresh token, before its random part.
    'TOKEN_DELIMITER': 'T',
}


def quilt_oauth_toolkit_setting(name):
    """
    Returns the value of ``name`` in QUILT_OAUTH_TOOLKIT, else the default of ``name``. Raises ImproperlyConfigured when
    QUILT_OAUTH_TOOLKIT is not a dict.
    """
    quilt_oauth_toolkit = getattr(settings, 'QUILT_OAUTH_TOOLKIT', {})

    if not isinstance(quilt_oauth_toolkit, dict):
        raise ImproperlyConfigured('QUILT_OAUTH_TOOLKIT is not a dict.')

    return quilt_oauth_toolkit.get(name, QUILT_OAUTH_TOOLKIT_DEFAULTS[name])
