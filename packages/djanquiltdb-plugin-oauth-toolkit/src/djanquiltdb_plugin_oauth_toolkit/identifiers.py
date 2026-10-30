"""
The format of client ids and tokens with a shard selector. The shard selector is the mapping value of a tenant, which
the mapping model maps onto the shard of the tenant. A session key of ``djanquiltdb.sessions`` has the same shard
selector. It matches ``QUILT_DB['SHARD_SELECTOR_REGEX']``.

An identifier is a prefix, a shard selector, a delimiter and a random part. For the tenant whose mapping value is
``acme``, a client id is ``OacmeI`` followed by 40 random characters, and a token is ``OacmeT`` followed by 30 random
characters.

The format is not signed, because the shard selector only names the shard in which to look up the identifier.
django-oauth-toolkit looks up a token by a checksum of the whole token, so when someone changes the shard selector of a
token, the toolkit does not find the token in the other shard.
"""

import functools
import re
from collections import namedtuple

from django.core.exceptions import ImproperlyConfigured
from django.core.signals import setting_changed
from django.dispatch import receiver
from djanquiltdb.utils import shard_selector_pattern, shard_selector_regex
from oauthlib.common import generate_token

from djanquiltdb_plugin_oauth_toolkit.settings import quilt_oauth_toolkit_setting

#: The characters that Django's slug path converter matches. The toolkit reads the client id of its device flow pages
#: from a URL path with that converter, so a prefix and a delimiter only have these characters, and so does the shard
#: selector of a client id while the device flow is configured. Basic authentication also splits the client id from the
#: secret at a colon, which is not one of these characters.
SLUG_CONVERTER_CHARACTERS = '[-A-Za-z0-9_]'

_SLUG_CONVERTER_VALUE = re.compile(SLUG_CONVERTER_CHARACTERS + '+')

CLIENT_ID_RANDOM_LENGTH = 40

# The length of a random token that oauthlib generates.
TOKEN_RANDOM_LENGTH = 30

#: The settings of the identifier format, once identifier_setting_problems() finds no problem with them.
#: ``selector_regex`` is the regex of a shard selector without anchors, and ``selector_pattern`` is that regex compiled
#: and anchored.
IdentifierSettings = namedtuple('IdentifierSettings', ['prefix', 'selector_regex', 'selector_pattern'])


def identifier_setting_problems():
    """
    Returns a description of each problem with the settings of the identifier format. These are the problems:

    - The shard selector regex does not compile.
    - A prefix or a delimiter is not a string, is empty, or has a character outside SLUG_CONVERTER_CHARACTERS.
    - The shard selector regex matches a delimiter, so the end of a shard selector is unclear.
    """
    problems = []
    regex = shard_selector_regex()

    try:
        selector_pattern = shard_selector_pattern()
    except re.error as error:
        problems.append(
            "QUILT_DB['SHARD_SELECTOR_REGEX'] {!r} is not a valid regular expression: {}.".format(regex, error)
        )
        selector_pattern = None

    for name in ('IDENTIFIER_PREFIX', 'CLIENT_ID_DELIMITER', 'TOKEN_DELIMITER'):
        value = quilt_oauth_toolkit_setting(name)

        if not isinstance(value, str) or not value:
            problems.append(
                "QUILT_OAUTH_TOOLKIT['{}'] is {!r}, and must be a string that is not empty.".format(name, value)
            )
        elif not _SLUG_CONVERTER_VALUE.fullmatch(value):
            problems.append(
                "QUILT_OAUTH_TOOLKIT['{}'] is {!r}, and must only have the characters {}.".format(
                    name, value, SLUG_CONVERTER_CHARACTERS
                )
            )
        elif name != 'IDENTIFIER_PREFIX' and selector_pattern is not None and selector_pattern.match(value):
            problems.append(
                "QUILT_OAUTH_TOOLKIT['{}'] is {!r}, and QUILT_DB['SHARD_SELECTOR_REGEX'] {!r} matches it, so the end "
                'of a shard selector is unclear. Pick a delimiter that the regex does not match.'.format(
                    name, value, regex
                )
            )

    return problems


@functools.cache
def checked_identifier_settings():
    """
    Returns the IdentifierSettings of the format. Raises ImproperlyConfigured when identifier_setting_problems() finds
    a problem. The result is cached until QUILT_DB, QUILT_SESSIONS or QUILT_OAUTH_TOOLKIT changes, so the settings are
    not checked again for every client id and token of a request.
    """
    problems = identifier_setting_problems()

    if problems:
        raise ImproperlyConfigured(' '.join(problems))

    return IdentifierSettings(
        quilt_oauth_toolkit_setting('IDENTIFIER_PREFIX'), shard_selector_regex(), shard_selector_pattern()
    )


@functools.cache
def _identifier_pattern(delimiter_setting):
    identifier_settings = checked_identifier_settings()
    delimiter = quilt_oauth_toolkit_setting(delimiter_setting)

    # The random part can have the delimiter in it, so the shard selector is matched from the start of the identifier.
    # The lookahead makes sure that the random part has at least one character.
    return re.compile(
        '^'
        + re.escape(identifier_settings.prefix)
        + '('
        + identifier_settings.selector_regex
        + ')'
        + re.escape(delimiter)
        + '(?=.)'
    )


@receiver(setting_changed)
def reset_cached_identifier_settings(setting, **kwargs):
    """
    Clears the cached settings and patterns of the identifier format when override_settings() changes QUILT_DB,
    QUILT_SESSIONS or QUILT_OAUTH_TOOLKIT.
    """
    if setting in ('QUILT_DB', 'QUILT_SESSIONS', 'QUILT_OAUTH_TOOLKIT'):
        checked_identifier_settings.cache_clear()
        _identifier_pattern.cache_clear()


def device_flow_is_configured():
    """
    Returns whether ``OAUTH2_PROVIDER['OAUTH_DEVICE_VERIFICATION_URI']`` is set, which the device authorization
    endpoint of the toolkit needs.
    """
    from oauth2_provider.settings import oauth2_settings

    return bool(oauth2_settings.OAUTH_DEVICE_VERIFICATION_URI)


class IdentifierFormat:
    """
    The format of one kind of identifier. An identifier of the format is the prefix, a shard selector, the delimiter
    that the setting ``delimiter_setting`` names, and ``random_length`` random letters and digits. When
    ``read_from_device_flow_paths`` is True, the device flow pages of the toolkit read an identifier of the format from
    a URL path.
    """

    def __init__(self, delimiter_setting, random_length, read_from_device_flow_paths=False):
        self.delimiter_setting = delimiter_setting
        self.random_length = random_length
        self.read_from_device_flow_paths = read_from_device_flow_paths

    @property
    def delimiter(self):
        return quilt_oauth_toolkit_setting(self.delimiter_setting)

    def pattern(self):
        """
        Returns the pattern that matches the start of an identifier, from the prefix up to and including the delimiter.
        Its first group is the shard selector. Raises ImproperlyConfigured when the settings of the format have a
        problem.
        """
        return _identifier_pattern(self.delimiter_setting)

    def generate(self, selector):
        """
        Returns a new identifier for the shard selector ``selector``. Raises ImproperlyConfigured when the settings of
        the format have a problem, when the selector does not match ``QUILT_DB['SHARD_SELECTOR_REGEX']``, and when the
        selector followed by the delimiter matches it too, so that the delimiter reads as part of the selector. An
        identifier that the device flow pages read from a URL path also raises ImproperlyConfigured when the device
        flow is configured and the selector has a character outside SLUG_CONVERTER_CHARACTERS.
        """
        identifier_settings = checked_identifier_settings()
        selector = str(selector)

        if not identifier_settings.selector_pattern.match(selector):
            raise ImproperlyConfigured(
                "The shard selector {!r} does not match QUILT_DB['SHARD_SELECTOR_REGEX'] {!r}.".format(
                    selector, identifier_settings.selector_regex
                )
            )

        if identifier_settings.selector_pattern.match(selector + self.delimiter):
            raise ImproperlyConfigured(
                "The shard selector {!r} followed by QUILT_OAUTH_TOOLKIT['{}'] {!r} also matches "
                "QUILT_DB['SHARD_SELECTOR_REGEX'], so the delimiter reads as part of the shard selector.".format(
                    selector, self.delimiter_setting, self.delimiter
                )
            )

        if (
            self.read_from_device_flow_paths
            and device_flow_is_configured()
            and not _SLUG_CONVERTER_VALUE.fullmatch(selector)
        ):
            raise ImproperlyConfigured(
                "The shard selector {!r} has a character outside {}. OAUTH2_PROVIDER['OAUTH_DEVICE_VERIFICATION_URI'] "
                "is set, so the project uses the device flow, whose pages read a client id from a URL path with Django's "
                'slug converter, which does not match a client id with this shard selector.'.format(
                    selector, SLUG_CONVERTER_CHARACTERS
                )
            )

        return '{}{}{}{}'.format(
            identifier_settings.prefix,
            selector,
            self.delimiter,
            generate_token(self.random_length),
        )

    def parse(self, identifier):
        """
        Returns the shard selector of ``identifier``, or None when it is not an identifier of this format.
        """
        if not isinstance(identifier, str):
            return None

        match = self.pattern().match(identifier)

        return None if match is None else match.group(1)


CLIENT_ID_FORMAT = IdentifierFormat('CLIENT_ID_DELIMITER', CLIENT_ID_RANDOM_LENGTH, read_from_device_flow_paths=True)
TOKEN_FORMAT = IdentifierFormat('TOKEN_DELIMITER', TOKEN_RANDOM_LENGTH)


def generate_client_id_for_selector(selector):
    """
    Returns a new client id for the shard selector ``selector``.
    """
    return CLIENT_ID_FORMAT.generate(selector)


def parse_client_id(client_id):
    """
    Returns the shard selector of ``client_id``, or None when it is not a client id of the format.
    """
    return CLIENT_ID_FORMAT.parse(client_id)


def generate_token_for_selector(selector):
    """
    Returns a new access or refresh token for the shard selector ``selector``.
    """
    return TOKEN_FORMAT.generate(selector)


def parse_token(token):
    """
    Returns the shard selector of ``token``, or None when it is not a token of the format.
    """
    return TOKEN_FORMAT.parse(token)
