"""
The system checks of the plugin. ``install()`` imports this module, which registers them.
"""

from django.apps import apps
from django.conf import settings
from django.core import checks
from django.utils.module_loading import import_string

from djanquiltdb_plugin_oauth_toolkit.identifiers import identifier_setting_problems
from djanquiltdb_plugin_oauth_toolkit.middleware import TOOLKIT_APP_NAME
from djanquiltdb_plugin_oauth_toolkit.settings import QUILT_OAUTH_TOOLKIT_DEFAULTS

CHECK_ID_PREFIX = 'djanquiltdb_plugin_oauth_toolkit'

PLUGIN_APP_NAME = 'djanquiltdb_plugin_oauth_toolkit'

#: The settings of the toolkit that name its models, which are all sharded.
TOOLKIT_MODEL_SETTINGS = (
    'APPLICATION_MODEL',
    'ACCESS_TOKEN_MODEL',
    'REFRESH_TOKEN_MODEL',
    'GRANT_MODEL',
    'ID_TOKEN_MODEL',
    'DEVICE_GRANT_MODEL',
)

SESSION_MIDDLEWARE = 'django.contrib.sessions.middleware.SessionMiddleware'

#: The middleware of the toolkit that looks up tokens, which has to come after the middleware of the plugin.
TOOLKIT_TOKEN_MIDDLEWARE = (
    'oauth2_provider.middleware.OAuth2TokenMiddleware',
    'oauth2_provider.middleware.OAuth2ExtraTokenMiddleware',
)


def _error(number, message, hint=None):
    return checks.Error(message, hint=hint, id='{}.E{:03d}'.format(CHECK_ID_PREFIX, number))


def _setting_messages():
    quilt_oauth_toolkit = getattr(settings, 'QUILT_OAUTH_TOOLKIT', {})

    if not isinstance(quilt_oauth_toolkit, dict):
        return [_error(2, 'QUILT_OAUTH_TOOLKIT is not a dict.')]

    messages = [
        _error(
            1,
            'QUILT_OAUTH_TOOLKIT has the key {!r}, which the plugin does not read.'.format(key),
            hint='The plugin reads the keys {}.'.format(', '.join(QUILT_OAUTH_TOOLKIT_DEFAULTS)),
        )
        for key in quilt_oauth_toolkit
        if key not in QUILT_OAUTH_TOOLKIT_DEFAULTS
    ]

    return messages + [_error(3, problem) for problem in identifier_setting_problems()]


def _model_messages():
    from djanquiltdb import ShardingMode
    from djanquiltdb.utils import get_declared_sharding_mode, get_mapping_class
    from oauth2_provider.settings import oauth2_settings

    messages = []

    if get_mapping_class() is None:
        messages.append(
            _error(
                4,
                'QUILT_DB has no MAPPING_MODEL, and the shard selector of a client id or a token is a mapping value.',
                hint="Set QUILT_DB['MAPPING_MODEL'].",
            )
        )

    unsharded_models = [
        label
        for label in (getattr(oauth2_settings, name) for name in TOOLKIT_MODEL_SETTINGS)
        if get_declared_sharding_mode(apps.get_model(label)) != ShardingMode.SHARDED
    ]

    if unsharded_models:
        messages.append(
            _error(
                4,
                'The models {} of django-oauth-toolkit are not sharded.'.format(', '.join(unsharded_models)),
                hint=(
                    "Set QUILT_DB['OVERRIDE_SHARDING_MODE'][('oauth2_provider',)] = ShardingMode.SHARDED, so that each "
                    'tenant has its own applications, grants and tokens.'
                ),
            )
        )

    return messages


def _generator_messages():
    from oauth2_provider.settings import oauth2_settings

    from djanquiltdb_plugin_oauth_toolkit.generators import (
        ShardSelectorClientIdGenerator,
        generate_access_token,
        generate_refresh_token,
    )

    messages = []
    client_id_generator_class = oauth2_settings.CLIENT_ID_GENERATOR_CLASS

    if not (
        isinstance(client_id_generator_class, type)
        and issubclass(client_id_generator_class, ShardSelectorClientIdGenerator)
    ):
        messages.append(
            _error(
                5,
                "OAUTH2_PROVIDER['CLIENT_ID_GENERATOR_CLASS'] is not ShardSelectorClientIdGenerator, so a client id "
                'has no shard selector.',
                hint=(
                    "Set OAUTH2_PROVIDER['CLIENT_ID_GENERATOR_CLASS'] to "
                    "'djanquiltdb.base.oauth_toolkit.ShardSelectorClientIdGenerator'."
                ),
            )
        )

    if oauth2_settings.ACCESS_TOKEN_GENERATOR is not generate_access_token:
        messages.append(
            _error(
                5,
                "OAUTH2_PROVIDER['ACCESS_TOKEN_GENERATOR'] is not generate_access_token, so an access token has no "
                'shard selector.',
                hint=(
                    "Set OAUTH2_PROVIDER['ACCESS_TOKEN_GENERATOR'] to "
                    "'djanquiltdb.utils.oauth_toolkit.generate_access_token'."
                ),
            )
        )

    # Without a refresh token generator, oauthlib generates refresh tokens with the access token generator.
    if oauth2_settings.REFRESH_TOKEN_GENERATOR not in (None, generate_refresh_token):
        messages.append(
            _error(
                5,
                "OAUTH2_PROVIDER['REFRESH_TOKEN_GENERATOR'] is not generate_refresh_token, so a refresh token has no "
                'shard selector.',
                hint=(
                    "Set OAUTH2_PROVIDER['REFRESH_TOKEN_GENERATOR'] to "
                    "'djanquiltdb.utils.oauth_toolkit.generate_refresh_token'."
                ),
            )
        )

    return messages


def _middleware_classes_that_import():
    """
    Returns the class of each entry of MIDDLEWARE that imports and is a class, in the order of MIDDLEWARE. Django
    reports an entry that does not import when it loads the request handler. A middleware that is a function is
    skipped, because every middleware that the checks look for is a class.
    """
    middleware_classes = []

    for middleware_path in settings.MIDDLEWARE:
        try:
            middleware = import_string(middleware_path)
        except ImportError:
            continue

        if isinstance(middleware, type):
            middleware_classes.append(middleware)

    return middleware_classes


def _middleware_messages():
    from djanquiltdb_plugin_oauth_toolkit.middleware import OAuthToolkitUseShardForMiddleware

    middleware_classes = _middleware_classes_that_import()
    plugin_middleware_positions = [
        position
        for position, middleware_class in enumerate(middleware_classes)
        if issubclass(middleware_class, OAuthToolkitUseShardForMiddleware)
    ]

    if not plugin_middleware_positions:
        return [
            _error(
                6,
                'MIDDLEWARE does not have OAuthToolkitUseShardForMiddleware, so no request enters the shard of its '
                'client id or token.',
                hint=(
                    "Put 'djanquiltdb.middleware.oauth_toolkit.OAuthToolkitUseShardForMiddleware' in MIDDLEWARE, in "
                    "place of 'djanquiltdb.middleware.UseShardForMiddleware'."
                ),
            )
        ]

    messages = []
    first_plugin_middleware_position = plugin_middleware_positions[0]

    for path in TOOLKIT_TOKEN_MIDDLEWARE:
        token_middleware_class = import_string(path)

        if any(
            issubclass(middleware_class, token_middleware_class)
            for middleware_class in middleware_classes[:first_plugin_middleware_position]
        ):
            messages.append(
                _error(
                    7,
                    'MIDDLEWARE has {} before OAuthToolkitUseShardForMiddleware, so it looks up a token before the '
                    'shard of the token is entered.'.format(path.rsplit('.', 1)[1]),
                    hint='Move it after OAuthToolkitUseShardForMiddleware.',
                )
            )

    session_middleware_class = import_string(SESSION_MIDDLEWARE)

    if any(
        issubclass(middleware_class, session_middleware_class)
        for middleware_class in middleware_classes[first_plugin_middleware_position:]
    ):
        messages.append(
            _error(
                7,
                'MIDDLEWARE has SessionMiddleware after OAuthToolkitUseShardForMiddleware, so the middleware of the '
                'plugin cannot read the shard selector of the session.',
                hint='Move SessionMiddleware before OAuthToolkitUseShardForMiddleware.',
            )
        )

    return messages


def _command_messages():
    app_names = [app_config.name for app_config in apps.get_app_configs()]

    if PLUGIN_APP_NAME in app_names and app_names.index(PLUGIN_APP_NAME) < app_names.index(TOOLKIT_APP_NAME):
        return []

    return [
        checks.Warning(
            '{} is not in INSTALLED_APPS before {}, so cleartokens is the command of the toolkit, which fails outside '
            'a shard.'.format(PLUGIN_APP_NAME, TOOLKIT_APP_NAME),
            hint="List '{}' before '{}' in INSTALLED_APPS.".format(PLUGIN_APP_NAME, TOOLKIT_APP_NAME),
            id='{}.W002'.format(CHECK_ID_PREFIX),
        )
    ]


def _toolkit_feature_messages():
    from oauth2_provider.settings import oauth2_settings

    messages = []

    if oauth2_settings.CIMD_ENABLED:
        messages.append(
            _error(
                8,
                "OAUTH2_PROVIDER['CIMD_ENABLED'] is on. The client id of a Client ID Metadata Document is a URL, which "
                'has no shard selector.',
                hint='Turn CIMD_ENABLED off.',
            )
        )

    if oauth2_settings.DCR_ENABLED:
        messages.append(
            checks.Warning(
                "OAUTH2_PROVIDER['DCR_ENABLED'] is on. Dynamic client registration registers an application in the "
                'shard of the session, and its registration access tokens have no shard selector.',
                hint='Register applications in the shard of their tenant, or make sure that every registration '
                'request has a session of that tenant.',
                id='{}.W001'.format(CHECK_ID_PREFIX),
            )
        )

    return messages


@checks.register(checks.Tags.compatibility)
def check_oauth_toolkit_setup(app_configs, **kwargs):
    """
    Reports each setting of the plugin and of django-oauth-toolkit that stops a client id or a token from naming its
    shard, a request from entering that shard, or the cleartokens command of the plugin from replacing the toolkit's.
    """
    messages = _setting_messages()

    if not apps.is_installed(TOOLKIT_APP_NAME):
        return messages + [
            _error(
                4,
                'oauth2_provider is not in INSTALLED_APPS.',
                hint='Install django-oauth-toolkit as its documentation describes.',
            )
        ]

    return (
        messages
        + _model_messages()
        + _generator_messages()
        + _middleware_messages()
        + _toolkit_feature_messages()
        + _command_messages()
    )
