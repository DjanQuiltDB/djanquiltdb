"""
The middleware of the plugin, which DjanQuiltDB serves as ``djanquiltdb.middleware.oauth_toolkit``. The sources that a
subclass can list are served from here too.
"""

from django.core.exceptions import ObjectDoesNotExist
from django.http import JsonResponse
from django.urls import Resolver404, resolve
from djanquiltdb.middleware import BaseUseShardForMiddleware, ExceptionMiddlewareMixin
from djanquiltdb.utils import session_shard_selector_attribute_name

from djanquiltdb_plugin_oauth_toolkit.sources import (
    SOURCE_SESSION,
    FoundShardSelector,
    ShardSelectorRequestError,
    resolve_request_shard_selector,
    selector_from_access_token_parameter,
    selector_from_basic_auth_header,
    selector_from_bearer_header,
    selector_from_client_id_parameter,
    selector_from_id_token_hint,
    selector_from_path_client_id,
)

__all__ = [
    'SHARD_SELECTOR_SOURCES_BY_URL_NAME',
    'FoundShardSelector',
    'OAuthToolkitUseShardForMiddleware',
    'ShardSelectorRequestError',
    'selector_from_access_token_parameter',
    'selector_from_basic_auth_header',
    'selector_from_bearer_header',
    'selector_from_client_id_parameter',
    'selector_from_id_token_hint',
    'selector_from_path_client_id',
]

#: The application namespace of the toolkit's URLs.
TOOLKIT_APP_NAME = 'oauth2_provider'

_CLIENT_CREDENTIAL_SOURCES = (selector_from_basic_auth_header, selector_from_client_id_parameter)
_TOKEN_SOURCES = (selector_from_bearer_header, selector_from_access_token_parameter)
_SESSION_PAGE_SOURCES = (selector_from_bearer_header,)

#: The sources of each URL of the toolkit, by URL name. The sources of a URL read the client id or the token where the
#: toolkit reads it on that URL. The token that an endpoint acts on is not a source (e.g. the token to revoke or to
#: introspect, a refresh token, an authorization code or a device code). The toolkit looks up such a token in the shard
#: of the client, and does not find it there when it belongs to another shard.
SHARD_SELECTOR_SOURCES_BY_URL_NAME = {
    'token': _CLIENT_CREDENTIAL_SOURCES,
    'revoke-token': _CLIENT_CREDENTIAL_SOURCES,
    'device-authorization': _CLIENT_CREDENTIAL_SOURCES,
    'introspect': _CLIENT_CREDENTIAL_SOURCES + _TOKEN_SOURCES,
    'authorize': (selector_from_client_id_parameter,),
    'device-confirm': (selector_from_path_client_id,),
    'device-grant-status': (selector_from_path_client_id,),
    'dcr-register-management': (selector_from_path_client_id,),
    'rp-initiated-logout': (selector_from_client_id_parameter, selector_from_id_token_hint),
    'device': _SESSION_PAGE_SOURCES,
    'list': _SESSION_PAGE_SOURCES,
    'register': _SESSION_PAGE_SOURCES,
    'detail': _SESSION_PAGE_SOURCES,
    'update': _SESSION_PAGE_SOURCES,
    'delete': _SESSION_PAGE_SOURCES,
    'authorized-token-list': _SESSION_PAGE_SOURCES,
    'authorized-token-delete': _SESSION_PAGE_SOURCES,
    'dcr-register': _SESSION_PAGE_SOURCES,
    'oidc-connect-discovery-info': (),
    'jwks-info': (),
    'oauth-server-metadata': (),
    'oauth-server-metadata-issuer': (),
    'oauth-resource-metadata': (),
    'oauth-resource-metadata-path': (),
}

#: The error of each URL of the toolkit that needs a shard even when the request names none, because the toolkit
#: looks up the client before it can answer.
ERROR_WITHOUT_SHARD_SELECTOR_BY_URL_NAME = {
    'token': 'invalid_client',
    'revoke-token': 'invalid_client',
    'introspect': 'access_denied',
}


class OAuthToolkitUseShardForMiddleware(BaseUseShardForMiddleware, ExceptionMiddlewareMixin):
    """
    Enters the shard of each request, as ``UseShardForMiddleware`` does. Put it in ``MIDDLEWARE`` in place of
    ``UseShardForMiddleware``. On a URL of the toolkit, the shard selector comes from the client id or the token that
    the toolkit reads on that URL, as ``SHARD_SELECTOR_SOURCES_BY_URL_NAME`` lists them. On every other URL, it comes
    from the Bearer header or the ``access_token`` parameter. A request without a client id or a token gets the shard
    selector of its session.

    A request whose client id or token names another shard than its session gets a 400 response, because the user of
    the session is stored in the shard of the session. A session whose shard selector the mapping table no longer has
    leaves the request without a shard, as with ``UseShardForMiddleware``.
    """

    shard_selector_sources_by_url_name = SHARD_SELECTOR_SOURCES_BY_URL_NAME
    default_shard_selector_sources = (selector_from_bearer_header, selector_from_access_token_parameter)
    error_without_shard_selector_by_url_name = ERROR_WITHOUT_SHARD_SELECTOR_BY_URL_NAME

    def get_session_shard_selector(self, request):
        """
        Returns the shard selector of the session as a string, from the attribute of the session that
        ``QUILT_DB['SESSION_SHARD_SELECTOR_KEY']`` names, or None when the request has no session or the session has no
        shard selector.
        """
        selector = getattr(getattr(request, 'session', None), session_shard_selector_attribute_name(), None)

        return None if selector is None else str(selector)

    def toolkit_url_name(self, resolver_match):
        """
        Returns the URL name of ``resolver_match`` when it is a URL of the toolkit, and None otherwise.
        """
        if resolver_match is None or TOOLKIT_APP_NAME not in resolver_match.app_names:
            return None

        return resolver_match.url_name

    def get_mapping_value(self, request):
        try:
            resolver_match = resolve(request.path_info)
        except Resolver404:
            resolver_match = None

        url_name = self.toolkit_url_name(resolver_match)
        sources = self.shard_selector_sources_by_url_name.get(url_name, self.default_shard_selector_sources)
        found = resolve_request_shard_selector(request, resolver_match, sources)
        session_selector = self.get_session_shard_selector(request)
        error_without_shard_selector = self.error_without_shard_selector_by_url_name.get(url_name)

        if found is not None and session_selector is not None and found.selector != session_selector:
            raise ShardSelectorRequestError.for_conflicting_sources(
                FoundShardSelector(session_selector, SOURCE_SESSION, None), found
            )

        if found is None and session_selector is not None:
            found = FoundShardSelector(session_selector, SOURCE_SESSION, error_without_shard_selector)

        if found is None:
            if error_without_shard_selector is not None:
                raise ShardSelectorRequestError.for_missing_shard_selector(error_without_shard_selector)

            return None

        request._shard_selector_found = found

        return found.selector

    def process_request(self, request):
        try:
            return super().process_request(request)
        except ShardSelectorRequestError as error:
            self._disable_shard(request)

            return self.error_response(request, error)

    def _enable_shard_for(self, request, target_value):
        try:
            super()._enable_shard_for(request, target_value)
        except ObjectDoesNotExist:
            found = request._shard_selector_found

            if found.error is None:
                # The mapping table no longer has the shard selector of the session, and the URL needs no shard, so the
                # request goes on without a shard, as with UseShardForMiddleware. djanquiltdb.sessions reads the session
                # as empty.
                request._mapping_value = None
                return

            raise ShardSelectorRequestError.for_unknown_shard(found)

    def error_response(self, request, error):
        """
        Returns the response for a request that names no shard that it can run in. The response has a JSON body with
        the OAuth error, as the error responses of the toolkit have, and a 401 response has the ``WWW-Authenticate``
        header that oauthlib sends. Override this method to render the error in another way.
        """
        response = JsonResponse(
            {'error': error.error, 'error_description': error.description}, status=error.status_code
        )
        response['Cache-Control'] = 'no-store'

        if error.status_code == 401:
            response['WWW-Authenticate'] = 'Bearer error="{}", error_description="{}"'.format(
                error.error, error.description
            )

        return response
