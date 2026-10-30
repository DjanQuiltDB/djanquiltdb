"""
Functions that read a shard selector from the places in a request where django-oauth-toolkit reads a client id or a
token. Each of these sources takes the Django request and its resolver match, and returns a FoundShardSelector, or None
when the request has nothing for the source to read. When the request has a value that is not an identifier of the
format, the source raises ShardSelectorRequestError with the OAuth error that the endpoint gives for such a value.

The sources only read the request, and they never change it, so the toolkit reads the same identifiers afterwards.
"""

import base64
import json
from collections import namedtuple
from urllib.parse import unquote_plus

from django.http import QueryDict

from djanquiltdb_plugin_oauth_toolkit.identifiers import parse_client_id, parse_token

SOURCE_BEARER_HEADER = 'Bearer authorization header'
SOURCE_ACCESS_TOKEN_PARAMETER = 'access_token parameter'
SOURCE_BASIC_HEADER = 'Basic authorization header'
SOURCE_CLIENT_ID_PARAMETER = 'client_id parameter'
SOURCE_PATH_CLIENT_ID = 'client_id in the URL path'
SOURCE_ID_TOKEN_HINT = 'id_token_hint parameter'
SOURCE_SESSION = 'session'

#: The HTTP status of each OAuth error that a source can give.
STATUS_CODE_BY_ERROR = {
    'invalid_token': 401,
    'invalid_client': 401,
    'invalid_request': 400,
    'access_denied': 403,
}

#: The URL names of the toolkit's endpoints that authenticate the client. On these endpoints, a client id that names no
#: shard is an invalid_client.
CLIENT_AUTHENTICATION_URL_NAMES = frozenset({'token', 'revoke-token', 'introspect'})

FORM_CONTENT_TYPE = 'application/x-www-form-urlencoded'


#: A shard selector that a source found. ``error`` is the OAuth error of the request when no mapping row has the
#: selector, or None when the request then goes on without a shard.
FoundShardSelector = namedtuple('FoundShardSelector', ['selector', 'source', 'error'])


class ShardSelectorRequestError(Exception):
    """
    Raised when a request names no shard that it can run in. ``error`` is the OAuth error code, ``status_code`` is the
    HTTP status for that error, and ``source`` is the place in the request that the error is about, or None.
    """

    def __init__(self, error, description, source=None):
        super().__init__(description)
        self.error = error
        self.status_code = STATUS_CODE_BY_ERROR[error]
        self.description = description
        self.source = source

    @classmethod
    def for_unparseable_source(cls, source, error):
        return cls(error, 'The {} does not name a shard.'.format(source), source)

    @classmethod
    def for_conflicting_sources(cls, first, second):
        return cls(
            'invalid_request',
            'The {} and the {} name different shards.'.format(first.source, second.source),
            second.source,
        )

    @classmethod
    def for_unknown_shard(cls, found):
        return cls(found.error, 'The {} names a shard that does not exist.'.format(found.source), found.source)

    @classmethod
    def for_missing_shard_selector(cls, error):
        return cls(error, 'The request names no shard.')


def form_body(request):
    """
    Returns the parameters of the body of ``request`` when the body is form-encoded, as the OAuth RFCs ask, and an
    empty QueryDict for any other body. A multipart body is not parsed here, so that the view can set the upload
    handlers of the request.
    """
    if request.method == 'POST' and request.content_type == FORM_CONTENT_TYPE:
        return request.POST

    return QueryDict()


def request_parameters(request):
    """
    Returns the parameters of ``request`` where the toolkit reads them, which is the form-encoded body of a POST and
    the query string of any other method.
    """
    return form_body(request) if request.method == 'POST' else request.GET


def _authorization_header(request):
    return request.META.get('HTTP_AUTHORIZATION')


def selector_from_bearer_header(request, resolver_match):
    """
    Reads a token from an ``Authorization`` header of the scheme Bearer, as oauthlib reads it. The header has two words,
    and the scheme can be in any case.
    """
    words = (_authorization_header(request) or '').split()

    if len(words) != 2 or words[0].lower() != 'bearer':
        return None

    selector = parse_token(words[1])

    if selector is None:
        raise ShardSelectorRequestError.for_unparseable_source(SOURCE_BEARER_HEADER, 'invalid_token')

    return FoundShardSelector(selector, SOURCE_BEARER_HEADER, 'invalid_token')


def selector_from_access_token_parameter(request, resolver_match):
    """
    Reads a token from the ``access_token`` parameter of a form-encoded body or of the query string. oauthlib only
    reads the parameter when the request has no ``Authorization`` header. The query string is not read when
    ``COMPLIANT_BCP_RFC9700_ACCESS_TOKEN_TRANSPORT`` is on, because the toolkit then refuses a token in the query
    string before it looks up the token.
    """
    from oauth2_provider.settings import oauth2_settings

    if _authorization_header(request) is not None:
        return None

    token = form_body(request).get('access_token')

    if not token and not oauth2_settings.COMPLIANT_BCP_RFC9700_ACCESS_TOKEN_TRANSPORT:
        token = request.GET.get('access_token')

    if not token:
        return None

    selector = parse_token(token)

    if selector is None:
        raise ShardSelectorRequestError.for_unparseable_source(SOURCE_ACCESS_TOKEN_PARAMETER, 'invalid_token')

    return FoundShardSelector(selector, SOURCE_ACCESS_TOKEN_PARAMETER, 'invalid_token')


def _basic_auth_client_id(request):
    # These are the steps of the toolkit's OAuth2Validator._authenticate_basic_auth(). A header that fails one of them
    # is not read, and the client id is then read from the body.
    words = (_authorization_header(request) or '').split(' ', 1)

    if len(words) != 2 or words[0] != 'Basic':
        return None

    try:
        credentials = base64.b64decode(words[1]).decode(request.encoding or 'utf-8')
    except ValueError:
        # b64decode() raises ValueError for credentials with a character outside ASCII, and its subclass
        # binascii.Error for credentials that are not base64. decode() raises UnicodeDecodeError, which is also a
        # subclass of ValueError.
        return None

    if ':' not in credentials:
        return None

    return unquote_plus(credentials.split(':', 1)[0])


def selector_from_basic_auth_header(request, resolver_match):
    """
    Reads a client id from an ``Authorization`` header of the scheme Basic, as the toolkit reads it.
    """
    client_id = _basic_auth_client_id(request)

    if client_id is None:
        return None

    selector = parse_client_id(client_id)

    if selector is None:
        raise ShardSelectorRequestError.for_unparseable_source(SOURCE_BASIC_HEADER, 'invalid_client')

    return FoundShardSelector(selector, SOURCE_BASIC_HEADER, 'invalid_client')


def selector_from_client_id_parameter(request, resolver_match):
    """
    Reads a client id from the ``client_id`` parameter, which is in the form-encoded body of a POST and in the query
    string of any other method. A client id that names no shard is an invalid_client on an endpoint that authenticates
    the client, and an invalid_request elsewhere.
    """
    client_id = request_parameters(request).get('client_id')

    if not client_id:
        return None

    url_name = None if resolver_match is None else resolver_match.url_name
    error = 'invalid_client' if url_name in CLIENT_AUTHENTICATION_URL_NAMES else 'invalid_request'
    selector = parse_client_id(client_id)

    if selector is None:
        raise ShardSelectorRequestError.for_unparseable_source(SOURCE_CLIENT_ID_PARAMETER, error)

    return FoundShardSelector(selector, SOURCE_CLIENT_ID_PARAMETER, error)


def selector_from_path_client_id(request, resolver_match):
    """
    Reads a client id from the ``client_id`` argument of the URL pattern. The URL patterns of the toolkit's device flow
    pages have this argument.
    """
    client_id = None if resolver_match is None else resolver_match.kwargs.get('client_id')

    if client_id is None:
        return None

    selector = parse_client_id(client_id)

    if selector is None:
        raise ShardSelectorRequestError.for_unparseable_source(SOURCE_PATH_CLIENT_ID, 'invalid_request')

    return FoundShardSelector(selector, SOURCE_PATH_CLIENT_ID, 'invalid_request')


def _id_token_audience(id_token_hint):
    from jwcrypto import jws
    from jwcrypto.common import JWException

    # The toolkit reads the audience of the hint in the same way, before it checks the signature.
    unverified_token = jws.JWS()

    try:
        unverified_token.deserialize(id_token_hint)
        claims = json.loads(unverified_token.objects['payload'].decode('utf-8'))
    except JWException, ValueError:
        return None

    audience = claims.get('aud') if isinstance(claims, dict) else None

    return [audience] if isinstance(audience, str) else audience


def selector_from_id_token_hint(request, resolver_match):
    """
    Reads the client ids from the ``aud`` claim of the ``id_token_hint`` parameter of an OIDC logout request, which is
    in the form-encoded body of a POST and in the query string of any other method. The signature of the hint is not
    checked, because the toolkit checks it with the key of the application, inside the shard. Every client id in the
    claim has to name the same shard.
    """
    id_token_hint = request_parameters(request).get('id_token_hint')

    if not id_token_hint:
        return None

    audience = _id_token_audience(id_token_hint)
    selectors = (
        {parse_client_id(client_id) if isinstance(client_id, str) else None for client_id in audience}
        if isinstance(audience, list) and audience
        else {None}
    )

    if len(selectors) != 1 or None in selectors:
        raise ShardSelectorRequestError.for_unparseable_source(SOURCE_ID_TOKEN_HINT, 'invalid_request')

    return FoundShardSelector(selectors.pop(), SOURCE_ID_TOKEN_HINT, 'invalid_request')


def resolve_request_shard_selector(request, resolver_match, sources):
    """
    Runs each of ``sources`` on the request, and returns the first shard selector that is found, or None when no
    source finds one. Raises ShardSelectorRequestError when two sources find different shard selectors.
    """
    first = None

    for source in sources:
        found = source(request, resolver_match)

        if found is None:
            continue

        if first is None:
            first = found
        elif found.selector != first.selector:
            raise ShardSelectorRequestError.for_conflicting_sources(first, found)

    return first
